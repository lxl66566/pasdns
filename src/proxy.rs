use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use fastrace::prelude::*;
use hickory_net::runtime::Time;
use hickory_proto::{
    op::{Header, HeaderCounts, Message, Metadata, ResponseCode},
    rr::Record,
    serialize::binary::{BinDecodable, BinDecoder},
};
use hickory_server::{
    server::{Request, RequestHandler, ResponseHandler, ResponseInfo},
    zone_handler::MessageResponseBuilder,
};
use log::{debug, warn};

use crate::{
    cache::{CacheKey, CacheLookupResult, DnsCache},
    config::AppConfig,
    forwarder,
    router::Router,
    upstream::UpstreamManager,
};

pub struct DnsProxy {
    #[allow(dead_code)]
    config: Arc<AppConfig>,
    router: Router,
    upstream_manager: UpstreamManager,
    cache: Option<DnsCache>,
    proxy_addr: Option<SocketAddr>,
}

fn fallback_response_info(request: &Request) -> ResponseInfo {
    let mut metadata = Metadata::new(
        request.metadata.id,
        hickory_proto::op::MessageType::Response,
        request.metadata.op_code,
    );
    metadata.recursion_available = true;
    ResponseInfo::from(Header {
        metadata,
        counts: HeaderCounts::default(),
    })
}

impl DnsProxy {
    pub fn new(
        config: Arc<AppConfig>,
        router: Router,
        upstream_manager: UpstreamManager,
        cache: Option<DnsCache>,
    ) -> Self {
        let proxy_addr = if config.proxy.enabled {
            config
                .proxy
                .addr
                .as_ref()
                .and_then(|a| a.parse::<SocketAddr>().ok())
        } else {
            None
        };
        Self {
            config,
            router,
            upstream_manager,
            cache,
            proxy_addr,
        }
    }

    pub fn cache(&self) -> Option<&DnsCache> {
        self.cache.as_ref()
    }

    #[allow(clippy::unused_self)]
    fn extract_query_info(&self, request: &Request) -> Option<(String, u16)> {
        let queries = request.queries.queries();
        let query = queries.first()?;
        let raw = query.name().to_string();
        let name = raw.strip_suffix('.').unwrap_or(&raw).to_string();
        let record_type = query.query_type();
        Some((name, record_type.into()))
    }

    fn extract_min_ttl(response_bytes: &[u8]) -> u64 {
        let mut decoder = BinDecoder::new(response_bytes);
        let Ok(msg) = Message::read(&mut decoder) else {
            return 300;
        };
        let min_ttl = msg.answers.iter().map(|r| r.ttl).min().unwrap_or(300);
        u64::from(min_ttl)
    }

    async fn send_servfail<R: ResponseHandler>(
        &self,
        request: &Request,
        mut rh: R,
    ) -> ResponseInfo {
        let builder = MessageResponseBuilder::from_message_request(request);
        let response = builder.error_msg(&request.metadata, ResponseCode::ServFail);
        rh.send_response(response)
            .await
            .unwrap_or_else(|_| fallback_response_info(request))
    }

    async fn forward_and_respond<R: ResponseHandler>(
        &self,
        request: &Request,
        raw_query: &[u8],
        group: &str,
        root: &Span,
        mut rh: R,
    ) -> ResponseInfo {
        let Some(target) = self.upstream_manager.pick_upstream(group) else {
            warn!(target: "pasdns::proxy", "no upstream available for group '{group}'");
            return self.send_servfail(request, rh).await;
        };

        debug!(
            target: "pasdns::proxy",
            "forwarding {} -> {} ({:?})",
            request
                .queries
                .queries()
                .first()
                .map(|q| q.name().to_string())
                .unwrap_or_default(),
            target.addr,
            target.protocol,
        );

        let start = std::time::Instant::now();
        let forward_result = {
            let _fwd_span = Span::enter_with_parent("dns-forward", root);
            forwarder::forward(
                raw_query,
                target.addr,
                target.protocol,
                self.proxy_addr,
                target.doh_url.as_deref(),
            )
            .await
        };
        let elapsed = start.elapsed();

        let response_bytes = match forward_result {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(target: "pasdns::proxy", "forward to {} failed: {e}", target.addr);
                return self.send_servfail(request, rh).await;
            }
        };

        self.upstream_manager
            .record_latency(target.addr, group, elapsed);

        let mut decoder = BinDecoder::new(&response_bytes);
        let msg = match Message::read(&mut decoder) {
            Ok(m) => m,
            Err(e) => {
                warn!(target: "pasdns::proxy", "failed to parse upstream response: {e}");
                return self.send_servfail(request, rh).await;
            }
        };

        if let Some(ref cache) = self.cache
            && let Some((name, record_type)) = self.extract_query_info(request)
        {
            let ttl = Self::extract_min_ttl(&response_bytes);
            let key = CacheKey { name, record_type };
            cache
                .insert(key, response_bytes.clone(), Duration::from_secs(ttl))
                .await;
        }

        let mut meta = Metadata::response_from_request(&request.metadata);
        meta.recursion_available = true;
        meta.response_code = msg.response_code;

        let builder = MessageResponseBuilder::from_message_request(request);
        let response = builder.build(
            meta,
            &msg.answers,
            &msg.authorities,
            &[] as &[Record],
            &msg.additionals,
        );
        rh.send_response(response)
            .await
            .unwrap_or_else(|_| fallback_response_info(request))
    }

    async fn send_cached_response<R: ResponseHandler>(
        &self,
        request: &Request,
        response_bytes: &[u8],
        mut rh: R,
    ) -> ResponseInfo {
        let mut decoder = BinDecoder::new(response_bytes);
        let Ok(msg) = Message::read(&mut decoder) else {
            return self.send_servfail(request, rh).await;
        };

        let mut meta = Metadata::response_from_request(&request.metadata);
        meta.recursion_available = true;
        meta.response_code = msg.response_code;

        let builder = MessageResponseBuilder::from_message_request(request);
        let response = builder.build(
            meta,
            &msg.answers,
            &msg.authorities,
            &[] as &[Record],
            &msg.additionals,
        );
        rh.send_response(response)
            .await
            .unwrap_or_else(|_| fallback_response_info(request))
    }
}

impl RequestHandler for DnsProxy {
    #[allow(clippy::type_complexity)]
    fn handle_request<'life0, 'life1, 'async_trait, R, T>(
        &'life0 self,
        request: &'life1 Request,
        response_handle: R,
    ) -> Pin<Box<dyn Future<Output = ResponseInfo> + Send + 'async_trait>>
    where
        R: 'async_trait + ResponseHandler,
        T: 'async_trait + Time,
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            let Some((name, record_type)) = self.extract_query_info(request) else {
                return self.send_servfail(request, response_handle).await;
            };

            let root = Span::root("dns-request", SpanContext::random());
            root.add_property(|| ("domain", name.clone()));
            root.add_property(|| ("record_type", record_type.to_string()));

            let group = self.router.route(&name);
            root.add_property(|| ("group", group.to_string()));

            let raw_query = request.as_slice();

            if let Some(ref cache) = self.cache {
                let key = CacheKey {
                    name: name.clone(),
                    record_type,
                };

                {
                    let _lookup = Span::enter_with_parent("cache-lookup", &root);
                    match cache.lookup(&key).await {
                        CacheLookupResult::Fresh(bytes) => {
                            root.add_event(Event::new("cache-hit-fresh"));
                            debug!(target: "pasdns::proxy", "cache hit (fresh): {name}");
                            return self
                                .send_cached_response(request, &bytes, response_handle)
                                .await;
                        }
                        CacheLookupResult::Stale(bytes) => {
                            root.add_event(Event::new("cache-hit-stale"));
                            debug!(target: "pasdns::proxy", "cache hit (stale): {name}");
                            return self
                                .send_cached_response(request, &bytes, response_handle)
                                .await;
                        }
                        CacheLookupResult::Miss => {
                            root.add_event(Event::new("cache-miss"));
                            debug!(target: "pasdns::proxy", "cache miss: {name}");
                        }
                    }
                }
            }

            self.forward_and_respond(request, raw_query, group, &root, response_handle)
                .await
        })
    }
}
