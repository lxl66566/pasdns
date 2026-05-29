use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

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
use tracing::{debug, warn};

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
        Self {
            config,
            router,
            upstream_manager,
            cache,
        }
    }

    #[allow(dead_code)]
    pub fn cache(&self) -> Option<&DnsCache> {
        self.cache.as_ref()
    }

    #[allow(clippy::unused_self)]
    fn extract_query_info(&self, request: &Request) -> Option<(String, u16)> {
        let queries = request.queries.queries();
        let query = queries.first()?;
        let name = query.name().to_string();
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
        mut rh: R,
    ) -> ResponseInfo {
        let Some((upstream, protocol)) = self.upstream_manager.pick_upstream(group) else {
            warn!(group, "no upstream available");
            return self.send_servfail(request, rh).await;
        };

        debug!(
            domain = %request
                .queries
                .queries()
                .first()
                .map(|q| q.name().to_string())
                .unwrap_or_default(),
            %upstream,
            ?protocol,
            "forwarding"
        );

        let start = std::time::Instant::now();
        let fwd_protocol = match protocol {
            crate::config::Protocol::Udp | crate::config::Protocol::Tcp => protocol,
            _ => crate::config::Protocol::Udp,
        };
        let forward_result = forwarder::forward(raw_query, upstream, fwd_protocol).await;
        let elapsed = start.elapsed();

        let response_bytes = match forward_result {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(%upstream, error = %e, "forward failed");
                return self.send_servfail(request, rh).await;
            }
        };

        self.upstream_manager
            .record_latency(upstream, group, elapsed);

        let mut decoder = BinDecoder::new(&response_bytes);
        let msg = match Message::read(&mut decoder) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "failed to parse upstream response");
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

            let group = self.router.route(&name);
            let raw_query = request.as_slice();

            if let Some(ref cache) = self.cache {
                let key = CacheKey {
                    name: name.clone(),
                    record_type,
                };
                match cache.lookup(&key).await {
                    CacheLookupResult::Fresh(bytes) => {
                        debug!(%name, "cache hit (fresh)");
                        return self
                            .send_cached_response(request, &bytes, response_handle)
                            .await;
                    }
                    CacheLookupResult::Stale(bytes) => {
                        debug!(%name, "cache hit (stale), serving stale");
                        return self
                            .send_cached_response(request, &bytes, response_handle)
                            .await;
                    }
                    CacheLookupResult::Miss => {
                        debug!(%name, "cache miss");
                    }
                }
            }

            self.forward_and_respond(request, raw_query, group, response_handle)
                .await
        })
    }
}
