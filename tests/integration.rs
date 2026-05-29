use std::{
    net::{Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use hickory_proto::{
    op::{Message, MessageType, OpCode, Query, ResponseCode},
    rr::{Name, RData, Record, RecordType},
    serialize::binary::{BinDecodable, BinDecoder, BinEncodable, BinEncoder},
};
use hickory_server::server::Server;
use pasdns::{
    cache::{CacheKey, CacheLookupResult, DnsCache},
    config::*,
    persist::{self, PersistedEntry},
    proxy::DnsProxy,
    router::Router,
    upstream::UpstreamManager,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn build_query(name: &str, record_type: RecordType, id: u16) -> Vec<u8> {
    let mut msg = Message::new(id, MessageType::Query, OpCode::Query);
    msg.metadata.recursion_desired = true;
    let mut query = Query::new();
    query.set_name(Name::from_str(name).unwrap());
    query.set_query_type(record_type);
    msg.add_query(query);
    let mut buf = Vec::new();
    msg.emit(&mut BinEncoder::new(&mut buf)).unwrap();
    buf
}

fn parse_response(data: &[u8]) -> Message {
    Message::read(&mut BinDecoder::new(data)).unwrap()
}

async fn send_udp_query(addr: SocketAddr, query: &[u8]) -> Vec<u8> {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    socket.send_to(query, addr).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let len = tokio::time::timeout(Duration::from_secs(3), socket.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    buf.truncate(len);
    buf
}

async fn send_tcp_query(addr: SocketAddr, query: &[u8]) -> Vec<u8> {
    let mut stream =
        tokio::time::timeout(Duration::from_secs(3), tokio::net::TcpStream::connect(addr))
            .await
            .unwrap()
            .unwrap();
    let len = u16::try_from(query.len()).unwrap();
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(query).await.unwrap();
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await.unwrap();
    let resp_len = u16::from_be_bytes(len_buf) as usize;
    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).await.unwrap();
    resp
}

struct MockUpstream {
    addr: SocketAddr,
    _handle: tokio::task::JoinHandle<()>,
}

fn build_response(msg: &mut Message, ip: Ipv4Addr) -> Vec<u8> {
    msg.metadata.message_type = MessageType::Response;
    msg.metadata.response_code = ResponseCode::NoError;
    msg.metadata.recursion_available = true;
    if let Some(query) = msg.queries.first() {
        msg.add_answer(Record::from_rdata(
            query.name().clone(),
            300,
            RData::A(hickory_proto::rr::rdata::A(ip)),
        ));
    }
    let mut buf = Vec::new();
    msg.emit(&mut BinEncoder::new(&mut buf)).unwrap();
    buf
}

impl MockUpstream {
    async fn udp(ip: Ipv4Addr) -> Self {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                let (len, src) = match socket.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                let mut msg = match Message::read(&mut BinDecoder::new(&buf[..len])) {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let resp = build_response(&mut msg, ip);
                let _ = socket.send_to(&resp, src).await;
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }

    async fn tcp(ip: Ipv4Addr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let ip = ip;
                tokio::spawn(async move {
                    let mut len_buf = [0u8; 2];
                    if stream.read_exact(&mut len_buf).await.is_err() {
                        return;
                    }
                    let len = u16::from_be_bytes(len_buf) as usize;
                    let mut qbuf = vec![0u8; len];
                    if stream.read_exact(&mut qbuf).await.is_err() {
                        return;
                    }
                    let mut msg = match Message::read(&mut BinDecoder::new(&qbuf)) {
                        Ok(m) => m,
                        Err(_) => return,
                    };
                    let resp = build_response(&mut msg, ip);
                    let rlen = (resp.len() as u16).to_be_bytes();
                    let _ = stream.write_all(&rlen).await;
                    let _ = stream.write_all(&resp).await;
                });
            }
        });
        Self {
            addr,
            _handle: handle,
        }
    }
}

async fn start_udp_server(groups: Vec<GroupConfig>, routes: Vec<RouteConfig>) -> TestServer {
    let config = Arc::new(AppConfig {
        server: ServerConfig::default(),
        group: groups,
        route: routes,
        cache: CacheConfig {
            enabled: true,
            max_entries: 1000,
            default_ttl_secs: 3600,
            serve_stale_secs: 300,
            persistence: PersistenceConfig::default(),
        },
        geosite_path: None,
        proxy: ProxyConfig::default(),
    });
    let router = Router::new(&config.route, "default", None);
    let upstream = UpstreamManager::new(&config.group);
    let cache = DnsCache::new(1000, 300);
    let proxy = DnsProxy::new(config, router, upstream, Some(cache.clone()));
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = udp.local_addr().unwrap();
    let mut server = Server::new(proxy);
    server.register_socket(udp);
    tokio::spawn(async move {
        let _ = server.block_until_done().await;
    });
    TestServer { addr, cache }
}

async fn start_tcp_server(groups: Vec<GroupConfig>) -> TestServer {
    let config = Arc::new(AppConfig {
        server: ServerConfig::default(),
        group: groups,
        route: vec![],
        cache: CacheConfig {
            enabled: true,
            max_entries: 1000,
            default_ttl_secs: 3600,
            serve_stale_secs: 300,
            persistence: PersistenceConfig::default(),
        },
        geosite_path: None,
        proxy: ProxyConfig::default(),
    });
    let router = Router::new(&[], "default", None);
    let upstream = UpstreamManager::new(&config.group);
    let cache = DnsCache::new(1000, 300);
    let proxy = DnsProxy::new(config, router, upstream, Some(cache.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut server = Server::new(proxy);
    server.register_listener(listener, Duration::from_secs(30), 4096);
    tokio::spawn(async move {
        let _ = server.block_until_done().await;
    });
    TestServer { addr, cache }
}

struct TestServer {
    addr: SocketAddr,
    cache: DnsCache,
}

#[tokio::test]
async fn forward_udp_query_and_get_response() {
    let upstream = MockUpstream::udp(Ipv4Addr::new(1, 2, 3, 4)).await;
    let server = start_udp_server(
        vec![GroupConfig {
            name: "default".into(),
            strategy: Strategy::RoundRobin,
            upstreams: vec![UpstreamAddr::Simple(upstream.addr.to_string())],
        }],
        vec![],
    )
    .await;

    let query = build_query("example.com", RecordType::A, 0x1234);
    let resp = parse_response(&send_udp_query(server.addr, &query).await);

    assert_eq!(resp.id, 0x1234);
    assert_eq!(resp.response_code, ResponseCode::NoError);
    assert_eq!(resp.answers.len(), 1);
    assert_eq!(resp.answers[0].record_type(), RecordType::A);
    assert_eq!(resp.answers[0].name.to_string(), "example.com.");
    match &resp.answers[0].data {
        RData::A(a) => assert_eq!(a.0, Ipv4Addr::new(1, 2, 3, 4)),
        other => panic!("expected A record, got {other:?}"),
    }
}

#[tokio::test]
async fn forward_tcp_query_and_get_response() {
    let upstream = MockUpstream::tcp(Ipv4Addr::new(10, 20, 30, 40)).await;
    let server = start_tcp_server(vec![GroupConfig {
        name: "default".into(),
        strategy: Strategy::RoundRobin,
        upstreams: vec![UpstreamAddr::Detailed {
            addr: upstream.addr.to_string(),
            protocol: Protocol::Tcp,
        }],
    }])
    .await;

    let query = build_query("tcp-test.com", RecordType::A, 0x5678);
    let resp = parse_response(&send_tcp_query(server.addr, &query).await);

    assert_eq!(resp.id, 0x5678);
    assert_eq!(resp.response_code, ResponseCode::NoError);
    assert_eq!(resp.answers.len(), 1);
    match &resp.answers[0].data {
        RData::A(a) => assert_eq!(a.0, Ipv4Addr::new(10, 20, 30, 40)),
        other => panic!("expected A record, got {other:?}"),
    }
}

#[tokio::test]
async fn cache_hit_returns_cached_response() {
    let upstream = MockUpstream::udp(Ipv4Addr::new(5, 5, 5, 5)).await;
    let server = start_udp_server(
        vec![GroupConfig {
            name: "default".into(),
            strategy: Strategy::RoundRobin,
            upstreams: vec![UpstreamAddr::Simple(upstream.addr.to_string())],
        }],
        vec![],
    )
    .await;

    let query = build_query("cached.example", RecordType::A, 100);
    let resp1 = parse_response(&send_udp_query(server.addr, &query).await);
    assert_eq!(resp1.answers.len(), 1);

    let resp2 = parse_response(&send_udp_query(server.addr, &query).await);
    assert_eq!(resp2.answers.len(), 1);
    assert_eq!(resp1.answers[0].data, resp2.answers[0].data);

    let key = CacheKey {
        name: "cached.example".to_string(),
        record_type: RecordType::A.into(),
    };
    assert!(matches!(
        server.cache.lookup(&key).await,
        CacheLookupResult::Fresh(_)
    ));
}

#[tokio::test]
async fn route_directs_to_correct_upstream_group() {
    let upstream_a = MockUpstream::udp(Ipv4Addr::new(1, 1, 1, 1)).await;
    let upstream_b = MockUpstream::udp(Ipv4Addr::new(2, 2, 2, 2)).await;

    let server = start_udp_server(
        vec![
            GroupConfig {
                name: "group-a".into(),
                strategy: Strategy::RoundRobin,
                upstreams: vec![UpstreamAddr::Simple(upstream_a.addr.to_string())],
            },
            GroupConfig {
                name: "group-b".into(),
                strategy: Strategy::RoundRobin,
                upstreams: vec![UpstreamAddr::Simple(upstream_b.addr.to_string())],
            },
        ],
        vec![
            RouteConfig {
                domains: vec!["a.example".into()],
                geosite: vec![],
                group: "group-a".into(),
            },
            RouteConfig {
                domains: vec!["b.example".into()],
                geosite: vec![],
                group: "group-b".into(),
            },
        ],
    )
    .await;

    let resp_a = parse_response(
        &send_udp_query(server.addr, &build_query("a.example", RecordType::A, 1)).await,
    );
    assert_eq!(
        resp_a.response_code,
        ResponseCode::NoError,
        "a.example response code"
    );
    assert!(!resp_a.answers.is_empty(), "a.example should have answers");
    match &resp_a.answers[0].data {
        RData::A(a) => assert_eq!(a.0, Ipv4Addr::new(1, 1, 1, 1)),
        other => panic!("expected A, got {other:?}"),
    }

    let resp_b = parse_response(
        &send_udp_query(server.addr, &build_query("b.example", RecordType::A, 2)).await,
    );
    assert_eq!(
        resp_b.response_code,
        ResponseCode::NoError,
        "b.example response code"
    );
    assert!(!resp_b.answers.is_empty(), "b.example should have answers");
    match &resp_b.answers[0].data {
        RData::A(a) => assert_eq!(a.0, Ipv4Addr::new(2, 2, 2, 2)),
        other => panic!("expected A, got {other:?}"),
    }
}

#[tokio::test]
async fn servfail_when_no_upstream_available() {
    let server = start_udp_server(vec![], vec![]).await;
    let query = build_query("no-upstream.example", RecordType::A, 42);
    let resp = parse_response(&send_udp_query(server.addr, &query).await);
    assert_eq!(resp.response_code, ResponseCode::ServFail);
}

#[tokio::test]
async fn wildcard_route_match() {
    let upstream = MockUpstream::udp(Ipv4Addr::new(9, 9, 9, 9)).await;
    let server = start_udp_server(
        vec![GroupConfig {
            name: "default".into(),
            strategy: Strategy::RoundRobin,
            upstreams: vec![UpstreamAddr::Simple(upstream.addr.to_string())],
        }],
        vec![RouteConfig {
            domains: vec!["*.wild.example".into()],
            geosite: vec![],
            group: "default".into(),
        }],
    )
    .await;

    let resp = parse_response(
        &send_udp_query(
            server.addr,
            &build_query("sub.wild.example", RecordType::A, 7),
        )
        .await,
    );
    assert_eq!(resp.response_code, ResponseCode::NoError);
    assert_eq!(resp.answers.len(), 1);
}

#[test]
fn config_toml_parse() {
    let toml = r#"
geosite_path = "geosite.dat"

[server]
listen = ["0.0.0.0:5353"]
protocol = ["udp", "tcp"]

[[group]]
name = "default"
strategy = "round-robin"
upstreams = ["8.8.8.8:53", "1.1.1.1:53"]

[[group]]
name = "china"
strategy = "fastest"
upstreams = [
    { addr = "223.5.5.5:53", protocol = "udp" },
    { addr = "119.29.29.29:53", protocol = "udp" },
]

[[route]]
domains = ["baidu.com", "*.baidu.com"]
group = "china"

[cache]
enabled = true
max_entries = 50000
serve_stale_secs = 600

[cache.persistence]
enabled = true
path = "pasdns.cache"

[proxy]
enabled = false
addr = "127.0.0.1:1080"
"#;

    let config: AppConfig = toml::from_str(toml).expect("failed to parse config");
    assert_eq!(config.server.listen, vec!["0.0.0.0:5353"]);
    assert_eq!(config.group.len(), 2);
    assert_eq!(config.group[0].strategy, Strategy::RoundRobin);
    assert_eq!(config.group[1].strategy, Strategy::Fastest);
    assert!(config.cache.enabled);
    assert!(!config.proxy.enabled);
    assert_eq!(config.geosite_path.as_deref(), Some("geosite.dat"));
}

#[test]
fn persist_roundtrip() {
    let dir = std::env::temp_dir().join("pasdns_test_persist");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("test.cache");

    persist::save_cache(
        vec![
            PersistedEntry {
                name: "example.com".to_string(),
                record_type: 1,
                response_bytes: vec![1, 2, 3],
                cached_at_unix_secs: 1000,
                ttl_secs: 300,
            },
            PersistedEntry {
                name: "test.org".to_string(),
                record_type: 28,
                response_bytes: vec![4, 5, 6, 7],
                cached_at_unix_secs: 2000,
                ttl_secs: 600,
            },
        ],
        &path,
    )
    .unwrap();

    let loaded = persist::load_cache(&path).unwrap();
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].name, "example.com");
    assert_eq!(loaded[1].name, "test.org");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn persist_atomic_write_no_residue() {
    let dir = std::env::temp_dir().join("pasdns_test_atomic");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("atomic.cache");

    persist::save_cache(
        vec![PersistedEntry {
            name: "atomic.com".to_string(),
            record_type: 1,
            response_bytes: vec![0xAA; 1024],
            cached_at_unix_secs: 1000,
            ttl_secs: 300,
        }],
        &path,
    )
    .unwrap();

    assert!(path.exists());
    assert!(
        !dir.join("pasdns.cache.tmp").exists(),
        "temp file should not remain after atomic write",
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn strategy_serde_roundtrip() {
    for s in [Strategy::RoundRobin, Strategy::Fastest, Strategy::MinAvg10] {
        let json = serde_json::to_string(&s).unwrap();
        let back: Strategy = serde_json::from_str(&json).unwrap();
        assert_eq!(s, back);
    }
    assert_eq!(
        serde_json::from_str::<Strategy>("\"min-avg-10\"").unwrap(),
        Strategy::MinAvg10,
    );
}
