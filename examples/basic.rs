use std::{sync::Arc, time::Duration};

use pasdns::{
    cache::DnsCache, config::AppConfig, proxy::DnsProxy, router::Router, upstream::UpstreamManager,
};

#[tokio::main]
async fn main() {
    let config = AppConfig::default();
    let config = Arc::new(config);
    let router = Router::new(&[], "default", None);
    let upstream = UpstreamManager::new(&[]);
    let cache = DnsCache::new(1000, 300);

    let proxy = DnsProxy::new(config, router, upstream, Some(cache));

    println!(
        "DNS proxy created: {:?}",
        proxy.cache().map(|_| "cache enabled")
    );
    println!("This example creates a DnsProxy instance programmatically.");
    println!("To run a full DNS server, see the pasdns binary.");

    tokio::time::sleep(Duration::from_secs(1)).await;
}
