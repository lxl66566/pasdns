use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use config_file2::LoadConfigFile;
use fastrace::collector::{Config, ConsoleReporter};
use log::{error, info, warn};
use pasdns::{
    cache,
    config::{AppConfig, Protocol},
    forwarder, persist,
    proxy::DnsProxy,
    router, signal, upstream,
};

fn find_default_group(config: &AppConfig) -> &str {
    config
        .group
        .iter()
        .find(|g| g.name == "default")
        .map_or_else(
            || config.group.first().map_or("default", |g| g.name.as_str()),
            |g| g.name.as_str(),
        )
}

async fn load_cache_from_disk(cache: &cache::DnsCache, path: &Path) {
    match persist::load_cache(path) {
        Ok(entries) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut loaded = 0u64;
            for entry in entries {
                let elapsed = now.saturating_sub(entry.cached_at_unix_secs);
                let remaining_ttl = entry.ttl_secs.saturating_sub(elapsed);
                if remaining_ttl == 0 {
                    continue;
                }
                let key = cache::CacheKey {
                    name: entry.name,
                    record_type: entry.record_type,
                };
                cache
                    .insert(
                        key,
                        entry.response_bytes,
                        Duration::from_secs(remaining_ttl),
                    )
                    .await;
                loaded += 1;
            }
            info!(target: "pasdns", "restored {loaded} cache entries from disk");
        }
        Err(e) => {
            warn!(target: "pasdns", "failed to load cache from disk: {e}");
        }
    }
}

fn save_cache_to_disk(cache: &cache::DnsCache, path: &Path) {
    let entries = cache.entries_for_persistence();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let persisted: Vec<persist::PersistedEntry> = entries
        .into_iter()
        .map(|(key, bytes, cached_at, ttl)| {
            let cached_at_unix = now.saturating_sub(cached_at.elapsed().as_secs());
            persist::PersistedEntry {
                name: key.name,
                record_type: key.record_type,
                response_bytes: bytes,
                cached_at_unix_secs: cached_at_unix,
                ttl_secs: ttl.as_secs(),
            }
        })
        .collect();
    match persist::save_cache(persisted, path) {
        Ok(()) => info!(target: "pasdns", "cache persisted to disk"),
        Err(e) => warn!(target: "pasdns", "failed to persist cache: {e}"),
    }
}

fn cleanup_temp(path: &Path) {
    let tmp = forwarder::temp_path_for(path);
    if tmp.exists() {
        let _ = std::fs::remove_file(&tmp);
    }
}

#[tokio::main]
async fn main() {
    fastrace::set_reporter(ConsoleReporter, Config::default());

    let config = match AppConfig::load_or_default("pasdns.toml") {
        Ok(c) => c,
        Err(e) => {
            error!(target: "pasdns", "failed to load config: {e}");
            std::process::exit(1);
        }
    };

    info!(
        target: "pasdns",
        "starting pasdns on {:?} ({:?})",
        config.server.listen,
        config.server.protocol,
    );

    let default_group = find_default_group(&config);
    let geosite_data = config
        .geosite_path
        .as_ref()
        .and_then(|p| router::load_geosite(Path::new(p)));
    let router = router::Router::new(&config.route, default_group, geosite_data.as_ref());
    let upstream_manager = upstream::UpstreamManager::new(&config.group);

    let dns_cache = if config.cache.enabled {
        let cache = cache::DnsCache::new(config.cache.max_entries, config.cache.serve_stale_secs);
        if config.cache.persistence.enabled {
            let path = Path::new(&config.cache.persistence.path);
            load_cache_from_disk(&cache, path).await;
        }
        Some(cache)
    } else {
        None
    };

    let config = Arc::new(config);
    let cache_for_shutdown = dns_cache.as_ref().and_then(|c| {
        if config.cache.persistence.enabled {
            Some((c.clone(), config.cache.persistence.path.clone()))
        } else {
            None
        }
    });

    let proxy = DnsProxy::new(Arc::clone(&config), router, upstream_manager, dns_cache);

    let mut server = hickory_server::server::Server::new(proxy);

    for addr_str in &config.server.listen {
        let addr: std::net::SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                error!(target: "pasdns", "invalid listen address '{addr_str}': {e}");
                continue;
            }
        };

        for proto in &config.server.protocol {
            match proto {
                Protocol::Udp => match tokio::net::UdpSocket::bind(addr).await {
                    Ok(socket) => {
                        info!(target: "pasdns", "listening on UDP {addr}");
                        server.register_socket(socket);
                    }
                    Err(e) => error!(target: "pasdns", "failed to bind UDP {addr}: {e}"),
                },
                Protocol::Tcp => match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => {
                        info!(target: "pasdns", "listening on TCP {addr}");
                        server.register_listener(listener, Duration::from_secs(30), 4096);
                    }
                    Err(e) => error!(target: "pasdns", "failed to bind TCP {addr}: {e}"),
                },
                _ => {
                    warn!(target: "pasdns", "protocol {proto:?} not yet implemented");
                }
            }
        }
    }

    if config.proxy.enabled
        && let Some(proxy_addr) = config.proxy.addr.as_ref()
    {
        info!(target: "pasdns", "upstream proxy enabled: {proxy_addr}");
    }

    info!(target: "pasdns", "pasdns ready");

    signal::install();

    tokio::select! {
        result = server.block_until_done() => {
            if let Err(e) = &result {
                error!(target: "pasdns", "server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!(target: "pasdns", "received ctrl+c, shutting down...");
        }
        _ = signal::wait_for_shutdown() => {
            info!(target: "pasdns", "received shutdown signal, shutting down...");
        }
    }

    if let Some((cache, path)) = &cache_for_shutdown {
        let path = Path::new(path);
        save_cache_to_disk(cache, path);
        cleanup_temp(path);
    }

    signal::notify_cleanup_done();

    if let Err(e) = server.shutdown_gracefully().await {
        warn!(target: "pasdns", "graceful shutdown error: {e}");
    }

    fastrace::flush();
    info!(target: "pasdns", "pasdns stopped");
}
