mod cache;
mod config;
mod forwarder;
mod persist;
mod proxy;
mod router;
mod upstream;

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use config::AppConfig;
use config_file2::LoadConfigFile;
use proxy::DnsProxy;
use tracing_subscriber::EnvFilter;

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
            tracing::info!(loaded, "restored cache entries from disk");
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to load cache from disk");
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
        Ok(()) => tracing::info!("cache persisted to disk"),
        Err(e) => tracing::warn!(error = %e, "failed to persist cache"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();

    let config = match AppConfig::load_or_default("pasdns.toml") {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "failed to load config");
            std::process::exit(1);
        }
    };

    tracing::info!(listen = ?config.server.listen, protocols = ?config.server.protocol, "starting pasdns");

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
                tracing::error!(addr = addr_str, error = %e, "invalid listen address");
                continue;
            }
        };

        for proto in &config.server.protocol {
            match proto {
                config::Protocol::Udp => match tokio::net::UdpSocket::bind(addr).await {
                    Ok(socket) => {
                        tracing::info!(%addr, "listening on UDP");
                        server.register_socket(socket);
                    }
                    Err(e) => tracing::error!(%addr, error = %e, "failed to bind UDP"),
                },
                config::Protocol::Tcp => match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => {
                        tracing::info!(%addr, "listening on TCP");
                        server.register_listener(listener, Duration::from_secs(30), 4096);
                    }
                    Err(e) => tracing::error!(%addr, error = %e, "failed to bind TCP"),
                },
                _ => {
                    tracing::warn!(?proto, "protocol not yet implemented");
                }
            }
        }
    }

    tracing::info!("pasdns ready");

    tokio::select! {
        result = server.block_until_done() => {
            if let Err(e) = &result {
                tracing::error!(error = %e, "server error");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down...");
            if let Some((cache, path)) = &cache_for_shutdown {
                save_cache_to_disk(cache, Path::new(path));
            }
            if let Err(e) = server.shutdown_gracefully().await {
                tracing::warn!(error = %e, "graceful shutdown error");
            }
        }
    }

    tracing::info!("pasdns stopped");
}
