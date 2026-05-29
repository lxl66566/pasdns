use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use dashmap::DashMap;
use moka::future::Cache;
use tokio::sync::Mutex;

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct CacheKey {
    pub name: String,
    pub record_type: u16,
}

#[derive(Clone)]
pub struct CacheEntry {
    pub response_bytes: Vec<u8>,
    pub cached_at: Instant,
    pub ttl: Duration,
}

impl CacheEntry {
    pub fn is_fresh(&self) -> bool {
        self.cached_at.elapsed() < self.ttl
    }

    pub fn is_stale_within(&self, serve_stale: Duration) -> bool {
        let elapsed = self.cached_at.elapsed();
        elapsed >= self.ttl && elapsed < self.ttl + serve_stale
    }
}

pub enum CacheLookupResult {
    Fresh(Vec<u8>),
    Stale(Vec<u8>),
    Miss,
}

struct DnsCacheInner {
    cache: Cache<CacheKey, CacheEntry>,
    serve_stale_duration: Duration,
    #[allow(dead_code)]
    inflight: DashMap<CacheKey, Arc<Mutex<()>>>,
}

#[derive(Clone)]
pub struct DnsCache {
    inner: Arc<DnsCacheInner>,
}

impl DnsCache {
    pub fn new(max_entries: u64, serve_stale_secs: u64) -> Self {
        let cache = Cache::builder().max_capacity(max_entries).build();
        Self {
            inner: Arc::new(DnsCacheInner {
                cache,
                serve_stale_duration: Duration::from_secs(serve_stale_secs),
                inflight: DashMap::new(),
            }),
        }
    }

    pub async fn lookup(&self, key: &CacheKey) -> CacheLookupResult {
        if let Some(entry) = self.inner.cache.get(key).await {
            if entry.is_fresh() {
                return CacheLookupResult::Fresh(entry.response_bytes.clone());
            }
            if entry.is_stale_within(self.inner.serve_stale_duration) {
                return CacheLookupResult::Stale(entry.response_bytes.clone());
            }
        }
        CacheLookupResult::Miss
    }

    pub async fn insert(&self, key: CacheKey, response_bytes: Vec<u8>, ttl: Duration) {
        let entry = CacheEntry {
            response_bytes,
            cached_at: Instant::now(),
            ttl,
        };
        self.inner.cache.insert(key, entry).await;
    }

    #[allow(dead_code)]
    pub async fn get_or_fetch<F, Fut>(&self, key: CacheKey, fetch_fn: F) -> Option<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Option<Vec<u8>>>,
    {
        match self.lookup(&key).await {
            CacheLookupResult::Fresh(bytes) | CacheLookupResult::Stale(bytes) => {
                return Some(bytes);
            }
            CacheLookupResult::Miss => {}
        }

        let mutex = self
            .inner
            .inflight
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = mutex.lock().await;

        match self.lookup(&key).await {
            CacheLookupResult::Fresh(bytes) | CacheLookupResult::Stale(bytes) => {
                self.inner.inflight.remove(&key);
                return Some(bytes);
            }
            CacheLookupResult::Miss => {}
        }

        let result = fetch_fn().await;
        self.inner.inflight.remove(&key);
        result
    }

    pub fn entries_for_persistence(&self) -> Vec<(CacheKey, Vec<u8>, Instant, Duration)> {
        let mut result = Vec::new();
        for (key, value) in &self.inner.cache {
            result.push((
                key.as_ref().clone(),
                value.response_bytes.clone(),
                value.cached_at,
                value.ttl,
            ));
        }
        result
    }
}
