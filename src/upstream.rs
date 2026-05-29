use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use dashmap::DashMap;

use crate::config::{GroupConfig, Protocol, Strategy, UpstreamAddr};

pub struct UpstreamManager {
    groups: HashMap<String, UpstreamGroup>,
}

struct UpstreamGroup {
    upstreams: Vec<(SocketAddr, Protocol)>,
    strategy: Strategy,
    counter: AtomicUsize,
    latencies: DashMap<SocketAddr, LatencyStats>,
}

struct LatencyStats {
    samples: VecDeque<Duration>,
}

const MAX_SAMPLES: usize = 10;

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: VecDeque::new(),
        }
    }

    fn record(&mut self, duration: Duration) {
        if self.samples.len() >= MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(duration);
    }

    fn avg(&self) -> Option<Duration> {
        if self.samples.is_empty() {
            return None;
        }
        let sum: Duration = self.samples.iter().sum();
        #[allow(clippy::cast_possible_truncation)]
        Some(sum / self.samples.len() as u32)
    }
}

impl UpstreamGroup {
    fn new(config: &GroupConfig) -> Self {
        let upstreams: Vec<(SocketAddr, Protocol)> = config
            .upstreams
            .iter()
            .map(|addr| match addr {
                UpstreamAddr::Simple(s) => {
                    (s.parse().expect("invalid upstream address"), Protocol::Udp)
                }
                UpstreamAddr::Detailed { addr, protocol } => {
                    (addr.parse().expect("invalid upstream address"), *protocol)
                }
            })
            .collect();
        Self {
            upstreams,
            strategy: config.strategy,
            counter: AtomicUsize::new(0),
            latencies: DashMap::new(),
        }
    }

    fn pick_upstream(&self) -> Option<(SocketAddr, Protocol)> {
        if self.upstreams.is_empty() {
            return None;
        }
        match self.strategy {
            Strategy::RoundRobin => Some(self.pick_round_robin()),
            Strategy::Fastest | Strategy::MinAvg10 => {
                self.pick_by_latency().or(Some(self.pick_round_robin()))
            }
        }
    }

    fn pick_round_robin(&self) -> (SocketAddr, Protocol) {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.upstreams.len();
        self.upstreams[idx]
    }

    fn pick_by_latency(&self) -> Option<(SocketAddr, Protocol)> {
        let mut best: Option<(SocketAddr, Protocol, Duration)> = None;
        for (addr, proto) in &self.upstreams {
            if let Some(stats) = self.latencies.get(addr)
                && let Some(avg) = stats.avg()
                && best.is_none_or(|(_, _, d)| avg < d)
            {
                best = Some((*addr, *proto, avg));
            }
        }
        best.map(|(a, p, _)| (a, p))
    }
}

impl UpstreamManager {
    pub fn new(groups: &[GroupConfig]) -> Self {
        let groups = groups
            .iter()
            .map(|g| (g.name.clone(), UpstreamGroup::new(g)))
            .collect();
        Self { groups }
    }

    pub fn pick_upstream(&self, group: &str) -> Option<(SocketAddr, Protocol)> {
        self.groups.get(group)?.pick_upstream()
    }

    pub fn record_latency(&self, upstream: SocketAddr, group: &str, duration: Duration) {
        if let Some(g) = self.groups.get(group) {
            g.latencies
                .entry(upstream)
                .or_insert_with(LatencyStats::new)
                .value_mut()
                .record(duration);
        }
    }
}
