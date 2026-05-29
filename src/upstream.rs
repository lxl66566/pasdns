use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use dashmap::DashMap;
use log::warn;

use crate::config::{GroupConfig, Protocol, Strategy, UpstreamAddr};

pub struct UpstreamManager {
    groups: HashMap<String, UpstreamGroup>,
}

pub(crate) struct UpstreamTarget {
    pub(crate) addr: SocketAddr,
    pub(crate) protocol: Protocol,
    pub(crate) doh_url: Option<String>,
}

struct UpstreamGroup {
    targets: Vec<UpstreamTarget>,
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
        let targets: Vec<UpstreamTarget> = config
            .upstreams
            .iter()
            .filter_map(|addr| match addr {
                UpstreamAddr::Simple(s) => {
                    let socket_addr: SocketAddr = s.parse().ok()?;
                    Some(UpstreamTarget {
                        addr: socket_addr,
                        protocol: Protocol::Udp,
                        doh_url: None,
                    })
                }
                UpstreamAddr::Detailed { addr, protocol } => match protocol {
                    Protocol::Doh => {
                        let parsed = http::Uri::try_from(addr.as_str()).ok()?;
                        let host = parsed.host()?;
                        let port = parsed.port_u16().unwrap_or(443);
                        let socket_addr: SocketAddr = format!("{host}:{port}").parse().ok()?;
                        Some(UpstreamTarget {
                            addr: socket_addr,
                            protocol: Protocol::Doh,
                            doh_url: Some(addr.clone()),
                        })
                    }
                    _ => {
                        let socket_addr: SocketAddr = addr.parse().ok()?;
                        Some(UpstreamTarget {
                            addr: socket_addr,
                            protocol: *protocol,
                            doh_url: None,
                        })
                    }
                },
            })
            .collect();
        if targets.len() != config.upstreams.len() {
            warn!(
                target: "pasdns::upstream",
                "group '{}': some upstreams failed to parse",
                config.name,
            );
        }
        Self {
            targets,
            strategy: config.strategy,
            counter: AtomicUsize::new(0),
            latencies: DashMap::new(),
        }
    }

    fn pick_upstream(&self) -> Option<&UpstreamTarget> {
        if self.targets.is_empty() {
            return None;
        }
        match self.strategy {
            Strategy::RoundRobin => Some(self.pick_round_robin()),
            Strategy::Fastest | Strategy::MinAvg10 => {
                self.pick_by_latency().or(Some(self.pick_round_robin()))
            }
        }
    }

    fn pick_round_robin(&self) -> &UpstreamTarget {
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % self.targets.len();
        &self.targets[idx]
    }

    fn pick_by_latency(&self) -> Option<&UpstreamTarget> {
        let mut best: Option<(&UpstreamTarget, Duration)> = None;
        for target in &self.targets {
            if let Some(stats) = self.latencies.get(&target.addr)
                && let Some(avg) = stats.avg()
                && best.as_ref().is_none_or(|(_, d)| avg < *d)
            {
                best = Some((target, avg));
            }
        }
        best.map(|(t, _)| t)
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

    pub(crate) fn pick_upstream(&self, group: &str) -> Option<&UpstreamTarget> {
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
