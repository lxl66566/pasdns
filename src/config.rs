use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub group: Vec<GroupConfig>,
    #[serde(default)]
    pub route: Vec<RouteConfig>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub geosite_path: Option<String>,
    #[serde(default)]
    pub proxy: ProxyConfig,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: Vec<String>,
    #[serde(default = "default_protocol")]
    pub protocol: Vec<Protocol>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            protocol: default_protocol(),
        }
    }
}

fn default_listen() -> Vec<String> {
    vec!["127.0.0.1:53".to_string()]
}

fn default_protocol() -> Vec<Protocol> {
    vec![Protocol::Udp, Protocol::Tcp]
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "kebab-case")]
pub enum Protocol {
    #[default]
    Udp,
    Tcp,
    Dot,
    Doh,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct GroupConfig {
    pub name: String,
    #[serde(default)]
    pub strategy: Strategy,
    pub upstreams: Vec<UpstreamAddr>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum UpstreamAddr {
    Simple(String),
    Detailed {
        addr: String,
        #[serde(default)]
        protocol: Protocol,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum Strategy {
    #[default]
    RoundRobin,
    Fastest,
    #[serde(rename = "min-avg-10")]
    MinAvg10,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RouteConfig {
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default)]
    pub geosite: Vec<String>,
    pub group: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CacheConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_max_entries")]
    pub max_entries: u64,
    #[serde(default = "default_ttl")]
    pub default_ttl_secs: u64,
    #[serde(default = "default_serve_stale")]
    pub serve_stale_secs: u64,
    #[serde(default)]
    pub persistence: PersistenceConfig,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: default_max_entries(),
            default_ttl_secs: default_ttl(),
            serve_stale_secs: default_serve_stale(),
            persistence: PersistenceConfig::default(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_max_entries() -> u64 {
    10000
}

fn default_ttl() -> u64 {
    3600
}

fn default_serve_stale() -> u64 {
    300
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct PersistenceConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_persistence_path")]
    pub path: String,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            path: default_persistence_path(),
        }
    }
}

fn default_persistence_path() -> String {
    "pasdns.cache".to_string()
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct ProxyConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub addr: Option<String>,
}
