//! Main application configuration (upstream package `config`).
//!
//! [`Config`] composes every package's canonical configuration type, one per TOML
//! table. The only section with a type of its own here is `[metrics]`
//! ([`MetricsConfig`]): the main configuration exposes the metrics tags as a plain
//! map, where `metrics::Config` keeps Go's nil/empty distinction.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use cabby_core::toml_config::{self, string_list};

use crate::metrics::StatsdConfig;

/// Process environment passed explicitly (presets and CLI read `FLY_*`, `HEROKU_*`,
/// `PORT` and `ANYCABLE_*` variables), so tests never mutate the global environment.
pub type EnvMap = BTreeMap<String, String>;

/// `[redis]` (upstream `redis.RedisConfig`).
pub use crate::redis::RedisConfig;

/// `[sse]` (upstream `sse.Config`).
pub use crate::sse::Config as SseConfig;

/// `[ds]` (upstream `ds.Config`).
pub use crate::ds::Config as DsConfig;

/// `[app]` (upstream `node.Config`).
pub use crate::node::Config as AppConfig;

/// `[http_broadcast]` (upstream `broadcast.HTTPConfig`).
pub use crate::broadcast::HttpConfig as HttpBroadcastConfig;

/// `[embedded_nats]` (upstream `enats.Config`).
pub use crate::enats::Config as EmbeddedNatsConfig;

/// `[broker]` (upstream `broker.Config`).
pub use cabby_core::broker::Config as BrokerConfig;

/// `[rpc]` (upstream `rpc.Config`).
pub use crate::rpc::Config as RpcConfig;

/// `[metrics]` (upstream `metrics.Config`) as the main configuration exposes it:
/// the same settings as [`crate::metrics::Config`], with the tags as a map
/// (empty when unset) and conversions both ways.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct MetricsConfig {
    pub log: bool,
    /// Deprecated upstream; kept for configuration compatibility.
    pub log_interval: i64,
    pub rotate_interval: i64,
    /// Path to an mruby formatter script.
    pub log_formatter: String,
    /// Print only these metrics (`None`: all).
    pub log_filter: Option<Vec<String>>,
    /// `http_path`: Prometheus endpoint path (empty: disabled).
    pub http: String,
    pub host: String,
    pub port: i64,
    pub tags: HashMap<String, String>,
    pub statsd: StatsdConfig,
}

cabby_core::toml_section!(MetricsConfig, "metrics.Config" {
    "log" => log,
    "LogInterval" => log_interval,
    "rotate_interval" => rotate_interval,
    "LogFormatter" => log_formatter,
    "log_filter" => log_filter,
    "http_path" => http,
    "host" => host,
    "port" => port,
    "tags" => tags,
    "statsd" => statsd,
});

impl MetricsConfig {
    /// Upstream `metrics.NewConfig()`.
    pub fn new() -> Self {
        (&crate::metrics::Config::new()).into()
    }

    /// Renders the annotated TOML section (upstream `metrics.Config.ToToml`).
    pub fn to_toml(&self) -> String {
        crate::metrics::Config::from(self).to_toml()
    }
}

impl From<&crate::metrics::Config> for MetricsConfig {
    fn from(c: &crate::metrics::Config) -> Self {
        Self {
            log: c.log,
            log_interval: c.log_interval,
            rotate_interval: c.rotate_interval,
            log_formatter: c.log_formatter.clone(),
            log_filter: c.log_filter.clone(),
            http: c.http.clone(),
            host: c.host.clone(),
            port: c.port,
            tags: c.tags.clone().unwrap_or_default(),
            statsd: c.statsd.clone(),
        }
    }
}

impl From<&MetricsConfig> for crate::metrics::Config {
    fn from(c: &MetricsConfig) -> Self {
        Self {
            log: c.log,
            log_interval: c.log_interval,
            rotate_interval: c.rotate_interval,
            log_formatter: c.log_formatter.clone(),
            log_filter: c.log_filter.clone(),
            http: c.http.clone(),
            host: c.host.clone(),
            port: c.port,
            tags: (!c.tags.is_empty()).then(|| c.tags.clone()),
            statsd: c.statsd.clone(),
        }
    }
}

/// Upstream `config.Config`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Config {
    /// `node_id`: the application instance ID (generated at start unless set).
    pub id: String,
    /// Whether `id` came from the configuration file (upstream `UserProvidedID`).
    pub user_provided_id: bool,
    pub secret: String,
    pub broadcast_key: String,
    /// `noauth`: skip connection authentication.
    pub skip_auth: bool,
    /// `public`: no authentication, unsigned streams and broadcasts.
    pub public_mode: bool,
    pub broadcast_adapters: Vec<String>,
    pub pubsub_adapter: String,
    /// `presets`: `None` means "detect from the environment" (upstream nil slice),
    /// `Some(vec![])` disables presets explicitly.
    pub user_presets: Option<Vec<String>>,
    /// `[logging]`
    pub log: cabby_core::logger::Config,
    pub server: crate::server::Config,
    pub app: AppConfig,
    pub ws: crate::ws::Config,
    pub rpc: RpcConfig,
    pub broker: BrokerConfig,
    pub redis: RedisConfig,
    /// `[redis_pubsub_broadcast]`
    pub legacy_redis_broadcast: crate::broadcast::LegacyRedisConfig,
    /// `[redis_stream_broadcast]`
    pub redis_broadcast: crate::broadcast::RedisConfig,
    /// `[nats_broadcast]`
    pub nats_broadcast: crate::broadcast::LegacyNatsConfig,
    pub http_broadcast: HttpBroadcastConfig,
    pub redis_pubsub: crate::pubsub::RedisConfig,
    pub nats_pubsub: crate::pubsub::NatsConfig,
    pub nats: crate::nats::NatsConfig,
    pub disconnector_disabled: bool,
    /// `[disconnector]`
    pub disconnect_queue: crate::node::DisconnectQueueConfig,
    pub metrics: MetricsConfig,
    pub jwt: cabby_core::identity::JwtConfig,
    pub embedded_nats: EmbeddedNatsConfig,
    pub sse: SseConfig,
    pub streams: cabby_core::streams::Config,
    pub pusher: crate::pusher::Config,
    pub ds: DsConfig,
    pub api: crate::api::Config,
    pub config_file_path: Option<PathBuf>,
    /// Configuration file keys that match no setting (dotted paths).
    pub unknown_config_keys: Vec<String>,
}

// Untagged upstream fields decode by their Go names (case-insensitively);
// `UnknownConfigKeys` is excluded (`toml:"-"`).
cabby_core::toml_section!(Config, "config.Config" {
    "node_id" => id,
    "UserProvidedID" => user_provided_id,
    "secret" => secret,
    "broadcast_key" => broadcast_key,
    "noauth" => skip_auth,
    "public" => public_mode,
    "broadcast_adapters" => broadcast_adapters,
    "pubsub_adapter" => pubsub_adapter,
    "presets" => user_presets,
    "logging" => log,
    "server" => server,
    "app" => app,
    "ws" => ws,
    "rpc" => rpc,
    "broker" => broker,
    "redis" => redis,
    "redis_pubsub_broadcast" => legacy_redis_broadcast,
    "redis_stream_broadcast" => redis_broadcast,
    "nats_broadcast" => nats_broadcast,
    "http_broadcast" => http_broadcast,
    "redis_pubsub" => redis_pubsub,
    "nats_pubsub" => nats_pubsub,
    "nats" => nats,
    "DisconnectorDisabled" => disconnector_disabled,
    "disconnector" => disconnect_queue,
    "metrics" => metrics,
    "jwt" => jwt,
    "embedded_nats" => embedded_nats,
    "sse" => sse,
    "streams" => streams,
    "pusher" => pusher,
    "ds" => ds,
    "api" => api,
    "ConfigFilePath" => config_file_path,
});

/// A configuration failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// A random URL-friendly ID (go-nanoid `Nanoid(size)`).
fn nanoid(size: usize) -> String {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut bytes = vec![0u8; size];
    // Upstream ignores generator errors too (the ID is then empty); the OS
    // generator does not fail in practice.
    if getrandom::fill(&mut bytes).is_err() {
        return String::new();
    }
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

/// A Go `os.ReadFile` error text (`open <path>: no such file or directory`).
fn read_error(path: &str, e: &std::io::Error) -> String {
    let op = if e.kind() == std::io::ErrorKind::IsADirectory { "read" } else { "open" };
    let text = e.to_string();
    let text = text.split(" (os error").next().unwrap_or(&text);
    let mut chars = text.chars();
    let text = match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => String::new(),
    };
    format!("{op} {path}: {text}")
}

impl Config {
    /// Defaults (upstream `config.NewConfig`).
    pub fn new() -> Self {
        Self {
            id: nanoid(6),
            server: crate::server::Config::new(),
            broadcast_adapters: vec!["http".to_owned(), "redis".to_owned()],
            broker: BrokerConfig::new(),
            log: cabby_core::logger::Config::new(),
            app: AppConfig::new(),
            ws: crate::ws::Config::new(),
            metrics: MetricsConfig::new(),
            rpc: RpcConfig::new(),
            redis: RedisConfig::new(),
            redis_broadcast: crate::broadcast::RedisConfig::new(),
            legacy_redis_broadcast: crate::broadcast::LegacyRedisConfig::new(),
            nats_broadcast: crate::broadcast::LegacyNatsConfig::new(),
            http_broadcast: HttpBroadcastConfig::new(),
            redis_pubsub: crate::pubsub::RedisConfig::new(),
            nats_pubsub: crate::pubsub::NatsConfig::new(),
            nats: crate::nats::NatsConfig::new(),
            disconnect_queue: crate::node::DisconnectQueueConfig::new(),
            jwt: cabby_core::identity::JwtConfig::new(""),
            embedded_nats: EmbeddedNatsConfig::new(),
            sse: SseConfig::new(),
            streams: cabby_core::streams::Config::new(),
            pusher: crate::pusher::Config::new(),
            ds: DsConfig::new(),
            api: crate::api::Config::new(),
            ..Default::default()
        }
    }

    /// Decodes `config_file_path` over the current values and records unknown keys
    /// (upstream `Config.LoadFromFile`).
    pub fn load_from_file(&mut self) -> Result<(), ConfigError> {
        let path = self.config_file_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError(format!("failed to read config file, cause: {}", read_error(&path, &e))))?;

        let prev_id = self.id.clone();
        let unknown = toml_config::decode(self, &contents)
            .map_err(|e| ConfigError(format!("failed to parse TOML configuration, cause: {e}")))?;

        if self.id != prev_id {
            self.user_provided_id = true;
        }

        self.unknown_config_keys = unknown;

        Ok(())
    }

    /// Renders the full annotated configuration file (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();

        out.push_str(
            "# AnyCable server configuration.\n# Read more at https://docs.anycable.io/anycable-go/configuration\n\n",
        );
        out.push_str("# General settings\n\n");

        out.push_str("# Public mode disables connection authentication, pub/sub streams and broadcasts verification\n");
        out.push_str(if self.public_mode { "public = true\n\n" } else { "# public = false\n\n" });

        out.push_str("# Disable connection authentication only\n");
        out.push_str(if self.skip_auth { "noauth = true\n\n" } else { "# noauth = false\n\n" });

        out.push_str("# Application instance ID\n");
        if self.user_provided_id {
            out.push_str(&format!("node_id = \"{}\"\n\n", self.id));
        } else {
            out.push_str("# node_id = \"<auto-generated at each server start>\"\n\n");
        }

        out.push_str("# The application secret key\n");
        if self.secret.is_empty() {
            out.push_str("secret = \"none\"\n\n");
        } else {
            out.push_str(&format!("secret = \"{}\"\n\n", self.secret));
        }

        out.push_str("# Broadcasting adapters for app-to-clients messages\n");
        out.push_str(&format!("broadcast_adapters = {}\n\n", string_list(&self.broadcast_adapters)));

        out.push_str("# Broadcasting authorization key\n");
        if !self.broadcast_key.is_empty() {
            out.push_str(&format!("broadcast_key = \"{}\"\n\n", self.broadcast_key));
        } else if !self.secret.is_empty() {
            out.push_str("# broadcast_key = \"<auto-generated from the application secret>\"\n\n");
        } else {
            out.push_str("broadcast_key = \"none\"\n\n");
        }

        out.push_str("# Pub/sub adapter for inter-node communication\n");
        if self.pubsub_adapter.is_empty() {
            out.push_str("# pubsub_adapter = \"redis\" # or \"nats\"\n\n");
        } else {
            out.push_str(&format!("pubsub_adapter = \"{}\"\n\n", self.pubsub_adapter));
        }

        out.push_str("# User-provided configuration presets\n");
        match self.user_presets.as_deref() {
            Some(presets) if !presets.is_empty() => {
                out.push_str(&format!("presets = {}\n\n", string_list(presets)));
            }
            _ => out.push_str("# presets = [\"broker\"]\n\n"),
        }

        let sections: [(&str, String); 13] = [
            ("# Server configuration\n[server]\n", self.server.to_toml()),
            ("# Logging configuration\n[logging]\n", self.log.to_toml()),
            ("# RPC configuration\n[rpc]\n", self.rpc.to_toml()),
            ("# Broker configuration\n[broker]\n", self.broker.to_toml()),
            ("# JWT configuration\n[jwt]\n", self.jwt.to_toml()),
            ("# Pub/sub (signed) streams configuration\n[streams]\n", self.streams.to_toml()),
            ("# WebSockets configuration\n[ws]\n", self.ws.to_toml()),
            ("# SSE configuration\n[sse]\n", self.sse.to_toml()),
            ("# Pusher compatibility configuration\n[pusher]\n", self.pusher.to_toml()),
            ("# Durable Streams configuration\n[ds]\n", self.ds.to_toml()),
            ("# API configuration\n[api]\n", self.api.to_toml()),
            ("# Redis configuration\n[redis]\n", self.redis.to_toml()),
            ("# NATS configuration\n[nats]\n", self.nats.to_toml()),
        ];
        for (header, body) in sections {
            out.push_str(header);
            out.push_str(&body);
        }

        out.push_str("# Broadcast adapters configuration\n[http_broadcast]\n");
        out.push_str(&self.http_broadcast.to_toml());
        out.push_str("[redis_stream_broadcast]\n");
        out.push_str(&self.redis_broadcast.to_toml());
        out.push_str("[redis_pubsub_broadcast]\n");
        out.push_str(&self.legacy_redis_broadcast.to_toml());
        out.push_str("[nats_broadcast]\n");
        out.push_str(&self.nats_broadcast.to_toml());

        out.push_str("# Pub/sub adapters configuration\n[redis_pubsub]\n");
        out.push_str(&self.redis_pubsub.to_toml());
        out.push_str("[nats_pubsub]\n");
        out.push_str(&self.nats_pubsub.to_toml());

        out.push_str("# Metrics configuration\n[metrics]\n");
        out.push_str(&self.metrics.to_toml());

        out.push_str("# App configuration\n[app]\n");
        out.push_str(&self.app.to_toml());

        out.push_str("# Disconnector configuration\n[disconnector]\n");
        out.push_str(&self.disconnect_queue.to_toml());

        out.push_str("# Embedded NATS configuration\n[embedded_nats]\n");
        out.push_str(&self.embedded_nats.to_toml());

        out
    }

    /// The presets to apply: the user's, or those detected from `env` (upstream `Presets`).
    pub fn presets(&self, env: &EnvMap) -> Vec<String> {
        if let Some(presets) = &self.user_presets {
            return presets.clone();
        }

        let mut presets = Vec::new();
        if ["FLY_APP_NAME", "FLY_ALLOC_ID", "FLY_REGION"].iter().all(|k| env.contains_key(*k)) {
            presets.push("fly".to_owned());
        }
        if ["HEROKU_APP_ID", "HEROKU_DYNO_ID"].iter().all(|k| env.contains_key(*k)) {
            presets.push("heroku".to_owned());
        }
        presets
    }

    /// Applies the presets (upstream `LoadPresets`).
    pub fn load_presets(&mut self, env: &EnvMap) -> Result<(), ConfigError> {
        let presets = self.presets(env);

        if presets.is_empty() {
            return Ok(());
        }

        let defaults = Config::new();

        for preset in &presets {
            match preset.as_str() {
                "fly" => self.load_fly_preset(&defaults, env)?,
                "heroku" => self.load_heroku_preset(&defaults, env)?,
                "broker" => self.load_broker_preset(&defaults),
                _ => {}
            }
        }

        Ok(())
    }

    fn load_fly_preset(&mut self, defaults: &Config, env: &EnvMap) -> Result<(), ConfigError> {
        if self.server.host == defaults.server.host {
            self.server.host = "0.0.0.0".to_owned();
        }

        let region = env.get("FLY_REGION").ok_or_else(|| ConfigError("FLY_REGION env is missing".to_owned()))?;
        let app_name = env.get("FLY_APP_NAME").ok_or_else(|| ConfigError("FLY_APP_NAME env is missing".to_owned()))?;
        let app_id = env.get("FLY_ALLOC_ID").map(String::as_str).unwrap_or_default();

        let redis_enabled = self.redis.url != defaults.redis.url;

        // Use the same port for HTTP broadcasts by default
        if self.http_broadcast.port == defaults.http_broadcast.port {
            self.http_broadcast.port = self.server.port;
        }

        if self.embedded_nats.name == defaults.embedded_nats.name && !app_id.is_empty() {
            // Unique, to avoid duplicates during deployments
            self.embedded_nats.name = format!("fly-{region}-{app_id}-{}", nanoid(3));
        }

        if self.embedded_nats.service_addr == defaults.embedded_nats.service_addr {
            self.embedded_nats.service_addr = "nats://0.0.0.0:4222".to_owned();
        }

        if self.embedded_nats.cluster_addr == defaults.embedded_nats.cluster_addr {
            self.embedded_nats.cluster_addr = "nats://0.0.0.0:5222".to_owned();
        }

        if self.embedded_nats.cluster_name == defaults.embedded_nats.cluster_name {
            self.embedded_nats.cluster_name = format!("{app_name}-{region}-cluster");
        }

        // Upstream checks for a nil slice; an empty list counts as unset here.
        if self.embedded_nats.routes.is_empty() {
            self.embedded_nats.routes = vec![format!("nats://{region}.{app_name}.internal:5222")];
        }

        if self.embedded_nats.gateway_advertise == defaults.embedded_nats.gateway_advertise {
            self.embedded_nats.gateway_advertise = format!("{region}.{app_name}.internal:7222");
        }

        // Enable embedded NATS by default unless another adapter is set for pub/sub
        // or a Redis URL is provided
        if self.pubsub_adapter == defaults.pubsub_adapter {
            if redis_enabled {
                self.pubsub_adapter = "redis".to_owned();
            } else {
                self.pubsub_adapter = "nats".to_owned();

                // NATS hasn't been configured, so we can embed it
                if !self.embedded_nats.enabled || self.nats.servers == defaults.nats.servers {
                    self.embedded_nats.enabled = true;
                    self.nats.servers = self.embedded_nats.service_addr.clone();
                    self.broadcast_adapters = vec!["http".to_owned(), "nats".to_owned()];
                }
            }
        }

        if !redis_enabled && self.broadcast_adapters == defaults.broadcast_adapters {
            self.broadcast_adapters = vec!["http".to_owned()];
        }

        if let Some(rpc_name) = env.get("ANYCABLE_FLY_RPC_APP_NAME")
            && self.rpc.host == defaults.rpc.host
        {
            self.rpc.host = format!("dns:///{region}.{rpc_name}.internal:50051");
        }

        Ok(())
    }

    fn load_heroku_preset(&mut self, defaults: &Config, env: &EnvMap) -> Result<(), ConfigError> {
        if self.server.host == defaults.server.host {
            self.server.host = "0.0.0.0".to_owned();
        }

        if self.http_broadcast.port == defaults.http_broadcast.port
            && let Some(port) = env.get("PORT").filter(|p| !p.is_empty())
        {
            self.http_broadcast.port = go_atoi(port)?;
        }

        Ok(())
    }

    fn load_broker_preset(&mut self, defaults: &Config) {
        let redis_enabled = self.redis.url != defaults.redis.url;
        let enats_enabled = self.embedded_nats.enabled;

        if self.broker.adapter == defaults.broker.adapter {
            self.broker.adapter = if enats_enabled { "nats" } else { "memory" }.to_owned();
        }

        if self.broadcast_adapters == defaults.broadcast_adapters {
            self.broadcast_adapters = if enats_enabled {
                vec!["http".to_owned(), "nats".to_owned()]
            } else if redis_enabled {
                vec!["http".to_owned(), "redisx".to_owned(), "redis".to_owned()]
            } else {
                vec!["http".to_owned()]
            };
        }

        if self.pubsub_adapter == defaults.pubsub_adapter {
            if enats_enabled {
                self.pubsub_adapter = "nats".to_owned();
            } else if redis_enabled {
                self.pubsub_adapter = "redis".to_owned();
            }
        }
    }
}

/// Go `strconv.Atoi`, with its error text.
fn go_atoi(s: &str) -> Result<i64, ConfigError> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(ConfigError(format!("strconv.Atoi: parsing {}: invalid syntax", toml_config::go_quote(s))));
    }
    s.parse()
        .map_err(|_| ConfigError(format!("strconv.Atoi: parsing {}: value out of range", toml_config::go_quote(s))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanoid_uses_the_url_alphabet() {
        let id = nanoid(6);
        assert_eq!(id.len(), 6);
        assert!(id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'));
    }

    #[test]
    fn atoi_errors_match_go() {
        assert_eq!(go_atoi("42"), Ok(42));
        assert_eq!(go_atoi("4x").unwrap_err().0, "strconv.Atoi: parsing \"4x\": invalid syntax");
    }
}
