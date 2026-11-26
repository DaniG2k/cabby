//! Main application configuration (upstream package `config`).
//!
//! [`Config`] composes every package's canonical configuration type, one per TOML
//! table. The sections with a type of their own here are `[metrics]` and `[sqlite]`
//! ([`MetricsConfig`], [`SqliteConfig`]): the main configuration exposes the metrics
//! tags as a plain map, where `metrics::Config` keeps Go's nil/empty distinction.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use cabby_core::toml_config::{self, string_list, toml_quote};

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

/// `[sqlite]`: the SQLite broker's database (`--sqlite_path`,
/// `CABBY_SQLITE_PATH`). A Cabby-only section (deviation D-065), so it has no
/// `ANYCABLE_*` alias.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqliteConfig {
    /// The database file; required with `broker.adapter = "sqlite"`.
    pub path: String,
}

cabby_core::toml_section!(SqliteConfig, "sqlite.Config" {
    "path" => path,
});

impl SqliteConfig {
    /// Renders the annotated TOML section.
    pub fn to_toml(&self) -> String {
        let mut out =
            String::from("# Database file for --broker=sqlite (created with mode 0600 in a 0700 directory)\n");
        if self.path.is_empty() {
            out.push_str("# path = \"/var/lib/cabby/cabby.db\"\n");
        } else {
            out.push_str(&format!("path = {}\n", toml_quote(&self.path)));
        }
        out.push('\n');
        out
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
    /// `[sqlite]` (Cabby only, D-065).
    pub sqlite: SqliteConfig,
    /// `[redis_broker]` (Cabby only, D-069).
    pub redis_broker: crate::broker::RedisBrokerConfig,
    /// `[postgres]` (Cabby only, D-070, D-071).
    pub postgres: crate::postgres::PostgresConfig,
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
    /// Start-up messages found while the configuration is parsed, before the
    /// logger exists; the runner logs them (D-060, D-061). Never written to a
    /// configuration file.
    pub startup_notes: Vec<StartupNote>,
}

/// A start-up message recorded while parsing (see [`Config::startup_notes`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupNote {
    /// `path` exists but ranks below the configuration file in use (D-060).
    IgnoredConfigFile { path: String, used: String },
    /// `CABBY_<X>` and `ANYCABLE_<X>` are both set, to different values; the
    /// `CABBY_*` value is used (D-061). Values are never recorded.
    EnvConflict { cabby: String, anycable: String },
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
    "sqlite" => sqlite,
    "redis_broker" => redis_broker,
    "postgres" => postgres,
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
            redis_broker: crate::broker::RedisBrokerConfig::new(),
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
            postgres: crate::postgres::PostgresConfig::new(),
            ..Default::default()
        }
    }

    /// Decodes `config_file_path` over the current values and records unknown keys
    /// (upstream `Config.LoadFromFile`).
    pub fn load_from_file(&mut self) -> Result<(), ConfigError> {
        let path = self.config_file_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default();
        let contents = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError(format!("failed to read config file, cause: {}", read_error(&path, &e))))?;
        self.load_from_str(&contents)
    }

    /// Decodes the contents of a configuration file over the current values.
    fn load_from_str(&mut self, contents: &str) -> Result<(), ConfigError> {
        let prev_id = self.id.clone();
        let unknown = toml_config::decode(self, contents)
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
            out.push_str(&format!("node_id = {}\n\n", toml_quote(&self.id)));
        } else {
            out.push_str("# node_id = \"<auto-generated at each server start>\"\n\n");
        }

        out.push_str("# The application secret key\n");
        if self.secret.is_empty() {
            // D-086: "none" is not accepted as the application secret.
            out.push_str("# secret = \"<not set>\"\n\n");
        } else {
            out.push_str(&format!("secret = {}\n\n", toml_quote(&self.secret)));
        }

        out.push_str("# Broadcasting adapters for app-to-clients messages\n");
        out.push_str(&format!("broadcast_adapters = {}\n\n", string_list(&self.broadcast_adapters)));

        out.push_str("# Broadcasting authorization key\n");
        if !self.broadcast_key.is_empty() {
            out.push_str(&format!("broadcast_key = {}\n\n", toml_quote(&self.broadcast_key)));
        } else if !self.secret.is_empty() {
            out.push_str("# broadcast_key = \"<auto-generated from the application secret>\"\n\n");
        } else {
            out.push_str("broadcast_key = \"none\"\n\n");
        }

        out.push_str("# Pub/sub adapter for inter-node communication\n");
        if self.pubsub_adapter.is_empty() {
            out.push_str("# pubsub_adapter = \"redis\" # or \"nats\", \"postgres\"\n\n");
        } else {
            out.push_str(&format!("pubsub_adapter = {}\n\n", toml_quote(&self.pubsub_adapter)));
        }

        out.push_str("# User-provided configuration presets\n");
        match self.user_presets.as_deref() {
            Some(presets) if !presets.is_empty() => {
                out.push_str(&format!("presets = {}\n\n", string_list(presets)));
            }
            _ => out.push_str("# presets = [\"broker\"]\n\n"),
        }

        let sections: [(&str, String); 16] = [
            ("# Server configuration\n[server]\n", self.server.to_toml()),
            ("# Logging configuration\n[logging]\n", self.log.to_toml()),
            ("# RPC configuration\n[rpc]\n", self.rpc.to_toml()),
            ("# Broker configuration\n[broker]\n", self.broker.to_toml()),
            ("# SQLite broker configuration (--broker=sqlite)\n[sqlite]\n", self.sqlite.to_toml()),
            ("# Redis broker configuration (--broker=redis)\n[redis_broker]\n", self.redis_broker.to_toml()),
            (
                "# Postgres adapters configuration (--pubsub, --broadcast_adapter, --broker=postgres)\n[postgres]\n",
                self.postgres.to_toml(),
            ),
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

        if let Some(rpc_name) = env.get("CABBY_FLY_RPC_APP_NAME").or_else(|| env.get("ANYCABLE_FLY_RPC_APP_NAME"))
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

    /// Pins D-061: the Fly preset reads `CABBY_FLY_RPC_APP_NAME`, then
    /// `ANYCABLE_FLY_RPC_APP_NAME`.
    #[test]
    fn deviation_d061_fly_rpc_app_name() {
        let rpc_host = |extra: &[(&str, &str)]| {
            let env: EnvMap = [("FLY_APP_NAME", "app"), ("FLY_ALLOC_ID", "1234"), ("FLY_REGION", "ams")]
                .iter()
                .chain(extra)
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect();
            let mut c = Config::new();
            c.load_presets(&env).expect("presets");
            c.rpc.host
        };
        let both = [("CABBY_FLY_RPC_APP_NAME", "rpc-a"), ("ANYCABLE_FLY_RPC_APP_NAME", "rpc-b")];
        assert_eq!(rpc_host(&both), "dns:///ams.rpc-a.internal:50051");
        assert_eq!(rpc_host(&[("ANYCABLE_FLY_RPC_APP_NAME", "rpc-b")]), "dns:///ams.rpc-b.internal:50051");
    }

    #[test]
    fn atoi_errors_match_go() {
        assert_eq!(go_atoi("42"), Ok(42));
        assert_eq!(go_atoi("4x").unwrap_err().0, "strconv.Atoi: parsing \"4x\": invalid syntax");
    }

    #[test]
    fn sqlite_path_comes_from_the_sqlite_section() {
        let mut c = Config::new();
        let unknown = toml_config::decode(&mut c, "[sqlite]\npath = \"/var/lib/cabby/cabby.db\"\n").unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!("/var/lib/cabby/cabby.db", c.sqlite.path);
    }

    #[test]
    fn printed_configuration_has_a_sqlite_section_that_reads_back() {
        assert!(Config::new().to_toml().contains(
            "# SQLite broker configuration (--broker=sqlite)\n[sqlite]\n\
             # Database file for --broker=sqlite (created with mode 0600 in a 0700 directory)\n\
             # path = \"/var/lib/cabby/cabby.db\"\n\n"
        ));

        let mut c = Config::new();
        c.sqlite.path = "/data/cabby.db".into();
        let mut back = Config::new();
        let unknown = toml_config::decode(&mut back, &c.to_toml()).unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!("/data/cabby.db", back.sqlite.path);
    }

    #[test]
    fn redis_broker_settings_come_from_the_redis_broker_section() {
        let mut c = Config::new();
        assert_eq!(crate::broker::RedisBrokerConfig::new(), c.redis_broker);
        let unknown = toml_config::decode(&mut c, "[redis_broker]\nkey_prefix = \"app\"\ntimeout = 2\n").unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!(("app", 2), (c.redis_broker.key_prefix.as_str(), c.redis_broker.timeout));
    }

    #[test]
    fn printed_configuration_has_a_redis_broker_section_that_reads_back() {
        assert!(Config::new().to_toml().contains(
            "# Redis broker configuration (--broker=redis)\n[redis_broker]\n\
             # Prefix of the keys the Redis broker writes (--broker=redis)\nkey_prefix = \"__cabby__\"\n"
        ));
        let mut c = Config::new();
        c.redis_broker.key_prefix = "app:prod".into();
        c.redis_broker.timeout = 7;
        let mut back = Config::new();
        let unknown = toml_config::decode(&mut back, &c.to_toml()).unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!(c.redis_broker, back.redis_broker);
    }

    /// Pins D-069: `--presets=broker` keeps an explicit Redis broker.
    #[test]
    fn broker_preset_keeps_an_explicit_redis_broker() {
        let mut c = Config::new();
        c.user_presets = Some(vec!["broker".to_owned()]);
        c.broker.adapter = "redis".to_owned();
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("redis", c.broker.adapter);
    }

    /// Pins D-065: `--presets=broker` only fills in a default broker.
    #[test]
    fn broker_preset_keeps_an_explicit_sqlite_broker() {
        let mut c = Config::new();
        c.user_presets = Some(vec!["broker".to_owned()]);
        c.broker.adapter = "sqlite".to_owned();
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("sqlite", c.broker.adapter);

        let mut d = Config::new();
        d.user_presets = Some(vec!["broker".to_owned()]);
        d.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("memory", d.broker.adapter, "the preset still picks a broker when none was chosen");
    }

    /// `--print-config` output loads back to the configuration it was printed from.
    mod print_config {
        use cabby_core::toml_config::TomlSection;

        use super::*;

        const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/print_config");

        fn fixture(name: &str) -> String {
            std::fs::read_to_string(format!("{FIXTURES}/{name}")).unwrap_or_else(|e| panic!("{name}: {e}"))
        }

        /// The default configuration prints what Cabby 0.4.1 printed, plus the
        /// `[redis]` `sentinel_tls` setting (D-115).
        #[test]
        fn the_default_configuration_prints_as_before() {
            assert_eq!(Config::new().to_toml(), fixture("default.expected.toml"));
        }

        /// A configuration with plain values in every string setting prints what
        /// Cabby 0.4.1 printed, plus the `[redis]` `sentinel_tls` setting (D-115).
        #[test]
        fn plain_values_print_as_before() {
            let mut c = Config::new();
            c.load_from_str(&fixture("plain_values.toml")).unwrap();
            assert!(c.unknown_config_keys.is_empty(), "{:?}", c.unknown_config_keys);
            assert_eq!(c.to_toml(), fixture("plain_values.expected.toml"));
        }

        /// Pins D-115: `[redis]` `sentinel_tls` prints commented out when off and
        /// as set when on, and loads back over a default configuration; the
        /// adapters' own Redis sections read it too.
        #[test]
        fn deviation_d115_sentinel_tls_loads_back() {
            for value in [false, true] {
                let mut c = Config::new();
                c.redis.sentinel_tls = value;
                let printed = c.to_toml();
                let line = if value { "\nsentinel_tls = true\n" } else { "\n# sentinel_tls = true\n" };
                assert!(printed.contains(line), "{printed}");
                let mut back = Config::new();
                back.load_from_str(&printed).unwrap();
                assert!(back.unknown_config_keys.is_empty(), "{:?}", back.unknown_config_keys);
                assert_eq!(c.redis, back.redis);
            }

            let mut c = Config::new();
            c.load_from_str(
                "[redis_pubsub.Redis]\nsentinel_tls = true\n\
                 [redis_stream_broadcast.redis]\nsentinel_tls = true\n\
                 [redis_pubsub_broadcast.redis]\nsentinel_tls = true\n",
            )
            .unwrap();
            assert!(c.unknown_config_keys.is_empty(), "{:?}", c.unknown_config_keys);
            let adapters = [&c.redis_pubsub.redis, &c.redis_broadcast.redis, &c.legacy_redis_broadcast.redis];
            assert!(adapters.iter().all(|r| r.as_ref().is_some_and(|r| r.sentinel_tls)), "{adapters:?}");
            assert!(!c.redis.sentinel_tls);
        }

        /// The sections of the configuration file and their fields (the loader's
        /// field tables), by their path in the file.
        fn sections() -> Vec<(&'static str, &'static [&'static str])> {
            vec![
                ("", Config::FIELDS),
                ("rpc", RpcConfig::FIELDS),
                ("logging", cabby_core::logger::Config::FIELDS),
                ("server", crate::server::Config::FIELDS),
                ("server.ssl", crate::server::SslConfig::FIELDS),
                ("app", AppConfig::FIELDS),
                ("ws", crate::ws::Config::FIELDS),
                ("broker", BrokerConfig::FIELDS),
                ("sqlite", SqliteConfig::FIELDS),
                ("redis_broker", crate::broker::RedisBrokerConfig::FIELDS),
                ("postgres", crate::postgres::PostgresConfig::FIELDS),
                ("redis", RedisConfig::FIELDS),
                ("redis_pubsub_broadcast", crate::broadcast::LegacyRedisConfig::FIELDS),
                ("redis_pubsub_broadcast.redis", RedisConfig::FIELDS),
                ("redis_stream_broadcast", crate::broadcast::RedisConfig::FIELDS),
                ("redis_stream_broadcast.redis", RedisConfig::FIELDS),
                ("nats_broadcast", crate::broadcast::LegacyNatsConfig::FIELDS),
                ("nats_broadcast.nats", crate::nats::NatsConfig::FIELDS),
                ("http_broadcast", HttpBroadcastConfig::FIELDS),
                ("redis_pubsub", crate::pubsub::RedisConfig::FIELDS),
                ("redis_pubsub.Redis", RedisConfig::FIELDS),
                ("nats_pubsub", crate::pubsub::NatsConfig::FIELDS),
                ("nats_pubsub.NATS", crate::nats::NatsConfig::FIELDS),
                ("nats", crate::nats::NatsConfig::FIELDS),
                ("disconnector", crate::node::DisconnectQueueConfig::FIELDS),
                ("metrics", MetricsConfig::FIELDS),
                ("metrics.statsd", StatsdConfig::FIELDS),
                ("jwt", cabby_core::identity::JwtConfig::FIELDS),
                ("embedded_nats", EmbeddedNatsConfig::FIELDS),
                ("sse", SseConfig::FIELDS),
                ("streams", cabby_core::streams::Config::FIELDS),
                ("pusher", crate::pusher::Config::FIELDS),
                ("ds", DsConfig::FIELDS),
                ("api", crate::api::Config::FIELDS),
            ]
        }

        /// Fields that load back as another value for a reason other than quoting:
        /// (field, cases, reason). A field ending in `.*` covers a section. Output
        /// that does not load, or that has another number of lines, always fails.
        const NOT_ROUND_TRIPPED: &[(&str, &[&str], &str)] = &[
            ("ConfigFilePath", &["*"], "not printed: the path of the file itself"),
            ("secret", &["empty"], "printed as upstream prints it"),
            ("broadcast_key", &["empty"], "printed as upstream prints it"),
            ("broadcast_adapters", &["empty list"], "an empty list prints as [\"\"], as upstream writes it"),
            ("presets", &["empty list"], "an empty list prints the commented-out example (presets are detected)"),
            ("ws.paths", &["empty list"], "an empty list prints as [\"\"], as upstream writes it"),
            ("embedded_nats.cluster_name", &["*"], "printed commented out while cluster_addr is empty"),
            ("metrics.log_filter", &["empty list"], "an empty filter prints commented out, which loads as no filter"),
            ("metrics.LogFormatter", &["*"], "not printed"),
            ("metrics.statsd.prefix", &["*"], "printed commented out while statsd.host is empty"),
            ("metrics.statsd.tags_format", &["*"], "printed commented out while statsd.host is empty"),
            ("http_broadcast.SecretBase", &["*"], "not printed"),
            ("http_broadcast.cors_hosts", &["*"], "not printed"),
            ("api.SecretBase", &["*"], "not printed"),
            ("api.cors_hosts", &["*"], "not printed"),
            ("pusher.CORSHosts", &["*"], "not printed"),
            ("rpc.SecretBase", &["*"], "not printed"),
            ("redis_pubsub_broadcast.redis.*", &["*"], "an adapter's own Redis settings are not printed"),
            ("redis_stream_broadcast.redis.*", &["*"], "an adapter's own Redis settings are not printed"),
            ("redis_pubsub.Redis.*", &["*"], "an adapter's own Redis settings are not printed"),
            ("nats_broadcast.nats.*", &["*"], "an adapter's own NATS settings are not printed"),
            ("nats_pubsub.NATS.*", &["*"], "an adapter's own NATS settings are not printed"),
        ];

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum Kind {
            String,
            List,
            Map,
            Section,
            Other,
        }

        fn document(section: &str, line: &str) -> String {
            if section.is_empty() { format!("{line}\n") } else { format!("[{section}]\n{line}\n") }
        }

        /// The kind of a field, from what the loader accepts for it.
        fn kind(section: &str, key: &str) -> Kind {
            let accepts = |value: &str| {
                let mut c = Config::new();
                c.load_from_str(&document(section, &format!("{key} = {value}"))).is_ok().then_some(c)
            };
            if accepts("\"x\"").is_some() {
                Kind::String
            } else if accepts("[\"x\"]").is_some() {
                Kind::List
            } else if let Some(c) = accepts("{ zz = \"x\" }") {
                if c.unknown_config_keys.is_empty() { Kind::Map } else { Kind::Section }
            } else {
                Kind::Other
            }
        }

        /// A TOML string with every character as a `\U` escape: independent of the
        /// printer's quoting.
        fn encode(value: &str) -> String {
            let escaped: String = value.chars().map(|c| format!("\\U{:08X}", c as u32)).collect();
            format!("\"{escaped}\"")
        }

        const VALUES: &[(&str, &str)] = &[
            ("plain", "value"),
            ("empty", ""),
            ("quote", "q\"x"),
            ("backslash", "b\\x"),
            ("newline", "n\nx"),
            ("tab", "a\tb"),
            ("bell", "\u{7}"),
            ("delete", "\u{7f}"),
            ("hash", "a#b"),
            ("equals", "a=b"),
            ("comma", "a,b"),
            ("e-acute", "é"),
            ("cjk", "日本"),
            ("nbsp", "\u{a0}"),
        ];

        /// The cases for a field: a label and the TOML value to set.
        fn cases(kind: Kind) -> Vec<(String, String)> {
            let list = |items: &[&str]| format!("[{}]", items.iter().map(|i| encode(i)).collect::<Vec<_>>().join(", "));
            match kind {
                Kind::String => VALUES.iter().map(|(label, v)| ((*label).to_owned(), encode(v))).collect(),
                Kind::List => VALUES
                    .iter()
                    .map(|(label, v)| ((*label).to_owned(), list(&[v])))
                    .chain([
                        ("two items".to_owned(), list(&["a,b", "c"])),
                        ("three items".to_owned(), list(&["x", "q\"x", "n\nx"])),
                        ("empty list".to_owned(), "[]".to_owned()),
                    ])
                    .collect(),
                Kind::Map => VALUES
                    .iter()
                    .map(|(label, v)| (format!("value {label}"), format!("{{ k = {} }}", encode(v))))
                    .chain(
                        [
                            ("key with space", "a b"),
                            ("key with dot", "a.b"),
                            ("empty key", ""),
                            ("key with quote", "q\"x"),
                        ]
                        .into_iter()
                        .chain([("key with newline", "n\nx"), ("non-ASCII key", "é"), ("bare key", "region-1_a")])
                        .map(|(label, k)| (label.to_owned(), format!("{{ {} = \"v\" }}", encode(k)))),
                    )
                    .chain([("two keys".to_owned(), TWO_KEYS.to_owned())])
                    .collect(),
                Kind::Section | Kind::Other => Vec::new(),
            }
        }

        const TWO_KEYS: &str = "{ \"a.b\" = \"1\", \"c d\" = \"2\" }";

        /// A plain value of the same shape as a case's: its output has as many lines.
        fn plain_of_shape(kind: Kind, value: &str) -> String {
            match kind {
                Kind::List => "[\"value\"]".to_owned(),
                Kind::Map if value == TWO_KEYS => "{ a = \"1\", b = \"2\" }".to_owned(),
                Kind::Map => "{ k = \"value\" }".to_owned(),
                _ => "\"value\"".to_owned(),
            }
        }

        fn allowed(path: &str, case: &str) -> Option<usize> {
            NOT_ROUND_TRIPPED.iter().position(|(field, cases, _)| {
                let matches_field = match field.strip_suffix(".*") {
                    Some(section) => path.starts_with(&format!("{section}.")),
                    None => *field == path,
                };
                matches_field && (cases.contains(&"*") || cases.contains(&case))
            })
        }

        /// Pins D-102: every string, string-list and map field of every section the
        /// loader knows, set to values with characters TOML escapes, prints with `to_toml`
        /// and loads back over a fresh default configuration to the same value. The
        /// output keeps its number of lines, so a commented-out setting stays one
        /// line.
        #[test]
        fn deviation_d102_every_string_setting_loads_back() {
            let sections = sections();
            let base = Config::new();
            let mut failures = Vec::new();
            let mut used = vec![false; NOT_ROUND_TRIPPED.len()];
            let mut checked = 0;

            for (section, fields) in &sections {
                for key in *fields {
                    let path = if section.is_empty() { (*key).to_owned() } else { format!("{section}.{key}") };
                    let kind = kind(section, key);
                    if kind == Kind::Section {
                        assert!(sections.iter().any(|(s, _)| *s == path), "{path}: a section missing from sections()");
                        continue;
                    }
                    let print = |value: &str| {
                        let set_document = document(section, &format!("{key} = {value}"));
                        let mut set = base.clone();
                        set.load_from_str(&set_document).unwrap_or_else(|e| panic!("{path} = {value}: {e}"));
                        (set.to_toml(), set_document)
                    };
                    for (case, value) in cases(kind) {
                        let (printed, set_document) = print(&value);
                        let lines = printed.lines().count();
                        let plain = print(&plain_of_shape(kind, &value)).0.lines().count();

                        let fail = |problem: String| format!("{path} ({case}): {problem}");
                        if lines != plain {
                            failures.push(fail(format!("prints {lines} lines instead of {plain}")));
                        }
                        checked += 1;
                        let mut loaded = base.clone();
                        match loaded.load_from_str(&printed) {
                            Err(e) => failures.push(fail(format!("does not load: {e}"))),
                            Ok(()) if !loaded.unknown_config_keys.is_empty() => {
                                failures.push(fail(format!("unknown keys {:?}", loaded.unknown_config_keys)));
                            }
                            Ok(()) => {
                                // Setting the value again changes nothing when it loaded back.
                                let mut again = loaded.clone();
                                again.load_from_str(&set_document).unwrap();
                                if again != loaded {
                                    match allowed(&path, &case) {
                                        Some(entry) => used[entry] = true,
                                        None => failures.push(fail("loads back as another value".to_owned())),
                                    }
                                }
                            }
                        }
                    }
                }
            }

            assert!(checked > 500, "only {checked} cases");
            assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
            let unused: Vec<_> = NOT_ROUND_TRIPPED.iter().zip(&used).filter(|(_, u)| !**u).map(|(e, _)| e.0).collect();
            assert!(unused.is_empty(), "entries of NOT_ROUND_TRIPPED that load back: {unused:?}");
        }
    }
}
