//! Facade for upstream package `config`.
//!
//! The bridge exchanges `configView`, the subset of `config.Config` that Cabby
//! models; inputs are applied over a fresh `config.NewConfig()`. Presets read the
//! process environment, so those calls run in a bridge process whose environment is
//! exactly the given [`EnvMap`].

use std::collections::HashMap;
use std::path::PathBuf;

use cabby_server::config::{
    AppConfig, BrokerConfig, Config, ConfigError, DsConfig, EmbeddedNatsConfig, EnvMap, HttpBroadcastConfig,
    MetricsConfig, RedisConfig, RpcConfig, SseConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::reference::common::GoParsed;
use crate::reference::logger::GoLoggerConfig;
use crate::reference::server::GoServerConfig;
use crate::reference::ws::GoWsConfig;
use crate::{Backend, backend, bridge};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoConfigView {
    secret: String,
    broadcast_adapters: Option<Vec<String>>,
    pub_sub_adapter: String,
    user_presets: Option<Vec<String>>,
    disconnector_disabled: bool,
    log: GoLoggerConfig,
    server: GoServerConfig,
    #[serde(rename = "WS")]
    ws: GoWsConfig,
    app: GoApp,
    #[serde(rename = "RPC")]
    rpc: GoRpc,
    broker: GoBroker,
    redis: GoRedis,
    #[serde(rename = "HTTPBroadcast")]
    http_broadcast: GoPort,
    metrics: GoMetrics,
    embedded_nats: GoEnats,
    #[serde(rename = "SSE")]
    sse: GoSse,
    #[serde(rename = "DS")]
    ds: GoDs,
    config_file_path: String,
    unknown_config_keys: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoApp {
    ping_interval: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoRpc {
    host: String,
    implementation: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoBroker {
    adapter: String,
}

#[derive(Serialize, Deserialize)]
struct GoRedis {
    #[serde(rename = "URL")]
    url: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoPort {
    port: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoMetrics {
    #[serde(rename = "HTTP")]
    http: String,
    tags: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoEnats {
    enabled: bool,
    name: String,
    service_addr: String,
    cluster_addr: String,
    cluster_name: String,
    routes: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoSse {
    enabled: bool,
    comment_pings: bool,
    ping_interval: i64,
}

#[derive(Serialize, Deserialize)]
struct GoDs {
    #[serde(rename = "SSEPingInterval")]
    sse_ping_interval: i64,
}

impl From<&Config> for GoConfigView {
    fn from(c: &Config) -> Self {
        GoConfigView {
            secret: c.secret.clone(),
            broadcast_adapters: Some(c.broadcast_adapters.clone()),
            pub_sub_adapter: c.pubsub_adapter.clone(),
            user_presets: c.user_presets.clone(),
            disconnector_disabled: c.disconnector_disabled,
            log: (&c.log).into(),
            server: (&c.server).into(),
            ws: (&c.ws).into(),
            app: GoApp { ping_interval: c.app.ping_interval },
            rpc: GoRpc { host: c.rpc.host.clone(), implementation: c.rpc.implementation.clone() },
            broker: GoBroker { adapter: c.broker.adapter.clone() },
            redis: GoRedis { url: c.redis.url.clone() },
            http_broadcast: GoPort { port: c.http_broadcast.port },
            metrics: GoMetrics {
                http: c.metrics.http.clone(),
                tags: (!c.metrics.tags.is_empty()).then(|| c.metrics.tags.clone()),
            },
            embedded_nats: GoEnats {
                enabled: c.embedded_nats.enabled,
                name: c.embedded_nats.name.clone(),
                service_addr: c.embedded_nats.service_addr.clone(),
                cluster_addr: c.embedded_nats.cluster_addr.clone(),
                cluster_name: c.embedded_nats.cluster_name.clone(),
                routes: (!c.embedded_nats.routes.is_empty()).then(|| c.embedded_nats.routes.clone()),
            },
            sse: GoSse {
                enabled: c.sse.enabled,
                comment_pings: c.sse.comment_pings,
                ping_interval: c.sse.ping_interval,
            },
            ds: GoDs { sse_ping_interval: c.ds.sse_ping_interval },
            config_file_path: c.config_file_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            unknown_config_keys: (!c.unknown_config_keys.is_empty()).then(|| c.unknown_config_keys.clone()),
        }
    }
}

impl From<GoConfigView> for Config {
    fn from(g: GoConfigView) -> Self {
        Config {
            secret: g.secret,
            broadcast_adapters: g.broadcast_adapters.unwrap_or_default(),
            pubsub_adapter: g.pub_sub_adapter,
            user_presets: g.user_presets,
            disconnector_disabled: g.disconnector_disabled,
            log: g.log.into(),
            server: g.server.into(),
            app: AppConfig { ping_interval: g.app.ping_interval, ..Default::default() },
            ws: g.ws.into(),
            rpc: RpcConfig { host: g.rpc.host, implementation: g.rpc.implementation },
            broker: BrokerConfig { adapter: g.broker.adapter },
            redis: RedisConfig { url: g.redis.url },
            http_broadcast: HttpBroadcastConfig { port: g.http_broadcast.port },
            metrics: MetricsConfig { http: g.metrics.http, tags: g.metrics.tags.unwrap_or_default() },
            embedded_nats: EmbeddedNatsConfig {
                enabled: g.embedded_nats.enabled,
                name: g.embedded_nats.name,
                service_addr: g.embedded_nats.service_addr,
                cluster_addr: g.embedded_nats.cluster_addr,
                cluster_name: g.embedded_nats.cluster_name,
                routes: g.embedded_nats.routes.unwrap_or_default(),
            },
            sse: SseConfig {
                enabled: g.sse.enabled,
                comment_pings: g.sse.comment_pings,
                ping_interval: g.sse.ping_interval,
            },
            ds: DsConfig { sse_ping_interval: g.ds.sse_ping_interval },
            config_file_path: (!g.config_file_path.is_empty()).then(|| PathBuf::from(g.config_file_path)),
            unknown_config_keys: g.unknown_config_keys.unwrap_or_default(),
        }
    }
}

fn env_pairs(env: &EnvMap) -> Vec<(&str, &str)> {
    env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
}

/// `config.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoConfigView>("config.NewConfig", json!({})).into(),
    }
}

/// `config.LoadFromFile()`.
pub fn load_from_file(c: &mut Config) -> Result<(), ConfigError> {
    match backend() {
        Backend::Rust => c.load_from_file(),
        Backend::Go => {
            let p: GoParsed<GoConfigView> =
                bridge::call("config.Config.LoadFromFile", json!({"Config": GoConfigView::from(&*c)}));
            *c = p.result.expect("config").into();
            p.error.map_or(Ok(()), |e| Err(ConfigError(e)))
        }
    }
}

/// `config.ToToml()`.
pub fn to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("config.Config.ToToml", json!({"Config": GoConfigView::from(c)})),
    }
}

/// `config.Presets()` with the process environment `env`. `None` is a nil Go slice
/// (kept distinct from an empty one; the Rust interface never returns nil).
pub fn presets(c: &Config, env: &EnvMap) -> Option<Vec<String>> {
    match backend() {
        Backend::Rust => Some(c.presets(env)),
        Backend::Go => {
            bridge::call_isolated(&env_pairs(env), "config.Config.Presets", json!({"Config": GoConfigView::from(c)}))
        }
    }
}

/// `config.LoadPresets(logger)` with the process environment `env`.
pub fn load_presets(c: &mut Config, env: &EnvMap) -> Result<(), ConfigError> {
    match backend() {
        Backend::Rust => c.load_presets(env),
        Backend::Go => {
            let p: GoParsed<GoConfigView> = bridge::call_isolated(
                &env_pairs(env),
                "config.Config.LoadPresets",
                json!({"Config": GoConfigView::from(&*c)}),
            );
            *c = p.result.expect("config").into();
            p.error.map_or(Ok(()), |e| Err(ConfigError(e)))
        }
    }
}
