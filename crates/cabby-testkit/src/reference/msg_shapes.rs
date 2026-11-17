//! Go JSON shapes shared by the hub/pubsub/redis/broadcast/nats/enats facades.
//!
//! Go structs without `json` tags marshal with their exported field names; the
//! `common` message types use their `json` tags. These conversions only move
//! fields; they carry no behavior.

use cabby_core::common::{RemoteCommandMessage, StreamMessage, StreamMessageMetadata};
use cabby_server::nats::NatsConfig;
use cabby_server::redis::RedisConfig;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

use crate::bridge;

/// A Go `(result, error)` pair as returned by the bridge.
#[derive(Deserialize)]
pub(crate) struct GoParsed<T> {
    pub result: Option<T>,
    pub error: Option<String>,
}

impl<T> GoParsed<T> {
    pub fn into_result(self) -> Result<Option<T>, String> {
        match self.error {
            Some(e) => Err(e),
            None => Ok(self.result),
        }
    }
}

/// Calls an op returning `{result: null, error}` (Go functions returning `error`).
pub(crate) fn go_unit(op: &str, args: Value) -> Result<(), String> {
    let p: GoParsed<Value> = bridge::call(op, args);
    p.into_result().map(|_| ())
}

/// Go `[]string`: `nil` marshals as `null`. Empty Rust vectors go out as `null`
/// so the Go side sees its zero value.
pub(crate) fn ser_go_strings<S: Serializer>(v: &[String], s: S) -> Result<S::Ok, S::Error> {
    if v.is_empty() { s.serialize_none() } else { v.serialize(s) }
}

pub(crate) fn de_go_strings<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

// --- common.StreamMessage / RemoteCommandMessage / RemoteDisconnectMessage ---

pub(crate) fn stream_message_json(m: &StreamMessage) -> Value {
    let mut v = json!({"stream": m.stream, "data": m.data});
    if let Some(meta) = &m.meta {
        v["meta"] = json!({"exclude_socket": meta.exclude_socket, "broadcast_type": meta.broadcast_type,
                           "transient": meta.transient});
    }
    if m.offset != 0 {
        v["offset"] = json!(m.offset);
    }
    if !m.epoch.is_empty() {
        v["epoch"] = json!(m.epoch);
    }
    v
}

#[derive(Deserialize)]
pub(crate) struct GoStreamMessage {
    #[serde(default)]
    stream: String,
    #[serde(default)]
    data: String,
    #[serde(default)]
    meta: Option<GoStreamMeta>,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    epoch: String,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct GoStreamMeta {
    exclude_socket: String,
    broadcast_type: String,
    transient: bool,
}

impl From<GoStreamMessage> for StreamMessage {
    fn from(g: GoStreamMessage) -> Self {
        StreamMessage {
            stream: g.stream,
            data: g.data,
            meta: g.meta.map(|m| StreamMessageMetadata {
                exclude_socket: m.exclude_socket,
                broadcast_type: m.broadcast_type,
                transient: m.transient,
            }),
            offset: g.offset,
            epoch: g.epoch,
        }
    }
}

pub(crate) fn remote_command_json(c: &RemoteCommandMessage) -> Value {
    let mut v = json!({"command": c.command});
    if !c.payload.is_empty() {
        v["payload"] = serde_json::from_str(&c.payload).expect("RemoteCommandMessage payload must be JSON");
    }
    v
}

#[derive(Deserialize)]
pub(crate) struct GoRemoteCommand {
    #[serde(default)]
    command: String,
    #[serde(default)]
    payload: Option<Value>,
}

impl From<GoRemoteCommand> for RemoteCommandMessage {
    fn from(g: GoRemoteCommand) -> Self {
        RemoteCommandMessage {
            command: g.command,
            payload: g.payload.map(|p| serde_json::to_string(&p).expect("json")).unwrap_or_default(),
        }
    }
}

// --- redis.RedisConfig (exported fields) ---

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct GoRedisConfig {
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "Sentinels")]
    sentinels: String,
    #[serde(rename = "SentinelDiscoveryInterval")]
    sentinel_discovery_interval: i64,
    #[serde(rename = "KeepalivePingInterval")]
    keepalive_ping_interval: i64,
    #[serde(rename = "TLSVerify")]
    tls_verify: bool,
    #[serde(rename = "TLSCACertPath")]
    tls_ca_cert_path: String,
    #[serde(rename = "TLSClientCertPath")]
    tls_client_cert_path: String,
    #[serde(rename = "TLSClientKeyPath")]
    tls_client_key_path: String,
    #[serde(rename = "MaxReconnectAttempts")]
    max_reconnect_attempts: i64,
    #[serde(rename = "DisableCache")]
    disable_cache: bool,
}

impl From<&RedisConfig> for GoRedisConfig {
    fn from(c: &RedisConfig) -> Self {
        GoRedisConfig {
            url: c.url.clone(),
            sentinels: c.sentinels.clone(),
            sentinel_discovery_interval: c.sentinel_discovery_interval,
            keepalive_ping_interval: c.keepalive_ping_interval,
            tls_verify: c.tls_verify,
            tls_ca_cert_path: c.tls_ca_cert_path.clone(),
            tls_client_cert_path: c.tls_client_cert_path.clone(),
            tls_client_key_path: c.tls_client_key_path.clone(),
            max_reconnect_attempts: c.max_reconnect_attempts,
            disable_cache: c.disable_cache,
        }
    }
}

impl From<GoRedisConfig> for RedisConfig {
    fn from(g: GoRedisConfig) -> Self {
        let mut c = RedisConfig::default();
        c.url = g.url;
        c.sentinels = g.sentinels;
        c.sentinel_discovery_interval = g.sentinel_discovery_interval;
        c.keepalive_ping_interval = g.keepalive_ping_interval;
        c.tls_verify = g.tls_verify;
        c.tls_ca_cert_path = g.tls_ca_cert_path;
        c.tls_client_cert_path = g.tls_client_cert_path;
        c.tls_client_key_path = g.tls_client_key_path;
        c.max_reconnect_attempts = g.max_reconnect_attempts;
        c.disable_cache = g.disable_cache;
        c
    }
}

pub(crate) fn go_redis(c: &Option<RedisConfig>) -> Option<GoRedisConfig> {
    c.as_ref().map(GoRedisConfig::from)
}

// --- nats.NATSConfig ---

#[derive(Serialize, Deserialize, Default)]
pub(crate) struct GoNatsConfig {
    #[serde(rename = "Servers")]
    servers: String,
    #[serde(rename = "DontRandomizeServers")]
    dont_randomize_servers: bool,
    #[serde(rename = "MaxReconnectAttempts")]
    max_reconnect_attempts: i64,
}

impl From<&NatsConfig> for GoNatsConfig {
    fn from(c: &NatsConfig) -> Self {
        GoNatsConfig {
            servers: c.servers.clone(),
            dont_randomize_servers: c.dont_randomize_servers,
            max_reconnect_attempts: c.max_reconnect_attempts,
        }
    }
}

impl From<GoNatsConfig> for NatsConfig {
    fn from(g: GoNatsConfig) -> Self {
        NatsConfig {
            servers: g.servers,
            dont_randomize_servers: g.dont_randomize_servers,
            max_reconnect_attempts: g.max_reconnect_attempts,
        }
    }
}

pub(crate) fn go_nats(c: &Option<NatsConfig>) -> Option<GoNatsConfig> {
    c.as_ref().map(GoNatsConfig::from)
}

// --- config ops registered by `msgConfigOps` in bridge/msg_handles.go ---

/// `<prefix>New`: the upstream constructor's defaults.
pub(crate) fn go_config_new<G: DeserializeOwned>(prefix: &str) -> G {
    bridge::call(&format!("{prefix}New"), json!({}))
}

/// `<prefix>ToToml`.
pub(crate) fn go_config_to_toml<G: Serialize>(prefix: &str, config: &G) -> String {
    bridge::call(&format!("{prefix}ToToml"), json!({"Config": config}))
}

/// `<prefix>DecodeToml`: `toml.Decode(toml, &config)`.
pub(crate) fn go_config_decode_toml<G: Serialize + DeserializeOwned>(
    prefix: &str,
    config: &G,
    toml: &str,
) -> Result<G, String> {
    let p: GoParsed<G> = bridge::call(&format!("{prefix}DecodeToml"), json!({"Config": config, "Toml": toml}));
    p.into_result().map(|r| r.expect("decoded config"))
}
