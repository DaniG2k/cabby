//! Facade for upstream package `redis`.
//!
//! [`Config`] wraps a `cabby_server::redis::RedisConfig` (deref to set fields).
//! On the Go backend it also owns a bridge handle, because upstream methods
//! mutate unexported state (`ToRueidisOptions` records the resolved hosts that
//! `IsCluster`/`Hostname`/`Hostnames` read).

use std::collections::{HashMap, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use cabby_server::redis::{self as rs, ClientOptions, RedisConfig, SentinelOptions, TlsOptions};
use serde::Deserialize;
use serde_json::{Value, json};

use super::msg_shapes::{GoParsed, GoRedisConfig, go_unit};
use crate::{Backend, backend, bridge};

pub struct Config {
    cfg: RedisConfig,
    go: Option<i64>,
}

impl Deref for Config {
    type Target = RedisConfig;
    fn deref(&self) -> &RedisConfig {
        &self.cfg
    }
}

impl DerefMut for Config {
    fn deref_mut(&mut self) -> &mut RedisConfig {
        &mut self.cfg
    }
}

#[derive(Deserialize)]
struct GoNewConfig {
    #[serde(rename = "Handle")]
    handle: i64,
    #[serde(rename = "Config")]
    config: GoRedisConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoOptions {
    init_address: Option<Vec<String>>,
    #[serde(rename = "SelectDB")]
    select_db: i64,
    username: String,
    password: String,
    keep_alive_ns: u64,
    dial_timeout_ns: u64,
    shuffle_init: bool,
    #[serde(rename = "TLS")]
    tls: Option<GoTls>,
    sentinel: GoSentinel,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoTls {
    insecure_skip_verify: bool,
    certificates: usize,
    #[serde(rename = "RootCAs")]
    root_cas: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoSentinel {
    master_set: String,
    username: String,
    password: String,
}

impl From<GoOptions> for ClientOptions {
    fn from(g: GoOptions) -> Self {
        ClientOptions {
            init_address: g.init_address.unwrap_or_default(),
            select_db: g.select_db,
            username: g.username,
            password: g.password,
            keep_alive: Duration::from_nanos(g.keep_alive_ns),
            dial_timeout: Duration::from_nanos(g.dial_timeout_ns),
            shuffle_init: g.shuffle_init,
            tls: g.tls.map(|t| TlsOptions {
                insecure_skip_verify: t.insecure_skip_verify,
                certificates: t.certificates,
                root_cas: t.root_cas,
            }),
            sentinel: SentinelOptions {
                master_set: g.sentinel.master_set,
                username: g.sentinel.username,
                password: g.sentinel.password,
            },
        }
    }
}

impl Config {
    /// Upstream `NewRedisConfig()`.
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Self { cfg: RedisConfig::new(), go: None },
            Backend::Go => {
                let g: GoNewConfig = bridge::call("redis.NewRedisConfig", json!({}));
                Self { cfg: g.config.into(), go: Some(g.handle) }
            }
        }
    }

    /// The plain config (e.g. to embed in another package's config).
    pub fn config(&self) -> RedisConfig {
        self.cfg.clone()
    }

    fn call<R: serde::de::DeserializeOwned>(&self, method: &str) -> R {
        bridge::call(
            "redis.ConfigCall",
            json!({"Handle": self.go.expect("Go config handle"), "Config": GoRedisConfig::from(&self.cfg),
                   "Method": method}),
        )
    }

    /// Upstream `ToRueidisOptions()`.
    pub fn to_rueidis_options(&mut self) -> Result<ClientOptions, String> {
        match backend() {
            Backend::Rust => self.cfg.to_client_options().map_err(|e| e.0),
            Backend::Go => {
                let p: GoParsed<GoOptions> = self.call("ToRueidisOptions");
                p.into_result().map(|o| o.expect("options").into())
            }
        }
    }

    pub fn is_cluster(&self) -> bool {
        match backend() {
            Backend::Rust => self.cfg.is_cluster(),
            Backend::Go => self.call("IsCluster"),
        }
    }

    pub fn is_sentinel(&self) -> bool {
        match backend() {
            Backend::Rust => self.cfg.is_sentinel(),
            Backend::Go => self.call("IsSentinel"),
        }
    }

    pub fn hostname(&self) -> String {
        match backend() {
            Backend::Rust => self.cfg.hostname(),
            Backend::Go => self.call("Hostname"),
        }
    }

    pub fn hostnames(&self) -> Vec<String> {
        match backend() {
            Backend::Rust => self.cfg.hostnames(),
            Backend::Go => self.call::<Option<Vec<String>>>("Hostnames").unwrap_or_default(),
        }
    }

    pub fn tls_client_cert_available(&self) -> bool {
        match backend() {
            Backend::Rust => self.cfg.tls_client_cert_available(),
            Backend::Go => self.call("TLSClientCertAvailable"),
        }
    }

    pub fn to_toml(&self) -> String {
        match backend() {
            Backend::Rust => self.cfg.to_toml(),
            Backend::Go => self.call("ToToml"),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// `toml.Decode(toml, &config)` on a copy of `config`, returning the result.
pub fn decode_toml(config: &RedisConfig, toml: &str) -> Result<RedisConfig, String> {
    match backend() {
        Backend::Rust => {
            let mut c = config.clone();
            c.decode_toml(toml).map(|()| c).map_err(|e| e.0)
        }
        Backend::Go => {
            let p: GoParsed<GoRedisConfig> = bridge::call(
                "redis.RedisConfigDecodeToml",
                json!({"Config": GoRedisConfig::from(config), "Toml": toml}),
            );
            p.into_result().map(|c| c.expect("config").into())
        }
    }
}

// ------------------------------------------------------------ streamer ---

type Entry = HashMap<String, String>;

struct RustCollector {
    received: Mutex<VecDeque<Entry>>,
    ready: Condvar,
    /// Test hook: close the streamer's client when an entry with this payload
    /// is handled for the first time.
    close_on: Mutex<Option<(String, Weak<rs::Streamer>)>>,
}

enum CollectorImpl {
    Rust(Arc<RustCollector>),
    Go(i64),
}

/// The stream handler of upstream streamer tests: every entry goes to a
/// `received` channel (capacity 10 upstream).
pub struct StreamCollector {
    inner: CollectorImpl,
}

impl StreamCollector {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => Self {
                inner: CollectorImpl::Rust(Arc::new(RustCollector {
                    received: Mutex::new(VecDeque::new()),
                    ready: Condvar::new(),
                    close_on: Mutex::new(None),
                })),
            },
            Backend::Go => Self { inner: CollectorImpl::Go(bridge::call("redis.NewStreamHandler", json!({}))) },
        }
    }

    fn handler(&self) -> rs::StreamHandler {
        let CollectorImpl::Rust(c) = &self.inner else { unreachable!() };
        let c = Arc::clone(c);
        Arc::new(move |msg: Entry| {
            let close = {
                let mut hook = c.close_on.lock().unwrap_or_else(|e| e.into_inner());
                match hook.as_ref() {
                    Some((payload, _)) if msg.get("payload") == Some(payload) => hook.take().map(|(_, s)| s),
                    _ => None,
                }
            };
            c.received.lock().unwrap_or_else(|e| e.into_inner()).push_back(msg);
            c.ready.notify_all();
            if let Some(streamer) = close.and_then(|w| w.upgrade()) {
                streamer.close_client_without_reconnect();
            }
            Ok(())
        })
    }

    /// Upstream `drainStream(t, received, count, timeout)`: entries until `count`
    /// arrived or none arrived for `timeout`.
    pub fn drain(&self, count: usize, timeout: Duration) -> Vec<Entry> {
        match &self.inner {
            CollectorImpl::Rust(c) => {
                let mut out = Vec::new();
                let mut q = c.received.lock().unwrap_or_else(|e| e.into_inner());
                while out.len() < count {
                    if let Some(m) = q.pop_front() {
                        out.push(m);
                        continue;
                    }
                    let (g, res) =
                        c.ready.wait_timeout_while(q, timeout, |q| q.is_empty()).unwrap_or_else(|e| e.into_inner());
                    q = g;
                    if res.timed_out() && q.is_empty() {
                        break;
                    }
                }
                out
            }
            CollectorImpl::Go(id) => bridge::call(
                "redis.StreamHandlerDrain",
                json!({"Handler": id, "Count": count, "TimeoutMs": timeout.as_millis() as u64}),
            ),
        }
    }

    /// Makes the handler close `streamer`'s Redis client (without reconnecting)
    /// the first time it handles an entry whose `payload` is `payload`, before
    /// the entry can be acknowledged. Upstream reaches into the streamer's
    /// unexported `client` and `reconnectAttempt` from its handler.
    pub fn close_client_on_payload(&self, streamer: &Streamer, payload: &str) {
        match (&self.inner, &streamer.inner) {
            (CollectorImpl::Rust(c), StreamerImpl::Rust(s)) => {
                *c.close_on.lock().unwrap_or_else(|e| e.into_inner()) = Some((payload.to_owned(), Arc::downgrade(s)));
            }
            _ => unimplemented!(
                "not_observable: closing Streamer.client and setting Streamer.reconnectAttempt from the handler \
                 requires unexported state"
            ),
        }
    }
}

impl Default for StreamCollector {
    fn default() -> Self {
        Self::new()
    }
}

enum StreamerImpl {
    Rust(Arc<rs::Streamer>),
    Go(i64),
}

/// Upstream `NewStreamer(stream, group, &config, l, StreamerWithHandler(h),
/// StreamerWithBlockMS(blockMs))`.
pub struct Streamer {
    inner: StreamerImpl,
}

impl Streamer {
    pub fn new(stream: &str, group: &str, config: &RedisConfig, handler: &StreamCollector, block_ms: i64) -> Self {
        match backend() {
            Backend::Rust => {
                let opts = rs::StreamerOptions {
                    block_ms: Some(block_ms),
                    consumer_name: None,
                    handler: Some(handler.handler()),
                };
                Self { inner: StreamerImpl::Rust(Arc::new(rs::Streamer::new(stream, group, config, opts))) }
            }
            Backend::Go => {
                let CollectorImpl::Go(h) = &handler.inner else { unreachable!() };
                Self {
                    inner: StreamerImpl::Go(bridge::call(
                        "redis.NewStreamer",
                        json!({"Stream": stream, "Group": group, "Config": GoRedisConfig::from(config),
                               "Handler": h, "BlockMs": block_ms}),
                    )),
                }
            }
        }
    }

    fn go(&self, op: &str, extra: Value) -> Value {
        let StreamerImpl::Go(id) = &self.inner else { unreachable!() };
        let mut args = json!({"Streamer": id});
        if let (Some(a), Some(e)) = (args.as_object_mut(), extra.as_object()) {
            a.extend(e.clone());
        }
        bridge::call(op, args)
    }

    pub fn start(&self) -> Result<(), String> {
        match &self.inner {
            StreamerImpl::Rust(s) => s.start().map_err(|e| e.0),
            StreamerImpl::Go(id) => go_unit("redis.StreamerStart", json!({"Streamer": id})),
        }
    }

    pub fn shutdown(&self) -> Result<(), String> {
        match &self.inner {
            StreamerImpl::Rust(s) => s.shutdown().map_err(|e| e.0),
            StreamerImpl::Go(id) => go_unit("redis.StreamerShutdown", json!({"Streamer": id})),
        }
    }

    /// `<-streamer.Ready()` with a deadline; false if it elapsed.
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        match &self.inner {
            StreamerImpl::Rust(s) => s.wait_ready(timeout),
            StreamerImpl::Go(_) => {
                self.go("redis.StreamerReady", json!({"TimeoutMs": timeout.as_millis() as u64})) == json!(true)
            }
        }
    }
}
