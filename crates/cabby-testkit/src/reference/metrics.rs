//! Facade for upstream package `metrics`.
//!
//! Go values (registries, counters, gauges, writers) live in the bridge and are
//! referred to by handle; every method is one bridge op calling the exported Go
//! method of the same name.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::StringMap;
use cabby_server::metrics as rs;
pub use cabby_server::metrics::{Config, MetricsError, StatsdConfig};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::runtime::{block_on_within, spawn};
use crate::sync::Watched;
use crate::{Backend, backend, bridge};

const AWAIT: Duration = Duration::from_secs(20);

#[derive(Deserialize)]
pub(crate) struct GoErr {
    pub text: String,
}

#[derive(Deserialize)]
struct HandleOrErr {
    handle: i64,
    error: Option<GoErr>,
}

#[derive(Deserialize)]
struct ErrOnly {
    error: Option<GoErr>,
}

// --- Config ------------------------------------------------------------------

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
struct GoStatsdConfig {
    host: String,
    prefix: String,
    tag_format: String,
    max_packet_size: i64,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    log: bool,
    log_interval: i64,
    rotate_interval: i64,
    log_formatter: String,
    log_filter: Option<Vec<String>>,
    #[serde(rename = "HTTP")]
    http: String,
    host: String,
    port: i64,
    tags: Option<StringMap>,
    statsd: GoStatsdConfig,
}

fn statsd_to_go(c: &StatsdConfig) -> GoStatsdConfig {
    GoStatsdConfig {
        host: c.host.clone(),
        prefix: c.prefix.clone(),
        tag_format: c.tag_format.clone(),
        max_packet_size: c.max_packet_size,
    }
}

fn statsd_from_go(g: GoStatsdConfig) -> StatsdConfig {
    StatsdConfig { host: g.host, prefix: g.prefix, tag_format: g.tag_format, max_packet_size: g.max_packet_size }
}

fn config_to_go(c: &Config) -> GoConfig {
    GoConfig {
        log: c.log,
        log_interval: c.log_interval,
        rotate_interval: c.rotate_interval,
        log_formatter: c.log_formatter.clone(),
        log_filter: c.log_filter.clone(),
        http: c.http.clone(),
        host: c.host.clone(),
        port: c.port,
        tags: c.tags.clone(),
        statsd: statsd_to_go(&c.statsd),
    }
}

fn config_from_go(g: GoConfig) -> Config {
    Config {
        log: g.log,
        log_interval: g.log_interval,
        rotate_interval: g.rotate_interval,
        log_formatter: g.log_formatter,
        log_filter: g.log_filter,
        http: g.http,
        host: g.host,
        port: g.port,
        tags: g.tags,
        statsd: statsd_from_go(g.statsd),
    }
}

fn config_op<T: serde::de::DeserializeOwned>(c: &Config, op: &str) -> T {
    bridge::call("metrics.Config", json!({"Config": config_to_go(c), "Op": op}))
}

/// `metrics.NewConfig()`
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => config_from_go(bridge::call("metrics.NewConfig", json!({}))),
    }
}

/// `metrics.NewStatsdConfig()`
pub fn new_statsd_config() -> StatsdConfig {
    match backend() {
        Backend::Rust => StatsdConfig::new(),
        Backend::Go => statsd_from_go(bridge::call("metrics.NewStatsdConfig", json!({}))),
    }
}

pub fn log_enabled(c: &Config) -> bool {
    match backend() {
        Backend::Rust => c.log_enabled(),
        Backend::Go => config_op(c, "LogEnabled"),
    }
}

pub fn http_enabled(c: &Config) -> bool {
    match backend() {
        Backend::Rust => c.http_enabled(),
        Backend::Go => config_op(c, "HTTPEnabled"),
    }
}

pub fn config_to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => config_op(c, "ToToml"),
    }
}

/// Decodes `toml` on top of `base` (Go: `toml.Decode(s, &base)`).
pub fn config_decode_toml(base: &Config, toml: &str) -> Result<Config, String> {
    match backend() {
        Backend::Rust => {
            let mut c = base.clone();
            c.decode_toml(toml).map(|()| c)
        }
        Backend::Go => {
            #[derive(Deserialize)]
            struct R {
                result: GoConfig,
                error: Option<GoErr>,
            }
            let r: R = bridge::call("metrics.Config.DecodeToml", json!({"Base": config_to_go(base), "Toml": toml}));
            match r.error {
                Some(e) => Err(e.text),
                None => Ok(config_from_go(r.result)),
            }
        }
    }
}

// --- Counter / Gauge -----------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Counter {
    Rust(Arc<rs::Counter>),
    Go(i64),
}

impl Counter {
    /// `metrics.NewCounter(name, desc)`
    pub fn new(name: &str, desc: &str) -> Self {
        match backend() {
            Backend::Rust => Counter::Rust(Arc::new(rs::Counter::new(name, desc))),
            Backend::Go => Counter::Go(bridge::call("metrics.NewCounter", json!({"Name": name, "Desc": desc}))),
        }
    }

    fn go<T: serde::de::DeserializeOwned>(h: i64, op: &str, n: u64) -> T {
        bridge::call("metrics.Counter", json!({"Handle": h, "Op": op, "N": n}))
    }

    pub fn name(&self) -> String {
        match self {
            Counter::Rust(c) => c.name().to_owned(),
            Counter::Go(h) => Self::go(*h, "Name", 0),
        }
    }

    pub fn value(&self) -> u64 {
        match self {
            Counter::Rust(c) => c.value(),
            Counter::Go(h) => Self::go(*h, "Value", 0),
        }
    }

    pub fn interval_value(&self) -> u64 {
        match self {
            Counter::Rust(c) => c.interval_value(),
            Counter::Go(h) => Self::go(*h, "IntervalValue", 0),
        }
    }

    pub fn inc(&self) -> u64 {
        match self {
            Counter::Rust(c) => c.inc(),
            Counter::Go(h) => Self::go(*h, "Inc", 0),
        }
    }

    pub fn add(&self, n: u64) -> u64 {
        match self {
            Counter::Rust(c) => c.add(n),
            Counter::Go(h) => Self::go(*h, "Add", n),
        }
    }

    pub fn update_delta(&self) {
        match self {
            Counter::Rust(c) => c.update_delta(),
            Counter::Go(h) => Self::go::<bool>(*h, "UpdateDelta", 0).then_some(()).expect("ack"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Gauge {
    Rust(Arc<rs::Gauge>),
    Go(i64),
}

impl Gauge {
    /// `metrics.NewGauge(name, desc)`
    pub fn new(name: &str, desc: &str) -> Self {
        match backend() {
            Backend::Rust => Gauge::Rust(Arc::new(rs::Gauge::new(name, desc))),
            Backend::Go => Gauge::Go(bridge::call("metrics.NewGauge", json!({"Name": name, "Desc": desc}))),
        }
    }

    fn go<T: serde::de::DeserializeOwned>(h: i64, op: &str, int: i64, u64v: u64) -> T {
        bridge::call("metrics.Gauge", json!({"Handle": h, "Op": op, "Int": int, "U64": u64v}))
    }

    pub fn name(&self) -> String {
        match self {
            Gauge::Rust(g) => g.name().to_owned(),
            Gauge::Go(h) => Self::go(*h, "Name", 0, 0),
        }
    }

    pub fn value(&self) -> u64 {
        match self {
            Gauge::Rust(g) => g.value(),
            Gauge::Go(h) => Self::go(*h, "Value", 0, 0),
        }
    }

    /// Upstream `Set(int)`.
    pub fn set(&self, v: i64) {
        match self {
            Gauge::Rust(g) => g.set(v),
            Gauge::Go(h) => {
                let _: bool = Self::go(*h, "Set", v, 0);
            }
        }
    }

    pub fn inc(&self) -> u64 {
        match self {
            Gauge::Rust(g) => g.inc(),
            Gauge::Go(h) => Self::go(*h, "Inc", 0, 0),
        }
    }

    pub fn dec(&self) -> u64 {
        match self {
            Gauge::Rust(g) => g.dec(),
            Gauge::Go(h) => Self::go(*h, "Dec", 0, 0),
        }
    }
}

// --- Metrics -------------------------------------------------------------------

/// A counter or gauge as seen inside `EachCounter` / `EachGauge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Visited {
    pub name: String,
    pub value: u64,
}

#[derive(Debug, Clone)]
pub enum Metrics {
    Rust(Arc<rs::Metrics>),
    Go(i64),
}

impl Metrics {
    /// `metrics.NewMetrics(nil, rotateIntervalSeconds, logger)`
    pub fn new(rotate_interval_seconds: u64) -> Self {
        match backend() {
            Backend::Rust => Metrics::Rust(Arc::new(rs::Metrics::new(Vec::new(), rotate_interval_seconds))),
            Backend::Go => {
                Metrics::Go(bridge::call("metrics.NewMetrics", json!({"RotateInterval": rotate_interval_seconds})))
            }
        }
    }

    /// `metrics.NewFromConfig(config, logger)`
    pub fn from_config(config: &Config) -> Result<Self, MetricsError> {
        match backend() {
            Backend::Rust => rs::Metrics::from_config(config).map(|m| Metrics::Rust(Arc::new(m))),
            Backend::Go => {
                let r: HandleOrErr = bridge::call("metrics.NewFromConfig", json!({"Config": config_to_go(config)}));
                match r.error {
                    Some(e) => Err(MetricsError(e.text)),
                    None => Ok(Metrics::Go(r.handle)),
                }
            }
        }
    }

    /// The Rust registry (Rust backend only).
    pub fn rust(&self) -> Arc<rs::Metrics> {
        match self {
            Metrics::Rust(m) => Arc::clone(m),
            Metrics::Go(_) => panic!("Go metrics have no Rust registry"),
        }
    }

    /// The bridge handle (Go backend only).
    pub fn go_handle(&self) -> i64 {
        match self {
            Metrics::Go(h) => *h,
            Metrics::Rust(_) => panic!("Rust metrics have no bridge handle"),
        }
    }

    fn go(h: i64, op: &str, args: Value) -> Value {
        let mut a = args;
        a["Handle"] = json!(h);
        a["Op"] = json!(op);
        bridge::call("metrics.Metrics", a)
    }

    pub fn register_counter(&self, name: &str, desc: &str) {
        match self {
            Metrics::Rust(m) => rs::Instrumenter::register_counter(m.as_ref(), name, desc),
            Metrics::Go(h) => {
                Self::go(*h, "RegisterCounter", json!({"Name": name, "Desc": desc}));
            }
        }
    }

    pub fn register_gauge(&self, name: &str, desc: &str) {
        match self {
            Metrics::Rust(m) => rs::Instrumenter::register_gauge(m.as_ref(), name, desc),
            Metrics::Go(h) => {
                Self::go(*h, "RegisterGauge", json!({"Name": name, "Desc": desc}));
            }
        }
    }

    pub fn gauge_set(&self, name: &str, value: u64) {
        match self {
            Metrics::Rust(m) => rs::Instrumenter::gauge_set(m.as_ref(), name, value),
            Metrics::Go(h) => {
                Self::go(*h, "GaugeSet", json!({"Name": name, "Value": value}));
            }
        }
    }

    pub fn default_tags(&self, tags: &[(&str, &str)]) {
        let tags: StringMap = tags.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect();
        match self {
            Metrics::Rust(m) => m.default_tags(tags),
            Metrics::Go(h) => {
                Self::go(*h, "DefaultTags", json!({"Tags": tags}));
            }
        }
    }

    /// `m.Counter(name)`; `None` for an unregistered name.
    pub fn counter(&self, name: &str) -> Option<Counter> {
        match self {
            Metrics::Rust(m) => m.counter(name).map(Counter::Rust),
            Metrics::Go(h) => Self::go(*h, "Counter", json!({"Name": name})).as_i64().map(Counter::Go),
        }
    }

    /// `m.Gauge(name)`; `None` for an unregistered name.
    pub fn gauge(&self, name: &str) -> Option<Gauge> {
        match self {
            Metrics::Rust(m) => m.gauge(name).map(Gauge::Rust),
            Metrics::Go(h) => Self::go(*h, "Gauge", json!({"Name": name})).as_i64().map(Gauge::Go),
        }
    }

    /// `m.EachCounter(f)`, with each counter's `Name()` and `Value()`.
    pub fn each_counter(&self, mut f: impl FnMut(&Visited)) {
        match self {
            Metrics::Rust(m) => {
                m.each_counter(&mut |c: &rs::Counter| f(&Visited { name: c.name().to_owned(), value: c.value() }))
            }
            Metrics::Go(h) => {
                let hs: Vec<i64> = serde_json::from_value(Self::go(*h, "EachCounter", json!({}))).expect("handles");
                for c in hs.into_iter().map(Counter::Go) {
                    f(&Visited { name: c.name(), value: c.value() });
                }
            }
        }
    }

    /// `m.EachGauge(f)`, with each gauge's `Name()` and `Value()`.
    pub fn each_gauge(&self, mut f: impl FnMut(&Visited)) {
        match self {
            Metrics::Rust(m) => {
                m.each_gauge(&mut |g: &rs::Gauge| f(&Visited { name: g.name().to_owned(), value: g.value() }))
            }
            Metrics::Go(h) => {
                let hs: Vec<i64> = serde_json::from_value(Self::go(*h, "EachGauge", json!({}))).expect("handles");
                for g in hs.into_iter().map(Gauge::Go) {
                    f(&Visited { name: g.name(), value: g.value() });
                }
            }
        }
    }

    pub fn interval_snapshot(&self) -> HashMap<String, u64> {
        match self {
            Metrics::Rust(m) => m.interval_snapshot(),
            Metrics::Go(h) => serde_json::from_value(Self::go(*h, "IntervalSnapshot", json!({}))).expect("snapshot"),
        }
    }

    /// Upstream unexported `m.rotate()`.
    pub fn rotate(&self) {
        match self {
            Metrics::Rust(m) => m.rotate(),
            Metrics::Go(_) => unimplemented!(
                "not_observable: Metrics.rotate is unexported; the exported Run loop only rotates after \
                 rotate_interval seconds of wall time"
            ),
        }
    }

    pub fn prometheus(&self) -> String {
        match self {
            Metrics::Rust(m) => m.prometheus(),
            Metrics::Go(h) => serde_json::from_value(Self::go(*h, "Prometheus", json!({}))).expect("text"),
        }
    }

    /// Serves `GET /` with `m.PrometheusHandler`; returns (status, body).
    pub fn prometheus_handler(&self) -> (u16, String) {
        match self {
            Metrics::Rust(m) => {
                let req = http::Request::get("/").body(()).expect("request");
                let res = m.prometheus_handler(&req);
                (res.status().as_u16(), res.into_body())
            }
            Metrics::Go(h) => {
                #[derive(Deserialize)]
                struct R {
                    code: u16,
                    body: String,
                }
                let r: R = serde_json::from_value(Self::go(*h, "PrometheusHandler", json!({}))).expect("response");
                (r.code, r.body)
            }
        }
    }

    /// Registers a writer double that records when `Run` is called (the upstream
    /// test's `signalWriter`).
    pub fn register_signal_writer(&self) -> SignalWriter {
        match self {
            Metrics::Rust(m) => {
                let w = Arc::new(RustSignalWriter { ran: Watched::new(false) });
                m.register_writer(Arc::clone(&w) as Arc<dyn rs::IntervalWriter>);
                SignalWriter::Rust(w)
            }
            Metrics::Go(h) => {
                SignalWriter::Go(Self::go(*h, "RegisterSignalWriter", json!({})).as_i64().expect("handle"))
            }
        }
    }

    /// Starts `m.Run()` in the background.
    pub fn spawn_run(&self) -> Running {
        match self {
            Metrics::Rust(m) => {
                let m = Arc::clone(m);
                Running::Rust(spawn(async move { m.run().await }))
            }
            Metrics::Go(h) => Running::Go(Self::go(*h, "Run", json!({})).as_i64().expect("call")),
        }
    }

    pub fn shutdown(&self) -> Result<(), MetricsError> {
        match self {
            Metrics::Rust(m) => block_on_within(AWAIT, m.shutdown()),
            Metrics::Go(h) => {
                let r: ErrOnly = serde_json::from_value(Self::go(*h, "Shutdown", json!({}))).expect("result");
                r.error.map_or(Ok(()), |e| Err(MetricsError(e.text)))
            }
        }
    }
}

/// A background `Run()`.
pub enum Running {
    Rust(tokio::task::JoinHandle<Result<(), MetricsError>>),
    Go(i64),
}

impl Running {
    /// Waits for `Run()` to return.
    pub fn wait(self) -> Result<(), MetricsError> {
        match self {
            Running::Rust(j) => block_on_within(AWAIT, j).expect("run task"),
            Running::Go(c) => {
                let r: ErrOnly = bridge::call("rpcm.Await", json!({"Call": c, "TimeoutMs": AWAIT.as_millis() as u64}));
                r.error.map_or(Ok(()), |e| Err(MetricsError(e.text)))
            }
        }
    }
}

pub struct RustSignalWriter {
    ran: Watched<bool>,
}

impl rs::IntervalWriter for RustSignalWriter {
    fn run(&self, _interval: u64) -> Result<(), MetricsError> {
        self.ran.update(|r| *r = true);
        Ok(())
    }
    fn stop(&self) {}
    fn write(&self, _metrics: &rs::Metrics) -> Result<(), MetricsError> {
        Ok(())
    }
}

pub enum SignalWriter {
    Rust(Arc<RustSignalWriter>),
    Go(i64),
}

impl SignalWriter {
    /// Whether `Run` was called within `deadline`.
    pub fn await_run(&self, deadline: Duration) -> bool {
        match self {
            SignalWriter::Rust(w) => w.ran.wait_until(deadline, |r| *r),
            SignalWriter::Go(h) => bridge::call(
                "metrics.SignalWriter.AwaitRun",
                json!({"Handle": h, "TimeoutMs": deadline.as_millis() as u64}),
            ),
        }
    }
}

// --- StatsD ----------------------------------------------------------------------

pub enum StatsdWriter {
    Rust(rs::StatsdWriter),
    Go(i64),
}

impl StatsdWriter {
    /// `metrics.NewStatsdWriter(config, tags, logger)` (`tags: None` is Go's nil map).
    pub fn new(config: &StatsdConfig, tags: Option<&[(&str, &str)]>) -> Self {
        let tags: Option<StringMap> = tags.map(|t| t.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect());
        match backend() {
            Backend::Rust => StatsdWriter::Rust(rs::StatsdWriter::new(config.clone(), tags)),
            Backend::Go => StatsdWriter::Go(bridge::call(
                "metrics.NewStatsdWriter",
                json!({"Config": statsd_to_go(config), "Tags": tags}),
            )),
        }
    }

    fn go(h: i64, op: &str, metrics: i64, interval: u64) -> Result<(), MetricsError> {
        let r: ErrOnly = bridge::call(
            "metrics.StatsdWriter",
            json!({"Handle": h, "Op": op, "Metrics": metrics, "Interval": interval}),
        );
        r.error.map_or(Ok(()), |e| Err(MetricsError(e.text)))
    }

    pub fn run(&self, interval: u64) -> Result<(), MetricsError> {
        match self {
            StatsdWriter::Rust(w) => rs::IntervalWriter::run(w, interval),
            StatsdWriter::Go(h) => Self::go(*h, "Run", 0, interval),
        }
    }

    pub fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        match self {
            StatsdWriter::Rust(w) => rs::IntervalWriter::write(w, &metrics.rust()),
            StatsdWriter::Go(h) => Self::go(*h, "Write", metrics.go_handle(), 0),
        }
    }

    pub fn stop(&self) {
        match self {
            StatsdWriter::Rust(w) => rs::IntervalWriter::stop(w),
            StatsdWriter::Go(h) => Self::go(*h, "Stop", 0, 0).expect("stop"),
        }
    }
}
