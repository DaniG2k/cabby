//! Anonymized usage telemetry (upstream package `telemetry`).
//!
//! The tracker posts a `boot` event on [`Tracker::collect`] and a `usage` event
//! (maxima of observed gauges) every 30 minutes. Process environment and clock are
//! explicit inputs so tests never mutate global state or reach a real endpoint.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cabby_core::common::StringMap;
use cabby_core::gojson;
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::http_client::{HttpClient, Origin};
use crate::metrics::Metrics;

/// Default telemetry endpoint. Tests must always override it.
pub const DEFAULT_ENDPOINT: &str = "https://telemetry.anycable.io";
/// Delay between usage events.
pub const USAGE_MEASUREMENT_DELAY: Duration = Duration::from_secs(30 * 60);
/// Interval between usage observations.
const USAGE_OBSERVATION_INTERVAL: Duration = Duration::from_secs(60);
/// Timeout of one telemetry request.
const SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// The tracking token, provided at build time (upstream sets it with `-ldflags`);
/// empty disables tracking.
const BUILD_TOKEN: &str = match option_env!("CABBY_TELEMETRY_TOKEN") {
    Some(t) => t,
    None => "",
};

/// The AnyCable version Cabby reports (upstream `version.Version()` at the
/// pinned commit).
const VERSION: &str = "1.6.21";

/// Tracker configuration (upstream `telemetry.Config`).
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Empty token disables tracking.
    pub token: String,
    pub endpoint: String,
    pub custom_props: StringMap,
    pub debug: bool,
}

impl Config {
    /// Upstream `NewConfig`: build-time token, default endpoint,
    /// `ANYCABLE_TELEMETRY_DEBUG=1` enables debug logging.
    pub fn new(env: &dyn EnvSource) -> Self {
        Self {
            token: BUILD_TOKEN.to_owned(),
            endpoint: DEFAULT_ENDPOINT.to_owned(),
            custom_props: StringMap::new(),
            debug: env.var("ANYCABLE_TELEMETRY_DEBUG").as_deref() == Some("1"),
        }
    }
}

/// Read access to the process environment (upstream reads `os.Getenv`).
pub trait EnvSource: Send + Sync {
    fn var(&self, name: &str) -> Option<String>;
    /// All variables, for the cluster fingerprint.
    fn vars(&self) -> Vec<(String, String)>;
}

/// A fixed environment.
#[derive(Debug, Clone, Default)]
pub struct StaticEnv(pub HashMap<String, String>);

impl EnvSource for StaticEnv {
    fn var(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
    fn vars(&self) -> Vec<(String, String)> {
        self.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
}

/// The process environment.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn var(&self, name: &str) -> Option<String> {
        std::env::var_os(name).map(|v| v.to_string_lossy().into_owned())
    }
    fn vars(&self) -> Vec<(String, String)> {
        std::env::vars_os().map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned())).collect()
    }
}

/// The server settings reported as feature flags (the subset of upstream
/// `config.Config` that `appProperties` reads).
#[derive(Debug, Clone, PartialEq)]
pub struct AppConfig {
    pub secret: String,
    pub skip_auth: bool,
    pub jwt_enabled: bool,
    pub public_streams: bool,
    pub turbo_streams: bool,
    pub cable_ready_streams: bool,
    pub embedded_nats: bool,
    pub broadcast_adapters: Vec<String>,
    pub pubsub_adapter: String,
    pub broker_adapter: String,
    pub ssl_available: bool,
    pub metrics_log_formatter: bool,
    pub statsd_enabled: bool,
    pub prometheus_enabled: bool,
    pub rpc_impl: String,
    /// Path of the TOML configuration file, if any (hashed into the fingerprint).
    pub config_file_path: String,
}

impl AppConfig {
    /// The values derived from the server's default configuration (upstream
    /// `config.NewConfig()`).
    pub fn defaults() -> Self {
        Self::from_config(&crate::config::Config::new())
    }

    /// The values `appProperties` reads from a server configuration.
    pub fn from_config(c: &crate::config::Config) -> Self {
        Self {
            secret: c.secret.clone(),
            skip_auth: c.skip_auth,
            jwt_enabled: c.jwt.enabled(),
            public_streams: c.streams.public,
            turbo_streams: c.streams.turbo,
            cable_ready_streams: c.streams.cable_ready,
            embedded_nats: c.embedded_nats.enabled,
            broadcast_adapters: c.broadcast_adapters.clone(),
            pubsub_adapter: c.pubsub_adapter.clone(),
            broker_adapter: c.broker.adapter.clone(),
            ssl_available: c.server.ssl.available(),
            metrics_log_formatter: !c.metrics.log_formatter.is_empty(),
            statsd_enabled: c.metrics.statsd.enabled(),
            prometheus_enabled: !c.metrics.http.is_empty(),
            rpc_impl: c.rpc.implementation(),
            config_file_path: c.config_file_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
        }
    }
}

/// A variable identifying a CI vendor (`Some(value)`: it must have that value).
type CiVar = (&'static str, Option<&'static str>);

/// CI vendors (hofstadter-io/cinful `vendors.json`): name and the variables
/// that identify them.
const CI_VENDORS: &[(&str, &[CiVar])] = &[
    ("AppVeyor", &[("APPVEYOR", None)]),
    ("Azure Pipelines", &[("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", None)]),
    ("Appcircle", &[("AC_APPCIRCLE", None)]),
    ("Bamboo", &[("bamboo_planKey", None)]),
    ("Bitbucket Pipelines", &[("BITBUCKET_COMMIT", None)]),
    ("Bitrise", &[("BITRISE_IO", None)]),
    ("Buddy", &[("BUDDY_WORKSPACE_ID", None)]),
    ("Buildkite", &[("BUILDKITE", None)]),
    ("CircleCI", &[("CIRCLECI", None)]),
    ("Cirrus CI", &[("CIRRUS_CI", None)]),
    ("AWS CodeBuild", &[("CODEBUILD_BUILD_ARN", None)]),
    ("Codefresh", &[("CF_BUILD_ID", None)]),
    ("Codeship", &[("CI_NAME", Some("codeship"))]),
    ("Drone", &[("DRONE", None)]),
    ("dsari", &[("DSARI", None)]),
    ("Expo Application Services", &[("EAS_BUILD", None)]),
    ("GitHub Actions", &[("GITHUB_ACTIONS", None)]),
    ("GitLab CI", &[("GITLAB_CI", None)]),
    ("GoCD", &[("GO_PIPELINE_LABEL", None)]),
    ("LayerCI", &[("LAYERCI", None)]),
    ("Hudson", &[("HUDSON_URL", None)]),
    ("Jenkins", &[("JENKINS_URL", None), ("BUILD_ID", None)]),
    ("Magnum CI", &[("MAGNUM", None)]),
    ("Netlify CI", &[("NETLIFY", None)]),
    ("Nevercode", &[("NEVERCODE", None)]),
    ("Render", &[("RENDER", None)]),
    ("Sail CI", &[("SAILCI", None)]),
    ("Semaphore", &[("SEMAPHORE", None)]),
    ("Screwdriver", &[("SCREWDRIVER", None)]),
    ("Shippable", &[("SHIPPABLE", None)]),
    ("Solano CI", &[("TDDIUM", None)]),
    ("Strider CD", &[("STRIDER", None)]),
    ("TaskCluster", &[("TASK_ID", None), ("RUN_ID", None)]),
    ("TeamCity", &[("TEAMCITY_VERSION", None)]),
    ("Travis CI", &[("TRAVIS", None)]),
    ("Vercel", &[("NOW_BUILDER", None)]),
    ("Visual Studio App Center", &[("APPCENTER_BUILD_ID", None)]),
];

/// Variables that indicate some CI when no vendor matched.
const CI_COMMON_ENV: &[&str] = &["CI", "CONTINUOUS_INTEGRATION", "BUILD_NUMBER", "RUN_ID"];

/// The CI vendor name, if running in CI (cinful `Info`).
fn ci_vendor(env: &dyn EnvSource) -> Option<&'static str> {
    let set = |k: &str| env.var(k).filter(|v| !v.is_empty());
    for (name, vars) in CI_VENDORS {
        let matched = vars.iter().any(|(k, expected)| match expected {
            None => set(k).is_some(),
            Some(v) => env.var(k).as_deref() == Some(*v),
        });
        if matched {
            return Some(name);
        }
    }
    CI_COMMON_ENV.iter().any(|k| set(k).is_some()).then_some("Common Var")
}

fn guess_platform(env: &dyn EnvSource) -> &'static str {
    let has = |k: &str| env.var(k).is_some();
    if has("FLY_APP_NAME") {
        return "fly";
    }
    if has("HEROKU_APP_ID") {
        return "heroku";
    }
    if has("RENDER_SERVICE_ID") {
        return "render";
    }
    if has("HATCHBOX_APP_NAME") {
        return "hatchbox";
    }
    if let Some(aws) = env.var("AWS_EXECUTION_ENV") {
        return match aws.as_str() {
            "AWS_ECS_FARGATE" => "ecs-fargate",
            "AWS_ECS_EC2" => "ecs-ec2",
            _ => "ecs",
        };
    }
    if has("ECS_CONTAINER_METADATA_URI") || has("ECS_CONTAINER_METADATA_URI_V4") {
        return "ecs";
    }
    if has("K_SERVICE") {
        return "cloud-run";
    }
    ""
}

fn platform_service_id(env: &dyn EnvSource) -> Option<String> {
    ["FLY_APP_NAME", "HEROKU_APP_ID", "RENDER_SERVICE_ID", "HATCHBOX_APP_NAME", "K_SERVICE"]
        .iter()
        .find_map(|k| env.var(k))
}

fn generate_digest(prefix: &str, parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for part in parts.iter().filter(|p| !p.is_empty()) {
        h.update(part.as_bytes());
    }
    let mut out = String::with_capacity(prefix.len() + 64);
    out.push_str(prefix);
    for b in h.finalize() {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// A unique cluster fingerprint to group events from instances of one cluster.
fn cluster_fingerprint(app: &AppConfig, env: &dyn EnvSource, args: &[String]) -> String {
    if let Some(id) = platform_service_id(env) {
        return generate_digest("P", &[&id]);
    }
    let platform = guess_platform(env);
    let mut vars: Vec<String> =
        env.vars().into_iter().filter(|(k, _)| k.starts_with("ANYCABLE")).map(|(k, v)| format!("{k}={v}")).collect();
    vars.sort();
    let vars = vars.join(",");
    let mut opts = args.to_vec();
    opts.sort();
    let opts = opts.join(",");
    let file = if app.config_file_path.is_empty() {
        String::new()
    } else {
        std::fs::read_to_string(&app.config_file_path).unwrap_or_default()
    };
    if vars.is_empty() && opts.is_empty() && file.is_empty() && platform.is_empty() {
        return "default".to_owned();
    }
    generate_digest("C", &[platform, &vars, &opts, &file])
}

/// go-nanoid `Nanoid(8)`.
fn nanoid(size: usize) -> String {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut bytes = vec![0u8; size];
    if getrandom::fill(&mut bytes).is_err() {
        return String::new();
    }
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

fn go_os() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        other => other,
    }
}

#[derive(Debug, Default)]
struct Usage {
    observations: BTreeMap<String, u64>,
    closed: bool,
}

struct Inner {
    id: String,
    fingerprint: String,
    token: String,
    client: Option<HttpClient>,
    metrics: Arc<Metrics>,
    app: AppConfig,
    env: Arc<dyn EnvSource>,
    custom_props: StringMap,
    usage: Mutex<Usage>,
    /// Number of observations made by the usage monitor.
    observed: watch::Sender<u64>,
    shutdown: CancellationToken,
    debug: bool,
}

/// Upstream `telemetry.Tracker`.
pub struct Tracker {
    inner: Option<Arc<Inner>>,
}

impl std::fmt::Debug for Tracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tracker").field("enabled", &self.enabled()).finish_non_exhaustive()
    }
}

fn debug(inner: &Inner, msg: &str) {
    if inner.debug {
        crate::metrics::log(cabby_core::logger::Level::Info, msg, &[]);
    }
}

impl Inner {
    fn app_properties(&self) -> BTreeMap<String, Value> {
        let a = &self.app;
        let mut props = BTreeMap::new();
        let mut set = |k: &str, v: Value| {
            props.insert(k.to_owned(), v);
        };
        set("version", Value::from(VERSION));
        set("os", Value::from(go_os()));
        set("mruby", Value::from(crate::mrb::supported()));
        let ci = ci_vendor(self.env.as_ref());
        set("ci", Value::from(ci.is_some()));
        if let Some(name) = ci {
            set("ci-name", Value::from(name));
        }
        set("deploy", Value::from(guess_platform(self.env.as_ref())));
        set("has-secret", Value::from(!a.secret.is_empty()));
        set("no-auth", Value::from(a.skip_auth));
        set("jwt", Value::from(a.jwt_enabled));
        set("public-streams", Value::from(a.public_streams));
        set("turbo", Value::from(a.turbo_streams));
        set("cr", Value::from(a.cable_ready_streams));
        set("enats", Value::from(a.embedded_nats));
        set("broadcast", Value::from(a.broadcast_adapters.join(",")));
        set("pubsub", Value::from(a.pubsub_adapter.clone()));
        set("broker", Value::from(a.broker_adapter.clone()));
        set("ssl", Value::from(a.ssl_available));
        set("mrb-printer", Value::from(a.metrics_log_formatter));
        set("statsd", Value::from(a.statsd_enabled));
        set("prom", Value::from(a.prometheus_enabled));
        set("rpc-impl", Value::from(a.rpc_impl.clone()));
        let plus = self.env.var("ANYCABLEPLUS_APP_NAME");
        set("plus", Value::from(plus.is_some()));
        if let Some(name) = plus {
            set("plus-name", Value::from(name));
        }
        for (k, v) in &self.custom_props {
            set(k, Value::from(v.clone()));
        }
        props
    }

    /// Upstream `Send`: posts one event (1s timeout); failures are only logged.
    async fn send(&self, event: &str, mut props: BTreeMap<String, Value>) {
        debug(self, &format!("send telemetry event: {event}"));
        // Avoid storing the IP address.
        props.insert("$ip".into(), Value::Null);
        props.insert("distinct_id".into(), Value::from(self.id.clone()));
        props.insert("cluster-fingerprint".into(), Value::from(self.fingerprint.clone()));
        props.insert("event".into(), Value::from(event));

        let Some(client) = &self.client else {
            return;
        };
        let payload = Bytes::from(gojson::to_vec(&props));
        let mut headers: Vec<(HeaderName, HeaderValue)> =
            vec![(CONTENT_TYPE, HeaderValue::from_static("application/json"))];
        if let Ok(token) = HeaderValue::from_str(&self.token) {
            headers.push((AUTHORIZATION, token));
        }
        match tokio::time::timeout(SEND_TIMEOUT, client.post("", &headers, payload)).await {
            Err(_) => debug(self, "timed out to send telemetry data"),
            Ok(Err(e)) => debug(self, &format!("failed to perform telemetry request: {}", e.message)),
            Ok(Ok(res)) if res.status == 401 => debug(self, "telemetry authenticated failed"),
            Ok(Ok(res)) if res.status != 200 => debug(self, &format!("telemetry request failed: {}", res.status)),
            Ok(Ok(_)) => {}
        }
    }

    fn observe_usage(&self) {
        let value = |name: &str| self.metrics.gauge(name).map_or(0, |g| g.value());
        let (clients, mem) = (value("clients_num"), value("mem_sys_bytes"));
        let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        for (key, val) in [("clients_max", clients), ("mem_sys_max", mem)] {
            let entry = usage.observations.entry(key.to_owned()).or_insert(val);
            *entry = (*entry).max(val);
        }
    }

    async fn collect_usage(&self) {
        let observations = {
            let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut usage.observations)
        };
        let mut props = self.app_properties();
        props.extend(observations.into_iter().map(|(k, v)| (k, Value::from(v))));
        self.send("usage", props).await;
    }
}

impl Tracker {
    /// A disabled tracker when `config.token` is empty.
    pub fn new(
        metrics: Arc<Metrics>,
        app: &AppConfig,
        config: &Config,
        env: Arc<dyn EnvSource>,
        args: Vec<String>,
    ) -> Self {
        if config.token.is_empty() {
            return Self { inner: None };
        }
        let client = Origin::parse(&config.endpoint).ok().and_then(|origin| {
            let tls = if origin.https {
                Some(
                    crate::client_tls::client_config(&crate::client_tls::TlsOptions {
                        root_ca: String::new(),
                        verify: true,
                        alpn: vec![b"http/1.1".to_vec()],
                    })
                    .ok()?,
                )
            } else {
                None
            };
            Some(HttpClient::new(origin, tls, 2, Duration::from_secs(90), None))
        });
        let fingerprint = cluster_fingerprint(app, env.as_ref(), &args);
        Self {
            inner: Some(Arc::new(Inner {
                id: nanoid(8),
                fingerprint,
                token: config.token.clone(),
                client,
                metrics,
                app: app.clone(),
                env,
                custom_props: config.custom_props.clone(),
                usage: Mutex::new(Usage::default()),
                observed: watch::Sender::new(0),
                shutdown: CancellationToken::new(),
                debug: config.debug,
            })),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn announce(&self) -> String {
        "Anonymized telemetry is on. Learn more: https://docs.anycable.io/anycable-go/telemetry".to_owned()
    }

    /// Sends the `boot` event (synchronously, 1s timeout), starts the usage monitor
    /// (first observation immediately, then every minute) and schedules the
    /// usage event.
    pub async fn collect(&self) {
        let Some(inner) = &self.inner else {
            return;
        };
        inner.send("boot", inner.app_properties()).await;

        let monitor = Arc::clone(inner);
        tokio::spawn(async move {
            loop {
                if monitor.usage.lock().unwrap_or_else(|e| e.into_inner()).closed {
                    return;
                }
                monitor.observe_usage();
                monitor.observed.send_modify(|n| *n += 1);
                tokio::select! {
                    () = monitor.shutdown.cancelled() => return,
                    () = tokio::time::sleep(USAGE_OBSERVATION_INTERVAL) => {}
                }
            }
        });

        let reporter = Arc::clone(inner);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = reporter.shutdown.cancelled() => return,
                    () = tokio::time::sleep(USAGE_MEASUREMENT_DELAY) => reporter.collect_usage().await,
                }
            }
        });
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        let Some(inner) = &self.inner else {
            return Ok(());
        };
        {
            let mut usage = inner.usage.lock().unwrap_or_else(|e| e.into_inner());
            if usage.closed {
                return Ok(());
            }
            usage.closed = true;
        }
        inner.shutdown.cancel();
        if let Some(client) = &inner.client {
            client.close_idle();
        }
        Ok(())
    }

    /// Records the current `clients_num` / `mem_sys_bytes` gauges, keeping maxima.
    pub fn observe_usage(&self) {
        if let Some(inner) = &self.inner {
            inner.observe_usage();
        }
    }

    /// Sends the `usage` event with the observed maxima and resets them.
    pub async fn collect_usage(&self) {
        if let Some(inner) = &self.inner {
            inner.collect_usage().await;
        }
    }

    /// Waits until the usage monitor has made at least `n` observations
    /// (readiness hook replacing upstream's sleep; `false` on deadline).
    pub async fn wait_for_observations(&self, n: u64, deadline: Duration) -> bool {
        let Some(inner) = &self.inner else {
            return false;
        };
        let mut rx = inner.observed.subscribe();
        tokio::time::timeout(deadline, rx.wait_for(|count| *count >= n)).await.is_ok_and(|r| r.is_ok())
    }
}

/// The version string reported in telemetry events (upstream `version.Version()`).
pub fn version() -> String {
    VERSION.to_owned()
}
