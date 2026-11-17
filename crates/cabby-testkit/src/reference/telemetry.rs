//! Facade for upstream package `telemetry`.
//!
//! The process environment is explicit (`env` pairs): the Rust tracker gets it
//! as an [`EnvSource`], the Go bridge applies it only while each op runs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cabby_server::telemetry as rs;
pub use cabby_server::telemetry::Config;
use serde::Deserialize;
use serde_json::json;

use super::metrics::Metrics;
use crate::runtime::block_on_within;
use crate::{Backend, backend, bridge};

const AWAIT: Duration = Duration::from_secs(20);

fn env_map(env: &[(&str, &str)]) -> HashMap<String, String> {
    env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    token: String,
    endpoint: String,
    custom_props: Option<HashMap<String, String>>,
    debug: bool,
}

/// `telemetry.NewConfig()` under the given environment.
pub fn new_config(env: &[(&str, &str)]) -> Config {
    match backend() {
        Backend::Rust => Config::new(&rs::StaticEnv(env_map(env))),
        Backend::Go => {
            let g: GoConfig = bridge::call("telemetry.NewConfig", json!({"Env": env_map(env)}));
            Config {
                token: g.token,
                endpoint: g.endpoint,
                custom_props: g.custom_props.unwrap_or_default(),
                debug: g.debug,
            }
        }
    }
}

/// `version.Version()` as reported by telemetry.
pub fn version() -> String {
    match backend() {
        Backend::Rust => rs::version(),
        Backend::Go => bridge::call("telemetry.Version", json!({})),
    }
}

pub enum Tracker {
    Rust(Arc<rs::Tracker>),
    Go { handle: i64, env: HashMap<String, String> },
}

fn assert_test_endpoint(config: &Config) {
    assert!(
        config.endpoint.starts_with("http://127.0.0.1:"),
        "telemetry tests must use a local recording endpoint, got {:?}",
        config.endpoint
    );
}

impl Tracker {
    /// `telemetry.NewTracker(metrics, config.NewConfig(), tconfig)` with `env` as
    /// the process environment.
    pub fn new(metrics: &Metrics, config: &Config, env: &[(&str, &str)]) -> Self {
        assert_test_endpoint(config);
        match backend() {
            Backend::Rust => Tracker::Rust(Arc::new(rs::Tracker::new(
                metrics.rust(),
                &rs::AppConfig::defaults(),
                config,
                Arc::new(rs::StaticEnv(env_map(env))),
                Vec::new(),
            ))),
            Backend::Go => {
                let go = GoConfig {
                    token: config.token.clone(),
                    endpoint: config.endpoint.clone(),
                    custom_props: Some(config.custom_props.clone()),
                    debug: config.debug,
                };
                let handle = bridge::call(
                    "telemetry.NewTracker",
                    json!({"Metrics": metrics.go_handle(), "Config": go, "Env": env_map(env)}),
                );
                Tracker::Go { handle, env: env_map(env) }
            }
        }
    }

    /// `tracker.Collect()`
    pub fn collect(&self) {
        match self {
            Tracker::Rust(t) => block_on_within(AWAIT, t.collect()),
            Tracker::Go { handle, env } => {
                let _: bool = bridge::call("telemetry.Tracker", json!({"Handle": handle, "Op": "Collect", "Env": env}));
            }
        }
    }

    /// Waits until the usage monitor started by `collect` made `n` observations.
    pub fn wait_for_observations(&self, n: u64, deadline: Duration) -> bool {
        match self {
            Tracker::Rust(t) => block_on_within(AWAIT, t.wait_for_observations(n, deadline)),
            Tracker::Go { .. } => {
                unimplemented!("not_observable: the usage monitor goroutine and its observations are unexported")
            }
        }
    }

    /// Upstream unexported `tracker.observeUsage()`.
    pub fn observe_usage(&self) {
        match self {
            Tracker::Rust(t) => t.observe_usage(),
            Tracker::Go { .. } => unimplemented!("not_observable: Tracker.observeUsage is unexported"),
        }
    }

    /// Upstream unexported `tracker.collectUsage()`.
    pub fn collect_usage(&self) {
        match self {
            Tracker::Rust(t) => block_on_within(AWAIT, t.collect_usage()),
            Tracker::Go { .. } => unimplemented!(
                "not_observable: Tracker.collectUsage is unexported and otherwise fires after 30 minutes"
            ),
        }
    }

    /// `tracker.Shutdown(ctx)`
    pub fn shutdown(&self) -> Result<(), String> {
        match self {
            Tracker::Rust(t) => block_on_within(AWAIT, t.shutdown()),
            Tracker::Go { handle, .. } => {
                #[derive(Deserialize)]
                struct E {
                    text: String,
                }
                #[derive(Deserialize)]
                struct R {
                    error: Option<E>,
                }
                let r: R = bridge::call("telemetry.Tracker", json!({"Handle": handle, "Op": "Shutdown"}));
                r.error.map_or(Ok(()), |e| Err(e.text))
            }
        }
    }
}
