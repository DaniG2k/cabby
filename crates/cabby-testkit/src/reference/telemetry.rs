//! Facade for upstream package `telemetry`.
//!
//! Cabby has no telemetry (D-058), so every Rust arm panics with the `retired:`
//! marker and the runner records the case as `retired` (D-063). The Go arms are
//! unchanged: the process environment is explicit (`env` pairs), and the Go
//! bridge applies it only while each op runs.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use super::metrics::Metrics;
use crate::{Backend, backend, bridge};

/// The marker the compatibility runner reads (D-063).
const RETIRED: &str = "retired: D-058: Cabby has no telemetry";

fn env_map(env: &[(&str, &str)]) -> HashMap<String, String> {
    env.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// Upstream `telemetry.Config`, as the translated test reads and writes it.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Config {
    /// Empty token disables tracking.
    pub token: String,
    pub endpoint: String,
    pub custom_props: HashMap<String, String>,
    pub debug: bool,
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
        Backend::Rust => panic!("{RETIRED}"),
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
        Backend::Rust => panic!("{RETIRED}"),
        Backend::Go => bridge::call("telemetry.Version", json!({})),
    }
}

/// A tracker in the Go reference.
pub struct Tracker {
    handle: i64,
    env: HashMap<String, String>,
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
        if backend() == Backend::Rust {
            panic!("{RETIRED}");
        }
        assert_test_endpoint(config);
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
        Tracker { handle, env: env_map(env) }
    }

    /// `tracker.Collect()`
    pub fn collect(&self) {
        let _: bool =
            bridge::call("telemetry.Tracker", json!({"Handle": self.handle, "Op": "Collect", "Env": self.env}));
    }

    /// Waits until the usage monitor started by `collect` made `n` observations.
    pub fn wait_for_observations(&self, _n: u64, _deadline: Duration) -> bool {
        unimplemented!("not_observable: the usage monitor goroutine and its observations are unexported")
    }

    /// Upstream unexported `tracker.observeUsage()`.
    pub fn observe_usage(&self) {
        unimplemented!("not_observable: Tracker.observeUsage is unexported")
    }

    /// Upstream unexported `tracker.collectUsage()`.
    pub fn collect_usage(&self) {
        unimplemented!("not_observable: Tracker.collectUsage is unexported and otherwise fires after 30 minutes")
    }

    /// `tracker.Shutdown(ctx)`
    pub fn shutdown(&self) -> Result<(), String> {
        #[derive(Deserialize)]
        struct E {
            text: String,
        }
        #[derive(Deserialize)]
        struct R {
            error: Option<E>,
        }
        let r: R = bridge::call("telemetry.Tracker", json!({"Handle": self.handle, "Op": "Shutdown"}));
        r.error.map_or(Ok(()), |e| Err(e.text))
    }
}
