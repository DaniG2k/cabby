//! Facade for upstream package `cli`.
//!
//! `NewConfigFromCLI` reads flag environment variables and `./anycable.toml`, so the
//! Go arm runs it in a bridge process whose environment is exactly `env` and whose
//! working directory is a fresh empty directory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use cabby_server::cli::{self as rs, CliError, CliOption, StringFlag};
use cabby_server::config::{Config, EnvMap};
use serde::Deserialize;
use serde_json::json;

use crate::reference::config::GoConfigView;
use crate::{Backend, backend, bridge};

/// Result of [`new_config_from_cli`].
#[derive(Debug)]
pub struct CliRun {
    /// Upstream `(config, err)`; upstream returns an empty config with an error.
    pub result: Result<Config, CliError>,
    /// Upstream's third return value: usage/version shown, no further action needed.
    pub interrupted: bool,
    /// Values parsed into the custom flags' destinations, by flag name.
    pub custom: HashMap<String, String>,
    /// Go only: `ToToml()` of the full Go configuration (the facade's `Config` is a
    /// subset, so it is rendered in the bridge).
    go_toml: Option<String>,
}

impl CliRun {
    /// `c.ToToml()` of the resulting configuration.
    pub fn to_toml(&self) -> String {
        match &self.go_toml {
            Some(t) => t.clone(),
            None => self.result.as_ref().expect("a configuration").to_toml(),
        }
    }
}

/// A custom string flag (`cli.StringFlag{Name, Value, Required}`).
#[derive(Debug, Clone)]
pub struct CustomFlag {
    pub name: String,
    pub value: String,
    pub required: bool,
}

/// `NewConfigFromCLI(args, WithCLICustomOptions(...))` with process environment `env`.
pub fn new_config_from_cli(args: &[&str], env: &EnvMap, custom_flags: &[CustomFlag]) -> CliRun {
    match backend() {
        Backend::Rust => {
            let flags: Vec<StringFlag> = custom_flags
                .iter()
                .map(|f| StringFlag {
                    name: f.name.clone(),
                    value: f.value.clone(),
                    required: f.required,
                    destination: Arc::new(Mutex::new(String::new())),
                })
                .collect();
            let opts = if flags.is_empty() { vec![] } else { vec![CliOption::CustomOptions(flags.clone())] };
            let res = rs::new_config_from_cli(args, env, opts);
            let custom = flags.iter().map(|f| (f.name.clone(), f.destination.lock().unwrap().clone())).collect();
            match res {
                Ok((config, interrupted)) => CliRun { result: Ok(config), interrupted, custom, go_toml: None },
                Err(e) => CliRun { result: Err(e), interrupted: false, custom, go_toml: None },
            }
        }
        Backend::Go => {
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            struct Out {
                config: GoConfigView,
                error: Option<String>,
                interrupted: bool,
                custom: HashMap<String, String>,
                toml: String,
            }
            let flags: Vec<_> = custom_flags
                .iter()
                .map(|f| json!({"Name": f.name, "Value": f.value, "Required": f.required}))
                .collect();
            let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            let out: Out =
                bridge::call_isolated(&env, "cli.NewConfigFromCLI", json!({"Args": args, "CustomStringFlags": flags}));
            CliRun {
                result: match out.error {
                    Some(e) => Err(CliError(e)),
                    None => Ok(out.config.into()),
                },
                interrupted: out.interrupted,
                custom: out.custom,
                go_toml: Some(out.toml),
            }
        }
    }
}

/// Upstream unexported `parseTags`. The Go arm observes it through its only caller:
/// `NewConfigFromCLI` with `--metrics_tags=<s>` sets `Metrics.Tags = parseTags(s)`.
pub fn parse_tags(s: &str) -> HashMap<String, String> {
    match backend() {
        Backend::Rust => rs::parse_tags(s),
        Backend::Go => {
            let flag = format!("--metrics_tags={s}");
            let run = new_config_from_cli(&["", "--ignore-config-path", &flag], &BTreeMap::new(), &[]);
            run.result.unwrap_or_else(|e| panic!("NewConfigFromCLI failed: {e}")).metrics.tags
        }
    }
}

/// Observations of the upstream `TestRunnerDrainsBroadcastsBeforeBrokerShutdown`
/// scenario: build a runner with the test doubles, run the node, shut down every
/// shutdownable in order.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct DrainOutcome {
    /// `NewRunner` error.
    pub new_runner_error: Option<String>,
    /// `runNode` error.
    pub run_node_error: Option<String>,
    /// The first `Shutdown` error among the shutdownables (`None`: all returned nil).
    pub shutdown_error: Option<String>,
    /// Stream message data received by the subscriber double, in order.
    pub delivered: Vec<String>,
    pub broker_closed: bool,
    pub subscriber_closed: bool,
    pub service_closed: bool,
    /// Failed in-callback assertions, unexpected calls and unmet mock expectations.
    pub violations: Vec<String>,
    /// Calls the doubles received, as `<double>.<Method>`.
    pub calls: BTreeSet<String>,
}

/// Runs the runner drain scenario. The Rust arm is `rust`: the test's own doubles
/// around [`rs::Runner`]. The Go arm runs the upstream test body with its mockery
/// doubles in an isolated bridge process, through the exported `NewRunner` +
/// `Runner.Embed()` (which calls `runNode`) + `Embedded.Shutdown` (which walks
/// `shutdownables` in order, stopping at the first error).
pub fn runner_drains_broadcasts_before_broker_shutdown(rust: impl FnOnce() -> DrainOutcome) -> DrainOutcome {
    match backend() {
        Backend::Rust => rust(),
        Backend::Go => bridge::call_isolated(&[], "cli.Runner.DrainsBroadcastsBeforeBrokerShutdown", json!({})),
    }
}
