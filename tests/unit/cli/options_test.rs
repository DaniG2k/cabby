//! upstream file: cli/options_test.go
//!
//! `NewConfigFromCLI` reads the process environment; the environment is passed
//! explicitly (against Go: the environment of a dedicated bridge process).

use std::collections::HashMap;

use cabby_server::config::EnvMap;
use cabby_testkit::reference::cli::{CliRun, new_config_from_cli, parse_tags};

use crate::support::{TempDir, env};

fn cli(args: &[&str]) -> CliRun {
    new_config_from_cli(args, &EnvMap::new(), &[])
}

/// upstream: cli/options_test.go#TestParseTagsArg
#[test]
fn test_parse_tags_arg() {
    let expected: HashMap<String, String> =
        [("env".to_owned(), "dev".to_owned()), ("rev".to_owned(), "1.1".to_owned())].into();

    let headers = parse_tags("env:dev,rev:1.1");
    assert_eq!(expected, headers);
}

mod test_comment_ping_intervals {
    use super::*;

    /// upstream: cli/options_test.go#TestCommentPingIntervals/defaults
    #[test]
    fn defaults() {
        let c = cli(&["", "--ignore-config-path"]).result.expect("require.NoError");

        assert_eq!(15, c.sse.ping_interval);
        assert_eq!(15, c.ds.sse_ping_interval);
    }

    /// upstream: cli/options_test.go#TestCommentPingIntervals/inherits_custom_global_ping_interval
    #[test]
    fn inherits_custom_global_ping_interval() {
        let c = cli(&["", "--ignore-config-path", "--ping_interval=30"]).result.expect("require.NoError");

        assert_eq!(30, c.sse.ping_interval);
        assert_eq!(30, c.ds.sse_ping_interval);
    }

    /// upstream: cli/options_test.go#TestCommentPingIntervals/sse_comment_pings_flag
    #[test]
    fn sse_comment_pings_flag() {
        let c = cli(&["", "--ignore-config-path", "--sse_comment_pings"]).result.expect("require.NoError");

        assert!(c.sse.comment_pings);
        assert_eq!(15, c.sse.ping_interval);
    }

    /// upstream: cli/options_test.go#TestCommentPingIntervals/component-specific_values_take_precedence
    #[test]
    fn component_specific_values_take_precedence() {
        let dir = TempDir::new();
        let conf = "[app]\nping_interval = 30\n\n[sse]\nping_interval = 20\n\n[ds]\nsse_ping_interval = 25\n";
        let path = dir.write("anycable.toml", conf);

        let flag = format!("--config-path={}", path.display());
        let c = cli(&["", &flag]).result.expect("require.NoError");

        assert_eq!(30, c.app.ping_interval);
        assert_eq!(20, c.sse.ping_interval);
        assert_eq!(25, c.ds.sse_ping_interval);
    }
}

mod test_unknown_config_keys {
    use super::*;

    /// Upstream helper `writeConfig` (its `require.NoError` is the parent test's own
    /// assertion, hence the parent annotation on every subtest using it).
    fn write_config(dir: &TempDir, conf: &str) -> String {
        format!("--config-path={}", dir.write("anycable.toml", conf).display())
    }

    /// upstream: cli/options_test.go#TestUnknownConfigKeys
    /// upstream: cli/options_test.go#TestUnknownConfigKeys/valid_configuration
    #[test]
    fn valid_configuration() {
        let dir = TempDir::new();
        let path = write_config(&dir, "secret = \"s3cr3t\"\n\n[sse]\nenabled = true\n");

        let c = cli(&["", &path, "--strict-config"]).result.expect("require.NoError");

        assert!(c.sse.enabled);
        assert!(c.unknown_config_keys.is_empty());
    }

    /// upstream: cli/options_test.go#TestUnknownConfigKeys
    /// upstream: cli/options_test.go#TestUnknownConfigKeys/unknown_keys_are_recorded
    #[test]
    fn unknown_keys_are_recorded() {
        let dir = TempDir::new();
        let path = write_config(&dir, "hots = \"0.0.0.0\"\n\n[redis]\nurll = \"redis://redis:6379\"\n");

        let c = cli(&["", &path]).result.expect("require.NoError");

        assert_eq!(vec!["hots", "redis.urll"], c.unknown_config_keys);
    }

    /// upstream: cli/options_test.go#TestUnknownConfigKeys
    /// upstream: cli/options_test.go#TestUnknownConfigKeys/strict_mode_fails_on_unknown_keys
    #[test]
    fn strict_mode_fails_on_unknown_keys() {
        let dir = TempDir::new();
        let path = write_config(&dir, "hots = \"0.0.0.0\"\n\n[redis]\nurll = \"redis://redis:6379\"\n");

        let err = cli(&["", &path, "--strict-config"]).result.expect_err("require.Error");

        assert!(err.to_string().contains("unknown keys in configuration file"), "{err}");
        assert!(err.to_string().contains("hots, redis.urll"), "{err}");
    }

    /// upstream: cli/options_test.go#TestUnknownConfigKeys
    /// upstream: cli/options_test.go#TestUnknownConfigKeys/strict_mode_via_env_var
    #[test]
    fn strict_mode_via_env_var() {
        // t.Setenv("ANYCABLE_STRICT_CONFIG", "true")
        let env = env(&[("ANYCABLE_STRICT_CONFIG", "true")]);

        let dir = TempDir::new();
        let path = write_config(&dir, "[redis]\nurll = \"redis://redis:6379\"\n");

        let err = new_config_from_cli(&["", &path], &env, &[]).result.expect_err("require.Error");

        assert!(err.to_string().contains("redis.urll"), "{err}");
    }

    /// upstream: cli/options_test.go#TestUnknownConfigKeys
    /// upstream: cli/options_test.go#TestUnknownConfigKeys/printed_configuration_has_no_unknown_keys
    #[test]
    fn printed_configuration_has_no_unknown_keys() {
        let run = cli(&[
            "",
            "--ignore-config-path",
            "--metrics_http=/metrics",
            "--metrics_tags=env:test",
            "--redis_url=redis://redis:6379",
            "--sse",
            "--embed_nats",
        ]);
        assert!(run.result.is_ok(), "{:?}", run.result);

        let dir = TempDir::new();
        let path = write_config(&dir, &run.to_toml());

        let c = cli(&["", &path, "--strict-config"]).result.expect("require.NoError");

        assert_eq!("/metrics", c.metrics.http);
        assert!(c.unknown_config_keys.is_empty());
    }
}

mod test_max_body_size_options {
    use super::*;

    /// upstream: cli/options_test.go#TestMaxBodySizeOptions/defaults
    #[test]
    fn defaults() {
        let c = cli(&["", "--ignore-config-path"]).result.expect("require.NoError");

        assert_eq!(1048576_i64, c.http_broadcast.max_body_size);
        assert_eq!(1048576_i64, c.api.max_body_size);
    }

    /// upstream: cli/options_test.go#TestMaxBodySizeOptions/flags
    #[test]
    fn flags() {
        let c = cli(&["", "--ignore-config-path", "--http_broadcast_max_body_size=5242880", "--api_max_body_size=0"])
            .result
            .expect("require.NoError");

        assert_eq!(5242880_i64, c.http_broadcast.max_body_size);
        assert_eq!(0_i64, c.api.max_body_size);
    }

    /// upstream: cli/options_test.go#TestMaxBodySizeOptions/env_vars
    #[test]
    fn env_vars() {
        // t.Setenv("ANYCABLE_HTTP_BROADCAST_MAX_BODY_SIZE", "10485760")
        // t.Setenv("ANYCABLE_API_MAX_BODY_SIZE", "2097152")
        let env =
            env(&[("ANYCABLE_HTTP_BROADCAST_MAX_BODY_SIZE", "10485760"), ("ANYCABLE_API_MAX_BODY_SIZE", "2097152")]);

        let c = new_config_from_cli(&["", "--ignore-config-path"], &env, &[]).result.expect("require.NoError");

        assert_eq!(10485760_i64, c.http_broadcast.max_body_size);
        assert_eq!(2097152_i64, c.api.max_body_size);
    }
}

/// upstream: cli/options_test.go#TestBrokerTTLValidation
#[test]
fn test_broker_ttl_validation() {
    for args in [
        ["--sessions_ttl=0", "--presence_ttl=0", "--history_ttl=0"],
        ["--sessions_ttl=31536000", "--presence_ttl=31536000", "--history_ttl=31536000"],
    ] {
        let mut argv = vec!["", "--ignore-config-path"];
        argv.extend(args);
        let res = cli(&argv).result;
        assert!(res.is_ok(), "{args:?}: {res:?}");
    }

    for arg in [
        "--sessions_ttl=-1",
        "--presence_ttl=31536001",
        "--history_ttl=9223372036854775807",
        "--sessions_ttl=18446744073709552",
    ] {
        let res = cli(&["", "--ignore-config-path", arg]).result;
        assert!(res.is_err(), "{arg}");
    }
}

/// regression-for: cli/options_test.go#TestBrokerTTLValidation
/// note: the error text names the setting, the bounds and the value, and the history TTL is checked first.
#[test]
fn broker_ttl_errors_name_the_setting() {
    let err = cli(&["", "--ignore-config-path", "--sessions_ttl=-1"]).result.expect_err("an error");
    assert_eq!("sessions_ttl must be between 0 and 31536000 seconds, got -1", err.to_string());

    let err =
        cli(&["", "--ignore-config-path", "--presence_ttl=31536001", "--history_ttl=-5"]).result.expect_err("an error");
    assert_eq!("history_ttl must be between 0 and 31536000 seconds, got -5", err.to_string());
}
