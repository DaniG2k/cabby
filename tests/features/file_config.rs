//! features/file_config.testfile: `--print-config` output, loading
//! `./anycable.toml` from the working directory, `--ignore-config-path`, and
//! CLI/env overrides of file values. Commands run with `ANYCABLE_*` removed
//! from the environment (`clean_env: true`) in a fresh working directory.

use toml::Value;

use crate::support::*;

fn dig<'a>(config: &'a toml::Table, path: &[&str]) -> Option<&'a Value> {
    let (first, rest) = path.split_first()?;
    rest.iter().try_fold(config.get(*first)?, |v, k| v.get(k))
}

/// `run` + `PerfectTOML.parse(stdout(:name))`: the runner's pipe carries stdout
/// and stderr, so anything on stderr would have broken the parse upstream.
fn print_config(args: &[&str], env: &[(&str, &str)], dir: &std::path::Path) -> toml::Table {
    let out = run_sut(args, env, dir);
    assert!(out.stderr.is_empty(), "unexpected stderr from {args:?}:\n{}", out.stderr);
    parse(&out.stdout)
}

fn parse(out: &str) -> toml::Table {
    toml::from_str(out).unwrap_or_else(|e| panic!("PerfectTOML.parse failed: {e}\n{out}"))
}

/// `$errors << ... unless expected == actual`.
fn check(errors: &mut Vec<String>, field: &str, expected: Option<Value>, actual: Option<&Value>) {
    if expected.as_ref() != actual {
        let shown = actual.map_or("<null>".to_owned(), ToString::to_string);
        let want = expected.map_or("nil".to_owned(), |v| v.to_string());
        errors.push(format!("Expected {field} to be {want}, got {shown}"));
    }
}

fn s(v: &str) -> Option<Value> {
    Some(Value::String(v.into()))
}

fn strs(v: &[&str]) -> Option<Value> {
    Some(Value::Array(v.iter().map(|x| Value::String((*x).into())).collect()))
}

fn tags() -> Option<Value> {
    let mut t = toml::Table::new();
    t.insert("env".into(), Value::String("production".into()));
    t.insert("node_id".into(), Value::String("xyz".into()));
    Some(Value::Table(t))
}

#[track_caller]
fn fail_on(errors: &[String]) {
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// upstream: features/file_config.testfile#scenario
/// note: upstream parses the runner's merged stdout+stderr; the port parses stdout and asserts stderr is empty; commands run in a fresh working directory with every ANYCABLE_* removed
#[test]
fn file_config() {
    let dir = TempDir::new("file-config");

    // Generate a configuration file with provided values
    let generated = run_sut(
        &[
            "--debug",
            "--noauth",
            "--port",
            "2024",
            "--broadcast_adapter=http,redisx",
            "--sse",
            "--metrics_tags=env:production,node_id:xyz",
            "--statsd_tags_format=datadog",
            "--print-config",
        ],
        &[
            ("ANYCABLE_SECRET", "file-secret"),
            ("ANYCABLE_NODE_ID", "node-1"),
            ("ANYCABLE_STATSD_HOST", "localhost:8125"),
        ],
        &dir.0,
    );
    // `sh -c '... --print-config > ./anycable.toml'`: stdout only.
    std::fs::write(dir.0.join("anycable.toml"), &generated.stdout).expect("writing anycable.toml");
    assert!(dir.0.join("anycable.toml").exists(), "Config file hasn't been generated");

    // Load from the default local path
    let config = print_config(&["--print-config"], &[], &dir.0);
    let mut errors = Vec::new();
    // top-level params
    check(&mut errors, "node ID", s("node-1"), config.get("node_id"));
    check(&mut errors, "noauth", Some(Value::Boolean(true)), config.get("noauth"));
    check(&mut errors, "secret", s("file-secret"), config.get("secret"));
    check(&mut errors, "broadcast adapters", strs(&["http", "redisx"]), config.get("broadcast_adapters"));
    check(&mut errors, "debug", Some(Value::Boolean(true)), dig(&config, &["logging", "debug"]));
    // nested params
    check(&mut errors, "server.port", Some(Value::Integer(2024)), dig(&config, &["server", "port"]));
    check(&mut errors, "sse.enabled", Some(Value::Boolean(true)), dig(&config, &["sse", "enabled"]));
    check(&mut errors, "metrics.tags", tags(), dig(&config, &["metrics", "tags"]));
    check(&mut errors, "metrics.statsd.host", s("localhost:8125"), dig(&config, &["metrics", "statsd", "host"]));
    check(&mut errors, "metrics.statsd.tags_format", s("datadog"), dig(&config, &["metrics", "statsd", "tags_format"]));
    // defaults
    check(&mut errors, "rpc.proxy_headers", strs(&["cookie"]), dig(&config, &["rpc", "proxy_headers"]));
    fail_on(&errors);

    // Ignoring the config file
    let config = print_config(&["--ignore-config-path", "--print-config"], &[], &dir.0);
    let mut errors = Vec::new();
    check(&mut errors, "node ID", None, config.get("node_id"));
    check(&mut errors, "secret", s("none"), config.get("secret"));
    fail_on(&errors);

    // Overriding config data
    let config = print_config(
        &["--port=2025", "--statsd_tags_format=influx", "--print-config"],
        &[
            ("ANYCABLE_NODE_ID", "node-2"),
            ("ANYCABLE_PORT", "312"),
            ("ANYCABLE_SECRET", "override"),
            ("ANYCABLE_DEBUG", "false"),
        ],
        &dir.0,
    );
    let mut errors = Vec::new();
    // top-level params
    check(&mut errors, "node ID", s("node-2"), config.get("node_id"));
    check(&mut errors, "noauth", Some(Value::Boolean(true)), config.get("noauth"));
    check(&mut errors, "secret", s("override"), config.get("secret"));
    check(&mut errors, "broadcast adapters", strs(&["http", "redisx"]), config.get("broadcast_adapters"));
    // nested params
    check(&mut errors, "server.port", Some(Value::Integer(2025)), dig(&config, &["server", "port"]));
    check(&mut errors, "sse.enabled", Some(Value::Boolean(true)), dig(&config, &["sse", "enabled"]));
    check(&mut errors, "metrics.tags", tags(), dig(&config, &["metrics", "tags"]));
    check(&mut errors, "metrics.statsd.host", s("localhost:8125"), dig(&config, &["metrics", "statsd", "host"]));
    check(&mut errors, "metrics.statsd.tags_format", s("influx"), dig(&config, &["metrics", "statsd", "tags_format"]));
    check(&mut errors, "debug", None, dig(&config, &["logging", "debug"]));
    fail_on(&errors);
}
