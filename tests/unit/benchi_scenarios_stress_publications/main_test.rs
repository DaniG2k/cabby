//! upstream file: etc/benchi/scenarios/stress_publications/main_test.go

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cabby_testkit::http_stub::{self, HttpStub, Response};
use cabby_testkit::reference::benchi::stress_publications::{HookPool, RunOpts, run};
use cabby_testkit::reference::benchi::{ServerConfig, build_server};

/// Upstream `parseSummary`: the stdout key=value block as a map.
fn parse_summary(out: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for line in out.trim_end_matches('\n').split('\n') {
        if line.is_empty() {
            continue;
        }
        let (k, v) = line.split_once('=').unwrap_or_else(|| panic!("malformed summary line {line:?}"));
        m.insert(k.to_owned(), v.to_owned());
    }
    m
}

/// Upstream `atoi`.
fn atoi(s: Option<&String>) -> i64 {
    let s = s.map_or("", String::as_str);
    s.parse().unwrap_or_else(|_| panic!("expected integer, got {s:?}"))
}

fn get<'a>(m: &'a HashMap<String, String>, k: &str) -> Option<&'a str> {
    m.get(k).map(String::as_str)
}

/// Closes the first pool client right after the pool is built.
fn close_first_client(pool: &HookPool<'_>) {
    let mut first = true;
    pool.each(&mut |c| {
        if first {
            c.close();
            first = false;
        }
    });
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_SmallSmoke
#[test]
fn test_stress_publications_small_smoke() {
    let out =
        run(&["-c", "10", "-r", "10", "-d", "1s", "-S", "5", "-s", "2", "--drain-timeout", "5s"], RunOpts::default());
    assert_eq!(0, out.code, "expected exit 0 — stdout={:?} stderr={:?}", out.stdout, out.stderr);

    let summary = parse_summary(&out.stdout);
    assert_eq!(Some("10"), get(&summary, "clients_complete"), "all 10 clients should drain to expected");
    assert_eq!(Some("0"), get(&summary, "clients_short"));
    assert_eq!(Some("0"), get(&summary, "publications_dropped"));

    // Absolute-deadline scheduling: at -r 10 -d 1s the schedule is exactly 10 ticks.
    assert_eq!(Some("10"), get(&summary, "publications_issued"), "exact tick count for -r 10 -d 1s");
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_CadenceUnderArtificialLatency
#[test]
fn test_stress_publications_cadence_under_artificial_latency() {
    // A real server fronted by a slow proxy; the scenario points its publisher at the
    // proxy. The sleep is the injected latency (workload), not synchronization.
    let server = build_server(ServerConfig::default()).expect("BuildServer");
    let broadcast_url = server.broadcast_url();
    let proxy = HttpStub::start(move |req| {
        std::thread::sleep(Duration::from_millis(50));
        match http_stub::post(&broadcast_url, "application/json", &req.body) {
            Ok((status, body)) => Response { status, body },
            Err(e) => Response { status: 500, body: format!("{e:#}").into_bytes() },
        }
    });

    let out = run(
        &["-c", "1", "-r", "100", "-d", "1s", "-S", "1", "-s", "1", "--drain-timeout", "3s", "--max-inflight", "4096"],
        RunOpts { broadcast_url: Some(proxy.url()), ..Default::default() },
    );
    // The exit code may or may not be 0: the scenario's clients subscribe to its own
    // server and never see proxy-routed messages. Only the cadence is asserted.
    let _ = out.code;

    let summary = parse_summary(&out.stdout);
    let issued = atoi(summary.get("publications_issued"));
    let dropped = atoi(summary.get("publications_dropped"));
    // Exactly 100 ticks for -r 100 -d 1s; HTTP latency lives on the workers.
    assert_eq!(100, issued, "cadence is decoupled from HTTP: exact schedule of -r 100 -d 1s");
    assert_eq!(0, dropped, "max-inflight=4096 was plenty; drops indicate a regression in the scheduler-decoupling");

    let _ = server.shutdown();
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_OneClientShort
#[test]
fn test_stress_publications_one_client_short() {
    // The hook closes one client after the pool is built; its count never grows and
    // reconciliation marks the run incomplete.
    let out = run(
        &["-c", "5", "-r", "10", "-d", "500ms", "-S", "1", "-s", "1", "--drain-timeout", "2s"],
        RunOpts { setup_hook: Some(Box::new(close_first_client)), ..Default::default() },
    );
    assert_ne!(0, out.code, "scenario must exit non-zero when a client is short");

    let summary = parse_summary(&out.stdout);
    assert_eq!(Some("1"), get(&summary, "clients_short"), "exactly one client was closed and should be short");
    assert!(atoi(summary.get("messages_missing")) > 0, "the closed client should contribute missing messages");
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_SubscribeFailureAborts
#[test]
fn test_stress_publications_subscribe_failure_aborts() {
    // One client gets the empty stream name, which the server rejects; at
    // tolerance=0 the pool build aborts.
    let streams = vec![vec!["a".to_owned()], vec![String::new()], vec!["c".to_owned()]];
    let out = run(
        &["-c", "3", "-r", "10", "-d", "500ms", "-S", "3", "-s", "1", "--setup-failure-tolerance", "0"],
        RunOpts { stream_subsets: Some(streams), ..Default::default() },
    );
    assert_ne!(0, out.code, "subscribe failure at tolerance=0 must abort with non-zero exit");
    assert!(out.stdout.contains("pool setup failed"), "stdout should explain the abort: {}", out.stdout);
    assert!(out.stdout.contains("client 1"), "stdout should name the offending client: {}", out.stdout);
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_DrainTimeoutHonored
#[test]
fn test_stress_publications_drain_timeout_honored() {
    // With a client closed, "all clients drained" is unreachable; the drain must give
    // up at its timeout: 500ms publish + 1s drain ≈ 1.5s, with 2s slack.
    let start = Instant::now();
    let out = run(
        &["-c", "5", "-r", "10", "-d", "500ms", "-S", "1", "-s", "1", "--drain-timeout", "1s"],
        RunOpts { setup_hook: Some(Box::new(close_first_client)), ..Default::default() },
    );
    let elapsed = start.elapsed();

    assert_ne!(0, out.code);
    assert!(elapsed < Duration::from_millis(3500), "scenario hung past drain timeout: ran {elapsed:?}");
    let summary = parse_summary(&out.stdout);
    assert!(atoi(summary.get("clients_short")) >= 1, "incomplete drain should be reported, not hang");
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_HighCadenceMatchesSchedule
#[test]
fn test_stress_publications_high_cadence_matches_schedule() {
    // -r 500 -d 1s: 500 ticks at 2ms. Absolute-deadline scheduling fires every tick
    // <= end, so issued is exact; late publishes are diagnostic only.
    let out =
        run(&["-c", "1", "-r", "500", "-d", "1s", "-S", "1", "-s", "1", "--drain-timeout", "3s"], RunOpts::default());
    assert_eq!(0, out.code, "expected exit 0 — stdout={:?} stderr={:?}", out.stdout, out.stderr);

    let summary = parse_summary(&out.stdout);
    assert_eq!(Some("500"), get(&summary, "publications_issued"), "absolute scheduling hits 500 exactly");
    assert_eq!(Some("0"), get(&summary, "publications_dropped"));
    // The key must exist and parse as an integer.
    atoi(summary.get("scheduler_late_publishes"));
}

/// upstream: etc/benchi/scenarios/stress_publications/main_test.go#TestStressPublications_FlagDefaults
#[test]
fn test_stress_publications_flag_defaults() {
    let out = run(&[], RunOpts::default());
    assert_ne!(0, out.code, "no flags should error rather than run a benchmark by accident");
    assert!(out.stderr.contains("Usage"), "expected flag usage on stderr: {}", out.stderr);
}
