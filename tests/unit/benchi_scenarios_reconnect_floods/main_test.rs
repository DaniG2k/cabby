//! upstream file: etc/benchi/scenarios/reconnect_floods/main_test.go

use std::time::{Duration, Instant};

use cabby_testkit::reference::benchi::reconnect_floods::{
    Sample, command_stall, parse_flags, run, run_interrupted_after,
};

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestParseFlags_RequiresClientsAndWaves
#[test]
fn test_parse_flags_requires_clients_and_waves() {
    let out = run(&["-c", "10"]);
    assert_eq!(2, out.code);
    assert!(out.stderr.contains("--waves must be set"), "{}", out.stderr);

    let out = run(&["-c", "10", "--waves", "5,20"]);
    assert_eq!(2, out.code);
    assert!(out.stderr.contains("exceeds -c"), "{}", out.stderr);
}

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestParseFlags_Waves
#[test]
fn test_parse_flags_waves() {
    let cfg = parse_flags(&["-c", "100", "--waves", "10, 20,30"]).expect("flags parse");
    assert_eq!(vec![10, 20, 30], cfg.waves);
    assert_eq!(3, cfg.s);
    assert_eq!(Duration::from_millis(100), cfg.rpc_latency);
}

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestRun_SmallWavesRecover
#[test]
fn test_run_small_waves_recover() {
    let out = run(&[
        "-c",
        "30",
        "-s",
        "2",
        "--waves",
        "10,20",
        "--wave-interval",
        "2s",
        "--settle",
        "100ms",
        "--recovery-timeout",
        "10s",
        "--rpc-latency",
        "2ms",
        "--backoff-min",
        "20ms",
        "--non-interactive",
    ]);

    let o = &out.stdout;
    assert_eq!(0, out.code, "{o}{}", out.stderr);
    assert!(o.contains("setup_ok=true\n"), "{o}");
    assert!(o.contains("recovered=true\n"), "{o}");
    assert!(o.contains("wave_1_dropped=10\n"), "{o}");
    assert!(o.contains("wave_2_dropped=20\n"), "{o}");
    assert!(o.contains("close_dropped=30\n"), "{o}");
    // The only closes are the 30 drops; each one is followed by a reconnect.
    assert!(o.contains("reconnects=30\n"), "{o}");
    assert!(!o.contains("wave_2_recovery_seconds=-1"), "{o}");
    // Runtime-specific adaptation: upstream reads the embedded disconnect queue
    // directly; with an external server the drain is observed through the
    // `disconnect_queue_size` metrics gauge (refreshed every second).
    assert!(!o.contains("wave_2_drained_seconds=-1"), "{o}");
    assert!(o.contains("disconnect_discarded=0\n"), "{o}");
    assert!(o.contains("rpc_command_after_close_calls=0\n"), "{o}");
    // The default 10 probes ping once per second for several seconds.
    assert!(!o.contains("probe_count=0\n"), "{o}");
}

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestRun_SetupFailsWhenSubscribeAlwaysTimesOut
#[test]
fn test_run_setup_fails_when_subscribe_always_times_out() {
    let out = run(&[
        "-c",
        "5",
        "--waves",
        "1",
        "--rpc-command-latency",
        "500ms",
        "--sub-retry-interval",
        "100ms",
        "--sub-retries-max",
        "0",
        "--setup-timeout",
        "1s",
        "--backoff-min",
        "50ms",
        "--non-interactive",
    ]);

    let o = &out.stdout;
    assert_eq!(1, out.code, "{o}");
    assert!(o.contains("setup_ok=false\n"), "{o}");
    assert!(o.contains("waves_fired=0\n"), "{o}");
    assert!(!o.contains("close_subscribe_timeout=0\n"), "{o}");
}

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestCommandStall
#[test]
fn test_command_stall() {
    // states[1] is Subscribing, rpc_done[1] is Command (ClientState::ALL and
    // RpcMethod::ALL order, as upstream's array indexes).
    let mk = |sec: u64, subscribing: i64, commands: i64| {
        let mut s = Sample { at: Duration::from_secs(sec), ..Default::default() };
        s.states[1] = subscribing;
        s.rpc_done[1] = commands;
        s
    };
    let samples = [
        mk(0, 0, 0),
        mk(1, 5, 10), // progress
        mk(2, 5, 10), // stall starts at 1s
        mk(3, 5, 10),
        mk(4, 5, 12), // progress
        mk(5, 5, 12), // stall again
    ];
    let (cur, longest) = command_stall(&samples);
    assert_eq!(Duration::from_secs(1), cur);
    assert_eq!(Duration::from_secs(2), longest);
}

/// upstream: etc/benchi/scenarios/reconnect_floods/main_test.go#TestRun_InterruptPrintsSummary
#[test]
fn test_run_interrupt_prints_summary() {
    // Slow Disconnect calls keep the queue non-empty, so without the interrupt the run
    // would wait for the queue to drain. Upstream passes a context that is done after
    // 2s to runWithConfig.
    let began = Instant::now();
    let out = run_interrupted_after(
        &[
            "-c",
            "10",
            "-s",
            "1",
            "--waves",
            "5",
            "--settle",
            "100ms",
            "--recovery-timeout",
            "1m",
            "--rpc-latency",
            "2ms",
            "--rpc-disconnect-latency",
            "3s",
            "--backoff-min",
            "20ms",
            "--non-interactive",
        ],
        Duration::from_secs(2),
    );

    let o = &out.stdout;
    assert_eq!(130, out.code, "{o}");
    assert!(began.elapsed() < Duration::from_secs(15));
    assert!(o.contains("interrupted=true\n"), "{o}");
    // Clients recover fast; the run then waits for the slow queue to drain.
    assert!(o.contains("recovered=true\n"), "{o}");
    assert!(!o.contains("wave_1_recovery_seconds=-1"), "{o}");
    assert!(o.contains("wave_1_drained_seconds=-1.00\n"), "{o}");
}
