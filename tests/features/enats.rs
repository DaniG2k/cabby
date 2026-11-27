//! Embedded NATS feature scenarios: a three-node super-cluster (two clusters
//! joined by a gateway) relaying broadcasts, and a three-node cluster with the
//! NATS broker (JetStream) sharing one epoch. All embedded NATS listeners get
//! free ports instead of the fixed 4242/4342/4442 ones; JetStream storage goes
//! to a per-test directory.
//!
//! Readiness: upstream sleeps (1 s between launches, 2 s for the servers to
//! find each other / for the epoch to propagate), then runs the wsdirector
//! scenario with `retrying(delay: 2)` (2 attempts). Here each node is launched
//! after the previous one listens on its cluster port, the sleeps become
//! probes (a NATS message published on the first node reaches the others; the
//! epoch seen on every node is the same), and the scenario keeps upstream's 2
//! attempts with a 2 s delay.

use std::path::Path;
use std::time::Duration;

use cabby_testkit::anyt_app::BENCHMARK_CHANNEL;
use cabby_testkit::ports::free_port;
use cabby_testkit::publish::{self, NatsSubscriber, Target};
use cabby_testkit::sut::Sut;
use cabby_testkit::wsdirector::{Client, Connection, Protocol, StepError};
use serde_json::{Value, json};

use crate::support::*;

const RETRY_DELAY: Duration = Duration::from_secs(2);
const ATTEMPTS: usize = 2;
const CLUSTER_DEADLINE: Duration = Duration::from_secs(40);

/// Readiness of the NATS cluster/super-cluster: a message published on
/// `from`'s embedded NATS reaches subscribers on every node in `to`.
fn await_nats_relay(from: u16, to: &[u16]) {
    let subject = cabby_testkit::services::unique("cabby_probe");
    let mut subs: Vec<_> = to
        .iter()
        .map(|p| NatsSubscriber::subscribe(local(*p), &subject).unwrap_or_else(|e| panic!("NATS probe: {e:#}")))
        .collect();
    let mut seen = vec![false; subs.len()];
    await_ready("the NATS cluster", CLUSTER_DEADLINE, || {
        publish::nats_publish(local(from), &subject, "probe").map_err(|e| format!("{e:#}"))?;
        for (s, seen) in subs.iter_mut().zip(seen.iter_mut()) {
            *seen = *seen || s.next(Duration::from_millis(200)).is_some();
        }
        if seen.iter().all(|s| *s) { Ok(()) } else { Err(format!("relayed to {seen:?}")) }
    });
}

fn nats(port: u16) -> String {
    format!("nats://127.0.0.1:{port}")
}

fn node(rpc: &str, flags: &[String]) -> Sut {
    let mut args: Vec<&str> = vec![rpc];
    args.extend(flags.iter().map(String::as_str));
    launch(&args, &[])
}

fn ac_client(sut: &Sut) -> Result<Client, StepError> {
    wsd(&ws_url(sut, "/cable"), Protocol::ActionCable, Connection::default())
}

fn hello() -> Value {
    json!({"message": "hello"})
}

/// upstream: features/enats_clusters.testfile#scenario
/// note: sleeps replaced by launching after cluster ports listen and a NATS relay probe across the super-cluster, then upstream's 2 attempts with a 2 s delay; the publisher performs after the subscribers subscribed (upstream race absorbed by its retry)
/// Adaptation: the publisher performs its broadcast after the subscribers have
/// subscribed (upstream runs the three clients concurrently without a
/// barrier, a race its retry absorbs).
#[test]
fn enats_clusters() {
    let app = rpc_app();
    let rpc = format!("--rpc_host={}", app.grpc_target());
    let store = TempDir::new("enats-clusters");
    let [a1, a2, a3, c1, c2, c3, g1, g3] = std::array::from_fn(|_| free_port());
    let dir = |n: &str| format!("--enats_store_dir={}", store.0.join(n).display());

    let mut node1 = node(
        &rpc,
        &[
            "--broadcast_adapter=nats".into(),
            "--embed_nats".into(),
            format!("--enats_addr={}", nats(a1)),
            format!("--enats_cluster={}", nats(c1)),
            format!("--enats_gateway={}", nats(g1)),
            dir("one"),
        ],
    );
    node1.process.wait_tcp(local(c1), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));
    let node2 = node(
        &rpc,
        &[
            "--broadcast_adapter=nats".into(),
            "--embed_nats".into(),
            format!("--enats_addr={}", nats(a2)),
            format!("--enats_cluster={}", nats(c2)),
            format!("--enats_cluster_routes={}", nats(c1)),
            dir("two"),
        ],
    );
    let node3 = node(
        &rpc,
        &[
            "--broadcast_adapter=nats".into(),
            "--embed_nats".into(),
            format!("--enats_addr={}", nats(a3)),
            format!("--enats_cluster={}", nats(c3)),
            "--enats_cluster_name=another-cluster".into(),
            format!("--enats_gateway={}", nats(g3)),
            format!("--enats_gateways=anycable-cluster:{}", nats(g1)),
            dir("three"),
        ],
    );
    // ANYCABLE_BROADCAST_ADAPTER=nats ANYCABLE_NATS_SERVERS=nats://localhost:4242
    app.set_broadcaster(Target::Nats { addr: local(a1), subject: "__anycable__".into() });
    // "We need to wait a bit for the NATS servers to find each other" (sleep 2)
    await_nats_relay(a1, &[a2, a3]);

    retrying(RETRY_DELAY, ATTEMPTS, || {
        let exact = |c: &mut Client, action: &str| {
            c.receive_channel(BENCHMARK_CHANNEL, &[], false, json!({"message": "hello", "action": action}), &[])
                .map(drop)
        };
        let mut publisher = step(ac_client(&node1))?;
        let mut subscriber = step(ac_client(&node2))?;
        let mut another = step(ac_client(&node3))?;
        let results = [
            ("publisher", publisher.subscribe(BENCHMARK_CHANNEL, &[]).map(drop)),
            ("subscriber", subscriber.subscribe(BENCHMARK_CHANNEL, &[]).map(drop)),
            ("another_subscriber", another.subscribe(BENCHMARK_CHANNEL, &[]).map(drop)),
        ];
        if let Some((g, Err(e))) = results.iter().find(|(_, r)| r.is_err()) {
            return Err(format!("Group {g}: {e}"));
        }
        publisher.perform(BENCHMARK_CHANNEL, &[], "broadcast", &hello());
        let results = [
            ("publisher", exact(&mut publisher, "broadcast").and_then(|()| exact(&mut publisher, "broadcastResult"))),
            ("subscriber", exact(&mut subscriber, "broadcast")),
            ("another_subscriber", exact(&mut another, "broadcast")),
        ];
        match results.iter().find(|(_, r)| r.is_err()) {
            Some((g, Err(e))) => Err(format!("Unexpected scenario result: Group {g}: {e}")),
            _ => Ok(()),
        }
    });
}

fn contains_epoch_kv(dir: &Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else { return false };
    entries.filter_map(Result::ok).any(|e| {
        let p = e.path();
        p.to_string_lossy().contains("KV__anycable_epoch_") || (p.is_dir() && contains_epoch_kv(&p))
    })
}

/// upstream: features/enats_broker.testfile#scenario
/// note: sleeps replaced by cluster-port readiness, a NATS relay probe and an epoch-propagation probe (nodes 2 and 3 deliver a broadcast with node 1's epoch), then upstream's 2 attempts with a 2 s delay; JetStream readiness via KV__anycable_epoch_ store files as upstream
#[test]
fn enats_broker() {
    let app = rpc_app();
    let rpc = format!("--rpc_host={}", app.grpc_target());
    let store = TempDir::new("enats-broker");
    let [a1, a2, a3, c1, c2, c3, g1] = std::array::from_fn(|_| free_port());
    let dir = |n: &str| format!("--enats_store_dir={}", store.0.join(n).display());
    let common = ["--broker=nats".to_owned(), "--pubsub=nats".to_owned()];

    let mut flags = common.to_vec();
    flags.extend([
        "--broadcast_adapter=http".into(),
        "--embed_nats".into(),
        format!("--enats_addr={}", nats(a1)),
        format!("--enats_cluster={}", nats(c1)),
        format!("--enats_gateway={}", nats(g1)),
        format!("--enats_cluster_routes={}", nats(c1)),
        dir("one"),
    ]);
    let mut node1 = node(&rpc, &flags);
    node1.process.wait_tcp(local(c1), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}")); // upstream: sleep 1
    let follower = |addr: u16, cluster: u16, name: &str| {
        let mut flags = common.to_vec();
        flags.extend([
            "--broadcast_adapter=nats".into(),
            "--embed_nats".into(),
            format!("--enats_addr={}", nats(addr)),
            format!("--enats_cluster={}", nats(cluster)),
            format!("--enats_cluster_routes={}", nats(c1)),
            dir(name),
        ]);
        let mut n = node(&rpc, &flags);
        n.process.wait_tcp(local(cluster), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}")); // upstream: sleep 1
        n
    };
    let node2 = follower(a2, c2, "two");
    let node3 = follower(a3, c3, "three");
    // ANYCABLE_BROADCAST_ADAPTER=http (to the first node's broadcast endpoint)
    app.set_broadcaster(Target::Http { addr: local(node1.broadcast_port), path: "/_broadcast".into(), key: None });
    node1.process.wait_tcp(local(node1.broadcast_port), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));

    // Wait for JetStream to be ready; we can do this by checking that the epoch
    // data has been written on disk
    let until = std::time::Instant::now() + READY_DEADLINE;
    while !contains_epoch_kv(&store.0) {
        assert!(std::time::Instant::now() < until, "JetStream is not ready");
        std::thread::sleep(Duration::from_millis(100));
    }

    let broadcast_from = |c: &mut Client, extra: &[(&str, Value)]| {
        c.receive_channel(BENCHMARK_CHANNEL, &[], true, json!({"message": "hello", "action": "broadcast"}), extra)
    };

    await_nats_relay(a1, &[a2, a3]);

    // We need to obtain epoch first
    let epoch = retrying(RETRY_DELAY, ATTEMPTS, || {
        let mut c = step(ac_client(&node1))?;
        step(c.subscribe(BENCHMARK_CHANNEL, &[]))?;
        c.perform(BENCHMARK_CHANNEL, &[], "broadcast", &hello());
        let received = step(broadcast_from(&mut c, &[("stream_id", json!("all"))]))?;
        match received.get("epoch").and_then(Value::as_str) {
            Some(e) if !e.is_empty() => Ok(e.to_owned()),
            _ => Err(format!("Epoch is missing: {received}")),
        }
    });

    // "Let's wait for epoch to be propagated to all nodes" (sleep 2): a broadcast
    // from node 1 arrives on nodes 2 and 3 carrying the same epoch.
    await_ready("epoch propagation", CLUSTER_DEADLINE, || {
        let mut probes = Vec::new();
        for n in [&node2, &node3] {
            let mut c = step(ac_client(n))?;
            step(c.subscribe(BENCHMARK_CHANNEL, &[]))?;
            probes.push(c);
        }
        let mut c = step(ac_client(&node1))?;
        step(c.subscribe(BENCHMARK_CHANNEL, &[]))?;
        c.perform(BENCHMARK_CHANNEL, &[], "broadcast", &hello());
        for p in &mut probes {
            let got = step(broadcast_from(p, &[("stream_id", json!("all"))]))?;
            if got.get("epoch") != Some(&json!(epoch)) {
                return Err(format!("epoch not propagated yet: {got}"));
            }
        }
        Ok(())
    });

    // `-s 4`: the subscriber groups have `multiplier: ":scale"`.
    let scale = 4;
    retrying(RETRY_DELAY, ATTEMPTS, || {
        let with_epoch = [("stream_id", json!("all")), ("epoch", json!(epoch))];
        let mut publisher = step(ac_client(&node1))?;
        step(publisher.subscribe(BENCHMARK_CHANNEL, &[]))?;
        let mut subscribers = Vec::new();
        for (group, n) in [("subscriber", &node2), ("another_subscriber", &node3)] {
            for _ in 0..scale {
                let mut c = step(ac_client(n)).map_err(|e| format!("Group {group}: {e}"))?;
                step(c.subscribe(BENCHMARK_CHANNEL, &[])).map_err(|e| format!("Group {group}: {e}"))?;
                subscribers.push((group, c));
            }
        }
        // wait_all
        publisher.perform(BENCHMARK_CHANNEL, &[], "broadcast", &hello());
        step(broadcast_from(&mut publisher, &with_epoch)).map_err(|e| format!("Group publisher: {e}"))?;
        step(publisher.receive_channel(
            BENCHMARK_CHANNEL,
            &[],
            false,
            json!({"message": "hello", "action": "broadcastResult"}),
            &[],
        ))
        .map_err(|e| format!("Group publisher: {e}"))?;
        for (group, c) in &mut subscribers {
            step(broadcast_from(c, &with_epoch)).map_err(|e| format!("Group {group}: {e}"))?;
        }
        Ok(())
    });
}

/// When the server process is killed (SIGKILL, no shutdown), its embedded NATS
/// server stops too and frees its port, as an in-process server does. Cabby's
/// `nats-server` child is stopped by a watchdog process that sees the killed
/// process's end of a pipe close.
/// regression-for: features/enats_broker.testfile#scenario
/// note: the port is free within 5 s of the SIGKILL (upstream: at once, the server is in-process)
#[test]
fn the_embedded_server_ends_with_a_killed_process() {
    let port = free_port();
    let mut sut = launch(&["--embed_nats", &format!("--enats_addr={}", nats(port))], &[]);
    wait_output(&sut, "the embedded NATS server", READY_DEADLINE, |out| out.contains("Embedded NATS server started"));
    assert!(std::net::TcpStream::connect(local(port)).is_ok(), "the embedded server listens");

    let killed = std::process::Command::new("kill")
        .args(["-KILL", &sut.process.pid().to_string()])
        .status()
        .expect("running kill");
    assert!(killed.success());
    sut.process.wait_exit(READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));

    await_ready(
        "the embedded server's port to be free",
        Duration::from_secs(5),
        || match std::net::TcpStream::connect(local(port)) {
            Err(_) => Ok(()),
            Ok(_) => Err("still accepting".to_owned()),
        },
    );
}

/// Ctrl-C in a terminal (SIGINT to the server's process group) shuts the
/// server down gracefully and it exits 0, as with an in-process NATS server
/// (upstream sets `NoSigs`). Cabby's `nats-server` child runs in a process
/// group of its own, so the signal reaches the server only, which stops the
/// child after the components that use it. The 1 s shutdown delay gives the
/// child's supervision (every 200 ms) time to see an exit, had the child got
/// the signal too.
/// regression-for: features/enats_broker.testfile#scenario
/// note: the server runs in a process group of its own, which the test signals as a terminal does
#[test]
fn ctrl_c_shuts_the_server_and_its_embedded_nats_down_cleanly() {
    use std::os::unix::process::CommandExt;

    let [port, broadcast_port, nats_port] = std::array::from_fn(|_| free_port());
    let store = TempDir::new("enats-ctrl-c");
    let mut cmd = std::process::Command::new(cabby_testkit::sut::sut_binary());
    cabby_testkit::sut::scrub_env(&mut cmd);
    cmd.env("ANYCABLE_DISABLE_TELEMETRY", "true")
        .args(["--host=127.0.0.1", &format!("--port={port}"), &format!("--http_broadcast_port={broadcast_port}")])
        .args(["--embed_nats", &format!("--enats_addr={}", nats(nats_port))])
        .args(["--broker=nats", "--pubsub=nats", "--shutdown_delay=1"])
        .arg(format!("--enats_store_dir={}", store.0.display()))
        .process_group(0);
    let mut server = cabby_testkit::process::Supervised::spawn("sut", cmd).unwrap_or_else(|e| panic!("{e:#}"));
    server.wait_tcp(local(port), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));

    let group = format!("-{}", server.pid());
    let sent = std::process::Command::new("kill").args(["-INT", "--", &group]).status().expect("running kill");
    assert!(sent.success(), "kill -INT -- {group}");
    let status = server.wait_exit(READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));

    let out = server.output();
    assert!(status.success(), "{status}. Output:\n{out}");
    assert!(!out.contains("exited unexpectedly"), "Output:\n{out}");
    assert!(std::net::TcpStream::connect(local(nats_port)).is_err(), "the embedded server stopped");
}
