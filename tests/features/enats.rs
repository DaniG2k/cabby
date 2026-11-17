//! Embedded NATS feature scenarios: a three-node super-cluster (two clusters
//! joined by a gateway) relaying broadcasts, and a three-node cluster with the
//! NATS broker (JetStream) sharing one epoch. All embedded NATS listeners get
//! free ports instead of the fixed 4242/4342/4442 ones; JetStream storage goes
//! to a per-test directory.
//!
//! Readiness: upstream sleeps (1 s between launches, 2 s for the servers to
//! find each other / for the epoch to propagate) and then retries the
//! wsdirector run once after 2 s. Here each node is launched after the
//! previous one listens on its cluster port, and the scenario is retried every
//! 2 s within a bounded window until the cluster delivers.

use std::path::Path;
use std::time::Duration;

use cabby_testkit::anyt_app::BENCHMARK_CHANNEL;
use cabby_testkit::ports::free_port;
use cabby_testkit::publish::Target;
use cabby_testkit::sut::Sut;
use cabby_testkit::wsdirector::{Client, Connection, Protocol, StepError};
use serde_json::{Value, json};

use crate::support::*;

const RETRY_DELAY: Duration = Duration::from_secs(2);
const CLUSTER_WINDOW: Duration = Duration::from_secs(40);

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
/// note: sleeps replaced by launching after cluster ports listen and retrying every 2 s within 40 s; the publisher performs after the subscribers subscribed (upstream race absorbed by its retry)
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

    retrying(RETRY_DELAY, CLUSTER_WINDOW, || {
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
/// note: sleeps replaced by cluster-port readiness and retrying every 2 s within 40 s; JetStream readiness via KV__anycable_epoch_ store files as upstream
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

    // We need to obtain epoch first
    let epoch = retrying(RETRY_DELAY, CLUSTER_WINDOW, || {
        let mut c = step(ac_client(&node1))?;
        step(c.subscribe(BENCHMARK_CHANNEL, &[]))?;
        c.perform(BENCHMARK_CHANNEL, &[], "broadcast", &hello());
        let received = step(broadcast_from(&mut c, &[("stream_id", json!("all"))]))?;
        match received.get("epoch").and_then(Value::as_str) {
            Some(e) if !e.is_empty() => Ok(e.to_owned()),
            _ => Err(format!("Epoch is missing: {received}")),
        }
    });

    // `-s 4`: the subscriber groups have `multiplier: ":scale"`.
    let scale = 4;
    retrying(RETRY_DELAY, CLUSTER_WINDOW, || {
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
