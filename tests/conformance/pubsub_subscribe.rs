//! Multi-node pub/sub as several nodes use it: a message is published to a
//! stream right after a client of this server got its subscription to it
//! confirmed, and the client must receive it. Each round subscribes to a
//! stream the node has no subscription to, so the node's pub/sub
//! subscription to it is new. Setup as in `issues.rs` (fake RPC application,
//! HTTP broadcast endpoint).

use std::time::Duration;

use cabby_server::postgres::pubsub::encode;
use cabby_server::postgres::shard;
use cabby_testkit::cable_client::{CableClient, Options};
use cabby_testkit::fake_rpc::FakeRpc;
use cabby_testkit::postgres::{PgScope, rust_only};
use cabby_testkit::services;
use cabby_testkit::sut::Sut;
use serde_json::json;

use crate::issues::{fake_app, identifier, start_server, subscribe};

/// Subscriptions, each followed at once by a publication.
const ROUNDS: usize = 200;

fn client(sut: &Sut) -> CableClient {
    let options = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), options).expect("connect");
    assert_eq!(Some("welcome"), c.receive().expect("welcome")["type"].as_str());
    c
}

fn payload(stream: &str, round: usize) -> String {
    json!({"stream": stream, "data": json!({"round": round}).to_string()}).to_string()
}

/// Runs the rounds: `stream(round)` names the round's stream, `publish`
/// publishes the payload once the subscription is confirmed, and `after`
/// runs once the round is over.
fn rounds(
    c: &mut CableClient,
    stream: impl Fn(usize) -> String,
    publish: impl Fn(&str, &str),
    after: impl Fn(&mut CableClient, &str),
) {
    let mut lost = Vec::new();
    for round in 0..ROUNDS {
        let stream = stream(round);
        subscribe(c, &stream);
        publish(&stream, &payload(&stream, round));
        match c.receive_within(Duration::from_secs(3)) {
            Ok(m) if m["message"]["round"] == round => {}
            Ok(m) => panic!("round {round}: unexpected message {m}"),
            Err(_) => lost.push(round),
        }
        after(c, &stream);
    }
    assert!(lost.is_empty(), "{} of {ROUNDS} messages were not delivered (rounds {lost:?})", lost.len());
}

/// external-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
/// note: Redis pub/sub; each round subscribes to a new stream and publishes to it with PUBLISH (as another node's broker does) as soon as the client has the confirmation.
/// note: Go waits for the SUBSCRIBE reply inside the subscribe command (rueidis returns from Do once Redis confirms); Cabby waits for Redis' confirmation before it sends the subscription's confirmation, at most 1 s (D-105).
#[test]
fn redis_messages_published_right_after_a_confirmation_are_delivered() {
    let redis = services::redis().scope("subrace");
    let fake = fake_app();
    let sut = start_server(&fake, &redis.url());
    let mut c = client(&sut);
    rounds(
        &mut c,
        |round| redis.name(&format!("s{round}")),
        |stream, payload| {
            redis.command(&["PUBLISH", stream, payload]).expect("PUBLISH");
        },
        |_, _| {},
    );
}

fn postgres_server(fake: &FakeRpc, scope: &PgScope) -> Sut {
    let config = scope.config();
    let args = [
        "--pubsub=postgres".to_owned(),
        format!("--postgres_url={}", config.url),
        format!("--postgres_schema={}", config.schema),
        format!("--postgres_channel={}", config.channel),
        format!("--rpc_host={}", fake.grpc_target()),
        "--broadcast_adapter=http".to_owned(),
    ];
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Sut::try_start(&args, &[]).unwrap_or_else(|e| panic!("{e:#}"))
}

/// regression-for: pubsub/subscriber_test.go#TestLegacySubscriber/Broadcast
/// note: Deviations D-070, D-105: Postgres pub/sub; each round subscribes to a stream, publishes to it with pg_notify on the stream's shard channel (as another node does) as soon as the client has the confirmation, and unsubscribes, so the shard is listened to again in the next round.
/// note: not_observable on Go: AnyCable has no Postgres adapters.
#[test]
fn deviation_d105_postgres_messages_published_right_after_a_confirmation_are_delivered() {
    rust_only();
    let scope = PgScope::new("subrace");
    let fake = fake_app();
    let sut = postgres_server(&fake, &scope);
    let mut c = client(&sut);
    let channel = format!("{}_{}", scope.config().channel, shard("pg_subrace"));
    rounds(
        &mut c,
        |_| "pg_subrace".to_owned(),
        |_, payload| scope.notify(&channel, &encode("s", payload.as_bytes())),
        |c, stream| {
            c.send(&json!({"command": "unsubscribe", "identifier": identifier(stream)}));
        },
    );
}
