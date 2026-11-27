//! Rust-only scenarios pinning fixes for open upstream issues at the level of
//! the Redis pub/sub subscriber (see compat/deviations.md, D-003 and D-004).
//! They run against both backends; a failure on Go is the upstream defect.
//!
//! The subscriber connects through `cabby_testkit::fault_proxy`, which can drop
//! a subscription behind the subscriber's back (an injected `UNSUBSCRIBE`),
//! leave its connection half-open, or hold its traffic.
//!
//! Also here: what the subscriber reports to the node while a subscription is
//! being set up (D-105; not observable on Go).

use std::time::{Duration, Instant};

use cabby_testkit::fault_proxy::FaultProxy;
use cabby_testkit::reference::pubsub::{Subscriber, TestHandler, new_redis_config};
use cabby_testkit::reference::redis::Config as RedisClientConfig;
use cabby_testkit::services::{self, RedisScope};
use serde_json::json;

use super::subscriber_test::ShutdownGuard;

const WAIT: Duration = Duration::from_secs(10);

struct Setup {
    scope: RedisScope,
    proxy: FaultProxy,
    handler: TestHandler,
    subscriber: ShutdownGuard,
}

fn setup(tag: &str) -> Setup {
    let scope = services::redis().scope(tag);
    let proxy = FaultProxy::start(services::redis().addr());
    let mut rconfig = RedisClientConfig::new();
    rconfig.url = scope.url_via(proxy.addr());
    let mut config = new_redis_config();
    config.channel = scope.name("__anycable_internal__");
    config.redis = Some(rconfig.config());
    let handler = TestHandler::new();
    let subscriber = ShutdownGuard(Subscriber::redis(&handler, &config).expect("NewRedisSubscriber"));
    subscriber.0.start().expect("Start");
    scope.wait_numsub(&config.channel, 1, WAIT).expect("internal channel subscribed");
    Setup { scope, proxy, handler, subscriber }
}

/// Publishes a broadcast for `stream` every 250 ms (through the test's own
/// connection) until the handler receives it or `within` passes.
fn delivered_within(s: &Setup, stream: &str, text: &str, within: Duration) -> bool {
    let payload = json!({"stream": stream, "data": text}).to_string();
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        s.scope.publish(stream, &payload).expect("PUBLISH");
        let wait_until = Instant::now() + Duration::from_millis(250);
        while Instant::now() < wait_until {
            if s.handler.receive().is_some_and(|m| m.stream == stream && m.data == text) {
                return true;
            }
        }
    }
    false
}

/// issue: anycable/anycable/issues/275
/// note: Redis drops the stream subscription (injected UNSUBSCRIBE on the subscriber connection) while the connection stays up.
/// note: Cabby invariant (D-003): the subscriber notices the unsolicited unsubscribe and resubscribes; delivery resumes within 15s.
#[test]
fn resubscribes_after_redis_drops_a_stream_subscription() {
    let s = setup("pubsub275u");
    let stream = s.scope.name("orders");
    s.subscriber.0.subscribe(&stream);
    s.scope.wait_numsub(&stream, 1, WAIT).expect("stream subscribed");
    assert!(s.proxy.await_subscriber(WAIT), "no Redis subscriber connection seen");
    assert!(delivered_within(&s, &stream, "before", WAIT), "baseline delivery failed");

    assert!(s.proxy.inject_command(&["UNSUBSCRIBE", &stream]) > 0, "nothing to inject into");
    services::wait_until("Redis drops the subscription", Duration::from_secs(5), || {
        (s.scope.numsub(&stream) == 0).then_some(())
    })
    .expect("subscription dropped");

    assert!(
        delivered_within(&s, &stream, "after", Duration::from_secs(15)),
        "broadcasts were not delivered within 15s after Redis dropped the subscription"
    );
    assert_eq!(1, s.scope.numsub(&stream), "exactly one subscription after recovery");
}

/// issue: anycable/anycable/issues/275
/// note: The Redis side of the subscriber connection closes while the subscriber's socket stays half-open (no FIN/RST).
/// note: Cabby invariant (D-003): the heartbeat detects the dead connection, the subscriber reconnects and resubscribes every stream; delivery resumes within 30s.
#[test]
fn recovers_after_the_subscriber_connection_goes_half_open() {
    let s = setup("pubsub275h");
    let stream = s.scope.name("orders");
    s.subscriber.0.subscribe(&stream);
    s.scope.wait_numsub(&stream, 1, WAIT).expect("stream subscribed");
    assert!(s.proxy.await_subscriber(WAIT), "no Redis subscriber connection seen");
    assert!(delivered_within(&s, &stream, "before", WAIT), "baseline delivery failed");

    assert!(s.proxy.sever_subscribers() > 0, "nothing to sever");
    assert!(
        delivered_within(&s, &stream, "after", Duration::from_secs(30)),
        "broadcasts were not delivered within 30s after the subscriber connection went half-open"
    );
}

/// issue: anycable/anycable/issues/263
/// note: Cabby invariant (D-004): once a stream is unsubscribed (even after repeated subscribe/unsubscribe churn), the subscriber holds no Redis subscription for it.
#[test]
fn releases_channels_of_unsubscribed_streams() {
    let s = setup("pubsub263");
    let streams: Vec<String> = (0..20).map(|i| s.scope.name(&format!("s{i}"))).collect();
    for st in &streams {
        s.subscriber.0.subscribe(st);
    }
    for st in &streams {
        s.scope.wait_numsub(st, 1, WAIT).expect("stream subscribed");
    }
    for _ in 0..3 {
        for st in &streams {
            s.subscriber.0.unsubscribe(st);
            s.subscriber.0.subscribe(st);
        }
    }
    for st in &streams {
        s.subscriber.0.unsubscribe(st);
    }
    for st in &streams {
        s.scope.wait_numsub(st, 0, WAIT).expect("stream channel released");
    }
    let pattern = format!("{}:s*", s.scope.prefix);
    let left = s.scope.command(&["PUBSUB", "CHANNELS", &pattern]).expect("PUBSUB CHANNELS");
    assert!(left.items().is_empty(), "leaked Redis subscriptions: {left:?}");
}

/// regression-for: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
/// note: Deviation D-105: while the connection to Redis is held (fault proxy frozen), a new stream's subscription is reported as not active; once Redis confirms it, it is active, Redis lists the subscriber, and a message published then is delivered.
/// note: not_observable on Go: its subscribers report nothing (Go's Subscribe returns once Redis confirms).
#[test]
fn deviation_d105_a_stream_is_pending_until_redis_confirms_it() {
    let s = setup("d105");
    let stream = s.scope.name("fresh");
    assert!(!s.handler.pending(&stream));

    s.proxy.freeze();
    s.subscriber.0.subscribe(&stream);
    let pending = s.handler.pending(&stream);
    s.proxy.thaw();
    assert!(pending, "the subscription was not reported while Redis had not confirmed it");

    services::wait_until("Redis confirms the subscription", WAIT, || (!s.handler.pending(&stream)).then_some(()))
        .expect("active");
    assert_eq!(1, s.scope.numsub(&stream));
    let payload = json!({"stream": stream, "data": "live"}).to_string();
    s.scope.publish(&stream, &payload).expect("PUBLISH");
    assert_eq!("live", s.handler.receive().expect("message").data);
}
