//! upstream file: pubsub/redis_test.go
//!
//! Real Redis at `CABBY_REDIS_URL` (required; upstream skips without Redis).
//! Each test uses its own channel prefix and ACL user (see
//! `cabby_testkit::services`), so channel names are `<prefix>:<upstream name>`.
//! Upstream also sets the unexported, test-only `trackingEvents` flag for its
//! waiters; the waiters here observe `PUBSUB NUMSUB` instead.

use std::time::Duration;

use cabby_core::common::StreamMessage;
use cabby_server::pubsub::RedisConfig;
use cabby_testkit::reference::pubsub::{
    Subscriber, TestHandler, new_redis_config, redis_config_decode_toml, redis_config_to_toml,
};
use cabby_testkit::reference::redis::Config as RedisClientConfig;
use cabby_testkit::services::{self, RedisScope};

use super::subscriber_test::{self as shared, Harness, ShutdownGuard};

/// Upstream waiters poll 5-6 times at 100-500 ms; one bounded deadline here.
const WAIT: Duration = Duration::from_secs(10);

struct Redis {
    scope: RedisScope,
    config: RedisConfig,
}

impl Redis {
    fn new() -> Self {
        let scope = services::redis().scope("pubsub");
        let mut rconfig = RedisClientConfig::new();
        rconfig.url = scope.url();
        let mut config = new_redis_config();
        config.channel = scope.name("__anycable_internal__");
        config.redis = Some(rconfig.config());
        Redis { scope, config }
    }
}

impl Harness for Redis {
    fn subscriber(&self, handler: &TestHandler) -> Subscriber {
        Subscriber::redis(handler, &self.config).expect("NewRedisSubscriber")
    }

    fn stream(&self, name: &str) -> String {
        self.scope.name(name)
    }

    fn internal(&self) -> String {
        self.config.channel.clone()
    }

    fn wait_subscriptions(&self, channel: &str, count: usize) -> Result<(), String> {
        self.scope.wait_numsub(channel, count as i64, WAIT).map_err(|e| e.to_string())
    }
}

/// upstream: pubsub/redis_test.go#TestRedisConfig__ToToml
#[test]
fn test_redis_config_to_toml() {
    let mut conf = new_redis_config();
    conf.channel = "_test_".into();

    let toml_str = redis_config_to_toml(&conf);

    assert!(toml_str.contains("channel = \"_test_\""), "{toml_str}");

    // Round-trip test
    let conf2 = redis_config_decode_toml(new_redis_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}

mod test_redis_common {
    use super::*;

    /// upstream: pubsub/redis_test.go#TestRedisCommon/Broadcast
    /// upstream: pubsub/redis_test.go#TestRedisCommon
    #[test]
    fn broadcast() {
        shared::broadcast(&Redis::new());
    }

    /// upstream: pubsub/redis_test.go#TestRedisCommon/Broadcast_commands
    /// upstream: pubsub/redis_test.go#TestRedisCommon
    #[test]
    fn broadcast_commands() {
        shared::broadcast_commands(&Redis::new());
    }

    /// upstream: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast
    /// upstream: pubsub/redis_test.go#TestRedisCommon
    #[test]
    fn subscribe_broadcast() {
        shared::subscribe_broadcast(&Redis::new());
    }

    /// upstream: pubsub/redis_test.go#TestRedisCommon/Re-transmit_commands
    /// upstream: pubsub/redis_test.go#TestRedisCommon
    #[test]
    fn retransmit_commands() {
        shared::retransmit_commands(&Redis::new());
    }

    /// upstream: pubsub/redis_test.go#TestRedisCommon/Subscribe_-_Broadcast_-_Unsubscribe_-_Broadcast
    /// note: replays the subscriptions of the earlier shared subtests first, so "a" is subscribed twice before one unsubscribe (Subscribe is idempotent).
    /// upstream: pubsub/redis_test.go#TestRedisCommon
    #[test]
    fn subscribe_broadcast_unsubscribe_broadcast() {
        shared::subscribe_broadcast_unsubscribe_broadcast(&Redis::new());
    }
}

/// upstream: pubsub/redis_test.go#TestRedisReconnect
/// note: only this test's ACL user is killed (upstream: CLIENT KILL TYPE pubsub on every client); restoration is observed as the subscriber's channels subscribed again.
///
/// Upstream drops connections with `CLIENT KILL TYPE pubsub` (every pub/sub
/// client on the server) and waits until `PUBSUB CHANNELS` is non-empty again.
/// Here only this test's ACL user is killed, and restoration is observed as the
/// subscriber's own channels being subscribed again.
#[test]
fn test_redis_reconnect() {
    let h = Redis::new();
    let handler = TestHandler::new();

    let subscriber = ShutdownGuard(Subscriber::redis(&handler, &h.config).expect("NewRedisSubscriber"));
    subscriber.0.start().expect("Start");

    h.wait_subscriptions(&h.internal(), 1).expect("wait internal");

    let reconnectos = h.stream("reconnectos");
    subscriber.0.subscribe(&reconnectos);
    h.wait_subscriptions(&reconnectos, 1).expect("wait reconnectos");

    let msg = |data: &str| StreamMessage { stream: reconnectos.clone(), data: data.into(), ..Default::default() };

    subscriber.0.broadcast(&msg("2022"));

    let received = handler.receive().expect("message");
    assert_eq!("2022", received.data);

    // Drop Redis pus/sub connections
    let killed = h.scope.kill_pubsub_connections(h.scope.user()).expect("CLIENT KILL");
    assert!(killed >= 1, "expected to kill the subscriber's pub/sub connection");
    h.wait_subscriptions(&h.internal(), 1).expect("pub/sub connection restored");

    h.wait_subscriptions(&reconnectos, 1).expect("wait reconnectos");

    subscriber.0.broadcast(&msg("2023"));

    let received = handler.receive().expect("message");
    assert_eq!("2023", received.data);
}
