//! upstream file: broadcast/legacy_redis_test.go
//!
//! Real Redis at `CABBY_REDIS_URL` (required; upstream skips without Redis).
//! Channels are unique per test (`<prefix>:<upstream name>`), the broadcaster
//! connects as the test's own ACL user, and the reconnect test kills only that
//! user's pub/sub connections (upstream: `CLIENT KILL TYPE pubsub`, every pub/sub
//! client on the server). The mock handler only expects `HandlePubSub`.

use std::collections::HashMap;
use std::time::Duration;

use cabby_server::broadcast::LegacyRedisConfig;
use cabby_testkit::reference::broadcast::{
    Broadcaster, HandlerCall, MockHandler, legacy_redis_config_decode_toml, legacy_redis_config_to_toml,
    new_legacy_redis_config,
};
use cabby_testkit::reference::redis::Config as RedisClientConfig;
use cabby_testkit::services::{self, RedisScope, wait_until};
use cabby_testkit::{Backend, backend};

/// Upstream `drainBroadcasts`: stop after 1 s without a message.
const DRAIN_IDLE: Duration = Duration::from_secs(1);
/// Upstream polls `PUBSUB CHANNELS` up to 11 times every 200-500 ms.
const WAIT: Duration = Duration::from_secs(10);

/// `defer broadcaster.Shutdown(...)`; skipped while a Rust stub is unwinding.
struct ShutdownGuard<'a>(&'a Broadcaster);

impl Drop for ShutdownGuard<'_> {
    fn drop(&mut self) {
        if backend() == Backend::Go || !std::thread::panicking() {
            let _ = self.0.shutdown();
        }
    }
}

/// `utils.ToJSON(map[string]string{"stream": stream, "data": data})` (sorted keys).
fn payload(stream: &str, data: &str) -> String {
    format!("{{\"data\":\"{data}\",\"stream\":\"{stream}\"}}")
}

/// The handler's `json.Unmarshal(data, &msg)` of each `HandlePubSub` call.
fn decode(calls: Vec<HandlerCall>) -> Vec<HashMap<String, String>> {
    calls
        .into_iter()
        .map(|c| {
            assert_eq!("HandlePubSub", c.kind, "unexpected mock call");
            serde_json::from_str(&c.payload).unwrap_or_default()
        })
        .collect()
}

/// Upstream `waitLegacyRedisPubSubSubscription`: the channel shows up in
/// `PUBSUB CHANNELS`.
fn wait_subscription(scope: &RedisScope, channel: &str) {
    wait_until(&format!("subscription to {channel}"), WAIT, || scope.channel_active(channel).then_some(()))
        .expect("Timeout waiting for pubsub subscription");
}

fn config(scope: &RedisScope, channel: &str) -> LegacyRedisConfig {
    let mut rconf = RedisClientConfig::new();
    rconf.url = scope.url();
    let mut config = new_legacy_redis_config();
    config.redis = Some(rconf.config());
    config.channel = scope.name(channel);
    config
}

/// upstream: broadcast/legacy_redis_test.go#TestLegacyRedisConfig__ToToml
#[test]
fn test_legacy_redis_config_to_toml() {
    let mut conf = new_legacy_redis_config();
    conf.channel = "_test_".into();

    let toml_str = legacy_redis_config_to_toml(&conf);

    assert!(toml_str.contains("channel = \"_test_\""), "{toml_str}");

    // Round-trip test
    let conf2 = legacy_redis_config_decode_toml(new_legacy_redis_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}

mod test_legacy_redis_broadcaster {
    use super::*;

    /// upstream: broadcast/legacy_redis_test.go#TestLegacyRedisBroadcaster/Handles_pubsub_broadcasts
    #[test]
    fn handles_pubsub_broadcasts() {
        let scope = services::redis().scope("legacy_redis");
        let config = config(&scope, "__anycable__");
        let handler = MockHandler::new();

        let broadcaster = Broadcaster::legacy_redis(&handler, &config);

        broadcaster.start().expect("Start");
        let _guard = ShutdownGuard(&broadcaster);

        // Wait for subscription to be established
        wait_subscription(&scope, &config.channel);

        scope.publish(&config.channel, &payload("legacy_test", "456_test")).expect("PUBLISH");

        let messages = decode(handler.drain(DRAIN_IDLE));
        assert_eq!(1, messages.len(), "Expected 1 message, got {}", messages.len());

        let msg = &messages[0];

        assert_eq!(Some("legacy_test"), msg.get("stream").map(String::as_str));
        assert_eq!(Some("456_test"), msg.get("data").map(String::as_str));
    }
}

/// upstream: broadcast/legacy_redis_test.go#TestLegacyRedisBroadcasterReconnect
#[test]
fn test_legacy_redis_broadcaster_reconnect() {
    let scope = services::redis().scope("legacy_redis");
    let config = config(&scope, "__anycable_legacy_reconnect_test__");
    let handler = MockHandler::new();

    let broadcaster = Broadcaster::legacy_redis(&handler, &config);

    broadcaster.start().expect("Start");
    let _guard = ShutdownGuard(&broadcaster);

    // Wait for subscription to be established
    wait_subscription(&scope, &config.channel);

    // Send first message
    scope.publish(&config.channel, &payload("reconnect_test", "before_disconnect")).expect("PUBLISH");

    let messages = decode(handler.drain(DRAIN_IDLE));
    assert_eq!(1, messages.len(), "Expected 1 message before disconnect, got {}", messages.len());
    assert_eq!(Some("before_disconnect"), messages[0].get("data").map(String::as_str));

    // Drop Redis pub/sub connections (mimics connection failure)
    scope.kill_pubsub_connections(scope.user()).expect("CLIENT KILL");
    // Upstream waitRedisPubSubConnectionsRestored: any channel active again.
    wait_subscription(&scope, &config.channel);

    // Wait for subscription to be re-established
    wait_subscription(&scope, &config.channel);

    // Send message after reconnection
    scope.publish(&config.channel, &payload("reconnect_test", "after_reconnect")).expect("PUBLISH");

    let messages = decode(handler.drain(DRAIN_IDLE));
    assert_eq!(1, messages.len(), "Expected 1 message after reconnect, got {}", messages.len());
    assert_eq!(Some("after_reconnect"), messages[0].get("data").map(String::as_str));
}
