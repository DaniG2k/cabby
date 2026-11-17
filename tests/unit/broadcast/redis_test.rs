//! upstream file: broadcast/redis_test.go
//!
//! Real Redis at `CABBY_REDIS_URL` (required; upstream skips without Redis).
//! Upstream uses the default stream `__anycable__` (destroying group `bx` in
//! `init`) and publishes/counts consumers through the broadcaster's unexported
//! streamer client. Here the stream is unique per test, the test publishes with
//! its own connection, and stream readers are counted for the test's ACL user
//! (upstream counts `cmd=xreadgroup` over all clients). The mock handler only
//! expects `HandleBroadcast`.

use std::collections::HashMap;
use std::time::Duration;

use cabby_server::broadcast::RedisConfig;
use cabby_testkit::reference::broadcast::{
    Broadcaster, HandlerCall, MockHandler, new_redis_config, redis_config_decode_toml, redis_config_to_toml,
};
use cabby_testkit::reference::redis::Config as RedisClientConfig;
use cabby_testkit::services::{self, RedisScope};
use cabby_testkit::{Backend, backend};

/// Upstream `drainBroadcasts`: stop after 1 s without a message.
const DRAIN_IDLE: Duration = Duration::from_secs(1);
/// Upstream `waitRedisStreamConsumers`: 6 attempts, 500 ms apart.
const CONSUMERS: Duration = Duration::from_secs(10);

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

fn decode(calls: Vec<HandlerCall>) -> Vec<HashMap<String, String>> {
    calls
        .into_iter()
        .map(|c| {
            assert_eq!("HandleBroadcast", c.kind, "unexpected mock call");
            serde_json::from_str(&c.payload).unwrap_or_default()
        })
        .collect()
}

fn setup() -> (RedisScope, RedisConfig, MockHandler) {
    let scope = services::redis().scope("broadcast_redis");
    let mut rconfig = RedisClientConfig::new();
    rconfig.url = scope.url();
    let mut config = new_redis_config();
    config.redis = Some(rconfig.config());
    config.stream = scope.name("__anycable__");

    config.stream_read_block_milliseconds = 500;

    (scope, config, MockHandler::new())
}

mod test_redis_broadcaster {
    use super::*;

    /// upstream: broadcast/redis_test.go#TestRedisBroadcaster/Handles_broadcasts
    #[test]
    fn handles_broadcasts() {
        let (scope, config, handler) = setup();

        let broadcaster = Broadcaster::redis(&handler, &config);

        broadcaster.start().expect("Start");
        let _guard = ShutdownGuard(&broadcaster);

        scope.wait_clients_with_cmd(scope.user(), "xreadgroup", 1, CONSUMERS).expect("No stream consumer were created");

        scope.xadd(&config.stream, "payload", &payload("any_test", "123_test")).expect("XADD");

        let messages = decode(handler.drain(DRAIN_IDLE));
        assert_eq!(1, messages.len(), "Expected 1 message, got {}", messages.len());

        let msg = &messages[0];

        assert_eq!(Some("any_test"), msg.get("stream").map(String::as_str));
        assert_eq!(Some("123_test"), msg.get("data").map(String::as_str));
    }

    /// upstream: broadcast/redis_test.go#TestRedisBroadcaster/With_multiple_subscribers
    #[test]
    fn with_multiple_subscribers() {
        let (scope, config, handler) = setup();

        let broadcaster = Broadcaster::redis(&handler, &config);
        broadcaster.start().expect("Start");
        let _guard = ShutdownGuard(&broadcaster);

        let broadcaster2 = Broadcaster::redis(&handler, &config);
        broadcaster2.start().expect("Start");
        let _guard2 = ShutdownGuard(&broadcaster2);

        scope.wait_clients_with_cmd(scope.user(), "xreadgroup", 2, CONSUMERS).expect("No stream consumer were created");

        for data in ["123_test", "124_test", "125_test"] {
            scope.xadd(&config.stream, "payload", &payload("any_test", data)).expect("XADD");
        }

        let messages = decode(handler.drain(DRAIN_IDLE));

        assert_eq!(3, messages.len(), "Expected 3 messages, got {}", messages.len());
    }
}

/// upstream: broadcast/redis_test.go#TestRedisConfig__ToToml
#[test]
fn test_redis_config_to_toml() {
    let mut config = new_redis_config();
    config.stream = "test_stream".into();
    config.group = "test_group".into();
    config.stream_read_block_milliseconds = 3000;

    let toml_str = redis_config_to_toml(&config);

    for needle in ["stream = \"test_stream\"", "group = \"test_group\"", "stream_read_block_milliseconds = 3000"] {
        assert!(toml_str.contains(needle), "expected {needle:?} in:\n{toml_str}");
    }

    // Round-trip test
    let config2 = redis_config_decode_toml(new_redis_config(), &toml_str).expect("decoding TOML");

    assert_eq!(config, config2);
}
