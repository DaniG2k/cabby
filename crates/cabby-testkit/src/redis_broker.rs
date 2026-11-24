//! The Redis broker (a Cabby addition, deviation D-069): brokers on a test's
//! Redis scope (its ACL user and its key prefix), a recording broadcaster,
//! and servers started with `--broker=redis`.
//!
//! AnyCable rejects `--broker=redis`, so every scenario calls [`rust_only`]
//! first; on the Go backend the runner records the case as not observable.

use std::sync::{Arc, Mutex};

use cabby_core::broker::{Broadcaster, BrokerError, Config};
use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_server::metrics::{Instrumenter, Metrics};
use cabby_server::redis::RedisConfig;

pub use cabby_server::broker::redis::{ACL_COMMANDS, ADD_LUA};
pub use cabby_server::broker::{RedisBroker, RedisBrokerConfig};

use crate::services::RedisScope;
use crate::sut::Sut;
use crate::{Backend, backend};

/// Reports the case as not observable on the Go backend.
pub fn rust_only() {
    if backend() == Backend::Go {
        unimplemented!(
            "not_observable: AnyCable rejects --broker=redis (Unsupported broker adapter: redis); the Redis broker is a Cabby addition (D-069)"
        );
    }
}

/// Broker options for a test: its own key prefix, the default timeout.
pub fn options(scope: &RedisScope) -> RedisBrokerConfig {
    RedisBrokerConfig { key_prefix: scope.prefix.clone(), ..RedisBrokerConfig::new() }
}

/// A broker on `url` with `options` (not started).
pub fn try_new_broker(
    url: &str,
    config: &Config,
    options: &RedisBrokerConfig,
    broadcaster: Option<Arc<dyn Broadcaster>>,
    metrics: Option<Arc<Metrics>>,
) -> Result<RedisBroker, BrokerError> {
    let mut redis = RedisConfig::new();
    redis.url = url.to_owned();
    RedisBroker::new(broadcaster, config, &redis, options, metrics.map(|m| m as Arc<dyn Instrumenter>))
}

/// A broker on the test's scope (not started).
pub fn new_broker(scope: &RedisScope, config: &Config, broadcaster: Option<Arc<dyn Broadcaster>>) -> RedisBroker {
    try_new_broker(&scope.url(), config, &options(scope), broadcaster, None).expect("Redis broker")
}

/// Metrics a broker can report to.
pub fn metrics() -> Arc<Metrics> {
    Arc::new(Metrics::new(Vec::new(), 0))
}

/// Records what a broker fans out.
#[derive(Default)]
pub struct Recorder {
    pub messages: Mutex<Vec<StreamMessage>>,
    pub subscribed: Mutex<Vec<String>>,
    pub unsubscribed: Mutex<Vec<String>>,
}

impl Recorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn messages(&self) -> Vec<StreamMessage> {
        self.messages.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Broadcaster for Recorder {
    fn broadcast(&self, msg: &StreamMessage) {
        self.messages.lock().unwrap_or_else(|e| e.into_inner()).push(msg.clone());
    }
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
    fn subscribe(&self, stream: &str) {
        self.subscribed.lock().unwrap_or_else(|e| e.into_inner()).push(stream.to_owned());
    }
    fn unsubscribe(&self, stream: &str) {
        self.unsubscribed.lock().unwrap_or_else(|e| e.into_inner()).push(stream.to_owned());
    }
}

/// HTTP broadcasts and the Redis broker on the test's scope, then `extra`.
/// Pub/sub is left unset: `--broker=redis` selects Redis pub/sub.
pub fn server_args(scope: &RedisScope, extra: &[&str]) -> Vec<String> {
    let mut args = vec![
        "--broadcast_adapter=http".to_owned(),
        "--broker=redis".to_owned(),
        format!("--redis_url={}", scope.url()),
        format!("--redis_broker_key_prefix={}", scope.prefix),
    ];
    args.extend(extra.iter().map(|a| (*a).to_owned()));
    args
}

pub fn start(scope: &RedisScope, extra: &[&str]) -> Sut {
    let args = server_args(scope, extra);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Sut::try_start(&args, &[]).unwrap_or_else(|e| panic!("{e:#}"))
}
