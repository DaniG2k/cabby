//! upstream file: pubsub/subscriber_test.go
//!
//! `SharedSubscriberTests` is translated as [`Shared`] plus one function per
//! subtest; `redis_test` and `nats_test` reuse them with their own [`Harness`].
//! Upstream subtests run in sequence on one pair of subscribers, so later
//! subtests see the subscriptions made by earlier ones (e.g. `"a"` is already
//! subscribed when "Subscribe - Broadcast - Unsubscribe - Broadcast" subscribes it
//! again and then unsubscribes once). Every Rust test starts its own subscribers
//! and first replays the subscriptions of the subtests before it
//! ([`replay_broadcast`], [`replay_subscribe_broadcast`]).
//!
//! Waiting for subscriptions: upstream `wait(subscriber, stream)` reads
//! subscriber internals (`RedisSubscriber.getEvent`, `NATSSubscriber.subscriptions`
//! plus a 100 ms sleep). Here readiness is observed on the server: the number of
//! subscriptions on the channel/subject must equal the number of subscribers
//! that are expected to hold one (tracked by [`Shared`]).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_testkit::reference::pubsub::{Subscriber, TestHandler};
use cabby_testkit::{Backend, backend};

/// What differs between subscriber implementations.
pub(crate) trait Harness {
    /// Upstream `subscriberFactory`.
    fn subscriber(&self, handler: &TestHandler) -> Subscriber;
    /// The channel/subject name used for upstream stream `name` (unique per test
    /// where the server is shared).
    fn stream(&self, name: &str) -> String;
    /// The internal channel (upstream `wait(subscriber, "internal")`).
    fn internal(&self) -> String;
    /// Waits until exactly `count` subscriptions exist on `channel`.
    fn wait_subscriptions(&self, channel: &str, count: usize) -> Result<(), String>;
}

/// `defer subscriber.Shutdown(...)`; skipped while a Rust stub is unwinding.
pub(crate) struct ShutdownGuard(pub Subscriber);

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        if backend() == Backend::Go || !std::thread::panicking() {
            let _ = self.0.shutdown();
        }
    }
}

pub(crate) struct Shared<'h> {
    h: &'h dyn Harness,
    pub handler: TestHandler,
    subscribers: Vec<ShutdownGuard>,
    handlers: Vec<TestHandler>,
    /// channel -> indexes of subscribers expected to be subscribed.
    expected: Mutex<HashMap<String, HashSet<usize>>>,
}

impl<'h> Shared<'h> {
    /// Upstream preamble: build and start the first subscriber, wait for its
    /// internal channel.
    pub fn start(h: &'h dyn Harness) -> Self {
        let handler = TestHandler::new();
        let mut s =
            Shared { h, handler, subscribers: Vec::new(), handlers: Vec::new(), expected: Mutex::new(HashMap::new()) };
        let subscriber = h.subscriber(&s.handler);
        s.add(subscriber);
        s
    }

    fn add(&mut self, subscriber: Subscriber) {
        subscriber.start().expect("subscriber.Start");
        let i = self.subscribers.len();
        self.subscribers.push(ShutdownGuard(subscriber));
        self.track(i, &self.h.internal(), true);
        self.wait(i, "internal").expect("wait internal");
    }

    /// Upstream multi-node preamble: `if !subscriber.IsMultiNode() { return }`,
    /// then a second handler and subscriber. Only multi-node subscribers have
    /// these subtests, so here it is asserted.
    pub fn start_multi(h: &'h dyn Harness) -> Self {
        let mut s = Self::start(h);
        assert!(s.sub(0).is_multi_node(), "multi-node subtests require a multi-node subscriber");
        let other_handler = TestHandler::new();
        let other = h.subscriber(&other_handler);
        s.handlers.push(other_handler);
        s.add(other);
        s
    }

    pub fn sub(&self, i: usize) -> &Subscriber {
        &self.subscribers[i].0
    }

    pub fn other_handler(&self) -> &TestHandler {
        &self.handlers[0]
    }

    pub fn stream(&self, name: &str) -> String {
        self.h.stream(name)
    }

    fn track(&self, i: usize, channel: &str, on: bool) {
        let mut e = self.expected.lock().unwrap_or_else(|e| e.into_inner());
        let set = e.entry(channel.to_owned()).or_default();
        if on {
            set.insert(i);
        } else {
            set.remove(&i);
        }
    }

    pub fn subscribe(&self, i: usize, name: &str) {
        let channel = self.stream(name);
        self.sub(i).subscribe(&channel);
        self.track(i, &channel, true);
    }

    pub fn unsubscribe(&self, i: usize, name: &str) {
        let channel = self.stream(name);
        self.sub(i).unsubscribe(&channel);
        self.track(i, &channel, false);
    }

    /// Upstream `wait(subscriber, stream)`: `"internal"` is the internal channel,
    /// `"-name"` waits for the unsubscription.
    pub fn wait(&self, _i: usize, name: &str) -> Result<(), String> {
        let channel = match name {
            "internal" => self.h.internal(),
            n => self.stream(n.strip_prefix('-').unwrap_or(n)),
        };
        let count = self.expected.lock().unwrap_or_else(|e| e.into_inner()).get(&channel).map_or(0, HashSet::len);
        self.h.wait_subscriptions(&channel, count)
    }
}

/// Subscriptions left by subtest "Broadcast".
fn replay_broadcast(s: &Shared) {
    s.subscribe(0, "test");
    s.wait(0, "test").expect("wait test");
}

/// Subscriptions left by subtest "Subscribe - Broadcast".
fn replay_subscribe_broadcast(s: &Shared) {
    s.subscribe(0, "a");
    s.subscribe(1, "b");
    s.subscribe(1, "a");
    s.wait(0, "a").expect("wait a");
    s.wait(1, "a").expect("wait a");
    s.wait(1, "b").expect("wait b");
}

fn smsg(stream: &str, data: &str) -> StreamMessage {
    StreamMessage { stream: stream.into(), data: data.into(), ..Default::default() }
}

/// Subtest "Broadcast".
pub(crate) fn broadcast(h: &dyn Harness) {
    let s = Shared::start(h);
    let test = s.stream("test");

    // Sbscribers may rely on known subscriptions
    s.subscribe(0, "test");
    s.wait(0, "test").expect("wait test");

    s.sub(0).broadcast(&smsg(&test, "boo"));

    let msg = s.handler.receive().expect("message");
    assert_eq!("boo", msg.data);
}

/// Subtest "Broadcast commands".
pub(crate) fn broadcast_commands(h: &dyn Harness) {
    let s = Shared::start(h);
    replay_broadcast(&s);

    s.sub(0).broadcast_command(&RemoteCommandMessage { command: "test".into(), payload: "{\"foo\":\"bar\"}".into() });

    let cmd = s.handler.receive_command().expect("command");
    assert_eq!("test", cmd.command);
}

/// Subtest "Subscribe - Broadcast".
pub(crate) fn subscribe_broadcast(h: &dyn Harness) {
    let s = Shared::start_multi(h);
    replay_broadcast(&s);
    let (a, b) = (s.stream("a"), s.stream("b"));

    s.subscribe(0, "a");
    s.subscribe(1, "b");
    s.subscribe(1, "a");

    s.wait(0, "a").expect("wait a");
    s.wait(1, "a").expect("wait a");
    s.wait(1, "b").expect("wait b");

    s.sub(0).broadcast(&smsg(&a, "1"));

    let msg = s.handler.receive().expect("message");
    assert_eq!("1", msg.data);
    assert_eq!(a, msg.stream);

    let next_msg = s.handler.receive();
    assert!(next_msg.is_none(), "Must broadcast message once");

    let msg = s.other_handler().receive().expect("message");
    assert_eq!("1", msg.data);
    assert_eq!(a, msg.stream);

    let next_msg = s.other_handler().receive();
    assert!(next_msg.is_none(), "Must broadcast message once");

    s.sub(0).broadcast(&smsg(&b, "2"));

    let msg = s.handler.receive();
    assert!(msg.is_none(), "Should not broadcast message for unknown stream");

    let msg = s.other_handler().receive().expect("message");
    assert_eq!("2", msg.data);
    assert_eq!(b, msg.stream);
}

/// Subtest "Re-transmit commands".
pub(crate) fn retransmit_commands(h: &dyn Harness) {
    let s = Shared::start_multi(h);
    replay_broadcast(&s);
    replay_subscribe_broadcast(&s);

    s.sub(0).broadcast_command(&RemoteCommandMessage { command: "test".into(), ..Default::default() });

    let cmd = s.handler.receive_command().expect("command");
    assert_eq!("test", cmd.command);

    let cmd = s.other_handler().receive_command().expect("command");
    assert_eq!("test", cmd.command);
}

/// Subtest "Subscribe - Broadcast - Unsubscribe - Broadcast".
pub(crate) fn subscribe_broadcast_unsubscribe_broadcast(h: &dyn Harness) {
    let s = Shared::start_multi(h);
    replay_broadcast(&s);
    replay_subscribe_broadcast(&s);
    let a = s.stream("a");

    // Already subscribed by the replayed subtest: subscribing again must be
    // idempotent, so the single unsubscribe below stops delivery.
    s.subscribe(0, "a");
    s.subscribe(1, "a");

    s.wait(0, "a").expect("wait a");
    s.wait(1, "a").expect("wait a");

    s.sub(0).broadcast(&smsg(&a, "1"));

    let msg = s.handler.receive().expect("message");
    assert_eq!("1", msg.data);
    assert_eq!(a, msg.stream);

    let msg = s.other_handler().receive().expect("message");
    assert_eq!("1", msg.data);
    assert_eq!(a, msg.stream);

    s.unsubscribe(0, "a");
    s.wait(0, "-a").expect("wait -a");

    s.sub(0).broadcast(&smsg(&a, "2"));

    let msg = s.handler.receive();
    assert!(msg.is_none(), "Should not broadcast message for unsubscribed stream");

    let msg = s.other_handler().receive().expect("message");
    assert_eq!("2", msg.data);
    assert_eq!(a, msg.stream);
}

/// Upstream `TestLegacySubscriber`: no server, subscriptions are no-ops and the
/// waiter returns nil immediately.
struct Legacy;

impl Harness for Legacy {
    fn subscriber(&self, handler: &TestHandler) -> Subscriber {
        Subscriber::legacy(handler)
    }

    fn stream(&self, name: &str) -> String {
        name.to_owned()
    }

    fn internal(&self) -> String {
        "internal".to_owned()
    }

    fn wait_subscriptions(&self, _channel: &str, _count: usize) -> Result<(), String> {
        Ok(())
    }
}

mod test_legacy_subscriber {
    use super::*;

    /// upstream: pubsub/subscriber_test.go#TestLegacySubscriber/Broadcast
    /// upstream: pubsub/subscriber_test.go#TestLegacySubscriber
    /// note: also asserts `!IsMultiNode()`, which upstream's early return before the multi-node subtests implies.
    #[test]
    fn broadcast() {
        super::broadcast(&Legacy);
        assert_single_node();
    }

    /// upstream: pubsub/subscriber_test.go#TestLegacySubscriber/Broadcast_commands
    /// upstream: pubsub/subscriber_test.go#TestLegacySubscriber
    /// note: also asserts `!IsMultiNode()`, which upstream's early return before the multi-node subtests implies.
    #[test]
    fn broadcast_commands() {
        super::broadcast_commands(&Legacy);
        assert_single_node();
    }

    /// Upstream: `if !subscriber.IsMultiNode() { return }`, so no multi-node
    /// subtests exist for the legacy subscriber.
    fn assert_single_node() {
        let handler = TestHandler::new();
        assert!(!Subscriber::legacy(&handler).is_multi_node());
    }
}
