//! Waiting for pub/sub subscriptions that are not active yet (D-105).
//!
//! With a multi-node pub/sub (Redis, Postgres) a node's first subscription to
//! a stream becomes active asynchronously, after the session is registered in
//! the hub. A message published before it is active never reaches the node,
//! though the client may already have its `confirm_subscription`. So a
//! command that adds streams waits, before its transmissions are sent, until
//! the subscriber reports those streams active ([`Activations`]), at most
//! [`ACTIVATION_TIMEOUT`]; then it goes on as before, with a warning.
//!
//! Nothing waits for a stream the subscriber does not report (a stream the
//! node is already subscribed to, a subscriber without a connection, or one
//! that does not track activations): the check is then one atomic load. The
//! wait holds no lock and gives the command pool seat up ([`blocking`]).

use std::time::Duration;

use cabby_core::log_warn;
use cabby_core::pubsub::Activations;

use super::{Session, blocking};

/// The longest a command waits for its streams' pub/sub subscriptions.
pub const ACTIVATION_TIMEOUT: Duration = Duration::from_secs(1);

/// Waits until the pub/sub subscriptions of `streams` are active, at most
/// [`ACTIVATION_TIMEOUT`].
pub(crate) fn wait_for_streams(activations: &Activations, s: &Session, streams: &[String]) {
    wait_with(activations, s, streams, ACTIVATION_TIMEOUT);
}

fn wait_with(activations: &Activations, s: &Session, streams: &[String], timeout: Duration) {
    if streams.is_empty() || !activations.any_pending(streams) {
        return;
    }
    for stream in blocking(|| activations.wait(streams, timeout)) {
        log_warn!(s.logger(), "pub/sub subscription is not active yet", "stream" => stream,
            "waited" => super::disconnect_queue::go_duration(timeout));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::super::test_support::{Fixture, wait_until};
    use super::*;

    /// D-105: the confirmation of a subscription is sent once the stream's
    /// pub/sub subscription is active, and a message published after it is
    /// delivered. The pub/sub double confirms only when the test says so;
    /// without the wait, the subscribe command returns (and its confirmation
    /// is queued) while the double has not confirmed.
    #[test]
    fn deviation_d105_a_subscription_is_confirmed_once_its_stream_is_active() {
        let f = Fixture::new();
        let s = f.session("a");
        let subscribe = f.spawn_subscribe(&s, "chat", None);
        f.pubsub.wait_requested("chat");

        wait_until("the command waits, or returns", || f.activations().waiting() > 0 || subscribe.is_finished());
        assert!(!subscribe.is_finished(), "the subscription was confirmed before its pub/sub subscription was active");
        // Published before the subscription is active: lost, as with Redis.
        f.pubsub.publish("chat", "before");
        assert_eq!(Vec::<String>::new(), f.conn("a").messages());

        f.pubsub.activate("chat");
        subscribe.join().unwrap();
        f.pubsub.publish("chat", "after");
        f.conn("a").wait_frames(2);
        assert_eq!(vec!["confirm_subscription:chat".to_owned(), "message:after".to_owned()], f.conn("a").frames());
    }

    /// D-105: a session subscribing while another one's first subscription to
    /// the stream is being set up waits for it too.
    #[test]
    fn deviation_d105_a_later_subscriber_waits_for_the_same_stream() {
        let f = Fixture::new();
        let (a, b) = (f.session("a"), f.session("b"));
        let first = f.spawn_subscribe(&a, "chat", None);
        f.pubsub.wait_requested("chat");
        wait_until("the first command waits", || f.activations().waiting() == 1 || first.is_finished());
        let second = f.spawn_subscribe(&b, "chat", None);
        wait_until("the second command waits", || f.activations().waiting() == 2 || second.is_finished());
        assert!(!first.is_finished() && !second.is_finished(), "a subscription was confirmed before it was active");
        assert_eq!(1, f.pubsub.subscribe_calls("chat"), "the stream is subscribed once");

        f.pubsub.activate("chat");
        first.join().unwrap();
        second.join().unwrap();
        f.pubsub.publish("chat", "after");
        for sid in ["a", "b"] {
            f.conn(sid).wait_frames(2);
            assert_eq!(vec!["confirm_subscription:chat".to_owned(), "message:after".to_owned()], f.conn(sid).frames());
        }
    }

    /// D-105: a stream that does not become active within the bound is
    /// confirmed anyway, as before.
    #[test]
    fn deviation_d105_the_wait_is_bounded() {
        let f = Fixture::new();
        let s = f.session("a");
        let started = Instant::now();
        f.spawn_subscribe(&s, "chat", None).join().unwrap();
        let waited = started.elapsed();
        assert!(waited >= ACTIVATION_TIMEOUT, "returned after {waited:?}");
        assert!(waited < ACTIVATION_TIMEOUT * 5, "returned after {waited:?}");
        f.conn("a").wait_frames(1);
        assert_eq!(vec!["confirm_subscription:chat".to_owned()], f.conn("a").frames());
        assert!(f.activations().any_pending(&["chat".to_owned()]));
    }

    /// Nothing waits for a stream that is not pending, and a pending stream
    /// stops the wait as soon as it is settled.
    #[test]
    fn only_pending_streams_are_waited_for() {
        let f = Fixture::new();
        let s = f.session("a");
        let activations = f.activations();
        let streams = vec!["x".to_owned()];
        let started = Instant::now();
        wait_with(&activations, &s, &streams, Duration::from_secs(10));
        assert!(started.elapsed() < Duration::from_secs(5));

        activations.requested("x");
        let waiter = {
            let (activations, s, streams) = (activations.clone(), s.clone(), streams.clone());
            std::thread::spawn(move || wait_with(&activations, &s, &streams, Duration::from_secs(10)))
        };
        wait_until("the wait starts", || activations.waiting() == 1);
        activations.settled("x");
        waiter.join().unwrap();
    }
}
