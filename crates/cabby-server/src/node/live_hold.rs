//! Live messages held while the history requested with a subscription is
//! sent (D-106).
//!
//! A subscribe command with `history` installs the subscription, then reads
//! the history from the broker and sends it. A message published in between
//! is delivered live at once, before the history batch, and is in that batch
//! too: the client gets it twice, and out of order. So, as Durable Streams do
//! (D-089), the session holds the subscription's live messages from before
//! the subscription is installed until the history is sent, then delivers
//! those the history did not contain (another epoch, or an offset past the
//! last one it delivered for the stream), in arrival order.
//!
//! Only sessions with a hold in progress look further than one atomic load
//! when a message is delivered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use cabby_core::common::StreamMessage;
use cabby_core::encoders::{EncodedMessageJson, SentFrame};

use super::Session;

/// A held message: its stream position (stream, epoch, offset), if it has
/// one, and its frame.
type Held = (Option<(String, String, u64)>, Arc<SentFrame>);

struct Hold {
    identifier: String,
    held: Vec<Held>,
}

/// The holds of one session.
#[derive(Default)]
pub(super) struct LiveHolds {
    active: AtomicUsize,
    holds: Mutex<Vec<Hold>>,
}

impl LiveHolds {
    fn lock(&self) -> MutexGuard<'_, Vec<Hold>> {
        self.holds.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether any hold is in progress (one atomic load).
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire) > 0
    }

    /// Starts holding the live messages of the subscription `identifier`.
    pub fn start(&self, identifier: &str) {
        let mut holds = self.lock();
        holds.push(Hold { identifier: identifier.to_owned(), held: Vec::new() });
        self.active.store(holds.len(), Ordering::Release);
    }

    /// Keeps `msg` if its subscription is held (`frame` encodes it); false
    /// when it is to be delivered now.
    pub fn keep(&self, msg: &dyn EncodedMessageJson, frame: impl FnOnce() -> Option<Arc<SentFrame>>) -> bool {
        let Some(reply) = msg.as_reply() else { return false };
        let mut holds = self.lock();
        let Some(hold) = holds.iter_mut().find(|h| h.identifier == reply.identifier) else { return false };
        let position = (reply.offset > 0 && !reply.epoch.is_empty())
            .then(|| (reply.stream_id.clone(), reply.epoch.clone(), reply.offset));
        if let Some(frame) = frame() {
            hold.held.push((position, frame));
        }
        true
    }

    /// How many messages are held (tests).
    #[cfg(test)]
    pub fn held(&self) -> usize {
        self.lock().iter().map(|h| h.held.len()).sum()
    }

    /// Ends the hold of `identifier`: `deliver` gets the held messages that
    /// `history` (what was sent) does not contain, in arrival order, before
    /// any later message of the subscription is delivered.
    pub fn release(&self, identifier: &str, history: &[StreamMessage], mut deliver: impl FnMut(Arc<SentFrame>)) {
        if !self.is_active() {
            return;
        }
        let mut holds = self.lock();
        let Some(i) = holds.iter().position(|h| h.identifier == identifier) else { return };
        let hold = holds.swap_remove(i);
        let mut last: HashMap<(&str, &str), u64> = HashMap::new();
        for m in history {
            let offset = last.entry((m.stream.as_str(), m.epoch.as_str())).or_default();
            *offset = (*offset).max(m.offset);
        }
        for (position, frame) in hold.held {
            let covered = position.is_some_and(|(stream, epoch, offset)| {
                last.get(&(stream.as_str(), epoch.as_str())).is_some_and(|last| offset <= *last)
            });
            if !covered {
                deliver(frame);
            }
        }
        self.active.store(holds.len(), Ordering::Release);
    }
}

/// Ends a hold that is still in progress when dropped, delivering every held
/// message.
pub(crate) struct HoldGuard {
    session: Arc<Session>,
    identifier: String,
}

impl HoldGuard {
    pub(super) fn new(session: &Arc<Session>, identifier: &str) -> Self {
        Self { session: session.clone(), identifier: identifier.to_owned() }
    }
}

impl Drop for HoldGuard {
    fn drop(&mut self) {
        self.session.release_live(&self.identifier, &[]);
    }
}

#[cfg(test)]
mod tests {
    use cabby_core::common::{EncodedMessage, HistoryRequest, Reply};
    use cabby_core::encoders::CachedEncodedMessage;

    use super::super::test_support::{Fixture, wait_until};
    use super::*;

    /// D-106: a message published after the subscription is installed and
    /// before the history is read reaches the client once, in the history,
    /// after the confirmation. The pause: the pub/sub double delivers the
    /// stream's messages but holds the command until the test reports the
    /// subscription active (D-105).
    #[test]
    fn deviation_d106_a_live_message_inside_the_history_is_delivered_once_in_order() {
        let f = Fixture::with_history();
        f.pubsub.publish("chat", "m1");
        f.pubsub.publish("chat", "m2");
        let s = f.session("a");
        let history = HistoryRequest { since: 1, ..Default::default() };
        let subscribe = f.spawn_subscribe(&s, "chat", Some(history));
        f.pubsub.wait_requested("chat");
        wait_until("the command waits", || f.activations().waiting() > 0 || subscribe.is_finished());

        // The subscription is installed: m3 is delivered live (and stored).
        f.pubsub.deliver("chat");
        f.pubsub.publish("chat", "m3");
        wait_until("m3 reaches the session", || s.held_live() == 1 || !f.conn("a").messages().is_empty());
        assert_eq!(Vec::<String>::new(), f.conn("a").frames(), "sent before the confirmation and the history");

        f.pubsub.activate("chat");
        subscribe.join().unwrap();
        f.pubsub.publish("chat", "m4");
        f.conn("a").wait_frames(6);
        assert_eq!(
            vec![
                "confirm_subscription:chat",
                "message:m1@1",
                "message:m2@2",
                "message:m3@3",
                "confirm_history:chat",
                "message:m4@4"
            ],
            f.conn("a").frames()
        );
    }

    fn live(identifier: &str, stream: &str, epoch: &str, offset: u64) -> CachedEncodedMessage {
        CachedEncodedMessage::new(EncodedMessage::Reply(Reply {
            identifier: identifier.into(),
            stream_id: stream.into(),
            epoch: epoch.into(),
            offset,
            message: Some(serde_json::json!(offset)),
            ..Default::default()
        }))
    }

    fn frame(n: u8) -> Option<Arc<SentFrame>> {
        Some(Arc::new(SentFrame::text(vec![n])))
    }

    /// D-106: what the history covers is dropped (same stream and epoch, at or
    /// below the last offset sent); everything else is delivered in order,
    /// and other subscriptions are not held.
    #[test]
    fn deviation_d106_held_messages_follow_the_history() {
        let holds = LiveHolds::default();
        assert!(!holds.is_active());
        holds.start("chat");
        assert!(holds.is_active());
        assert!(!holds.keep(&live("other", "a", "e", 1), || frame(0)), "another subscription is not held");
        assert!(holds.keep(&live("chat", "a", "e", 2), || frame(1)));
        assert!(holds.keep(&live("chat", "a", "e", 3), || frame(2)));
        assert!(holds.keep(&live("chat", "a", "f", 1), || frame(3)), "another epoch");
        assert!(holds.keep(&live("chat", "b", "e", 2), || frame(4)), "a stream without history");
        assert!(holds.keep(&live("chat", "a", "", 0), || frame(5)), "no position");
        let history: Vec<StreamMessage> = [("a", "e", 1), ("a", "e", 2), ("c", "e", 9)]
            .iter()
            .map(|(stream, epoch, offset)| StreamMessage {
                stream: (*stream).into(),
                epoch: (*epoch).into(),
                offset: *offset,
                ..Default::default()
            })
            .collect();
        let mut delivered = Vec::new();
        holds.release("chat", &history, |f| delivered.push(f.payload[0]));
        assert_eq!(vec![2, 3, 4, 5], delivered);
        assert!(!holds.is_active());
        assert!(!holds.keep(&live("chat", "a", "e", 4), || frame(6)), "the hold is over");
        holds.release("chat", &[], |_| panic!("nothing is held"));
    }
}
