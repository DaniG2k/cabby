//! Per-session channel subscriptions and their streams (upstream
//! `node/subscription_state.go`).

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

type Channels = HashMap<String, HashSet<String>>;

/// Channel identifiers (opaque strings, compared byte-for-byte) mapped to the
/// streams each subscription listens to. Safe to share between threads.
///
/// Upstream guards the map with an RW mutex; every operation here is a short
/// map access, so a plain mutex is cheaper. No user code runs under the lock.
#[derive(Debug, Default)]
pub struct SubscriptionState {
    channels: Mutex<Channels>,
}

impl SubscriptionState {
    /// Upstream `NewSubscriptionState`.
    #[allow(clippy::new_without_default)] // mirrors the upstream constructor
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Channels> {
        self.channels.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn has_channel(&self, id: &str) -> bool {
        self.lock().contains_key(id)
    }

    /// Registers a channel (replacing its streams with an empty set).
    pub fn add_channel(&self, id: &str) {
        self.lock().insert(id.to_owned(), HashSet::new());
    }

    pub fn remove_channel(&self, id: &str) {
        self.lock().remove(id);
    }

    /// Channel identifiers in unspecified order.
    pub fn channels(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    /// Channels with their streams (upstream `ToMap`).
    pub fn to_map(&self) -> HashMap<String, Vec<String>> {
        self.lock().iter().map(|(k, v)| (k.clone(), v.iter().cloned().collect())).collect()
    }

    /// Adds a stream to a known channel; unknown channels are ignored.
    pub fn add_channel_stream(&self, id: &str, stream: &str) {
        self.insert_channel_stream(id, stream);
    }

    /// [`SubscriptionState::add_channel_stream`], returning whether the stream
    /// was added: false for unknown channels and for streams the channel has.
    pub fn insert_channel_stream(&self, id: &str, stream: &str) -> bool {
        self.lock().get_mut(id).is_some_and(|streams| streams.insert(stream.to_owned()))
    }

    /// Whether `stream` can be added to `id`: the channel is known and does
    /// not have the stream yet.
    pub fn lacks_channel_stream(&self, id: &str, stream: &str) -> bool {
        self.lock().get(id).is_some_and(|streams| !streams.contains(stream))
    }

    pub fn remove_channel_stream(&self, id: &str, stream: &str) {
        self.take_channel_stream(id, stream);
    }

    /// [`SubscriptionState::remove_channel_stream`], returning whether the
    /// channel had the stream.
    pub fn take_channel_stream(&self, id: &str, stream: &str) -> bool {
        self.lock().get_mut(id).is_some_and(|streams| streams.remove(stream))
    }

    /// Clears a channel's streams and returns them (`None` for unknown channels,
    /// Go's nil slice).
    pub fn remove_channel_streams(&self, id: &str) -> Option<Vec<String>> {
        self.lock().get_mut(id).map(|streams| std::mem::take(streams).into_iter().collect())
    }

    /// A channel's streams in unspecified order (`None` for unknown channels).
    pub fn streams_for(&self, id: &str) -> Option<Vec<String>> {
        self.lock().get(id).map(|streams| streams.iter().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_take_report_membership_changes() {
        let subs = SubscriptionState::new();
        assert!(!subs.lacks_channel_stream("chat", "s"), "unknown channel");
        assert!(!subs.insert_channel_stream("chat", "s"), "unknown channel");

        subs.add_channel("chat");
        assert!(subs.lacks_channel_stream("chat", "s"));
        assert!(subs.insert_channel_stream("chat", "s"));
        assert!(!subs.lacks_channel_stream("chat", "s"));
        assert!(!subs.insert_channel_stream("chat", "s"), "already there");

        assert!(subs.take_channel_stream("chat", "s"));
        assert!(!subs.take_channel_stream("chat", "s"), "already removed");
        assert!(!subs.take_channel_stream("other", "s"), "unknown channel");
        assert_eq!(Some(Vec::new()), subs.streams_for("chat"));
    }
}
