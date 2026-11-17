//! Per-session channel subscriptions and their streams (upstream
//! `node/subscription_state.go`).

use std::collections::HashMap;

use cabby_core::pending;

/// Channel identifiers (opaque strings, compared byte-for-byte) mapped to the
/// streams each subscription listens to. Safe to share between threads.
#[derive(Debug)]
pub struct SubscriptionState {
    _private: (),
}

impl SubscriptionState {
    /// Upstream `NewSubscriptionState`.
    #[allow(clippy::new_without_default)] // no plausible default value before implementation
    pub fn new() -> Self {
        pending!("node::new_subscription_state")
    }

    pub fn has_channel(&self, _id: &str) -> bool {
        pending!("node::subscription_state_has_channel")
    }

    /// Registers a channel (replacing its streams with an empty set).
    pub fn add_channel(&self, _id: &str) {
        pending!("node::subscription_state_add_channel")
    }

    pub fn remove_channel(&self, _id: &str) {
        pending!("node::subscription_state_remove_channel")
    }

    /// Channel identifiers in unspecified order.
    pub fn channels(&self) -> Vec<String> {
        pending!("node::subscription_state_channels")
    }

    /// Channels with their streams (upstream `ToMap`).
    pub fn to_map(&self) -> HashMap<String, Vec<String>> {
        pending!("node::subscription_state_to_map")
    }

    /// Adds a stream to a known channel; unknown channels are ignored.
    pub fn add_channel_stream(&self, _id: &str, _stream: &str) {
        pending!("node::subscription_state_add_channel_stream")
    }

    pub fn remove_channel_stream(&self, _id: &str, _stream: &str) {
        pending!("node::subscription_state_remove_channel_stream")
    }

    /// Clears a channel's streams and returns them (`None` for unknown channels,
    /// Go's nil slice).
    pub fn remove_channel_streams(&self, _id: &str) -> Option<Vec<String>> {
        pending!("node::subscription_state_remove_channel_streams")
    }

    /// A channel's streams in unspecified order (`None` for unknown channels).
    pub fn streams_for(&self, _id: &str) -> Option<Vec<String>> {
        pending!("node::subscription_state_streams_for")
    }
}
