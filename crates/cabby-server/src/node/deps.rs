//! Minimal mirrors of upstream types the node needs from packages that are being
//! ported elsewhere (`hub`, `pubsub`). Signatures only, sized to what the node
//! and its translated tests use.

use std::sync::Arc;

use cabby_core::broker::Broadcaster;
use cabby_core::common::{RemoteCommandMessage, RemoteDisconnectMessage, StreamMessage};
use cabby_core::pending;

use super::Session;
use crate::cli::SubscriberHandler;

// TODO(merge): owned by hub. Replace with the hub module's `Hub` (which works on a
// `HubSession` interface rather than the concrete node session).
/// Routes stream messages to subscribed sessions (upstream `hub.Hub`).
pub struct Hub {
    _private: (),
}

impl Hub {
    /// Upstream `hub.NewHub(poolSize, logger)`.
    pub fn new(_pool_size: usize) -> Self {
        pending!("hub::new_hub")
    }

    /// Runs the hub until [`Hub::shutdown`] (blocks; run it on its own thread).
    pub fn run(&self) {
        pending!("hub::run")
    }

    pub fn shutdown(&self) {
        pending!("hub::shutdown")
    }

    /// Registered sessions.
    pub fn size(&self) -> usize {
        pending!("hub::size")
    }

    /// Distinct connection identifiers.
    pub fn uniq_size(&self) -> usize {
        pending!("hub::uniq_size")
    }

    pub fn streams_size(&self) -> usize {
        pending!("hub::streams_size")
    }

    pub fn add_session(&self, _session: Arc<Session>) {
        pending!("hub::add_session")
    }

    /// Unregisters a session and all its stream subscriptions.
    pub fn remove_session(&self, _session: &Arc<Session>) {
        pending!("hub::remove_session")
    }

    pub fn subscribe_session(&self, _session: &Arc<Session>, _stream: &str, _identifier: &str) {
        pending!("hub::subscribe_session")
    }

    pub fn unsubscribe_session(&self, _session: &Arc<Session>, _stream: &str, _identifier: &str) {
        pending!("hub::unsubscribe_session")
    }

    pub fn unsubscribe_session_from_channel(&self, _session: &Arc<Session>, _identifier: &str) {
        pending!("hub::unsubscribe_session_from_channel")
    }

    /// Delivers raw data to a stream (upstream `Broadcast(stream, data)`).
    pub fn broadcast(&self, _stream: &str, _data: &str) {
        pending!("hub::broadcast")
    }

    pub fn broadcast_message(&self, _msg: &StreamMessage) {
        pending!("hub::broadcast_message")
    }

    pub fn remote_disconnect(&self, _msg: &RemoteDisconnectMessage) {
        pending!("hub::remote_disconnect")
    }

    pub fn find_by_identifier(&self, _id: &str) -> Option<Arc<Session>> {
        pending!("hub::find_by_identifier")
    }

    pub fn sessions(&self) -> Vec<Arc<Session>> {
        pending!("hub::sessions")
    }
}

// TODO(merge): owned by pubsub. Replace with `pubsub::LegacySubscriber`.
/// Single-node subscriber that hands every message straight to the node
/// (upstream `pubsub.NewLegacySubscriber(node)`).
pub struct LegacySubscriber {
    _node: Arc<dyn SubscriberHandler>,
}

impl LegacySubscriber {
    pub fn new(node: Arc<dyn SubscriberHandler>) -> Self {
        Self { _node: node }
    }
}

impl Broadcaster for LegacySubscriber {
    fn broadcast(&self, _msg: &StreamMessage) {
        pending!("pubsub::legacy_subscriber_broadcast")
    }
    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {
        pending!("pubsub::legacy_subscriber_broadcast_command")
    }
    fn subscribe(&self, _stream: &str) {
        pending!("pubsub::legacy_subscriber_subscribe")
    }
    fn unsubscribe(&self, _stream: &str) {
        pending!("pubsub::legacy_subscriber_unsubscribe")
    }
}
