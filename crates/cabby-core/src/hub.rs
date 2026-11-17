//! Hub routing (upstream package `hub`): sessions, stream subscriptions sharded
//! over gates, broadcast fan-out and remote disconnects.
//!
//! Status: interfaces only; every behavior is [`pending!`].

use std::sync::Arc;

use crate::common::{DisconnectMessage, RemoteDisconnectMessage, Reply, StreamMessage};
use crate::pending;

// TODO(merge): owned by encoders. Minimal stand-in for `encoders.EncodedMessage`
// (a reply or disconnect notification that is encoded once per encoder).
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)] // TODO(merge) placeholder for encoders::EncodedMessage; boxing only adds noise
pub enum EncodedMessage {
    Reply(Reply),
    Disconnect(DisconnectMessage),
}

impl EncodedMessage {
    // TODO(merge): owned by encoders (`encoders.JSON.Encode`, payload of the frame).
    pub fn to_json(&self) -> Vec<u8> {
        pending!("encoders::json_encode")
    }
}

/// A session as seen by the hub (upstream `hub.HubSession`).
pub trait HubSession: Send + Sync {
    fn get_id(&self) -> String;
    fn get_identifiers(&self) -> String;
    fn send(&self, msg: &EncodedMessage);
    fn disconnect_with_message(&self, msg: &EncodedMessage, code: &str);
}

pub type SessionRef = Arc<dyn HubSession>;

/// Stores all sessions and their subscriptions; broadcasts are routed through
/// `pool_size` gates (shards) (upstream `hub.Hub`).
pub struct Hub {
    _pool_size: usize,
}

impl Hub {
    /// Upstream `hub.NewHub(poolSize, logger)`.
    pub fn new(_pool_size: usize) -> Self {
        pending!("hub::new_hub")
    }

    /// Processes queued registrations, broadcasts and disconnects until
    /// [`Hub::shutdown`] (upstream `go hub.Run()`).
    pub fn run(&self) {
        pending!("hub::run")
    }

    /// Enqueues `data` for every subscriber of `stream`.
    pub fn broadcast(&self, _stream: &str, _data: &str) {
        pending!("hub::broadcast")
    }

    /// Enqueues a pre-built stream message.
    pub fn broadcast_message(&self, _msg: StreamMessage) {
        pending!("hub::broadcast_message")
    }

    /// Enqueues a remote disconnect command.
    pub fn remote_disconnect(&self, _msg: RemoteDisconnectMessage) {
        pending!("hub::remote_disconnect")
    }

    /// Stops [`Hub::run`] and waits for it to return.
    pub fn shutdown(&self) {
        pending!("hub::shutdown")
    }

    /// Number of registered sessions.
    pub fn size(&self) -> usize {
        pending!("hub::size")
    }

    /// Number of unique connection identifiers.
    pub fn uniq_size(&self) -> usize {
        pending!("hub::uniq_size")
    }

    /// Number of unique streams across gates.
    pub fn streams_size(&self) -> usize {
        pending!("hub::streams_size")
    }

    pub fn add_session(&self, _session: SessionRef) {
        pending!("hub::add_session")
    }

    pub fn remove_session(&self, _session: &SessionRef) {
        pending!("hub::remove_session")
    }

    /// Upstream unexported `unsubscribeSessionFromAllChannels`, used by its tests.
    pub fn unsubscribe_session_from_all_channels(&self, _session: &SessionRef) {
        pending!("hub::unsubscribe_session_from_all_channels")
    }

    /// Removes every stream subscribed under the channel `identifier`.
    pub fn unsubscribe_session_from_channel(&self, _session: &SessionRef, _identifier: &str) {
        pending!("hub::unsubscribe_session_from_channel")
    }

    pub fn subscribe_session(&self, _session: &SessionRef, _stream: &str, _identifier: &str) {
        pending!("hub::subscribe_session")
    }

    pub fn unsubscribe_session(&self, _session: &SessionRef, _stream: &str, _identifier: &str) {
        pending!("hub::unsubscribe_session")
    }

    pub fn find_by_identifier(&self, _identifier: &str) -> Option<SessionRef> {
        pending!("hub::find_by_identifier")
    }

    pub fn sessions(&self) -> Vec<SessionRef> {
        pending!("hub::sessions")
    }
}

/// Cancellation scope of a gate's broadcast loop (upstream passes a
/// `context.Context` to `hub.NewGate`).
pub struct GateContext {
    _private: (),
}

impl GateContext {
    pub fn new() -> Self {
        pending!("hub::gate_context")
    }

    /// Cancels the context. Once this returns the gate's broadcast loop no longer
    /// delivers messages.
    pub fn cancel(&self) {
        pending!("hub::gate_context_cancel")
    }
}

impl Default for GateContext {
    fn default() -> Self {
        Self::new()
    }
}

/// One shard of the hub: subscriptions for some streams and their broadcast
/// queue (upstream `hub.Gate`).
pub struct Gate {
    _private: (),
}

impl Gate {
    /// Upstream `hub.NewGate(ctx, logger)`: starts the broadcast loop.
    pub fn new(_ctx: &GateContext) -> Self {
        pending!("hub::new_gate")
    }

    pub fn broadcast(&self, _msg: StreamMessage) {
        pending!("hub::gate_broadcast")
    }

    pub fn subscribe(&self, _session: &SessionRef, _stream: &str, _identifier: &str) {
        pending!("hub::gate_subscribe")
    }

    pub fn unsubscribe(&self, _session: &SessionRef, _stream: &str, _identifier: &str) {
        pending!("hub::gate_unsubscribe")
    }

    /// Number of unique streams.
    pub fn size(&self) -> usize {
        pending!("hub::gate_size")
    }

    /// Sizes of the internal indexes (upstream tests read the unexported
    /// `streams` and `sessionsStreams` maps): `(streams, sessions_streams)`.
    pub fn index_sizes(&self) -> (usize, usize) {
        pending!("hub::gate_index_sizes")
    }
}

/// Builds the reply delivered to subscribers of `identifier` (upstream unexported
/// `buildMessage`).
pub fn build_message(_msg: &StreamMessage, _identifier: &str) -> EncodedMessage {
    pending!("hub::build_message")
}
