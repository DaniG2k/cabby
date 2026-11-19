//! The node's views of the `hub` and `pubsub` packages: the canonical types from
//! `cabby_core`, specialized to the node session where generic.

/// Routes stream messages to subscribed sessions (upstream `hub.Hub`): the
/// canonical hub over the concrete node [`Session`](super::Session), which
/// implements [`cabby_core::hub::HubSession`].
pub type Hub = cabby_core::hub::Hub<super::Session>;

/// Single-node subscriber that hands every message straight to the node
/// (upstream `pubsub.NewLegacySubscriber(node)`).
pub use cabby_core::pubsub::LegacySubscriber;
