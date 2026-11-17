//! Action Cable WebSocket client (upstream `etc/benchi/client`, package `anycable`, a
//! hard fork of `cinemast/anycable-go-client`).
//!
//! One reader per connection routes server messages to per-subscription queues;
//! `ping` is answered with `pong`, `disconnect` closes the client, and
//! confirm/reject resolve the pending subscription.

use std::time::Duration;

use cabby_core::pending;
use serde::{Deserialize, Serialize};

use crate::BenchError;

/// Per-subscription queue capacity when [`Client::set_subscription_capacity`] was
/// not called (upstream `DefaultSubscriptionCapacity`).
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1000;

/// A channel identifier, rendered as the JSON string sent in `identifier`.
pub trait ChannelIdentifier {
    fn identifier(&self) -> String;
}

impl ChannelIdentifier for String {
    fn identifier(&self) -> String {
        self.clone()
    }
}

/// A client command (`{"command":..,"identifier":..,"data":..,"action":..}`);
/// absent fields are omitted, as with Go's `omitempty` pointers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
}

/// A server message. `message` keeps the raw JSON value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub message: Option<serde_json::Value>,
    #[serde(default)]
    pub identifier: Option<String>,
}

/// A subscription: its identifier, resolution flags and message queue.
pub struct Subscription {
    pub identifier: String,
    _messages: std::sync::Mutex<Option<std::sync::mpsc::Receiver<Event>>>,
}

impl Subscription {
    pub fn subscribed(&self) -> bool {
        pending!("benchi::cable::subscription_subscribed")
    }

    pub fn rejected(&self) -> bool {
        pending!("benchi::cable::subscription_rejected")
    }

    /// Next message on this subscription; `None` once the queue is closed (reject or
    /// connection loss).
    pub fn recv(&self) -> Option<Event> {
        pending!("benchi::cable::subscription_recv")
    }
}

pub struct Client {
    _url: String,
}

impl Client {
    /// Builds a client for `url`; nothing is dialed until [`Client::connect`].
    pub fn new(_url: &str) -> Self {
        pending!("benchi::cable::new_client")
    }

    pub fn set_subscription_capacity(&self, _capacity: usize) {
        pending!("benchi::cable::set_subscription_capacity")
    }

    /// Dials and waits for the welcome message; any other first message is an error.
    pub fn connect(&self) -> Result<(), BenchError> {
        pending!("benchi::cable::connect")
    }

    pub fn subscribe(&self, _identifier: &dyn ChannelIdentifier) -> Result<Subscription, BenchError> {
        pending!("benchi::cable::subscribe")
    }

    /// Subscribes and waits for confirmation, rejection, connection loss or `timeout`;
    /// errors name the identifier.
    pub fn subscribe_and_wait(
        &self,
        _identifier: &dyn ChannelIdentifier,
        _timeout: Duration,
    ) -> Result<Subscription, BenchError> {
        pending!("benchi::cable::subscribe_and_wait")
    }

    pub fn unsubscribe(&self, _subscription: &Subscription) -> Result<(), BenchError> {
        pending!("benchi::cable::unsubscribe")
    }

    /// Sends a `message` command with `data` set to the JSON encoding of `message`.
    pub fn send(&self, _identifier: &dyn ChannelIdentifier, _message: &serde_json::Value) -> Result<(), BenchError> {
        pending!("benchi::cable::send")
    }

    pub fn close(&self) -> Result<(), BenchError> {
        pending!("benchi::cable::close")
    }
}
