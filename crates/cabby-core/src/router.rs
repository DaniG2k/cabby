//! Channel-based routing of RPC commands to controllers (upstream package `router`).

use std::collections::HashMap;
use std::sync::Arc;

use crate::gojson;

use crate::common::{CommandResult, ConnectResult, Outcome, SessionEnv};
/// The RPC application interface (upstream `node.Controller`); the canonical
/// trait lives in `common` and is re-exported by the node.
pub use crate::common::{Controller, ControllerError};

/// Routes commands to per-channel controllers by the identifier's `channel` field,
/// falling back to the default controller (upstream `RouterController`).
pub struct RouterController {
    controller: Arc<dyn Controller>,
    routes: HashMap<String, Arc<dyn Controller>>,
}

impl RouterController {
    /// Upstream `NewRouterController(c)`.
    pub fn new(default: Arc<dyn Controller>) -> Self {
        RouterController { controller: default, routes: HashMap::new() }
    }

    /// Replaces the default controller (upstream `SetDefault`).
    pub fn set_default(&mut self, controller: Arc<dyn Controller>) {
        self.controller = controller;
    }

    /// Registers a controller for a channel name.
    pub fn route(&mut self, channel: &str, handler: Arc<dyn Controller>) -> Result<(), ControllerError> {
        if self.routes.contains_key(channel) {
            return Err(ControllerError::new(format!("Route has been already defined: {channel}")));
        }
        self.routes.insert(channel.to_owned(), handler);
        Ok(())
    }

    pub fn empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// Routed channel names (unordered, as upstream iterates a map).
    pub fn routes(&self) -> Vec<String> {
        self.routes.keys().cloned().collect()
    }

    /// The controller routed for the identifier's channel, if any.
    fn handler_for(&self, identifier: &str) -> Option<&Arc<dyn Controller>> {
        if self.routes.is_empty() {
            return None;
        }
        let channel = extract_channel(identifier);
        if channel.is_empty() {
            return None;
        }
        self.routes.get(&channel)
    }

    /// Upstream: a routed handler's answer wins unless it is `(nil, nil)`.
    fn dispatch(
        &self,
        identifier: &str,
        call: impl Fn(&dyn Controller) -> Outcome<CommandResult>,
    ) -> Outcome<CommandResult> {
        if let Some(handler) = self.handler_for(identifier) {
            let res = call(handler.as_ref());
            if res.result.is_some() || res.error.is_some() {
                return res;
            }
        }
        call(self.controller.as_ref())
    }
}

/// Upstream `extractChannel`: the `channel` string of a JSON identifier, or `""`
/// when the identifier is not a JSON object with a string `channel`.
fn extract_channel(identifier: &str) -> String {
    match gojson::decode_string_fields(identifier, "struct { Channel string }", &["channel"]) {
        Ok([channel]) => channel,
        Err(_) => String::new(),
    }
}

impl Controller for RouterController {
    fn start(&self) -> Result<(), ControllerError> {
        self.controller.start()
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        self.controller.shutdown()
    }

    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        self.controller.authenticate(sid, env)
    }

    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.dispatch(channel, |c| c.subscribe(sid, env, ids, channel))
    }

    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        self.dispatch(channel, |c| c.unsubscribe(sid, env, ids, channel))
    }

    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        self.dispatch(channel, |c| c.perform(sid, env, ids, channel, data))
    }

    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        self.controller.disconnect(sid, env, ids, subscriptions)
    }
}

#[cfg(test)]
mod tests {
    use super::extract_channel;

    #[test]
    fn extracts_the_channel_like_go_json_unmarshal() {
        assert_eq!("Chat", extract_channel(r#"{"channel":"Chat","id":1}"#));
        assert_eq!("", extract_channel("chat"));
        assert_eq!("", extract_channel(r#"{"id":1}"#));
        assert_eq!("", extract_channel(r#"{"channel":null}"#));
        assert_eq!("Chat", extract_channel(r#"{"Channel":"Chat"}"#));
        // A non-string channel is an unmarshal error upstream.
        assert_eq!("", extract_channel(r#"{"channel":1}"#));
    }
}
