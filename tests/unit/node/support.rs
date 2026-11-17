//! Helpers shared by the node translations: upstream `node_mocks_test.go`
//! (through the facade's `new_mock_node` / `new_mock_session*`), the receive
//! assertions of `broker_integration_test.go` and the helpers at the end of
//! `node_test.go`.

use std::time::{Duration, Instant};

use cabby_core::common::{Message, MessageData, StringMap};
use cabby_testkit::reference::node::{TestNode, TestSession};
use serde_json::Value;

pub use cabby_testkit::reference::node::DEADLINE;

pub fn map(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// `&common.Message{Identifier: identifier}`.
pub fn msg(identifier: &str) -> Message {
    Message { identifier: identifier.to_owned(), ..Default::default() }
}

/// `&common.Message{Identifier: identifier, Data: data}` with string data.
pub fn msg_data(identifier: &str, data: &str) -> Message {
    Message {
        identifier: identifier.to_owned(),
        data: Some(MessageData::Json(Value::String(data.to_owned()))),
        ..Default::default()
    }
}

/// A message with a command (`Command: "subscribe"` etc.).
pub fn cmd(command: &str, identifier: &str) -> Message {
    Message { command: command.to_owned(), identifier: identifier.to_owned(), ..Default::default() }
}

/// Upstream `requireReceive`: the next frame equals `expected` byte for byte.
pub fn require_receive(s: &TestSession, expected: &str) {
    let msg = s.read_string().unwrap_or_else(|e| panic!("expected {expected:?}, read failed: {e}"));
    assert_eq!(expected, msg);
}

/// Upstream `assertReceiveMsg`: the next frame decoded as a JSON object.
pub fn assert_receive_msg(s: &TestSession) -> Value {
    let msg = s.read().unwrap_or_else(|e| panic!("read failed: {e}"));
    let parsed: Value = serde_json::from_slice(&msg).expect("a JSON frame");
    assert!(parsed.is_object(), "expected a JSON object, got {parsed}");
    parsed
}

/// Upstream `assertReceive`: the next frame equals `expected` as JSON (object
/// key order is irrelevant; Go decodes into `map[string]interface{}`).
pub fn assert_receive(s: &TestSession, expected: &str) {
    let parsed = assert_receive_msg(s);
    let expected: Value = serde_json::from_str(expected).expect("expected JSON");
    assert_eq!(expected, parsed);
}

/// Upstream `readMessages(conn, count)`: `count` frames, failing on the first
/// read error (decorated with the number received when some were).
pub fn read_messages(s: &TestSession, count: usize) -> Result<Vec<String>, String> {
    let mut messages = Vec::new();
    for _ in 0..count {
        match s.read_string() {
            Ok(m) => messages.push(m),
            Err(e) if !messages.is_empty() => return Err(format!("received only {} messsages: {e}", messages.len())),
            Err(e) => return Err(e),
        }
    }
    Ok(messages)
}

/// Upstream `subscribeSessionToStream`: registers the session with the hub,
/// adds the channel and stream to its subscriptions, subscribes it in the hub
/// and the broker. Dropping the guard removes the session from the hub.
pub fn subscribe_session_to_stream<'a>(
    s: &'a TestSession,
    n: &'a TestNode,
    identifier: &str,
    stream: &str,
) -> RemoveOnDrop<'a> {
    n.hub_add_session(s);

    s.add_channel(identifier);
    s.add_channel_stream(identifier, stream);
    n.hub_subscribe_session(s, stream, identifier);
    n.broker_subscribe(stream);

    RemoveOnDrop { node: n, session: s }
}

/// `defer node.hub.RemoveSession(session)` (cleanup only).
pub struct RemoveOnDrop<'a> {
    node: &'a TestNode,
    session: &'a TestSession,
}

pub fn remove_on_drop<'a>(node: &'a TestNode, session: &'a TestSession) -> RemoveOnDrop<'a> {
    RemoveOnDrop { node, session }
}

impl Drop for RemoveOnDrop<'_> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.node.hub_remove_session(self.session);
        }
    }
}

/// testify `Eventually` / `EventuallyWithT` over state that offers no change
/// notification (the hub's registry): a bounded probe at the upstream tick,
/// failing after the upstream deadline.
pub fn eventually(what: &str, deadline: Duration, tick: Duration, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + deadline;
    loop {
        if cond() {
            return;
        }
        assert!(Instant::now() < end, "{what}: not reached within {deadline:?}");
        std::thread::sleep(tick);
    }
}

/// Asserts the frame is absent: the mock connection read times out
/// (upstream `msg, err := conn.Read(); assert.Nil(msg); assert.Error(err)`).
pub fn assert_no_message(s: &TestSession, what: &str) {
    let r = s.read();
    assert!(r.is_err(), "{what}: expected no message, got {:?}", r.map(String::from_utf8));
}

pub fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

pub fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}
