//! Scenario bodies of the local AnyT extensions (`etc/anyt/**` at the pinned
//! commit), run per conformance variant by `anyt_<variant>.rs`. The Ruby files
//! remain runnable through `compat/scripts/reference-conformance.sh`.

use cabby_testkit::cable_client::{CableClient, Options};
use serde_json::{Value, json};

use crate::anyt_support::*;

fn subscribe_acked(w: &World, c: &mut CableClient, id: &str) {
    c.send(&subscribe_request(id));
    assert_message(&ack(id), &w.recv(c));
}

/// `before`: client, client2 and client3 subscribe to `class`'s channel.
fn three_subscribed(w: &World, class: &str) -> (CableClient, CableClient, CableClient, String) {
    let id = identifier(&[("channel", json!(channel(class)))]);
    let mut client = w.default_client();
    let mut client2 = w.default_client();
    let mut client3 = w.default_client();
    client.send(&subscribe_request(&id));
    client2.send(&subscribe_request(&id));
    client3.send(&subscribe_request(&id));
    assert_message(&ack(&id), &w.recv(&mut client));
    assert_message(&ack(&id), &w.recv(&mut client2));
    assert_message(&ack(&id), &w.recv(&mut client3));
    (client, client2, client3, id)
}

// ------------------------------------------------------------------ broadcast_tests

/// broadcast_tests/options_test.rb
pub fn broadcast_to_others(v: Variant) {
    let w = World::start(v);
    let (mut client, mut client2, mut client3, id) = three_subscribed(&w, "BroadcastWithOptionsChannel");

    client.send(&json!({
        "command": "message",
        "identifier": id,
        "data": json!({"action": "speak", "content": "The Other Side"}).to_string(),
    }));

    let msg = json!({"identifier": id, "message": {"content": "The Other Side"}});
    assert_message(&msg, &w.recv(&mut client2));
    assert_message(&msg, &w.recv(&mut client3));
    assert_receive_times_out(&mut client); // "Client 1 should not receive the message"
}

/// broadcast_tests/whisper_test.rb
pub fn whisper_to_others(v: Variant) {
    let w = World::start(v);
    let (mut client, mut client2, mut client3, id) = three_subscribed(&w, "WhisperChannel");

    client.send(&json!({"command": "whisper", "identifier": id, "data": {"event": "typing", "user": "Vova"}}));

    let msg = json!({"identifier": id, "message": {"event": "typing", "user": "Vova"}});
    assert_message(&msg, &w.recv(&mut client2));
    assert_message(&msg, &w.recv(&mut client3));
    assert_receive_times_out(&mut client);
}

// ------------------------------------------------------------------ broker_tests

fn unix_now() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock").as_secs() as i64
}

fn history_msg(id: &str, user_id: i64, status: &str) -> Value {
    json!({"identifier": id, "message": {"data": {"user_id": user_id, "status": status}}})
}

/// `before`: two broadcasts to `history_a`, then AnyT's "sleep 2 # Make sure
/// broadcasts have been published". Replaced by a witness subscribed to the
/// stream beforehand: the broker stores a message before delivering it, so
/// once the witness has both, history holds them.
fn history_before(w: &World, id: &str) {
    let mut witness = w.default_client();
    subscribe_acked(w, &mut witness, id);

    w.broadcast("history_a", &json!({"data": {"user_id": 1, "status": "left"}}));
    w.broadcast("history_a", &json!({"data": {"user_id": 2, "status": "join"}}));

    assert_message(&history_msg(id, 1, "left"), &w.recv(&mut witness));
    assert_message(&history_msg(id, 2, "join"), &w.recv(&mut witness));
}

#[track_caller]
fn assert_equal(expected: &Value, actual: &Value) {
    assert!(ruby_eq(expected, actual), "assert_equal: expected {expected}, got {actual}");
}

/// broker_tests/history_test.rb
pub fn history_by_timestamp(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("HistoryChannel")))]);
    history_before(&w, &id);
    let mut client = w.default_client();

    client.send(&subscribe_request(&id));
    assert_equal(&ack(&id), &w.recv(&mut client));

    client.send(&json!({"command": "history", "identifier": id, "history": {"since": unix_now() - 30}}));

    assert_message(&history_msg(&id, 1, "left"), &w.recv(&mut client));
    assert_message(&history_msg(&id, 2, "join"), &w.recv(&mut client));
}

/// broker_tests/history_test.rb
pub fn history_by_offset(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("HistoryChannel")))]);
    history_before(&w, &id);
    let mut client = w.default_client();

    let subscribe = subscribe_request(&id);
    client.send(&subscribe);
    assert_equal(&ack(&id), &w.recv(&mut client));

    w.broadcast("history_a", &json!({"data": {"user_id": 42, "status": "alive"}}));

    let msg = history_msg(&id, 42, "alive");
    let received = w.recv(&mut client);
    assert_message(&msg, &received);
    for key in ["stream_id", "offset", "epoch"] {
        assert!(received.get(key).is_some(), "assert_includes {received}, {key:?}");
    }

    let mut another_client = w.default_client();
    another_client.send(&subscribe);
    assert_equal(&ack(&id), &w.recv(&mut another_client));

    let stream_id = received["stream_id"].as_str().expect("stream_id").to_owned();
    let offset = received["offset"].as_i64().expect("integer offset");
    another_client.send(&json!({
        "command": "history",
        "identifier": id,
        "history": {"streams": {stream_id: {"offset": offset - 1, "epoch": received["epoch"]}}},
    }));

    assert_message(&msg, &w.recv(&mut another_client));
}

/// broker_tests/history_test.rb
pub fn history_on_subscribe(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("HistoryChannel")))]);
    history_before(&w, &id);
    let mut client = w.default_client();

    client.send(&json!({"command": "subscribe", "identifier": id, "history": {"since": unix_now() - 30}}));

    assert_equal(&ack(&id), &w.recv(&mut client));
    assert_message(&history_msg(&id, 1, "left"), &w.recv(&mut client));
    assert_message(&history_msg(&id, 2, "join"), &w.recv(&mut client));
}

/// broker_tests/restore_test.rb. In the Ruby scenario the `before` block's
/// `client` is a local variable, so the scenario's `client.close` closes a new
/// `let(:client)` connection while the original session stays open; the port
/// keeps that.
pub fn restore_session_by_sid(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("SessionCacheChannel")))]);
    let ext = |o: Options| Options { protocol: "actioncable-v1-ext-json".into(), ..o };

    // before
    let mut before_client = w.client(ext(ignoring(&["ping"])));
    let welcome_msg = w.recv(&mut before_client);
    assert_message(&json!({"type": "welcome"}), &welcome_msg);
    assert!(welcome_msg.get("sid").is_some(), "assert_includes {welcome_msg}, \"sid\"");
    let sid = welcome_msg["sid"].as_str().expect("sid string").to_owned();
    before_client.send(&subscribe_request(&id));
    assert_equal(&ack(&id), &w.recv(&mut before_client));
    w.broadcast("cache_a", &json!({"data": {"user_id": 1, "status": "left"}}));
    assert_message(
        &json!({"identifier": id, "message": {"data": {"user_id": 1, "status": "left"}}}),
        &w.recv(&mut before_client),
    );

    // scenario
    w.default_client().close(false);

    let mut another_client =
        w.client(Options { headers: vec![("X-ANYCABLE-RESTORE-SID".into(), sid)], ..ext(ignoring(&["ping"])) });
    assert_message(&json!({"type": "welcome", "restored": true}), &w.recv(&mut another_client));

    w.broadcast("cache_a", &json!({"data": {"user_id": 2, "status": "join"}}));

    assert_message(
        &json!({"identifier": id, "message": {"data": {"user_id": 2, "status": "join"}}}),
        &w.recv(&mut another_client),
    );
    drop(before_client);
}

// ------------------------------------------------------------------ presence_tests

fn presence_client(w: &World, tag: &str, user: &str, name: Option<&str>) -> CableClient {
    let mut qs = format!("test={tag}&user_id={user}");
    if let Some(n) = name {
        qs.push_str(&format!("&name={n}"));
    }
    w.client(Options { qs, protocol: "actioncable-v1-ext-json".into(), ..ignoring(&["ping", "welcome"]) })
}

fn presence(id: &str, message: Value) -> Value {
    json!({"identifier": id, "type": "presence", "message": message})
}

/// presence_tests/perform_test.rb
pub fn presence_join_leave_on_perform(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("PresenceChannel")))]);
    let mut client = w.default_client();
    subscribe_acked(&w, &mut client, &id);
    let mut client2 = presence_client(&w, "presence_perform", "2024", None);
    subscribe_acked(&w, &mut client2, &id);

    client2.send(&json!({
        "command": "message",
        "identifier": id,
        "data": json!({"action": "follow", "name": "Vova"}).to_string(),
    }));

    let join = presence(&id, json!({"type": "join", "id": "2024", "info": {"name": "Vova"}}));
    assert_message(&join, &w.recv(&mut client));
    assert_message(&join, &w.recv(&mut client2));

    client2.send(&json!({"command": "message", "identifier": id, "data": json!({"action": "unfollow"}).to_string()}));

    let leave = presence(&id, json!({"type": "leave", "id": "2024"}));
    assert_message(&leave, &w.recv(&mut client));
    assert_message(&leave, &w.recv(&mut client2));
}

/// presence_tests/subscribe_test.rb. Inherent window: the leave event follows
/// the disconnect after the presence TTL (`ANYCABLE_PRESENCE_TTL=2`), within
/// AnyT's 5 s receive timeout.
pub fn presence_implicit_join_leave_on_disconnect(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("PresenceImplicitChannel")))]);
    let mut client = w.default_client();
    subscribe_acked(&w, &mut client, &id);
    let mut client2 = presence_client(&w, "presence_subscribe", "2024", Some("Bart"));
    subscribe_acked(&w, &mut client2, &id);

    let join = presence(&id, json!({"type": "join", "id": "2024", "info": {"name": "Bart"}}));
    assert_message(&join, &w.recv(&mut client));
    assert_message(&join, &w.recv(&mut client2));

    let mut client3 = presence_client(&w, "presence_subscribe", "2025", Some("Lisa"));
    subscribe_acked(&w, &mut client3, &id);

    let join = presence(&id, json!({"type": "join", "id": "2025", "info": {"name": "Lisa"}}));
    assert_message(&join, &w.recv(&mut client));
    assert_message(&join, &w.recv(&mut client2));
    assert_message(&join, &w.recv(&mut client3));

    client2.close(false);

    let leave = presence(&id, json!({"type": "leave", "id": "2024"}));
    assert_message(&leave, &w.recv(&mut client));
    assert_message(&leave, &w.recv(&mut client3));
}

/// presence_tests/subscribe_test.rb. Inherent windows: AnyT's 0.5 s "no event"
/// check, and the leave after the presence TTL (2 s).
pub fn presence_explicit_duplicate_sessions(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("PresenceExplicitChannel")))]);
    let mut client3 = presence_client(&w, "presence_subscribe", "2025", Some("Lisa"));

    client3.send(&subscribe_request(&id));
    assert_message(&ack(&id), &w.recv(&mut client3));
    assert_message(
        &presence(&id, json!({"type": "join", "id": "5202", "info": {"whois": "Lisa"}})),
        &w.recv(&mut client3),
    );

    let mut client2 = presence_client(&w, "presence_subscribe", "2024", Some("Bart"));
    subscribe_acked(&w, &mut client2, &id);

    let join = presence(&id, json!({"type": "join", "id": "4202", "info": {"whois": "Bart"}}));
    assert_message(&join, &w.recv(&mut client3));
    assert_message(&join, &w.recv(&mut client2));

    let mut client2_clone = presence_client(&w, "presence_subscribe", "2024", Some("Bart"));
    subscribe_acked(&w, &mut client2_clone, &id);

    client2.close(false);

    // no event should be sent
    assert_receive_times_out(&mut client3);

    client2_clone.close(false);

    assert_message(&presence(&id, json!({"type": "leave", "id": "4202"})), &w.recv(&mut client3));
}
