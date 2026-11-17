//! Scenario bodies of the AnyT core suite (anyt-core 1.4.0 `lib/anyt/tests/**`,
//! MIT, © palkan), run per conformance variant by `anyt_<variant>.rs`.
//!
//! Each function follows its Ruby scenario step by step, including the
//! feature's `before` block and the lazily created `let(:client)` /
//! `let(:client2)` clients (`build_client(ignore: %w[ping welcome])`). AnyT's
//! `after` hook (`close(allow_messages: true)` on every client) asserts nothing
//! and is replaced by dropping the clients.

use serde_json::{Value, json};

use crate::anyt_support::*;

// ------------------------------------------------------------------ core

/// core/ping_test.rb. Inherent window: the server's default `ping_interval`
/// (3 s), so two pings take about 6 s; each must arrive within AnyT's 5 s.
pub fn ping(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(ignoring(&["welcome"]));
    let mut previous = 0i64;
    for _ in 0..2 {
        let ping = w.recv(&mut client);
        let current = &ping["message"];
        assert_eq!(json!("ping"), ping["type"]);
        assert!(current.is_i64(), "assert_kind_of Integer: {current}");
        let current = current.as_i64().unwrap();
        assert!(previous < current, "refute_operator {previous}, :>=, {current}");
        previous = current;
    }
}

/// core/welcome_test.rb
pub fn welcome(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(ignoring(&["ping"]));
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));
}

// ------------------------------------------------------------------ features

fn channel_state_before(w: &World) -> (cabby_testkit::cable_client::CableClient, String) {
    let identifier = identifier(&[("channel", json!(channel("ChannelStateChannel"))), ("name", json!("chipolino"))]);
    let mut client = w.default_client();
    client.send(&subscribe_request(&identifier));
    assert_message(&ack(&identifier), &w.recv(&mut client));
    (client, identifier)
}

/// features/channel_state_test.rb
pub fn channel_state_kept_between_commands(v: Variant) {
    let w = World::start(v);
    let (mut client, identifier) = channel_state_before(&w);

    client.send(&json!({
        "command": "message",
        "identifier": identifier,
        "data": json!({"action": "tick"}).to_string(),
    }));

    let msg = json!({"identifier": identifier, "message": {"count": 3, "name": "chipolino"}});
    assert_message(&msg, &w.recv(&mut client));
}

/// features/channel_state_test.rb
pub fn channel_state_available_in_unsubscribe(v: Variant) {
    let w = World::start(v);
    let identifier2 = identifier(&[
        ("channel", json!(channel("ChannelStateChannel"))),
        ("name", json!("chipollone")),
        ("notify_disconnect", json!(true)),
    ]);
    let (mut client, identifier) = channel_state_before(&w);
    let mut client2 = w.default_client();

    client2.send(&subscribe_request(&identifier2));
    assert_message(&ack(&identifier2), &w.recv(&mut client2));

    client2.close(false);

    let msg = json!({"identifier": identifier, "message": {"data": "user left: chipollone"}});
    assert_message(&msg, &w.recv(&mut client));
}

/// features/remote_disconnect_test.rb. AnyT's `sleep 1` guards an Action
/// Cable-only race; with AnyCable the `welcome` arrives after the session is
/// registered, so the remote disconnect follows it directly.
///
/// The remote disconnect travels through the server's pub/sub, whose Redis
/// command channel is shared by every server on the shared Redis, so the
/// scenario is serialized across variants (all use `uid=26`) and publishes
/// only once this server's pub/sub has subscribed.
pub fn remote_disconnect_by_id(v: Variant) {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let w = World::start(v);
    let mut client =
        w.client(cabby_testkit::cable_client::Options { qs: "test=uid&uid=26".into(), ..ignoring(&["ping"]) });
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));

    w.await_redis_pubsub();
    w.app.remote_disconnect_uid("26");

    assert_message(&json!({"type": "disconnect", "reconnect": true, "reason": "remote"}), &w.recv(&mut client));
    client.wait_for_close(cabby_testkit::cable_client::RECEIVE_DEADLINE);
    assert!(client.is_closed());
}

/// features/server_restart_test.rb. `restart_server!` stops the server
/// (ChildProcess: SIGTERM, SIGKILL after 3 s) and starts it again; the
/// disconnect message is sent while stopping, so the port stops the server
/// and does not start a new one.
pub fn server_restart_disconnect_message(v: Variant) {
    let mut w = World::start(v);
    // `ignore: %(ping)` is the String "ping"; `String#include?` ignores pings.
    let mut client = w.client(cabby_testkit::cable_client::Options {
        qs: "test=reasons&reason=server_restart".into(),
        ..ignoring(&["ping"])
    });
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));

    w.sut.process.stop(std::time::Duration::from_secs(3)).expect("stopping the server");

    assert_message(
        &json!({"type": "disconnect", "reconnect": true, "reason": "server_restart"}),
        &client.receive().unwrap_or_else(|e| panic!("{e}\nSUT output:\n{}", w.sut.process.output())),
    );
}

// ------------------------------------------------------------------ request

/// request/channel_test.rb
pub fn request_channel_has_access_to_request(v: Variant) {
    let w = World::start(v);
    let mut client =
        w.client(cabby_testkit::cable_client::Options { qs: "token=secret".into(), ..ignoring(&["ping"]) });
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));

    let id = identifier(&[("channel", json!(channel("RequestChannel")))]);
    client.send(&subscribe_request(&id));

    assert_message(&ack(&id), &w.recv(&mut client));
}

fn with_qs(qs: &str) -> cabby_testkit::cable_client::Options {
    cabby_testkit::cable_client::Options { qs: qs.into(), ..Default::default() }
}

/// request/connection_test.rb
pub fn request_url_is_set(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(with_qs("test=request_url"));
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));
}

/// request/connection_test.rb
pub fn request_reject_without_cookies(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(with_qs("test=cookies"));
    client.wait_for_close(cabby_testkit::cable_client::RECEIVE_DEADLINE);
    assert!(client.is_closed());
}

/// request/connection_test.rb
pub fn request_accept_with_cookies(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(cabby_testkit::cable_client::Options {
        cookies: "username=john green".into(),
        ..with_qs("test=cookies")
    });
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));
}

/// request/connection_test.rb
pub fn request_reject_without_header(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(with_qs("test=headers"));
    client.wait_for_close(cabby_testkit::cable_client::RECEIVE_DEADLINE);
    assert!(client.is_closed());
}

/// request/connection_test.rb
pub fn request_accept_with_header(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(cabby_testkit::cable_client::Options {
        headers: vec![("x-api-token".into(), "abc".into())],
        ..with_qs("test=headers")
    });
    assert_message(&json!({"type": "welcome"}), &w.recv(&mut client));
}

/// request/disconnect_reasons_test.rb
pub fn request_disconnect_message_when_rejected(v: Variant) {
    let w = World::start(v);
    let mut client = w.client(with_qs("test=reasons&reason=unauthorized"));
    assert_message(&json!({"type": "disconnect", "reconnect": false, "reason": "unauthorized"}), &w.recv(&mut client));
    client.wait_for_close(cabby_testkit::cable_client::RECEIVE_DEADLINE);
    assert!(client.is_closed());
}

fn subscribe_acked(w: &World, c: &mut cabby_testkit::cable_client::CableClient, id: &str) {
    c.send(&subscribe_request(id));
    assert_message(&ack(id), &w.recv(c));
}

/// request/disconnection_test.rb
pub fn request_disconnect_unsubscribes_different_channels(v: Variant) {
    let w = World::start(v);
    let a = identifier(&[("channel", json!(channel("RequestAChannel")))]);
    let b = identifier(&[("channel", json!(channel("RequestBChannel")))]);
    let mut client = w.default_client();
    subscribe_acked(&w, &mut client, &a);
    subscribe_acked(&w, &mut client, &b);
    let mut client2 = w.default_client();
    subscribe_acked(&w, &mut client2, &a);
    subscribe_acked(&w, &mut client2, &b);

    client2.close(false);

    let msg = json!({"identifier": a, "message": {"data": "user left"}});
    let msg2 = json!({"identifier": b, "message": {"data": "user left"}});
    let msgs = [w.recv(&mut client), w.recv(&mut client)];
    assert_includes_message(&msgs, &msg);
    assert_includes_message(&msgs, &msg2);
}

/// request/disconnection_test.rb
pub fn request_disconnect_unsubscribes_same_channel_subscriptions(v: Variant) {
    let w = World::start(v);
    let c = channel("RequestCChannel");
    let id = identifier(&[("channel", json!(c))]);
    let id1 = identifier(&[("channel", json!(c)), ("id", json!(1))]);
    let id2 = identifier(&[("channel", json!(c)), ("id", json!(2))]);
    let mut client = w.default_client();
    subscribe_acked(&w, &mut client, &id);
    let mut client2 = w.default_client();
    subscribe_acked(&w, &mut client2, &id1);
    subscribe_acked(&w, &mut client2, &id2);

    client2.close(false);

    let msg = json!({"identifier": id, "message": {"data": "user left1"}});
    let msg2 = json!({"identifier": id, "message": {"data": "user left2"}});
    let msgs = [w.recv(&mut client), w.recv(&mut client)];
    assert_includes_message(&msgs, &msg);
    assert_includes_message(&msgs, &msg2);
}

// ------------------------------------------------------------------ streams

/// `before`: the default client subscribes to `class`'s channel and is acked.
fn subscribed(w: &World, class: &str) -> (cabby_testkit::cable_client::CableClient, String) {
    let id = identifier(&[("channel", json!(channel(class)))]);
    let mut client = w.default_client();
    subscribe_acked(w, &mut client, &id);
    (client, id)
}

fn message(id: &str, message: Value) -> Value {
    json!({"identifier": id, "message": message})
}

/// streams/broadcast_test.rb
pub fn broadcast_object(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "BroadcastDataToStreamChannel");

    w.broadcast("a", &json!({"data": {"user_id": 1, "status": "left", "meta": {"connection_time": "10s"}}}));

    let msg = message(&id, json!({"data": {"user_id": 1, "status": "left", "meta": {"connection_time": "10s"}}}));
    assert_message(&msg, &w.recv(&mut client));
}

/// streams/broadcast_test.rb
pub fn broadcast_custom_string(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "BroadcastDataToStreamChannel");

    w.broadcast("a", &json!("<script>alert('Message!');</script>"));

    assert_message(&message(&id, json!("<script>alert('Message!');</script>")), &w.recv(&mut client));
}

/// streams/broadcast_test.rb
pub fn broadcast_json_string(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "BroadcastDataToStreamChannel");

    w.broadcast("a", &json!(r#"{"script":{"alert":"Message!"}}"#));

    assert_message(&message(&id, json!(r#"{"script":{"alert":"Message!"}}"#)), &w.recv(&mut client));
}

fn two_subscribed(
    w: &World,
) -> (cabby_testkit::cable_client::CableClient, cabby_testkit::cable_client::CableClient, String) {
    let id = identifier(&[("channel", json!(channel("StreamsWithManyClientsChannel")))]);
    let mut client = w.default_client();
    let mut client2 = w.default_client();
    client.send(&subscribe_request(&id));
    client2.send(&subscribe_request(&id));
    assert_message(&ack(&id), &w.recv(&mut client));
    assert_message(&ack(&id), &w.recv(&mut client2));
    (client, client2, id)
}

/// streams/multiple_clients_test.rb
pub fn many_clients_receive(v: Variant) {
    let w = World::start(v);
    let (mut client, mut client2, id) = two_subscribed(&w);

    w.broadcast("a", &json!({"data": "X"}));

    let msg = message(&id, json!({"data": "X"}));
    assert_message(&msg, &w.recv(&mut client));
    assert_message(&msg, &w.recv(&mut client2));
}

/// streams/multiple_clients_test.rb
pub fn many_clients_one_unsubscribes(v: Variant) {
    let w = World::start(v);
    let (mut client, mut client2, id) = two_subscribed(&w);

    w.broadcast("a", &json!({"data": "X"}));
    let msg = message(&id, json!({"data": "X"}));
    assert_message(&msg, &w.recv(&mut client));
    assert_message(&msg, &w.recv(&mut client2));

    client.send(&json!({"command": "unsubscribe", "identifier": id}));
    // ActionCable doesn't provide an unsubscription ack (AnyT: `sleep 1`).
    w.barrier(&mut client);

    w.broadcast("a", &json!({"data": "Y"}));

    assert_message(&message(&id, json!({"data": "Y"})), &w.recv(&mut client2));
    assert_receive_times_out(&mut client);
}

/// streams/multiple_test.rb
pub fn multiple_streams_receive_both(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "MultipleStreamsChannel");

    w.broadcast("a", &json!({"data": "X"}));
    assert_message(&message(&id, json!({"data": "X"})), &w.recv(&mut client));

    w.broadcast("b", &json!({"data": "Y"}));
    assert_message(&message(&id, json!({"data": "Y"})), &w.recv(&mut client));
}

/// streams/multiple_test.rb
pub fn multiple_streams_none_after_unsubscribe(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "MultipleStreamsChannel");

    w.broadcast("a", &json!({"data": "X"}));
    assert_message(&message(&id, json!({"data": "X"})), &w.recv(&mut client));

    client.send(&json!({"command": "unsubscribe", "identifier": id}));
    w.barrier(&mut client); // AnyT: `sleep 1`

    w.broadcast("a", &json!({"data": "Y"}));
    w.broadcast("b", &json!({"data": "Z"}));

    assert_receive_times_out(&mut client);
}

/// streams/single_test.rb
pub fn single_stream_receives(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "SingleStreamChannel");

    w.broadcast("a", &json!({"data": "X"}));
    assert_message(&message(&id, json!({"data": "X"})), &w.recv(&mut client));

    w.broadcast("a", &json!({"data": "Y"}));
    assert_message(&message(&id, json!({"data": "Y"})), &w.recv(&mut client));
}

/// streams/single_test.rb
pub fn single_stream_none_after_unsubscribe(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "SingleStreamChannel");

    w.broadcast("a", &json!({"data": "X"}));
    assert_message(&message(&id, json!({"data": "X"})), &w.recv(&mut client));

    client.send(&json!({"command": "unsubscribe", "identifier": id}));
    w.barrier(&mut client); // AnyT: `sleep 1`

    w.broadcast("a", &json!({"data": "Y"}));

    assert_receive_times_out(&mut client);
}

/// streams/single_test.rb
pub fn single_stream_same_channel_different_params(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "SingleStreamChannel");
    let id2 = identifier(&[("channel", json!(channel("SingleStreamChannel"))), ("some_param", json!("test"))]);

    client.send(&subscribe_request(&id2));
    assert_message(&ack(&id2), &w.recv(&mut client));

    w.broadcast("a", &json!({"data": "XX"}));

    let received = [w.recv(&mut client), w.recv(&mut client)];
    assert_includes_message(&received, &message(&id, json!({"data": "XX"})));
    assert_includes_message(&received, &message(&id2, json!({"data": "XX"})));
}

/// streams/stop_test.rb. AnyT's two `sleep 0.2` become observations: the
/// session barrier after `unfollow`, and a witness subscribed to the same
/// channel that receives "Y" (so the server has processed the broadcast to "a")
/// before "Z" is broadcast to "b".
pub fn stop_streams_unsubscribes_from_stream(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "StopStreamsChannel");

    w.broadcast("a", &json!({"data": "X"}));
    assert_message(&message(&id, json!({"data": "X"})), &w.recv(&mut client));

    client.send(&json!({
        "command": "message",
        "identifier": id,
        "data": json!({"action": "unfollow", "name": "a"}).to_string(),
    }));
    w.barrier(&mut client); // AnyT: `sleep 0.2 # give some time to commit unsubscribe`
    let mut witness = w.default_client();
    subscribe_acked(&w, &mut witness, &id);

    w.broadcast("a", &json!({"data": "Y"}));
    // AnyT: `sleep 0.2 # "a" should be broadcasted first`
    assert_message(&message(&id, json!({"data": "Y"})), &w.recv(&mut witness));
    w.broadcast("b", &json!({"data": "Z"}));

    assert_message(&message(&id, json!({"data": "Z"})), &w.recv(&mut client));
}

// ------------------------------------------------------------------ subscriptions

/// subscriptions/ack_test.rb
pub fn ack_confirmation(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("SubscriptionAknowledgementChannel")))]);
    let mut client = w.default_client();

    client.send(&subscribe_request(&id));

    assert_message(&ack(&id), &w.recv(&mut client));
}

/// subscriptions/ack_test.rb
pub fn ack_rejection(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("SubscriptionAknowledgementRejectorChannel")))]);
    let mut client = w.default_client();

    client.send(&subscribe_request(&id));

    assert_message(&json!({"identifier": id, "type": "reject_subscription"}), &w.recv(&mut client));
}

/// subscriptions/params_test.rb
pub fn params_same_channel_different_params(v: Variant) {
    let w = World::start(v);
    let c = channel("SubscriptionWithParamsChannel");
    let mut client = w.default_client();

    let id1 = identifier(&[("channel", json!(c)), ("id", json!(1))]);
    client.send(&subscribe_request(&id1));
    assert_message(&ack(&id1), &w.recv(&mut client));

    let id2 = identifier(&[("channel", json!(c)), ("id", json!(2))]);
    client.send(&subscribe_request(&id2));
    assert_message(&ack(&id2), &w.recv(&mut client));
}

/// subscriptions/perform_test.rb
pub fn perform_without_arguments(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "SubscriptionPerformMethodsChannel");

    client.send(&json!({"command": "message", "identifier": id, "data": json!({"action": "tick"}).to_string()}));

    assert_message(&message(&id, json!("tock")), &w.recv(&mut client));
}

/// subscriptions/perform_test.rb
pub fn perform_with_arguments(v: Variant) {
    let w = World::start(v);
    let (mut client, id) = subscribed(&w, "SubscriptionPerformMethodsChannel");

    client.send(&json!({
        "command": "message",
        "identifier": id,
        "data": json!({"action": "echo", "text": "hello"}).to_string(),
    }));

    assert_message(&message(&id, json!({"response": "hello"})), &w.recv(&mut client));
}

/// subscriptions/transmissions_test.rb
pub fn transmissions_from_subscribed(v: Variant) {
    let w = World::start(v);
    let id = identifier(&[("channel", json!(channel("SubscriptionTransmissionsChannel")))]);
    let mut client = w.default_client();

    client.send(&subscribe_request(&id));

    assert_message(&message(&id, json!("hello")), &w.recv(&mut client));
    assert_message(&message(&id, json!("world")), &w.recv(&mut client));
    assert_message(&ack(&id), &w.recv(&mut client));
}
