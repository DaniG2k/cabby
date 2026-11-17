#!/usr/bin/env python3
"""Generates tests/conformance/anyt_<target>.rs: one `#[test]` per AnyT scenario
for each upstream Makefile `test-conformance*` target, with the `/// upstream:`
and `/// note:` annotations `cargo xtask inventory` reads. Scenario bodies live
in anyt_core.rs / anyt_ext.rs; variant setup in anyt_support.rs.

    python3 compat/scripts/gen_anyt_variants.py [OUT_DIR]   # default: tests/conformance
"""
import os
import sys

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "../../tests/conformance")
A = "anyt-core-1.4.0:lib/anyt/tests/"
E = "etc/anyt/"

CORE = [
    ("ping", A + "core/ping_test.rb#Ping/Client receives pings with timestamps"),
    ("welcome", A + 'core/welcome_test.rb#Welcome/Client receives "welcome" message'),
    ("channel_state_available_in_unsubscribe", A + "features/channel_state_test.rb#Channel state/Channel state is available in #unsubscribe callbacks"),
    ("channel_state_kept_between_commands", A + "features/channel_state_test.rb#Channel state/Channel state is kept between commands"),
    ("remote_disconnect_by_id", A + "features/remote_disconnect_test.rb#Remote disconnect/Close single connection by id"),
    ("server_restart_disconnect_message", A + "features/server_restart_test.rb#Server restart/Client receives disconnect message"),
    ("request_channel_has_access_to_request", A + "request/channel_test.rb#Request/Channel has access to request"),
    ("request_accept_with_cookies", A + "request/connection_test.rb#Request/Accepts when required cookies are set"),
    ("request_accept_with_header", A + "request/connection_test.rb#Request/Accepts when required header is set"),
    ("request_reject_without_cookies", A + "request/connection_test.rb#Request/Reject when required cookies are not set"),
    ("request_reject_without_header", A + "request/connection_test.rb#Request/Reject when required header is missing"),
    ("request_url_is_set", A + "request/connection_test.rb#Request/Url is set during connection"),
    ("request_disconnect_message_when_rejected", A + "request/disconnect_reasons_test.rb#Request/Receives disconnect message when rejected"),
    ("request_disconnect_unsubscribes_different_channels", A + "request/disconnection_test.rb#Request/Client disconnect invokes #unsubscribe callbacks for different channels"),
    ("request_disconnect_unsubscribes_same_channel_subscriptions", A + "request/disconnection_test.rb#Request/Client disconnect invokes #unsubscribe callbacks for multiple subscriptions from the same channel"),
    ("broadcast_json_string", A + "streams/broadcast_test.rb#Broadcast data to stream/Broadcast JSON string"),
    ("broadcast_custom_string", A + "streams/broadcast_test.rb#Broadcast data to stream/Broadcast custom string"),
    ("broadcast_object", A + "streams/broadcast_test.rb#Broadcast data to stream/Broadcast object"),
    ("many_clients_one_unsubscribes", A + "streams/multiple_clients_test.rb#Streams with many clients/Client receive messages when another client removes subscription"),
    ("many_clients_receive", A + "streams/multiple_clients_test.rb#Streams with many clients/Multiple clients receive messages from stream"),
    ("multiple_streams_none_after_unsubscribe", A + "streams/multiple_test.rb#Multiple streams/Client does not receive messages from any stream after removing subscription"),
    ("multiple_streams_receive_both", A + "streams/multiple_test.rb#Multiple streams/Client receives messages from both streams"),
    ("single_stream_none_after_unsubscribe", A + "streams/single_test.rb#Single stream/Client does not receive messages from the stream after removing subscription"),
    ("single_stream_receives", A + "streams/single_test.rb#Single stream/Client receives messages from the stream"),
    ("single_stream_same_channel_different_params", A + "streams/single_test.rb#Single stream/Client receives multiple messages when subscribed to the same channel with different params"),
    ("stop_streams_unsubscribes_from_stream", A + "streams/stop_test.rb#Stop streams/Client unsubscribes from the stream"),
    ("ack_confirmation", A + "subscriptions/ack_test.rb#Subscription aknowledgement/Client receives subscription confirmation"),
    ("ack_rejection", A + "subscriptions/ack_test.rb#Subscription aknowledgement/Client receives subscription rejection"),
    ("params_same_channel_different_params", A + "subscriptions/params_test.rb#Subscription with params/Client subscribes to the same channel with different params"),
    ("perform_with_arguments", A + "subscriptions/perform_test.rb#Subscription perform methods/Client perform actions with arguments"),
    ("perform_without_arguments", A + "subscriptions/perform_test.rb#Subscription perform methods/Client perform actions without arguments"),
    ("transmissions_from_subscribed", A + "subscriptions/transmissions_test.rb#Subscription transmissions/Client receives transmissions from #subscribed callback"),
]

BROADCAST = [
    ("broadcast_to_others", E + "broadcast_tests/options_test.rb#Broadcast with options/Only other clients receive the message when broadcasted to others"),
    ("whisper_to_others", E + "broadcast_tests/whisper_test.rb#Whisper/Only other clients receive the whisper message"),
]
BROKER = [
    ("history_by_offset", E + "broker_tests/history_test.rb#History/Fetching by offset"),
    ("history_by_timestamp", E + "broker_tests/history_test.rb#History/Fetching by timestamp"),
    ("history_on_subscribe", E + "broker_tests/history_test.rb#History/Subscribing with history"),
    ("restore_session_by_sid", E + "broker_tests/restore_test.rb#Session cache/Restore session by session ID"),
]
PRESENCE = [
    ("presence_join_leave_on_perform", E + "presence_tests/perform_test.rb#Presence/Join and leave channel while performing actions"),
    ("presence_implicit_join_leave_on_disconnect", E + "presence_tests/subscribe_test.rb#Presence/Join channel on subscribe with implicit identification and leave on disconnect"),
    ("presence_explicit_duplicate_sessions", E + "presence_tests/subscribe_test.rb#Presence/Join channel with explicit presence data and track duplicate sessions"),
]


BARRIER = "AnyT's `sleep 1` after unsubscribe is replaced by a barrier subscription (Cabby::BarrierChannel) on the same session (in-order command handling, compat/deviations.md); inherent window: AnyT's 0.5 s no-message check"
NOTES = {
    "ping": ["inherent window: the server's default ping_interval (3 s); two pings take about 6 s, each within AnyT's 5 s receive timeout"],
    "remote_disconnect_by_id": ["AnyT's Action Cable-only `sleep 1` is dropped: the welcome arrives after the session is registered (compat/deviations.md)", "serialized across variants and published after the server's Redis pub/sub subscribed: the pub/sub command channel is shared on the shared Redis"],
    "server_restart_disconnect_message": ["restart_server! is ported as stopping the server (SIGTERM, SIGKILL after 3 s as ChildProcess does); the disconnect is sent while stopping, the restart is not performed"],
    "many_clients_one_unsubscribes": [BARRIER],
    "multiple_streams_none_after_unsubscribe": [BARRIER],
    "single_stream_none_after_unsubscribe": [BARRIER],
    "stop_streams_unsubscribes_from_stream": ["the two `sleep 0.2` are replaced by a session barrier after `unfollow` and a witness client that receives Y before Z is broadcast"],
    "broadcast_to_others": ["inherent window: AnyT's 0.5 s no-message check"],
    "whisper_to_others": ["inherent window: AnyT's 0.5 s no-message check"],
    "history_by_offset": ["`sleep 2` after the before-block broadcasts is replaced by observing that the server handled them without subscribing to the history stream: the returned HTTP broadcast (after JetStream readiness for the NATS broker), or a sentinel broadcast to another stream for queued adapters"],
    "history_by_timestamp": ["`sleep 2` after the before-block broadcasts is replaced by observing that the server handled them without subscribing to the history stream: the returned HTTP broadcast (after JetStream readiness for the NATS broker), or a sentinel broadcast to another stream for queued adapters"],
    "history_on_subscribe": ["`sleep 2` after the before-block broadcasts is replaced by observing that the server handled them without subscribing to the history stream: the returned HTTP broadcast (after JetStream readiness for the NATS broker), or a sentinel broadcast to another stream for queued adapters"],
    "restore_session_by_sid": ["keeps the Ruby semantics: the before-block client stays open and the scenario's `client.close` closes a new let(:client)", "inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
    "presence_implicit_join_leave_on_disconnect": ["inherent window: the leave follows the disconnect after the presence TTL (2 s), within the 5 s receive timeout", "inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
    "presence_explicit_duplicate_sessions": ["inherent windows: AnyT's 0.5 s no-event check; the leave after the presence TTL (2 s)", "inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
    "channel_state_available_in_unsubscribe": ["inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
    "request_disconnect_unsubscribes_different_channels": ["inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
    "request_disconnect_unsubscribes_same_channel_subscriptions": ["inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages"],
}
COMMON = [
    "run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1",
    "on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)",
]
VARIANT_NOTES = {
    "Embedded": ["test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)"],
    "Ssl": ["TLS for WebSocket (--ssl_cert/--ssl_key, wss) and gRPC RPC (--rpc_enable_tls --rpc_tls_verify=false) with etc/ssl/server.crt; the client does not verify the certificate"],
}

# Makefile target -> (file, Variant, title, --require'd extension groups)
VARIANTS = [
    ("anyt_default", "Default", "test-conformance", "gRPC RPC, Redis broadcasts", []),
    ("anyt_ssl", "Ssl", "test-conformance-ssl", "TLS WebSocket and gRPC RPC, Redis broadcasts", []),
    ("anyt_embedded", "Embedded", "test-conformance-embedded", "settings from the environment, HTTP RPC, HTTP broadcasts at /broadcast", [BROADCAST]),
    ("anyt_http", "Http", "test-conformance-http", "HTTP RPC and HTTP broadcasts with secrets", [BROADCAST]),
    ("anyt_nats", "Nats", "test-conformance-nats", "NATS broadcasts", [BROADCAST]),
    ("anyt_nats_embedded", "NatsEmbedded", "test-conformance-nats-embedded", "NATS broadcasts via embedded NATS", [BROADCAST]),
    ("anyt_broker_http", "BrokerHttp", "test-conformance-broker-http", "memory broker, HTTP broadcasts", [BROADCAST, BROKER, PRESENCE]),
    ("anyt_broker_redis", "BrokerRedis", "test-conformance-broker-redis", "memory broker, redisx broadcasts", [BROADCAST, BROKER, PRESENCE]),
    ("anyt_broker_nats", "BrokerNats", "test-conformance-broker-nats", "NATS broker and pub/sub, HTTP broadcasts", [BROADCAST, BROKER]),
]

REQUIRES = {
    "anyt_default": "",
    "anyt_ssl": "",
    "anyt_embedded": " --require=etc/anyt/broadcast_tests/*.rb",
    "anyt_http": " --require=etc/anyt/broadcast_tests/*.rb",
    "anyt_nats": " --require=etc/anyt/broadcast_tests/*.rb",
    "anyt_nats_embedded": " --require=etc/anyt/broadcast_tests/*.rb",
    "anyt_broker_http": " --require=etc/anyt/**/*.rb",
    "anyt_broker_redis": " --require=etc/anyt/**/*.rb",
    "anyt_broker_nats": " --require=etc/anyt/broadcast_tests/*.rb,etc/anyt/broker_tests/*.rb",
}

for fname, variant, target, desc, groups in VARIANTS:
    lines = [
        f"//! AnyT scenarios as run by the Makefile target `{target}` ({desc}):",
        f"//! the anyt-core suite{REQUIRES[fname] and ' plus `' + REQUIRES[fname].strip() + '`'}.",
        "//! Scenario bodies live in `anyt_core.rs` and `anyt_ext.rs`; the server and",
        "//! application setup in `anyt_support.rs`.",
        "",
        "use crate::anyt_core as anyt;",
    ]
    if groups:
        lines.append("use crate::anyt_ext as ext;")
    lines += ["use crate::anyt_support::Variant;", "", "anyt_tests! { Variant::" + variant + ";"]
    def notes(name):
        ns = COMMON + VARIANT_NOTES.get(variant, []) + NOTES.get(name, [])
        return [f"    /// note: {n}" for n in ns]
    for name, uid in CORE:
        lines += [f"    /// upstream: {uid}"] + notes(name) + ["    #[test]", f"    fn {name}() = anyt::{name};"]
    for g in groups:
        for name, uid in g:
            lines += [f"    /// upstream: {uid}"] + notes(name) + ["    #[test]", f"    fn {name}() = ext::{name};"]
    lines.append("}")
    open(f"{OUT}/{fname}.rs", "w").write("\n".join(lines) + "\n")
print("ok")
