//! AnyT scenarios as run by the Makefile target `test-conformance-embedded` (settings from the environment, HTTP RPC, HTTP broadcasts at /broadcast):
//! the anyt-core suite plus `--require=etc/anyt/broadcast_tests/*.rb`.
//! Scenario bodies live in `anyt_core.rs` and `anyt_ext.rs`; the server and
//! application setup in `anyt_support.rs`.

use crate::anyt_core as anyt;
use crate::anyt_ext as ext;
use crate::anyt_support::Variant;

anyt_tests! { Variant::Embedded;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/core/ping_test.rb#Ping/Client receives pings with timestamps
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: the server's default ping_interval (3 s); two pings take about 6 s, each within AnyT's 5 s receive timeout
    #[test]
    fn ping() = anyt::ping;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/core/welcome_test.rb#Welcome/Client receives "welcome" message
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn welcome() = anyt::welcome;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/features/channel_state_test.rb#Channel state/Channel state is available in #unsubscribe callbacks
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages
    #[test]
    fn channel_state_available_in_unsubscribe() = anyt::channel_state_available_in_unsubscribe;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/features/channel_state_test.rb#Channel state/Channel state is kept between commands
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn channel_state_kept_between_commands() = anyt::channel_state_kept_between_commands;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/features/remote_disconnect_test.rb#Remote disconnect/Close single connection by id
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: AnyT's Action Cable-only `sleep 1` is dropped: the welcome arrives after the session is registered (compat/deviations.md)
    /// note: serialized across variants and published after the server's Redis pub/sub subscribed: the pub/sub command channel is shared on the shared Redis
    #[test]
    fn remote_disconnect_by_id() = anyt::remote_disconnect_by_id;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/features/server_restart_test.rb#Server restart/Client receives disconnect message
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: restart_server! is ported as stopping the server (SIGTERM, SIGKILL after 3 s as ChildProcess does); the disconnect is sent while stopping, the restart is not performed
    #[test]
    fn server_restart_disconnect_message() = anyt::server_restart_disconnect_message;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/channel_test.rb#Request/Channel has access to request
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_channel_has_access_to_request() = anyt::request_channel_has_access_to_request;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/connection_test.rb#Request/Accepts when required cookies are set
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_accept_with_cookies() = anyt::request_accept_with_cookies;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/connection_test.rb#Request/Accepts when required header is set
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_accept_with_header() = anyt::request_accept_with_header;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/connection_test.rb#Request/Reject when required cookies are not set
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_reject_without_cookies() = anyt::request_reject_without_cookies;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/connection_test.rb#Request/Reject when required header is missing
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_reject_without_header() = anyt::request_reject_without_header;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/connection_test.rb#Request/Url is set during connection
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_url_is_set() = anyt::request_url_is_set;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/disconnect_reasons_test.rb#Request/Receives disconnect message when rejected
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn request_disconnect_message_when_rejected() = anyt::request_disconnect_message_when_rejected;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/disconnection_test.rb#Request/Client disconnect invokes #unsubscribe callbacks for different channels
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages
    #[test]
    fn request_disconnect_unsubscribes_different_channels() = anyt::request_disconnect_unsubscribes_different_channels;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/request/disconnection_test.rb#Request/Client disconnect invokes #unsubscribe callbacks for multiple subscriptions from the same channel
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: AnyT's `client.close` waits 0.5 s and fails on unprocessed messages
    #[test]
    fn request_disconnect_unsubscribes_same_channel_subscriptions() = anyt::request_disconnect_unsubscribes_same_channel_subscriptions;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/broadcast_test.rb#Broadcast data to stream/Broadcast JSON string
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn broadcast_json_string() = anyt::broadcast_json_string;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/broadcast_test.rb#Broadcast data to stream/Broadcast custom string
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn broadcast_custom_string() = anyt::broadcast_custom_string;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/broadcast_test.rb#Broadcast data to stream/Broadcast object
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn broadcast_object() = anyt::broadcast_object;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/multiple_clients_test.rb#Streams with many clients/Client receive messages when another client removes subscription
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: AnyT's `sleep 1` after unsubscribe is replaced by a barrier subscription (Cabby::BarrierChannel) on the same session (in-order command handling, compat/deviations.md); inherent window: AnyT's 0.5 s no-message check
    #[test]
    fn many_clients_one_unsubscribes() = anyt::many_clients_one_unsubscribes;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/multiple_clients_test.rb#Streams with many clients/Multiple clients receive messages from stream
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn many_clients_receive() = anyt::many_clients_receive;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/multiple_test.rb#Multiple streams/Client does not receive messages from any stream after removing subscription
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: AnyT's `sleep 1` after unsubscribe is replaced by a barrier subscription (Cabby::BarrierChannel) on the same session (in-order command handling, compat/deviations.md); inherent window: AnyT's 0.5 s no-message check
    #[test]
    fn multiple_streams_none_after_unsubscribe() = anyt::multiple_streams_none_after_unsubscribe;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/multiple_test.rb#Multiple streams/Client receives messages from both streams
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn multiple_streams_receive_both() = anyt::multiple_streams_receive_both;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/single_test.rb#Single stream/Client does not receive messages from the stream after removing subscription
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: AnyT's `sleep 1` after unsubscribe is replaced by a barrier subscription (Cabby::BarrierChannel) on the same session (in-order command handling, compat/deviations.md); inherent window: AnyT's 0.5 s no-message check
    #[test]
    fn single_stream_none_after_unsubscribe() = anyt::single_stream_none_after_unsubscribe;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/single_test.rb#Single stream/Client receives messages from the stream
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn single_stream_receives() = anyt::single_stream_receives;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/single_test.rb#Single stream/Client receives multiple messages when subscribed to the same channel with different params
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn single_stream_same_channel_different_params() = anyt::single_stream_same_channel_different_params;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/streams/stop_test.rb#Stop streams/Client unsubscribes from the stream
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: the two `sleep 0.2` are replaced by a session barrier after `unfollow` and a witness client that receives Y before Z is broadcast
    #[test]
    fn stop_streams_unsubscribes_from_stream() = anyt::stop_streams_unsubscribes_from_stream;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/ack_test.rb#Subscription aknowledgement/Client receives subscription confirmation
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn ack_confirmation() = anyt::ack_confirmation;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/ack_test.rb#Subscription aknowledgement/Client receives subscription rejection
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn ack_rejection() = anyt::ack_rejection;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/params_test.rb#Subscription with params/Client subscribes to the same channel with different params
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn params_same_channel_different_params() = anyt::params_same_channel_different_params;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/perform_test.rb#Subscription perform methods/Client perform actions with arguments
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn perform_with_arguments() = anyt::perform_with_arguments;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/perform_test.rb#Subscription perform methods/Client perform actions without arguments
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn perform_without_arguments() = anyt::perform_without_arguments;
    /// upstream: anyt-core-1.4.0:lib/anyt/tests/subscriptions/transmissions_test.rb#Subscription transmissions/Client receives transmissions from #subscribed callback
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    #[test]
    fn transmissions_from_subscribed() = anyt::transmissions_from_subscribed;
    /// upstream: etc/anyt/broadcast_tests/options_test.rb#Broadcast with options/Only other clients receive the message when broadcasted to others
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: AnyT's 0.5 s no-message check
    #[test]
    fn broadcast_to_others() = ext::broadcast_to_others;
    /// upstream: etc/anyt/broadcast_tests/whisper_test.rb#Whisper/Only other clients receive the whisper message
    /// note: run once per Makefile test-conformance* target (anyt_<target>.rs), with upstream CI's job-wide REDIS_URL and ANYCABLE_DEBUG=1
    /// note: on the shared Redis/NATS services the broadcast channel/stream/subject and the Redis ACL user are per test and the application prefixes stream names (opaque to clients)
    /// note: test-conformance-embedded runs cmd/embedded-cable; instantiated with the server binary configured only through the environment, HTTP RPC, and the broadcast handler at /broadcast on the main port (ANYCABLE_HTTP_BROADCAST_PATH)
    /// note: inherent window: AnyT's 0.5 s no-message check
    #[test]
    fn whisper_to_others() = ext::whisper_to_others;
}
