//! upstream file: pusher/http_test.go
//!
//! `mocks.Handler` is set up with `On("HandleBroadcast", broadcastPayload)`: a call
//! with any other payload fails the mock, which the translation checks by comparing
//! every recorded broadcast. `TestGetUsersIntegration` uses the `pusher-http-go`
//! client against an `httptest` server; here the same client logic
//! (`cabby_testkit::pusher_auth::get_channel_users`) sends its signed requests to the
//! handler directly.

use cabby_server::pusher::Config;
use cabby_testkit::pusher_auth::{get_channel_users, md5_hex, now_timestamp};
use cabby_testkit::reference::pusher::{ApiBroker, HandlerMock, MemoryBroker, RestApi, Verifier, new_config};
use serde_json::json;

const PAYLOAD: &[u8] = br#"{"name":"test-event","channel":"test-channel","data":"{\"message\":\"hello world\"}"}"#;
const BROADCAST_PAYLOAD: &[u8] =
    br#"{"stream":"test-channel","data":"{\"event\":\"test-event\",\"data\":\"{\\\"message\\\":\\\"hello world\\\"}\"}"}"#;

fn http_config() -> Config {
    let mut config = new_config();
    config.app_id = "my-app-id".into();
    config.app_key = "my-app-key".into();
    config.secret = "test-push".into();
    config
}

fn verifier(config: &Config) -> Verifier {
    Verifier::new(&config.app_key, &config.secret)
}

/// Every `HandleBroadcast` call must match the mock's only expectation.
fn assert_broadcasts_match_expectation(api: &RestApi) {
    assert_eq!(Vec::<String>::new(), api.unexpected_calls());
    for b in api.broadcasts() {
        assert_eq!(String::from_utf8_lossy(BROADCAST_PAYLOAD), String::from_utf8_lossy(&b));
    }
}

/// Nothing reached `HandleBroadcast` (Cabby-only checks: upstream's mock
/// accepts the expected payload any number of times, including zero).
fn assert_nothing_broadcast(api: &RestApi) {
    assert_eq!(Vec::<Vec<u8>>::new(), api.broadcasts(), "broadcasts");
    assert_eq!(Vec::<String>::new(), api.unexpected_calls());
}

/// A signed `POST /events` target for `body` with this `auth_timestamp`.
fn signed_events_target(config: &Config, auth_timestamp: &str, body: &[u8]) -> String {
    let auth_params = format!(
        "auth_key={}&auth_timestamp={auth_timestamp}&auth_version=1.0&body_md5={}",
        config.app_key,
        md5_hex(body)
    );
    let auth_signature = verifier(config).sign(&format!("POST\n/events\n{auth_params}"));
    format!("/events?{auth_params}&auth_signature={auth_signature}")
}

mod test_http_handler {
    use super::*;

    /// upstream: pusher/http_test.go#TestHttpHandler/Handles_broadcasts
    #[test]
    fn handles_broadcasts() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);

        let auth_params = format!(
            "auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5={}",
            config.app_key,
            now_timestamp(),
            md5_hex(PAYLOAD)
        );

        let string_to_sign = format!("POST\n/events\n{auth_params}");
        let auth_signature = verifier(&config).sign(&string_to_sign);

        let rr = rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature={auth_signature}"), PAYLOAD);

        assert_eq!(200, rr.status);
        assert_broadcasts_match_expectation(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Handles_broadcasts
    /// Upstream never asserts that the broadcast happened (no `AssertExpectations`);
    /// this checks exactly one broadcast with the expected payload.
    #[test]
    fn handles_broadcasts_exactly_once() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);
        let auth_params = format!(
            "auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5={}",
            config.app_key,
            now_timestamp(),
            md5_hex(PAYLOAD)
        );
        let auth_signature = verifier(&config).sign(&format!("POST\n/events\n{auth_params}"));

        let rr = rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature={auth_signature}"), PAYLOAD);

        assert_eq!(200, rr.status);
        assert_eq!(b"{}".to_vec(), rr.body);
        assert_eq!(vec![BROADCAST_PAYLOAD.to_vec()], rest_api.broadcasts());
    }

    /// upstream: pusher/http_test.go#TestHttpHandler/Rejects_unsupported_methods
    #[test]
    fn rejects_unsupported_methods() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);

        let rr = rest_api.serve("DELETE", "/", PAYLOAD);

        assert_eq!(422, rr.status);
        assert_broadcasts_match_expectation(&rest_api);
    }

    /// upstream: pusher/http_test.go#TestHttpHandler/Rejects_when_authorization_data_is_incorrect
    #[test]
    fn rejects_when_authorization_data_is_incorrect() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);

        let auth_params = format!(
            "auth_key={}&body_md5={}&auth_version=1.0&auth_timestamp={}",
            config.app_key,
            md5_hex(PAYLOAD),
            now_timestamp()
        );

        let rr =
            rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature=qwewe123wqee1eqweqweqwe"), PAYLOAD);

        assert_eq!(401, rr.status);
        assert_broadcasts_match_expectation(&rest_api);
    }

    /// upstream: pusher/http_test.go#TestHttpHandler/Rejects_when_auth_timestamp_is_too_far_from_current_time
    #[test]
    fn rejects_when_auth_timestamp_is_too_far_from_current_time() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);

        let stale_timestamp = (now_timestamp().parse::<i64>().unwrap() - 15 * 60).to_string();
        let auth_params = format!(
            "auth_key={}&auth_timestamp={stale_timestamp}&auth_version=1.0&body_md5={}",
            config.app_key,
            md5_hex(PAYLOAD)
        );

        let string_to_sign = format!("POST\n/events\n{auth_params}");
        let auth_signature = verifier(&config).sign(&string_to_sign);

        let rr = rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature={auth_signature}"), PAYLOAD);

        assert_eq!(401, rr.status);
        assert_broadcasts_match_expectation(&rest_api);
    }

    /// upstream: pusher/http_test.go#TestHttpHandler/Rejects_when_body_does_not_match_signature
    #[test]
    fn rejects_when_body_does_not_match_signature() {
        let config = http_config();
        let rest_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &config);

        let auth_params = format!(
            "auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5={}",
            config.app_key,
            now_timestamp(),
            md5_hex(PAYLOAD)
        );

        let string_to_sign = format!("POST\n/events\n{auth_params}");
        let auth_signature = verifier(&config).sign(&string_to_sign);

        let another_payload = br#"{"name":"test-event","channel":"test-channel","data":"{\"message\":\"hola!\"}"}"#;

        let rr =
            rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature={auth_signature}"), another_payload);

        assert_eq!(401, rr.status);
        assert_broadcasts_match_expectation(&rest_api);
    }

    /// upstream: pusher/http_test.go#TestHttpHandler/Rejects_large_body_with_invalid_signature_without_reading_it
    #[test]
    fn rejects_large_body_with_invalid_signature_without_reading_it() {
        let config = http_config();
        let mut small_config = config.clone();
        small_config.max_body_size = 16;

        let small_api = RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, &small_config);

        let auth_params = format!(
            "auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5=deadbeef",
            config.app_key,
            now_timestamp()
        );

        let body = "x".repeat(1024);

        let rr = small_api.serve("POST", &format!("/events?{auth_params}&auth_signature=deadbeef"), body.as_bytes());

        assert_eq!(401, rr.status);
        assert_eq!(0, rr.body_read);
        assert_broadcasts_match_expectation(&small_api);
    }
}

/// Cabby-only checks next to `TestHttpHandler`: every rejected request
/// broadcasts nothing, and the timestamp and body limit checks the upstream
/// subtests do not reach.
mod rejected_requests {
    use super::*;

    fn api(config: &Config) -> RestApi {
        RestApi::new(ApiBroker::Mock, HandlerMock::AcceptBroadcasts, config)
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_unsupported_methods
    #[test]
    fn an_unsupported_method_broadcasts_nothing() {
        let config = http_config();
        let rest_api = api(&config);
        let signed = signed_events_target(&config, &now_timestamp(), PAYLOAD);

        assert_eq!(422, rest_api.serve("DELETE", "/", PAYLOAD).status);
        for method in ["PUT", "PATCH", "DELETE"] {
            assert_eq!(422, rest_api.serve(method, &signed, PAYLOAD).status, "{method}");
        }
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_when_authorization_data_is_incorrect
    #[test]
    fn incorrect_authorization_data_broadcasts_nothing() {
        let config = http_config();
        let rest_api = api(&config);
        let auth_params = format!(
            "auth_key={}&body_md5={}&auth_version=1.0&auth_timestamp={}",
            config.app_key,
            md5_hex(PAYLOAD),
            now_timestamp()
        );

        let rr =
            rest_api.serve("POST", &format!("/events?{auth_params}&auth_signature=qwewe123wqee1eqweqweqwe"), PAYLOAD);

        assert_eq!(401, rr.status);
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_when_auth_timestamp_is_too_far_from_current_time
    #[test]
    fn a_stale_auth_timestamp_broadcasts_nothing() {
        let config = http_config();
        let rest_api = api(&config);
        let stale = (now_timestamp().parse::<i64>().unwrap() - 15 * 60).to_string();

        assert_eq!(401, rest_api.serve("POST", &signed_events_target(&config, &stale, PAYLOAD), PAYLOAD).status);
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_when_auth_timestamp_is_too_far_from_current_time
    #[test]
    fn an_auth_timestamp_in_the_future_is_rejected() {
        let config = http_config();
        let rest_api = api(&config);
        let future = (now_timestamp().parse::<i64>().unwrap() + 15 * 60).to_string();

        assert_eq!(401, rest_api.serve("POST", &signed_events_target(&config, &future, PAYLOAD), PAYLOAD).status);
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_when_auth_timestamp_is_too_far_from_current_time
    #[test]
    fn a_malformed_auth_timestamp_is_rejected() {
        let config = http_config();
        let rest_api = api(&config);
        let now = now_timestamp();

        for timestamp in
            ["", "abc", format!("{now}abc").as_str(), format!("{now}.5").as_str(), "0x10", "99999999999999999999"]
        {
            let rr = rest_api.serve("POST", &signed_events_target(&config, timestamp, PAYLOAD), PAYLOAD);
            assert_eq!(401, rr.status, "auth_timestamp={timestamp:?}");
        }
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_when_body_does_not_match_signature
    #[test]
    fn a_body_that_does_not_match_the_signature_broadcasts_nothing() {
        let config = http_config();
        let rest_api = api(&config);
        let another_payload = br#"{"name":"test-event","channel":"test-channel","data":"{\"message\":\"hola!\"}"}"#;

        let rr = rest_api.serve("POST", &signed_events_target(&config, &now_timestamp(), PAYLOAD), another_payload);

        assert_eq!(401, rr.status);
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_large_body_with_invalid_signature_without_reading_it
    #[test]
    fn a_large_body_with_an_invalid_signature_broadcasts_nothing() {
        let mut config = http_config();
        config.max_body_size = 16;
        let rest_api = api(&config);
        let target = format!(
            "/events?auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5=deadbeef&auth_signature=deadbeef",
            config.app_key,
            now_timestamp()
        );

        let rr = rest_api.serve("POST", &target, "x".repeat(1024).as_bytes());

        assert_eq!(401, rr.status);
        assert_nothing_broadcast(&rest_api);
    }

    /// regression-for: pusher/http_test.go#TestHttpHandler/Rejects_large_body_with_invalid_signature_without_reading_it
    #[test]
    fn a_validly_signed_body_over_the_limit_is_rejected() {
        let mut config = http_config();
        config.max_body_size = 16;
        let rest_api = api(&config);
        assert!(PAYLOAD.len() > 16);

        let rr = rest_api.serve("POST", &signed_events_target(&config, &now_timestamp(), PAYLOAD), PAYLOAD);

        assert_eq!(413, rr.status);
        assert_nothing_broadcast(&rest_api);
    }
}

fn users_config() -> Config {
    let mut config = new_config();
    config.app_id = "app-id".into();
    config.app_key = "app-key".into();
    config.secret = "secret".into();
    config
}

/// `TestGetUsersIntegration` setup: a started memory broker and the REST API.
struct UsersFixture {
    config: Config,
    mem_broker: MemoryBroker,
    rest_api: RestApi,
}

impl UsersFixture {
    fn new() -> Self {
        let config = users_config();
        let mem_broker = MemoryBroker::new();
        mem_broker.start().expect("memory broker start");
        // `&mocks.Handler{}` without expectations: any broadcast is forbidden.
        let rest_api = RestApi::new(ApiBroker::Memory(mem_broker.clone()), HandlerMock::NoExpectations, &config);
        Self { config, mem_broker, rest_api }
    }

    /// `client.GetChannelUsers(channel)`.
    fn get_channel_users(&self, channel: &str) -> Result<Vec<String>, String> {
        let c = &self.config;
        get_channel_users(&c.app_id, &c.app_key, &c.secret, channel, |method, target| {
            let r = self.rest_api.serve(method, target, b"");
            (r.status, r.body)
        })
    }
}

impl Drop for UsersFixture {
    fn drop(&mut self) {
        // defer memBroker.Shutdown(...)
        if !std::thread::panicking() {
            assert!(self.rest_api.broadcasts().is_empty(), "unexpected broadcasts");
            assert_eq!(Vec::<String>::new(), self.rest_api.unexpected_calls());
            let _ = self.mem_broker.shutdown();
        }
    }
}

mod test_get_users_integration {
    use super::*;

    /// upstream: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_presence_channel_with_users
    /// upstream: pusher/http_test.go#TestGetUsersIntegration
    #[test]
    fn get_users_from_presence_channel_with_users() {
        let f = UsersFixture::new();
        let channel = "presence-room-1";

        f.mem_broker.presence_add(channel, "session-1", "user-1", json!({"name": "Alice"})).expect("no error");
        f.mem_broker.presence_add(channel, "session-2", "user-2", json!({"name": "Bob"})).expect("no error");

        let users = f.get_channel_users(channel).expect("no error");

        assert_eq!(2, users.len());

        let ids = [users[0].clone(), users[1].clone()];
        assert!(ids.contains(&"user-1".to_owned()));
        assert!(ids.contains(&"user-2".to_owned()));
    }

    /// upstream: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_empty_channel
    /// upstream: pusher/http_test.go#TestGetUsersIntegration
    #[test]
    fn get_users_from_empty_channel() {
        let f = UsersFixture::new();
        let channel = "presence-empty-room";

        let users = f.get_channel_users(channel).expect("no error");

        assert_eq!(0, users.len());
    }

    /// upstream: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_unknown_channel
    /// upstream: pusher/http_test.go#TestGetUsersIntegration
    #[test]
    fn get_users_from_unknown_channel() {
        let f = UsersFixture::new();
        let channel = "presence-unknown";

        let users = f.get_channel_users(channel).expect("no error");

        assert_eq!(0, users.len());
    }

    /// upstream: pusher/http_test.go#TestGetUsersIntegration/Get_users_from_non-presence_channel_returns_400
    /// upstream: pusher/http_test.go#TestGetUsersIntegration
    #[test]
    fn get_users_from_non_presence_channel_returns_400() {
        let f = UsersFixture::new();
        let channel = "private-room";

        let err = f.get_channel_users(channel).expect_err("an error");
        assert!(err.contains("400"), "{err}");
    }
}
