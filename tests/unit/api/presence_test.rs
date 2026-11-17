//! upstream file: api/presence_test.go
//!
//! `defer api.Shutdown(..) // nolint:errcheck` is [`defer_shutdown`] (runs on
//! drop, result ignored). `mocks.NewBroker(t)` / `mocks.NewHandler(t)` assert
//! their expectations on cleanup, after the deferred shutdown: that is the
//! `drop(api)` followed by the `assert_expectations` calls, which also reject
//! any unexpected (forbidden) mock call.

use cabby_core::common::{PresenceEvent, PresenceInfo};
use cabby_testkit::reference::api::*;
use serde_json::{Value, json};

/// `brk.On("PresenceInfo", stream, mock.Anything)`: one option, any value.
fn presence_info_call(stream: &str) -> BrokerCall {
    BrokerCall::PresenceInfo { stream: stream.into(), options: 1 }
}

mod test_presence_handler {
    use super::*;

    /// upstream: api/presence_test.go#TestPresenceHandler/Rejects_non-GET_requests
    #[test]
    fn rejects_non_get_requests() {
        let handler = MockHandler::new();
        let brk = MockBroker::new();

        let mut config = new_config();

        let api = defer_shutdown(new_server(&mut config, &brk, &handler).expect("new server"));
        api.start().expect("start");

        let rr = api.serve(&request("POST", "/api/presence/a/users", &[], b""));

        assert_eq!(422, rr.status);

        drop(api);
        brk.assert_expectations();
        handler.assert_expectations();
    }

    /// upstream: api/presence_test.go#TestPresenceHandler/Returns_presence_info
    #[test]
    fn returns_presence_info() {
        let handler = MockHandler::new();
        let brk = MockBroker::new();
        brk.on(BrokerCall::SupportsPresence, BrokerReturn::Bool(true));

        brk.on(
            presence_info_call("a"),
            BrokerReturn::PresenceInfo(Ok(PresenceInfo {
                total: 2,
                records: vec![
                    PresenceEvent { id: "33".into(), info: Some(json!({"name": "jack"})), ..Default::default() },
                    PresenceEvent { id: "44".into(), info: Some(json!({"name": "jill"})), ..Default::default() },
                ],
                ..Default::default()
            })),
        );

        let mut config = new_config();
        config.secret = "test-secret".into();

        let api = defer_shutdown(new_server(&mut config, &brk, &handler).expect("new server"));
        api.start().expect("start");

        let rr = api.serve(&request("GET", "/api/presence/a/users", &[("Authorization", "Bearer test-secret")], b""));

        assert_eq!(200, rr.status);

        let parsed: Value = serde_json::from_slice(&rr.body).expect("JSON body");

        assert_eq!(Some(2), parsed["total"].as_i64());

        let records = parsed["records"].as_array().expect("records");
        assert_eq!(2, records.len());
        assert_eq!("33", records[0]["id"]);
        assert_eq!("jack", records[0]["info"]["name"]);
        assert_eq!("44", records[1]["id"]);
        assert_eq!("jill", records[1]["info"]["name"]);

        drop(api);
        brk.assert_expectations();
        handler.assert_expectations();
    }

    /// upstream: api/presence_test.go#TestPresenceHandler/Returns_empty_presence_info_when_stream_has_no_presence_information
    #[test]
    fn returns_empty_presence_info_when_stream_has_no_presence_information() {
        let handler = MockHandler::new();
        let brk = MockBroker::new();

        brk.on(BrokerCall::SupportsPresence, BrokerReturn::Bool(true));

        brk.on(presence_info_call("b"), BrokerReturn::PresenceInfo(Ok(PresenceInfo::default())));

        let mut config = new_config();
        let api = defer_shutdown(new_server(&mut config, &brk, &handler).expect("new server"));
        api.start().expect("start");

        let rr = api.serve(&request("GET", "/api/presence/b/users", &[], b""));

        assert_eq!(200, rr.status);

        let parsed: serde_json::Map<String, Value> = serde_json::from_slice(&rr.body).expect("JSON object");

        assert_eq!(Some(0.0), parsed["total"].as_f64());

        assert!(!parsed.contains_key("records"));

        drop(api);
        brk.assert_expectations();
        handler.assert_expectations();
    }

    /// upstream: api/presence_test.go#TestPresenceHandler/Returns_not_implemented_when_presence_is_not_supported
    #[test]
    fn returns_not_implemented_when_presence_is_not_supported() {
        let handler = MockHandler::new();
        let brk = MockBroker::new();

        brk.on(BrokerCall::SupportsPresence, BrokerReturn::Bool(false));

        let mut config = new_config();
        let api = defer_shutdown(new_server(&mut config, &brk, &handler).expect("new server"));
        api.start().expect("start");

        let rr = api.serve(&request("GET", "/api/presence/b/users", &[], b""));

        assert_eq!(501, rr.status);

        drop(api);
        brk.assert_expectations();
        handler.assert_expectations();
    }
}
