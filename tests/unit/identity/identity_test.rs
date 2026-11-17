//! upstream file: identity/identity_test.go
//!
//! Upstream shares one pair of testify mocks across subtests; each Rust test
//! builds its own (expectations do not depend on earlier subtests). Mock
//! arguments drop the `context.Background()` that the Rust controller interface
//! does not take.

use std::sync::Arc;

use cabby_core::common::{CommandResult, ConnectResult, ControllerError, Outcome, SessionEnv, Status, StringMap};
use cabby_testkit::reference::identity::*;

/// `common.NewSessionEnv(url, headers)`: empty, non-nil connection and channel state.
fn new_session_env(url: &str, headers: Option<StringMap>) -> SessionEnv {
    SessionEnv {
        url: url.into(),
        headers,
        connection_state: Some(StringMap::new()),
        channel_states: Some(Default::default()),
        ..Default::default()
    }
}

fn cookie_env() -> SessionEnv {
    new_session_env("ws://demo.anycable.io/cable", Some([("cookie".to_owned(), "val=1;".to_owned())].into()))
}

fn ok<T>(result: T) -> Outcome<T> {
    Outcome { result: Some(result), error: None }
}

fn err<T>(message: &str) -> Outcome<T> {
    Outcome { result: None, error: Some(ControllerError::new(message)) }
}

fn nothing<T>() -> Outcome<T> {
    Outcome { result: None, error: None }
}

mod test_identifiable_controller {
    use super::*;

    struct Setup {
        controller: Arc<MockController>,
        identifier: Arc<MockIdentifier>,
        env: SessionEnv,
        command_result: CommandResult,
        subject: IdentifiableController,
    }

    fn setup() -> Setup {
        let controller = MockController::new();
        let identifier = MockIdentifier::new();
        let env = cookie_env();
        let command_result = CommandResult {
            transmissions: vec!["message_sent".into()],
            streams: vec!["chat_42".into()],
            ..Default::default()
        };
        let subject = new_identifiable_controller(&controller, &identifier);
        Setup { controller, identifier, env, command_result, subject }
    }

    fn authenticate_call(sid: &str, env: &SessionEnv) -> ControllerCall {
        ControllerCall::Authenticate { sid: sid.into(), env: Some(env.clone()) }
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Start
    #[test]
    fn start() {
        let s = setup();
        s.controller.on(ControllerCall::Start, ControllerReturn::Unit(Ok(())));

        assert_eq!(Ok(()), s.subject.start());

        s.controller.assert_called(&ControllerCall::Start);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Shutdown
    #[test]
    fn shutdown() {
        let s = setup();
        s.controller.on(ControllerCall::Shutdown, ControllerReturn::Unit(Ok(())));

        assert_eq!(Ok(()), s.subject.shutdown());

        s.controller.assert_called(&ControllerCall::Shutdown);
    }

    /// Upstream compares `res` with `expected`, the very pointer the identifier
    /// mock returned. `IdentifiableController` mutates that object (it fills a nil
    /// `CState` and sets `DisconnectInterest`), so upstream effectively compares
    /// the result with itself. Kept as: the result is the identifier's result,
    /// apart from the fields the controller writes into the shared object.
    fn assert_aliased_equal(expected: &ConnectResult, res: &ConnectResult) {
        let mut aliased = expected.clone();
        aliased.cstate = res.cstate.clone();
        aliased.disconnect_interest = res.disconnect_interest;
        assert_eq!(&aliased, res);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Authenticate_(success)
    #[test]
    fn authenticate_success() {
        let s = setup();
        let expected = ConnectResult {
            identifier: "test_ids".into(),
            transmissions: vec![r#"{"type":"welcome","sid":"2021"}"#.into()],
            disconnect_interest: -1,
            status: Status::Success,
            ..Default::default()
        };

        s.controller.on(authenticate_call("2021", &s.env), ControllerReturn::Connect(err("shouldn't be called")));
        s.identifier.on("2021", &s.env, ok(expected.clone()));

        let res = s.subject.authenticate("2021", &s.env);

        assert_eq!(None, res.error);
        assert_aliased_equal(&expected, res.result.as_ref().expect("result"));
        s.controller.assert_not_called(&authenticate_call("2021", &s.env));
    }

    /// regression-for: identity/identity_test.go#TestIdentifiableController/Authenticate_(success)
    /// The upstream equality is aliased (see `assert_aliased_equal`); this pins
    /// what the controller adds: an empty, non-nil CState and disconnect
    /// interest -1, even when the identifier left them unset.
    #[test]
    fn authenticate_success_sets_state_and_interest() {
        let s = setup();
        let identified = ConnectResult {
            identifier: "test_ids".into(),
            transmissions: vec![r#"{"type":"welcome","sid":"2021"}"#.into()],
            status: Status::Success,
            ..Default::default()
        };
        s.identifier.on("2021", &s.env, ok(identified.clone()));

        let res = s.subject.authenticate("2021", &s.env);

        assert_eq!(None, res.error);
        let expected = ConnectResult { cstate: Some(StringMap::new()), disconnect_interest: -1, ..identified };
        assert_eq!(Some(expected), res.result);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Authenticate_(failure)
    #[test]
    fn authenticate_failure() {
        let s = setup();
        let expected = ConnectResult { status: Status::Failure, ..Default::default() };

        s.controller.on(authenticate_call("2020", &s.env), ControllerReturn::Connect(err("shouldn't be called")));
        s.identifier.on("2020", &s.env, ok(expected.clone()));

        let res = s.subject.authenticate("2020", &s.env);

        assert_eq!(None, res.error);
        assert_aliased_equal(&expected, res.result.as_ref().expect("result"));
        s.controller.assert_not_called(&authenticate_call("2020", &s.env));
    }

    /// regression-for: identity/identity_test.go#TestIdentifiableController/Authenticate_(failure)
    /// Failures are returned with the same CState/interest adjustments.
    #[test]
    fn authenticate_failure_sets_state_and_interest() {
        let s = setup();
        s.identifier.on("2020", &s.env, ok(ConnectResult { status: Status::Failure, ..Default::default() }));

        let res = s.subject.authenticate("2020", &s.env);

        assert_eq!(None, res.error);
        let expected = ConnectResult {
            status: Status::Failure,
            cstate: Some(StringMap::new()),
            disconnect_interest: -1,
            ..Default::default()
        };
        assert_eq!(Some(expected), res.result);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Authenticate_(error)
    #[test]
    fn authenticate_error() {
        let s = setup();
        let expected_err = ControllerError::new("identifier failed");

        s.controller.on(authenticate_call("1998", &s.env), ControllerReturn::Connect(err("shouldn't be called")));
        s.identifier.on("1998", &s.env, err("identifier failed"));

        let res = s.subject.authenticate("1998", &s.env);

        assert_eq!(None, res.result);
        assert_eq!(Some(expected_err), res.error);
        s.controller.assert_not_called(&authenticate_call("1998", &s.env));
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Authenticate_(noop_->_passthrough)
    #[test]
    fn authenticate_noop_passthrough() {
        let s = setup();
        let expected = ConnectResult {
            identifier: "test_ids".into(),
            transmissions: vec![r#"{"type":"welcome","sid":"2022"}"#.into()],
            status: Status::Success,
            ..Default::default()
        };

        s.controller.on(authenticate_call("2022", &s.env), ControllerReturn::Connect(ok(expected.clone())));
        s.identifier.on("2022", &s.env, nothing());

        let res = s.subject.authenticate("2022", &s.env);

        assert_eq!(None, res.error);
        assert_eq!(Some(expected), res.result);
        s.controller.assert_called(&authenticate_call("2022", &s.env));
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Subscribe
    #[test]
    fn subscribe() {
        let s = setup();
        let call = ControllerCall::Subscribe {
            sid: "42".into(),
            env: Some(s.env.clone()),
            ids: "name=jack".into(),
            channel: "chat".into(),
        };
        s.controller.on(call.clone(), ControllerReturn::Command(ok(s.command_result.clone())));

        let res = s.subject.subscribe("42", &s.env, "name=jack", "chat");

        assert_eq!(None, res.error);
        assert_eq!(Some(s.command_result.clone()), res.result);

        s.controller.assert_called(&call);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Unsubscribe
    #[test]
    fn unsubscribe() {
        let s = setup();
        let call = ControllerCall::Unsubscribe {
            sid: "42".into(),
            env: Some(s.env.clone()),
            ids: "name=jack".into(),
            channel: "chat".into(),
        };
        s.controller.on(call.clone(), ControllerReturn::Command(ok(s.command_result.clone())));

        let res = s.subject.unsubscribe("42", &s.env, "name=jack", "chat");

        assert_eq!(None, res.error);
        assert_eq!(Some(s.command_result.clone()), res.result);

        s.controller.assert_called(&call);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Perform
    #[test]
    fn perform() {
        let s = setup();
        let call = ControllerCall::Perform {
            sid: "42".into(),
            env: Some(s.env.clone()),
            ids: "name=jack".into(),
            channel: "chat".into(),
            data: "ping".into(),
        };
        s.controller.on(call.clone(), ControllerReturn::Command(ok(s.command_result.clone())));

        let res = s.subject.perform("42", &s.env, "name=jack", "chat", "ping");

        assert_eq!(None, res.error);
        assert_eq!(Some(s.command_result.clone()), res.result);

        s.controller.assert_called(&call);
    }

    /// upstream: identity/identity_test.go#TestIdentifiableController/Disconnect
    #[test]
    fn disconnect() {
        let s = setup();
        let expected_err = ControllerError::new("foo");
        let call = ControllerCall::Disconnect {
            sid: "42".into(),
            env: Some(s.env.clone()),
            ids: "name=jack".into(),
            subscriptions: vec!["chat".into()],
        };
        s.controller.on(call.clone(), ControllerReturn::Unit(Err(expected_err.clone())));

        let res = s.subject.disconnect("42", &s.env, "name=jack", &["chat".to_owned()]);

        assert_eq!(Err(expected_err), res);

        s.controller.assert_called(&call);
    }
}

mod test_identifier_pipeline {
    use super::*;

    /// upstream: identity/identity_test.go#TestIdentifierPipeline/first_identifier_success
    #[test]
    fn first_identifier_success() {
        let mocked = MockIdentifier::new();
        let env = cookie_env();
        mocked.on("mock-ok", &env, ok(ConnectResult { identifier: "mock_welcome".into(), ..Default::default() }));

        let res = pipeline_with_public_identify(&mocked, "mock-ok", &env);

        assert_eq!(None, res.error);
        assert_eq!("mock_welcome", res.result.expect("result").identifier);
    }

    /// upstream: identity/identity_test.go#TestIdentifierPipeline/second_identifier_success
    #[test]
    fn second_identifier_success() {
        let mocked = MockIdentifier::new();
        let env = cookie_env();
        mocked.on("mock-nok", &env, nothing());

        let res = pipeline_with_public_identify(&mocked, "mock-nok", &env);

        assert_eq!(None, res.error);
        assert_eq!(r#"{"sid":"mock-nok"}"#, res.result.expect("result").identifier);
    }

    /// upstream: identity/identity_test.go#TestIdentifierPipeline/first_identifier_fail
    #[test]
    fn first_identifier_fail() {
        let mocked = MockIdentifier::new();
        let env = cookie_env();
        mocked.on("mock-err", &env, err("failed"));

        let res = pipeline_with_public_identify(&mocked, "mock-err", &env);

        assert!(res.error.is_some());
    }
}
