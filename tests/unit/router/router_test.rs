//! upstream file: router/router_test.go
//!
//! Upstream shares one router and its testify mocks across subtests, adding
//! expectations as it goes. Every subtest uses distinct call arguments, so each
//! Rust test builds the same router with only its own expectations
//! (`reference::router::Scenario`).

use cabby_core::common::{CommandResult, SessionEnv, StringMap};
use cabby_testkit::reference::common::new_session_env;
use cabby_testkit::reference::router::{Call, Return, Scenario, run};

fn env() -> SessionEnv {
    let headers: StringMap = [("cookie".to_owned(), "val=1;".to_owned())].into();
    new_session_env("ws://demo.anycable.io/cable", Some(headers))
}

mod test_router_controller_without_routes {
    use cabby_core::common::{ConnectResult, Status};

    use super::*;

    fn scenario(expectations: Vec<(Call, Return)>, call: Call) -> Scenario {
        Scenario {
            env: env(),
            routes: vec![],
            expectations: expectations.into_iter().map(|(c, r)| (String::new(), c, r)).collect(),
            call: Some(call),
        }
    }

    /// upstream: router/router_test.go#TestRouterControllerWithoutRoutes/Authenticate
    #[test]
    fn authenticate() {
        let expected = ConnectResult {
            identifier: "test_ids".into(),
            transmissions: vec!["{\"type\":\"welcome\"}".into()],
            status: Status::Success,
            ..ConnectResult::default()
        };

        let res = run(&scenario(
            vec![(Call::authenticate("2022"), Return::connect(expected.clone()))],
            Call::authenticate("2022"),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(expected), res.connect_result);
        assert!(res.called("", &Call::authenticate("2022")));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithoutRoutes/Start
    #[test]
    fn start() {
        let res = run(&scenario(vec![(Call::start(), Return::ok())], Call::start()));

        assert_eq!(None, res.error);

        assert!(res.called("", &Call::start()));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithoutRoutes/Shutdown
    #[test]
    fn shutdown() {
        let res = run(&scenario(vec![(Call::shutdown(), Return::ok())], Call::shutdown()));

        assert_eq!(None, res.error);

        assert!(res.called("", &Call::shutdown()));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithoutRoutes/Disconnect
    #[test]
    fn disconnect() {
        let call = Call::disconnect("42", "name=jack", &["chat"]);
        let res = run(&scenario(vec![(call.clone(), Return::error("foo"))], call.clone()));

        assert_eq!(Some("foo".to_owned()), res.error);

        assert!(res.called("", &call));
    }
}

mod test_router_controller_with_routes {
    use super::*;

    const CHAT: &str = "ChatChannel";
    const ECHO: &str = "EchoChannel";

    fn result(streams: &[&str]) -> CommandResult {
        CommandResult {
            transmissions: vec!["message_sent".into()],
            streams: streams.iter().map(|s| s.to_string()).collect(),
            ..CommandResult::default()
        }
    }

    fn command_result() -> CommandResult {
        result(&["missing"])
    }

    fn chat_result() -> CommandResult {
        result(&["chat_42"])
    }

    fn echo_result() -> CommandResult {
        result(&["hi"])
    }

    /// The upstream setup: a default mock plus ChatChannel and EchoChannel mocks.
    /// Expectations are `(controller, call, return)`, "" being the default controller.
    fn scenario(expectations: Vec<(&str, Call, Return)>, call: Option<Call>) -> Scenario {
        Scenario {
            env: env(),
            routes: vec![CHAT.into(), ECHO.into()],
            expectations: expectations.into_iter().map(|(n, c, r)| (n.to_owned(), c, r)).collect(),
            call,
        }
    }

    fn run_checked(s: &Scenario) -> cabby_testkit::reference::router::Outcome {
        let out = run(s);
        // require.NoError(t, subject.Route(...)) for both routes
        assert_eq!(vec![None, None], out.route_errors);
        out
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes
    #[test]
    fn routes_are_registered() {
        let out = run(&scenario(vec![], None));
        assert_eq!(vec![None, None], out.route_errors);
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Subscribe_(channel_with_params)
    #[test]
    fn subscribe_channel_with_params() {
        let channel = "{\"channel\":\"ChatChannel\",\"id\":\"42\"}";
        let call = Call::subscribe("42", "name=jack", channel);

        let res = run_checked(&scenario(
            vec![
                ("", call.clone(), Return::nil_with_error("Shouldn't be called")),
                (CHAT, call.clone(), Return::command(chat_result())),
            ],
            Some(call),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(chat_result()), res.command_result);
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Subscribe_(fallback)
    #[test]
    fn subscribe_fallback() {
        let call = Call::subscribe("42", "name=jack", "fallback");

        let res =
            run_checked(&scenario(vec![("", call.clone(), Return::command(command_result()))], Some(call.clone())));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Subscribe_(pass)
    #[test]
    fn subscribe_pass() {
        let channel = "{\"channel\":\"ChatChannel\",\"id\":\"2021\"}";
        let call = Call::subscribe("42", "name=jack", channel);

        let res = run_checked(&scenario(
            vec![("", call.clone(), Return::command(command_result())), (CHAT, call.clone(), Return::ok())],
            Some(call.clone()),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Unsubscribe_(fallback)
    #[test]
    fn unsubscribe_fallback() {
        let call = Call::unsubscribe("42", "name=jack", "fallback");

        let res =
            run_checked(&scenario(vec![("", call.clone(), Return::command(command_result()))], Some(call.clone())));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Unsubscribe_(pass)
    #[test]
    fn unsubscribe_pass() {
        let channel = "{\"channel\":\"ChatChannel\",\"id\":\"2021\"}";
        let call = Call::unsubscribe("42", "name=jack", channel);

        let res = run_checked(&scenario(
            vec![("", call.clone(), Return::command(command_result())), (CHAT, call.clone(), Return::ok())],
            Some(call.clone()),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Unsubscribe_(channel_with_params)
    #[test]
    fn unsubscribe_channel_with_params() {
        let channel = "{\"channel\":\"ChatChannel\",\"id\":\"42\"}";
        let call = Call::unsubscribe("42", "name=jack", channel);

        let res = run_checked(&scenario(
            vec![
                ("", call.clone(), Return::nil_with_error("Shouldn't be called")),
                (CHAT, call.clone(), Return::command(chat_result())),
            ],
            Some(call),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(chat_result()), res.command_result);
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Perform_(channel_w/o_params)
    #[test]
    fn perform_channel_without_params() {
        let channel = "{\"channel\":\"EchoChannel\"}";
        let call = Call::perform("42", "name=jack", channel, "ping");

        let res = run_checked(&scenario(
            vec![
                ("", call.clone(), Return::nil_with_error("Shouldn't be called")),
                (ECHO, call.clone(), Return::command(echo_result())),
            ],
            Some(call),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(echo_result()), res.command_result);
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Perform_(fallback)
    #[test]
    fn perform_fallback() {
        let call = Call::perform("42", "name=jack", "fallback", "ping");

        let res =
            run_checked(&scenario(vec![("", call.clone(), Return::command(command_result()))], Some(call.clone())));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }

    /// upstream: router/router_test.go#TestRouterControllerWithRoutes/Perform_(pass)
    #[test]
    fn perform_pass() {
        let channel = "{\"channel\":\"EchoChannel\"}";
        let call = Call::perform("42", "name=jack", channel, "pass");

        let res = run_checked(&scenario(
            vec![("", call.clone(), Return::command(command_result())), (ECHO, call.clone(), Return::ok())],
            Some(call.clone()),
        ));

        assert_eq!(None, res.error);
        assert_eq!(Some(command_result()), res.command_result);

        assert!(res.called("", &call));
    }
}
