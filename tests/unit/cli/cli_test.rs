//! upstream file: cli/cli_test.go

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_server::cli::{
    BroadcastHandler, Broadcaster, Broker, CliError, LogSink, Runner, RunnerOption, Shutdownable, Subscriber,
};
use cabby_server::config::EnvMap;
use cabby_testkit::reference::cli::{
    CustomFlag, DrainOutcome, new_config_from_cli, runner_drains_broadcasts_before_broker_shutdown,
};
use cabby_testkit::reference::config::new_config;

// --- TestRunnerDrainsBroadcastsBeforeBrokerShutdown --------------------------------
//
// Upstream uses mockery mocks with `Run` callbacks asserting the shutdown order,
// and the mocks assert at cleanup that every expected method was called. The Rust
// doubles below share one state: failed in-callback assertions and unexpected calls
// are recorded as violations (callbacks may run on runner threads, where a panic
// could be swallowed) and checked at the end together with the expected calls.

#[derive(Default)]
struct State {
    broker_closed: bool,
    subscriber_closed: bool,
    service_closed: bool,
    delivered: Vec<String>,
    violations: Vec<String>,
    calls: BTreeSet<String>,
}

type Shared = Arc<Mutex<State>>;

fn record(state: &Shared, call: impl Into<String>) {
    state.lock().unwrap().calls.insert(call.into());
}

fn check(state: &Shared, ok: impl FnOnce(&State) -> bool, message: &str) {
    let mut s = state.lock().unwrap();
    if !ok(&s) {
        s.violations.push(message.to_owned());
    }
}

/// mockery fails on calls without an expectation.
fn unexpected(state: &Shared, call: String) {
    state.lock().unwrap().violations.push(format!("mock: unexpected call {call}"));
}

struct Service(Shared);

impl Shutdownable for Service {
    fn shutdown(&self) -> Result<(), CliError> {
        record(&self.0, "service.Shutdown");
        self.0.lock().unwrap().service_closed = true;
        Ok(())
    }
}

struct MockSubscriber(Shared);

impl Shutdownable for MockSubscriber {
    fn shutdown(&self) -> Result<(), CliError> {
        record(&self.0, "subscriber.Shutdown");
        check(&self.0, |s| !s.service_closed, "shared services must outlive pub/sub");
        self.0.lock().unwrap().subscriber_closed = true;
        Ok(())
    }
}

impl Subscriber for MockSubscriber {
    fn start(&self) -> Result<(), CliError> {
        record(&self.0, "subscriber.Start");
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        record(&self.0, "subscriber.Broadcast");
        check(&self.0, |s| !s.subscriber_closed, "pub/sub must remain available while broadcasts drain");
        self.0.lock().unwrap().delivered.push(msg.data.clone());
    }

    fn broadcast_command(&self, _msg: &RemoteCommandMessage) {
        record(&self.0, "subscriber.BroadcastCommand");
        check(&self.0, |s| !s.subscriber_closed, "pub/sub must remain available while the broker shuts down");
    }

    fn subscribe(&self, stream: &str) {
        unexpected(&self.0, format!("subscriber.Subscribe({stream:?})"));
    }

    fn unsubscribe(&self, stream: &str) {
        unexpected(&self.0, format!("subscriber.Unsubscribe({stream:?})"));
    }

    fn is_multi_node(&self) -> bool {
        unexpected(&self.0, "subscriber.IsMultiNode()".into());
        false
    }
}

struct AppBroker {
    state: Shared,
    subscriber: Arc<MockSubscriber>,
}

impl Shutdownable for AppBroker {
    fn shutdown(&self) -> Result<(), CliError> {
        record(&self.state, "broker.Shutdown");
        check(&self.state, |s| !s.service_closed, "shared services must outlive the broker");
        self.state.lock().unwrap().broker_closed = true;
        self.subscriber.broadcast_command(&RemoteCommandMessage::default());
        Ok(())
    }
}

impl Broker for AppBroker {
    fn start(&self) -> Result<(), CliError> {
        record(&self.state, "broker.Start");
        Ok(())
    }

    fn announce(&self) -> String {
        record(&self.state, "broker.Announce");
        "test broker".into()
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), CliError> {
        record(&self.state, "broker.HandleBroadcast");
        check(&self.state, |s| !s.service_closed, "shared services must remain available while broadcasts drain");
        if self.state.lock().unwrap().broker_closed {
            return Err(CliError("broker is closed".into()));
        }
        self.subscriber.broadcast(msg);
        Ok(())
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), CliError> {
        unexpected(&self.state, format!("broker.HandleCommand({msg:?})"));
        Ok(())
    }
}

struct MockBroadcaster {
    state: Shared,
    index: usize,
    handler: Arc<dyn BroadcastHandler>,
}

impl Shutdownable for MockBroadcaster {
    // A reader may finish an in-flight publication while Shutdown waits for it.
    fn shutdown(&self) -> Result<(), CliError> {
        record(&self.state, format!("broadcaster{}.Shutdown", self.index));
        let payload = format!(r#"{{"stream":"shutdown-test","data":"{}"}}"#, self.index);
        self.handler.handle_broadcast(payload.as_bytes())
    }
}

impl Broadcaster for MockBroadcaster {
    fn start(&self) -> Result<(), CliError> {
        record(&self.state, format!("broadcaster{}.Start", self.index));
        Ok(())
    }

    fn is_fanout(&self) -> bool {
        unexpected(&self.state, format!("broadcaster{}.IsFanout()", self.index));
        false
    }
}

/// The Rust arm of the scenario: `NewRunner`, `runNode`, then every shutdownable's
/// `Shutdown` in order (upstream asserts NoError on each; the first error is kept).
fn rust_scenario() -> DrainOutcome {
    let mut c = new_config();
    c.rpc.implementation = "none".into();
    c.disconnector_disabled = true;

    let state: Shared = Arc::default();
    let service = Arc::new(Service(state.clone()));
    let subscriber = Arc::new(MockSubscriber(state.clone()));

    let (sub, st) = (subscriber.clone(), state.clone());
    let options = vec![
        RunnerOption::Name("shutdown-test".into()),
        RunnerOption::Logger(LogSink::Discard),
        RunnerOption::DefaultRpcController,
        RunnerOption::Shutdownable(service),
        RunnerOption::Subscriber(Box::new(move |_handler, _config| Ok(sub as Arc<dyn Subscriber>))),
        RunnerOption::Broker(Box::new(move |_broadcaster, _config| {
            Ok(Arc::new(AppBroker { state: st, subscriber }) as Arc<dyn Broker>)
        })),
        RunnerOption::Broadcasters({
            let state = state.clone();
            Box::new(move |handler, _config| {
                Ok((0..2)
                    .map(|index| {
                        Arc::new(MockBroadcaster { state: state.clone(), index, handler: handler.clone() })
                            as Arc<dyn Broadcaster>
                    })
                    .collect())
            })
        }),
    ];

    let mut out = DrainOutcome::default();
    match Runner::new(c, options) {
        Err(e) => out.new_runner_error = Some(e.0),
        Ok(mut runner) => match runner.run_node() {
            Err(e) => out.run_node_error = Some(e.0),
            Ok(()) => {
                for shutdownable in runner.shutdownables() {
                    if let Err(e) = shutdownable.shutdown() {
                        out.shutdown_error.get_or_insert(e.0);
                    }
                }
            }
        },
    }

    let s = state.lock().unwrap();
    out.delivered = s.delivered.clone();
    out.broker_closed = s.broker_closed;
    out.subscriber_closed = s.subscriber_closed;
    out.service_closed = s.service_closed;
    out.violations = s.violations.clone();
    out.calls = s.calls.clone();
    out
}

/// upstream: cli/cli_test.go#TestRunnerDrainsBroadcastsBeforeBrokerShutdown
/// note: Go arm runs the upstream body with its mockery doubles in the bridge through the exported NewRunner + Runner.Embed() (calls runNode) + Embedded.Shutdown (walks runner.shutdownables in order); Embedded.Shutdown stops at the first error where upstream asserts NoError per shutdownable, so the outcomes agree on pass.
/// note: in-callback assertions, unexpected calls and mockery's AssertExpectations failures are collected as violations instead of failing from callback threads.
#[test]
fn test_runner_drains_broadcasts_before_broker_shutdown() {
    let out = runner_drains_broadcasts_before_broker_shutdown(rust_scenario);

    assert_eq!(None, out.new_runner_error, "require.NoError(NewRunner)");
    assert_eq!(None, out.run_node_error, "require.NoError(runner.runNode())");
    assert_eq!(None, out.shutdown_error, "assert.NoError(shutdownable.Shutdown(...))");

    assert_eq!(Vec::<String>::new(), out.violations);
    assert_eq!(vec!["0", "1"], out.delivered);
    assert!(out.broker_closed);
    assert!(out.subscriber_closed);
    assert!(out.service_closed);

    // mockery's cleanup: every expected method was called.
    for call in [
        "service.Shutdown",
        "subscriber.Start",
        "subscriber.Broadcast",
        "subscriber.BroadcastCommand",
        "subscriber.Shutdown",
        "broker.Announce",
        "broker.Start",
        "broker.HandleBroadcast",
        "broker.Shutdown",
        "broadcaster0.Start",
        "broadcaster0.Shutdown",
        "broadcaster1.Start",
        "broadcaster1.Shutdown",
    ] {
        assert!(out.calls.contains(call), "expected call {call} did not happen");
    }
}

/// upstream: cli/cli_test.go#TestCliConfig
#[test]
fn test_cli_config() {
    // Note: args[0] is the program name, so "-h" is not parsed as a flag here.
    let run = new_config_from_cli(&["-h"], &EnvMap::new(), &[]);
    assert!(run.result.is_ok(), "{:?}", run.result);
}

/// The upstream case passes `-h` as the program name (`args[0]`), so it never
/// exercises the help flag. With `-h` as an argument, usage is shown and the run is
/// reported as interrupted without an error.
/// regression-for: cli/cli_test.go#TestCliConfig
#[test]
fn test_cli_config_help_flag_interrupts() {
    let run = new_config_from_cli(&["", "-h"], &EnvMap::new(), &[]);
    assert!(run.result.is_ok(), "{:?}", run.result);
    assert!(run.interrupted);
}

/// upstream: cli/cli_test.go#TestCliConfigCustom
#[test]
fn test_cli_config_custom() {
    let flag = CustomFlag { name: "custom".into(), value: "bar".into(), required: false };

    let run = new_config_from_cli(&["", "-custom=foo"], &EnvMap::new(), &[flag]);

    assert!(run.result.is_ok(), "{:?}", run.result);
    assert_eq!("foo", run.custom["custom"]);
}
