//! Graceful shutdown on SIGINT/SIGTERM (upstream `utils.GracefulSignals` as
//! `Runner.setupSignalHandlers` configures it).
//!
//! The first signal starts the shutdown: every shutdownable runs in order, on a
//! thread of its own, with a context whose deadline is the shutdown timeout.
//! A second signal cancels that context (steps waiting on it return early);
//! once the steps have returned, the runner stops with "Immediate termination
//! requested". Either way the runner's [`Runner::run`](super::Runner::run)
//! returns `Ok`, unless a step panicked: the panic is logged (`shutdown step
//! panicked`), the later steps still run, and `run` returns the error, so the
//! process exits 1 (D-117; a panic ends a Go process at once, with status 2).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use cabby_core::logger::{Attr, Level, LogValue};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use super::adapters::{Events, ShutdownContext};
use super::{CliError, Shutdownable};
use crate::rpc::Context;

/// Installs the handlers (upstream `Listen`); they run on the current runtime.
pub(super) fn listen(
    shutdownables: Vec<Arc<dyn Shutdownable>>,
    shutdown_ctx: ShutdownContext,
    timeout: Duration,
    events: Events,
) -> Result<(), CliError> {
    let install = |kind: SignalKind| signal(kind).map_err(|e| CliError(format!("failed to listen to signals: {e}")));
    let mut term = install(SignalKind::terminate())?;
    let mut int = install(SignalKind::interrupt())?;

    let (signals_tx, signals) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            if signals_tx.send(()).is_err() {
                return;
            }
        }
    });
    tokio::spawn(handle(signals, shutdownables, shutdown_ctx, timeout, events));
    Ok(())
}

/// Shuts down on the first of `signals`, and terminates at once on the second.
async fn handle(
    mut signals: mpsc::UnboundedReceiver<()>,
    shutdownables: Vec<Arc<dyn Shutdownable>>,
    shutdown_ctx: ShutdownContext,
    timeout: Duration,
    events: Events,
) {
    if signals.recv().await.is_none() {
        return;
    }

    let cancel = CancellationToken::new();
    shutdown_ctx.set(Context {
        cancel: cancel.clone(),
        deadline: Some(Instant::now() + timeout),
        ..Context::default()
    });

    let (done_tx, mut done) = oneshot::channel();
    let spawned = std::thread::Builder::new().name("cabby-shutdown".into()).spawn(move || {
        crate::metrics::log(
            Level::Info,
            &format!(
                "Shutting down... (hit Ctrl-C to stop immediately or wait for up to {}s for graceful shutdown)",
                timeout.as_secs()
            ),
            &[],
        );
        let _ = done_tx.send(run_steps(shutdownables));
    });
    if spawned.is_err() {
        let _ = events.send(Err(CliError("failed to start the shutdown".into())));
        return;
    }

    let result = tokio::select! {
        result = &mut done => result,
        Some(()) = signals.recv() => {
            cancel.cancel();
            // Wait for the steps to react to the cancellation.
            let result = done.await;
            crate::metrics::log(Level::Warn, "Immediate termination requested. Stopped", &[]);
            result
        }
    };
    let _ = events.send(result.unwrap_or_else(|_| Err(CliError("the shutdown ended unexpectedly".into()))));
}

/// Runs every step in order. Upstream ignores the steps' errors; a step that
/// panics is logged and the next ones still run (D-117).
fn run_steps(shutdownables: Vec<Arc<dyn Shutdownable>>) -> Result<(), CliError> {
    let mut panicked: Option<String> = None;
    for shutdownable in shutdownables {
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| shutdownable.shutdown())) {
            let text = panic_text(panic.as_ref());
            crate::metrics::log(
                Level::Error,
                "shutdown step panicked",
                &[Attr::new("error", LogValue::String(text.clone()))],
            );
            panicked.get_or_insert(text);
        }
    }
    panicked.map_or(Ok(()), |text| Err(CliError(format!("shutdown step panicked: {text}"))))
}

/// The message of a panic payload (`panic!` with a literal or a format string).
fn panic_text(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc as std_mpsc;

    use cabby_core::common::{
        CommandResult, ConnectResult, Controller, ControllerError, Outcome, RemoteCommandMessage, SessionEnv,
        StreamMessage,
    };

    use super::super::{Broker, LogSink, Runner, RunnerOption, Subscriber};
    use super::*;
    use crate::config::Config;

    /// How long a test waits for something it requires.
    const DEADLINE: Duration = Duration::from_secs(5);

    type Ran = Arc<Mutex<Vec<&'static str>>>;

    /// A step that records that it ran; it panics after that when `panics`.
    struct Step {
        name: &'static str,
        ran: Ran,
        panics: bool,
    }

    impl Shutdownable for Step {
        fn shutdown(&self) -> Result<(), CliError> {
            self.ran.lock().unwrap().push(self.name);
            if self.panics {
                panic!("{} failed", self.name);
            }
            Ok(())
        }
    }

    fn step(name: &'static str, ran: &Ran, panics: bool) -> Arc<dyn Shutdownable> {
        Arc::new(Step { name, ran: ran.clone(), panics })
    }

    /// The signal handling of [`listen`] on its own runtime, with signals the
    /// test sends and the runner's event channel (whose sender the runner keeps,
    /// so a lost result is a wait, not a disconnection).
    struct Harness {
        _runtime: tokio::runtime::Runtime,
        signals: mpsc::UnboundedSender<()>,
        events: std_mpsc::Receiver<Result<(), CliError>>,
        _runner_events: Events,
    }

    impl Harness {
        fn start(steps: Vec<Arc<dyn Shutdownable>>, ctx: ShutdownContext) -> Self {
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
            let (signals, rx) = mpsc::unbounded_channel();
            let (events_tx, events) = std_mpsc::channel();
            runtime.spawn(handle(rx, steps, ctx, Duration::from_secs(30), events_tx.clone()));
            Self { _runtime: runtime, signals, events, _runner_events: events_tx }
        }

        fn signal(&self) {
            self.signals.send(()).expect("the handler listens");
        }

        fn result(&self) -> Result<(), CliError> {
            self.events.recv_timeout(DEADLINE).expect("the shutdown ended")
        }
    }

    /// Pins D-117: a step that panics is reported, and the steps after it still
    /// run. Before, the panic ended the shutdown thread: the runner never got a
    /// result (the process waited until it was killed), and later signals were
    /// not handled any more.
    #[test]
    fn deviation_d117_a_panicking_step_is_reported_and_the_next_ones_run() {
        let ran = Ran::default();
        let steps = vec![step("first", &ran, false), step("second", &ran, true), step("third", &ran, false)];
        let harness = Harness::start(steps, ShutdownContext::new(Duration::from_secs(30)));

        harness.signal();

        assert_eq!(Err(CliError("shutdown step panicked: second failed".into())), harness.result());
        assert_eq!(vec!["first", "second", "third"], *ran.lock().unwrap());
    }

    /// Without panics the shutdown ends with `Ok`, after every step.
    #[test]
    fn a_shutdown_runs_every_step_in_order() {
        let ran = Ran::default();
        let steps = vec![step("first", &ran, false), step("second", &ran, false)];
        let harness = Harness::start(steps, ShutdownContext::new(Duration::from_secs(30)));

        harness.signal();

        assert_eq!(Ok(()), harness.result());
        assert_eq!(vec!["first", "second"], *ran.lock().unwrap());
    }

    /// A step that waits for the shutdown context to be cancelled.
    struct UntilCancelled {
        ctx: ShutdownContext,
        started: Mutex<std_mpsc::Sender<()>>,
        cancelled: Arc<AtomicBool>,
    }

    impl Shutdownable for UntilCancelled {
        fn shutdown(&self) -> Result<(), CliError> {
            let ctx = self.ctx.get();
            let _ = self.started.lock().unwrap().send(());
            let until = std::time::Instant::now() + Duration::from_secs(10);
            while !ctx.cancel.is_cancelled() && std::time::Instant::now() < until {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.cancelled.store(ctx.cancel.is_cancelled(), Ordering::SeqCst);
            Ok(())
        }
    }

    /// A second signal cancels the context the steps run with; the shutdown
    /// then ends once they return, with `Ok` (upstream `HandleForceTerminate`).
    #[test]
    fn a_second_signal_cancels_the_steps_context() {
        let ctx = ShutdownContext::new(Duration::from_secs(30));
        let (started_tx, started) = std_mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let ran = Ran::default();
        let waiting = Arc::new(UntilCancelled {
            ctx: ctx.clone(),
            started: Mutex::new(started_tx),
            cancelled: cancelled.clone(),
        });
        let harness = Harness::start(vec![waiting, step("after", &ran, false)], ctx);

        harness.signal();
        started.recv_timeout(DEADLINE).expect("the step started");
        harness.signal();

        assert_eq!(Ok(()), harness.result());
        assert!(cancelled.load(Ordering::SeqCst), "the step saw the cancellation");
        assert_eq!(vec!["after"], *ran.lock().unwrap(), "the later steps still ran");
    }

    // --- The runner's steps --------------------------------------------------------

    /// Which component's shutdown panics.
    #[derive(Clone, Copy, PartialEq)]
    enum Panicking {
        Controller,
        Broker,
        Subscriber,
    }

    struct TestController(Panicking);

    impl Controller for TestController {
        fn start(&self) -> Result<(), ControllerError> {
            Ok(())
        }
        fn shutdown(&self) -> Result<(), ControllerError> {
            assert!(self.0 != Panicking::Controller, "controller shutdown failed");
            Ok(())
        }
        fn authenticate(&self, _: &str, _: Option<&SessionEnv>) -> Outcome<ConnectResult> {
            Outcome { result: None, error: Some(ControllerError::new("not used")) }
        }
        fn subscribe(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str) -> Outcome<CommandResult> {
            Outcome { result: None, error: Some(ControllerError::new("not used")) }
        }
        fn unsubscribe(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str) -> Outcome<CommandResult> {
            Outcome { result: None, error: Some(ControllerError::new("not used")) }
        }
        fn perform(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str, _: &str) -> Outcome<CommandResult> {
            Outcome { result: None, error: Some(ControllerError::new("not used")) }
        }
        fn disconnect(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &[String]) -> Result<(), ControllerError> {
            Ok(())
        }
    }

    struct TestBroker(Panicking);

    impl Shutdownable for TestBroker {
        fn shutdown(&self) -> Result<(), CliError> {
            assert!(self.0 != Panicking::Broker, "broker shutdown failed");
            Ok(())
        }
    }

    impl Broker for TestBroker {
        fn start(&self) -> Result<(), CliError> {
            Ok(())
        }
        fn announce(&self) -> String {
            "Using test broker".into()
        }
        fn handle_broadcast(&self, _: &StreamMessage) -> Result<(), CliError> {
            Ok(())
        }
        fn handle_command(&self, _: &RemoteCommandMessage) -> Result<(), CliError> {
            Ok(())
        }
    }

    struct TestSubscriber(Panicking);

    impl Shutdownable for TestSubscriber {
        fn shutdown(&self) -> Result<(), CliError> {
            assert!(self.0 != Panicking::Subscriber, "subscriber shutdown failed");
            Ok(())
        }
    }

    impl Subscriber for TestSubscriber {
        fn start(&self) -> Result<(), CliError> {
            Ok(())
        }
        fn broadcast(&self, _: &StreamMessage) {}
        fn broadcast_command(&self, _: &RemoteCommandMessage) {}
        fn subscribe(&self, _: &str) {}
        fn unsubscribe(&self, _: &str) {}
        fn is_multi_node(&self) -> bool {
            false
        }
    }

    /// Pins D-117 for the runner's own steps: whichever of the node (its
    /// controller), the broker and the pub/sub subscriber panics while it shuts
    /// down, the shutdown is reported failed and the steps after it (a custom
    /// shutdownable, the metrics) still run.
    #[test]
    fn deviation_d117_a_panicking_component_does_not_stop_the_shutdown() {
        for (panicking, message) in [
            (Panicking::Controller, "controller shutdown failed"),
            (Panicking::Broker, "broker shutdown failed"),
            (Panicking::Subscriber, "subscriber shutdown failed"),
        ] {
            let ran = Ran::default();
            let options = vec![
                RunnerOption::Name("Cabby".into()),
                RunnerOption::Logger(LogSink::Default),
                RunnerOption::Controller(Box::new(move |_, _| Ok(Arc::new(TestController(panicking))))),
                RunnerOption::Subscriber(Box::new(move |_, _| Ok(Arc::new(TestSubscriber(panicking))))),
                RunnerOption::Broker(Box::new(move |_, _| Ok(Arc::new(TestBroker(panicking))))),
                RunnerOption::Shutdownable(step("custom", &ran, false)),
            ];
            let mut runner = Runner::new(Config::new(), options).expect("a runner");
            runner.run_node().expect("the node starts");
            let harness = Harness::start(runner.shutdownables(), ShutdownContext::new(Duration::from_secs(30)));

            harness.signal();

            assert_eq!(Err(CliError(format!("shutdown step panicked: {message}"))), harness.result());
            assert_eq!(vec!["custom"], *ran.lock().unwrap(), "the later steps ran ({message})");
        }
    }
}
