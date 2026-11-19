//! Graceful shutdown on SIGINT/SIGTERM (upstream `utils.GracefulSignals` as
//! `Runner.setupSignalHandlers` configures it).
//!
//! The first signal starts the shutdown: every shutdownable runs in order, on a
//! thread of its own, with a context whose deadline is the shutdown timeout.
//! A second signal cancels that context (steps waiting on it return early);
//! once the steps have returned, the runner stops with "Immediate termination
//! requested". Either way the runner's [`Runner::run`](super::Runner::run)
//! returns `Ok`.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use cabby_core::logger::Level;
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

    tokio::spawn(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }

        let cancel = CancellationToken::new();
        shutdown_ctx.set(Context {
            cancel: cancel.clone(),
            deadline: Some(Instant::now() + timeout),
            ..Context::default()
        });

        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let stopped = events.clone();
        let spawned = std::thread::Builder::new().name("cabby-shutdown".into()).spawn(move || {
            crate::metrics::log(
                Level::Info,
                &format!(
                    "Shutting down... (hit Ctrl-C to stop immediately or wait for up to {}s for graceful shutdown)",
                    timeout.as_secs()
                ),
                &[],
            );
            for shutdownable in shutdownables {
                // Upstream ignores the handlers' errors.
                let _ = shutdownable.shutdown();
            }
            let _ = done_tx.send(());
            let _ = stopped.send(Ok(()));
        });
        if spawned.is_err() {
            let _ = events.send(Err(CliError("failed to start the shutdown".into())));
            return;
        }

        let mut done_rx = done_rx;
        tokio::select! {
            _ = &mut done_rx => {}
            _ = term.recv() => force_terminate(&cancel, done_rx, &events).await,
            _ = int.recv() => force_terminate(&cancel, done_rx, &events).await,
        }
    });
    Ok(())
}

async fn force_terminate(cancel: &CancellationToken, done: tokio::sync::oneshot::Receiver<()>, events: &Events) {
    cancel.cancel();
    // Wait for the handlers to react to the cancellation.
    let _ = done.await;
    crate::metrics::log(Level::Warn, "Immediate termination requested. Stopped", &[]);
    let _ = events.send(Ok(()));
}
