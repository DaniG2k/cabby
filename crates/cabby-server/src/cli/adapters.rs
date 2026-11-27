//! Adapters between the components' interfaces and the runner's views of them
//! (`cli::{Broker, Subscriber, Broadcaster, BroadcastHandler, Shutdownable}`),
//! plus the synchronous node view of the asynchronous RPC controller.

use std::fmt::Display;
use std::future::Future;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

use cabby_core::broadcast::{self as core_broadcast, BroadcastError};
use cabby_core::broker::{self as core_broker, BrokerError, Cacheable, PresenceInfoOptions};
use cabby_core::common::{
    CommandResult, ConnectResult, Controller, ControllerError, Outcome, PresenceEvent, PresenceInfo,
    RemoteCommandMessage, SessionEnv, StreamMessage,
};
use cabby_core::pubsub::{self as core_pubsub, PubSubError};
use serde_json::Value;
use tokio::runtime::{Handle, RuntimeFlavor};

use super::{BroadcastHandler, Broadcaster, Broker, CliError, Shutdownable, Subscriber};
use crate::node::Node;
use crate::rpc::{self, Context};

/// What the runner waits for: `Ok` to stop, `Err` when a component failed.
pub(super) type Events = Sender<Result<(), CliError>>;

/// Runs `fut` to completion on `handle` from synchronous code, wherever that
/// code runs (a plain thread, a Tokio worker, or a current-thread runtime).
pub(crate) fn block_on<F>(handle: &Handle, fut: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match Handle::try_current() {
        Err(_) => handle.block_on(fut),
        Ok(current) if current.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(fut))
        }
        Ok(_) => std::thread::scope(|s| s.spawn(|| handle.block_on(fut)).join().expect("a blocking runtime call")),
    }
}

/// A channel whose errors are forwarded to the runner (components report
/// failures after start through their own typed channel). The forwarding
/// thread exits once the component drops its sender.
pub(super) fn forward_errors<E: Display + Send + 'static>(events: &Events, what: &'static str) -> Sender<E> {
    forward_errors_with(events, move |e| format!("{what}: {e}"))
}

/// [`forward_errors`] with the component's error text as is (upstream sends
/// the error itself to the runner's error channel).
pub(super) fn forward_errors_verbatim<E: Display + Send + 'static>(events: &Events) -> Sender<E> {
    forward_errors_with(events, |e: E| e.to_string())
}

fn forward_errors_with<E: Display + Send + 'static>(
    events: &Events,
    text: impl FnOnce(E) -> String + Send + 'static,
) -> Sender<E> {
    let (tx, rx) = mpsc::channel::<E>();
    let events = events.clone();
    let spawned = std::thread::Builder::new().name("cabby-errors".into()).spawn(move || {
        if let Ok(e) = rx.recv() {
            let _ = events.send(Err(CliError(text(e))));
        }
    });
    drop(spawned);
    tx
}

// --- Shutdown context ---------------------------------------------------------

/// The context shutdownables run with (upstream passes the signal handler's
/// `ctx`): set when a shutdown starts; otherwise a fresh deadline of the
/// configured shutdown timeout.
#[derive(Clone)]
pub(super) struct ShutdownContext {
    current: Arc<Mutex<Option<Context>>>,
    timeout: Duration,
}

impl ShutdownContext {
    pub(super) fn new(timeout: Duration) -> Self {
        Self { current: Arc::default(), timeout }
    }

    pub(super) fn set(&self, ctx: Context) {
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx);
    }

    pub(super) fn get(&self) -> Context {
        let current = self.current.lock().unwrap_or_else(PoisonError::into_inner).clone();
        current.unwrap_or_else(|| Context { deadline: Some(Instant::now() + self.timeout), ..Context::default() })
    }
}

/// Time left until the context's deadline (zero when done).
pub(super) fn remaining(ctx: &Context) -> Duration {
    if ctx.cancel.is_cancelled() {
        return Duration::ZERO;
    }
    ctx.deadline.map_or(Duration::from_secs(3600), |d| d.saturating_duration_since(Instant::now()))
}

/// Runs a server's graceful shutdown (`shutdown`, given the time left in
/// `ctx`) on `runtime`, and returns as soon as `ctx` is cancelled (a second
/// signal), as Go's `http.Server.Shutdown(ctx)` does. The shutdown is polled
/// first, so the server stops accepting connections even then.
pub(super) fn shut_down_server<F, E>(
    runtime: &Handle,
    ctx: &ShutdownContext,
    shutdown: impl FnOnce(Duration) -> F,
) -> Result<(), CliError>
where
    F: Future<Output = Result<(), E>> + Send,
    E: Display,
{
    let ctx = ctx.get();
    let stop = shutdown(remaining(&ctx));
    block_on(runtime, async move {
        tokio::select! {
            biased;
            result = stop => result.map_err(|e| CliError(e.to_string())),
            () = ctx.cancel.cancelled() => Err(CliError("context canceled".into())),
        }
    })
}

/// A named shutdown step (upstream `Shutdownable` implementations).
pub(super) struct ShutdownFn<F>(pub(super) F);

impl<F: Fn() -> Result<(), CliError> + Send + Sync> Shutdownable for ShutdownFn<F> {
    fn shutdown(&self) -> Result<(), CliError> {
        (self.0)()
    }
}

// --- Node as a handler --------------------------------------------------------

/// The node as the broadcast handler of broadcasters and the API.
pub(super) struct NodeHandler(pub(super) Arc<Node>);

impl BroadcastHandler for NodeHandler {
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), CliError> {
        self.0.handle_broadcast(json).map_err(|e| CliError(e.0))
    }

    fn handle_pubsub(&self, json: &[u8]) {
        self.0.handle_pub_sub(json);
    }
}

impl core_broadcast::Handler for NodeHandler {
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), BroadcastError> {
        self.0.handle_broadcast(json).map_err(|e| BroadcastError(e.0))
    }

    fn handle_pubsub(&self, json: &[u8]) {
        self.0.handle_pub_sub(json);
    }
}

impl crate::api::BroadcastHandler for NodeHandler {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), crate::api::ApiError> {
        self.0.handle_broadcast(payload).map_err(|e| crate::api::ApiError { message: e.0 })
    }

    fn handle_pubsub(&self, payload: &[u8]) {
        self.0.handle_pub_sub(payload);
    }
}

// --- Subscribers --------------------------------------------------------------

/// A pub/sub subscriber (core interface) as the runner sees it, and as the
/// broadcaster brokers publish through.
pub(super) struct CoreSubscriber {
    pub(super) inner: Arc<dyn core_pubsub::Subscriber>,
    pub(super) events: Events,
}

impl Shutdownable for CoreSubscriber {
    fn shutdown(&self) -> Result<(), CliError> {
        self.inner.shutdown().map_err(|e| CliError(e.0))
    }
}

impl Subscriber for CoreSubscriber {
    fn start(&self) -> Result<(), CliError> {
        let done = forward_errors::<PubSubError>(&self.events, "pub/sub failed");
        self.inner.start(done).map_err(|e| CliError(e.0))
    }

    fn broadcast(&self, msg: &StreamMessage) {
        self.inner.broadcast(msg);
    }

    fn broadcast_command(&self, msg: &RemoteCommandMessage) {
        self.inner.broadcast_command(msg);
    }

    fn subscribe(&self, stream: &str) {
        self.inner.subscribe(stream);
    }

    fn unsubscribe(&self, stream: &str) {
        self.inner.unsubscribe(stream);
    }

    fn is_multi_node(&self) -> bool {
        self.inner.is_multi_node()
    }
}

/// A runner subscriber as the broadcaster brokers publish through (upstream
/// passes the `pubsub.Subscriber` where a `broker.Broadcaster` is expected).
pub(super) struct SubscriberBroadcaster(pub(super) Arc<dyn Subscriber>);

impl core_broker::Broadcaster for SubscriberBroadcaster {
    fn broadcast(&self, msg: &StreamMessage) {
        self.0.broadcast(msg);
    }

    fn broadcast_command(&self, msg: &RemoteCommandMessage) {
        self.0.broadcast_command(msg);
    }

    fn subscribe(&self, stream: &str) {
        self.0.subscribe(stream);
    }

    fn unsubscribe(&self, stream: &str) {
        self.0.unsubscribe(stream);
    }
}

// --- Brokers ------------------------------------------------------------------

/// A broker (core interface) as the runner sees it.
pub(super) struct CoreBroker(pub(super) Arc<dyn core_broker::Broker>);

impl Shutdownable for CoreBroker {
    fn shutdown(&self) -> Result<(), CliError> {
        self.0.shutdown().map_err(|e| CliError(e.message))
    }
}

impl Broker for CoreBroker {
    fn start(&self) -> Result<(), CliError> {
        self.0.start().map_err(|e| CliError(e.message))
    }

    fn announce(&self) -> String {
        self.0.announce()
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), CliError> {
        self.0.handle_broadcast(msg).map_err(|e| CliError(e.message))
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), CliError> {
        self.0.handle_command(msg).map_err(|e| CliError(e.message))
    }
}

/// A broker given as the runner's view only (a custom broker factory) as the
/// node sees it: publications go to it; everything else behaves like the
/// legacy broker over the subscriber (no history, presence or session cache).
pub(super) struct NodeBroker {
    pub(super) broker: Arc<dyn Broker>,
    pub(super) legacy: core_broker::Legacy,
}

fn broker_error(e: CliError) -> BrokerError {
    BrokerError { message: e.0 }
}

impl core_broker::Broker for NodeBroker {
    fn start(&self) -> Result<(), BrokerError> {
        self.broker.start().map_err(broker_error)
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        self.broker.shutdown().map_err(broker_error)
    }
    fn announce(&self) -> String {
        self.broker.announce()
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.broker.handle_broadcast(msg).map_err(broker_error)
    }
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.broker.handle_command(msg).map_err(broker_error)
    }
    fn subscribe(&self, stream: &str) -> String {
        self.legacy.subscribe(stream)
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.legacy.unsubscribe(stream)
    }
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.legacy.history_from(stream, epoch, offset)
    }
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.legacy.history_since(stream, ts)
    }
    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        self.legacy.commit_session(sid, session)
    }
    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        self.legacy.restore_session(sid)
    }
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        self.legacy.touch_session(sid)
    }
    fn supports_presence(&self) -> bool {
        self.legacy.supports_presence()
    }
    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.legacy.presence_add(stream, sid, pid, info)
    }
    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.legacy.presence_remove(stream, sid)
    }
    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        self.legacy.presence_info(stream, opts)
    }
    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        self.legacy.touch_presence(sid)
    }
    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.legacy.peak(stream)
    }
}

// --- Broadcasters -------------------------------------------------------------

/// A broadcaster (core interface) as the runner sees it.
pub(super) struct CoreBroadcaster {
    pub(super) inner: Arc<dyn core_broadcast::Broadcaster>,
    pub(super) events: Events,
}

impl Shutdownable for CoreBroadcaster {
    fn shutdown(&self) -> Result<(), CliError> {
        self.inner.shutdown().map_err(|e| CliError(e.0))
    }
}

impl Broadcaster for CoreBroadcaster {
    fn start(&self) -> Result<(), CliError> {
        let done = forward_errors::<BroadcastError>(&self.events, "broadcaster failed");
        self.inner.start(done).map_err(|e| CliError(e.0))
    }

    fn is_fanout(&self) -> bool {
        self.inner.is_fanout()
    }
}

// --- RPC controller -----------------------------------------------------------

/// The RPC controller (asynchronous) as the node's synchronous controller: each
/// call blocks the calling thread (a command pool thread) until the RPC
/// completes on the runtime. The thread gives its command pool seat up while it
/// waits ([`crate::node::blocking`]), so other sessions' commands never queue
/// behind RPC calls, and session calls carry the session's close signal
/// ([`crate::node::call_context`]): a session that closes cancels its pending or
/// in-flight call (upstream passes `s.closeCtx`).
pub(super) struct RpcController {
    inner: rpc::Controller,
    runtime: Handle,
}

impl RpcController {
    pub(super) fn new(inner: rpc::Controller, runtime: Handle) -> Self {
        Self { inner, runtime }
    }
}

fn controller_error(e: rpc::RpcError) -> ControllerError {
    ControllerError::new(e.to_string())
}

fn outcome<T>(o: rpc::RpcOutcome<T>) -> Outcome<T> {
    Outcome { result: o.result, error: o.error.map(controller_error) }
}

fn env_or_default(env: Option<&SessionEnv>) -> SessionEnv {
    env.cloned().unwrap_or_default()
}

/// The context of a session call: cancelled when the session closes.
fn session_context() -> Context {
    Context { cancel: crate::node::call_context::close_context().unwrap_or_default(), ..Context::default() }
}

impl RpcController {
    /// Runs an RPC call to completion with the command pool seat given up.
    fn call<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        crate::node::blocking(|| block_on(&self.runtime, fut))
    }
}

impl Controller for RpcController {
    fn start(&self) -> Result<(), ControllerError> {
        let _guard = self.runtime.enter();
        self.inner.start().map_err(controller_error)
    }

    fn shutdown(&self) -> Result<(), ControllerError> {
        block_on(&self.runtime, self.inner.shutdown()).map_err(controller_error)
    }

    fn authenticate(&self, sid: &str, env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        let env = env_or_default(env);
        outcome(self.call(self.inner.authenticate(&session_context(), sid, &env)))
    }

    fn subscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        let env = env_or_default(env);
        outcome(self.call(self.inner.subscribe(&session_context(), sid, &env, ids, channel)))
    }

    fn unsubscribe(&self, sid: &str, env: Option<&SessionEnv>, ids: &str, channel: &str) -> Outcome<CommandResult> {
        let env = env_or_default(env);
        outcome(self.call(self.inner.unsubscribe(&session_context(), sid, &env, ids, channel)))
    }

    fn perform(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> Outcome<CommandResult> {
        let env = env_or_default(env);
        outcome(self.call(self.inner.perform(&session_context(), sid, &env, ids, channel, data)))
    }

    fn disconnect(
        &self,
        sid: &str,
        env: Option<&SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        let env = env_or_default(env);
        // Upstream: `context.Background()` (the session is gone already).
        self.call(self.inner.disconnect(&Context::default(), sid, &env, ids, subscriptions)).map_err(controller_error)
    }
}

/// A router route whose controller is attached later: upstream registers the
/// Pusher channel controller after the node and broker exist, while Cabby builds
/// the router up front. Calls before attachment fail with an application error.
#[derive(Default)]
pub(super) struct DeferredController(std::sync::OnceLock<std::sync::Arc<dyn cabby_core::common::Controller>>);

impl DeferredController {
    pub(super) fn attach(&self, c: std::sync::Arc<dyn cabby_core::common::Controller>) {
        let _ = self.0.set(c);
    }

    fn get(&self) -> Result<&std::sync::Arc<dyn cabby_core::common::Controller>, cabby_core::common::ControllerError> {
        self.0.get().ok_or_else(|| cabby_core::common::ControllerError::new("controller is not ready"))
    }
}

impl cabby_core::common::Controller for DeferredController {
    fn start(&self) -> Result<(), cabby_core::common::ControllerError> {
        self.0.get().map_or(Ok(()), |c| c.start())
    }
    fn shutdown(&self) -> Result<(), cabby_core::common::ControllerError> {
        self.0.get().map_or(Ok(()), |c| c.shutdown())
    }
    fn authenticate(
        &self,
        sid: &str,
        env: Option<&cabby_core::common::SessionEnv>,
    ) -> cabby_core::common::Outcome<cabby_core::common::ConnectResult> {
        match self.get() {
            Ok(c) => c.authenticate(sid, env),
            Err(e) => cabby_core::common::Outcome { result: None, error: Some(e) },
        }
    }
    fn subscribe(
        &self,
        sid: &str,
        env: Option<&cabby_core::common::SessionEnv>,
        ids: &str,
        channel: &str,
    ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
        match self.get() {
            Ok(c) => c.subscribe(sid, env, ids, channel),
            Err(e) => cabby_core::common::Outcome { result: None, error: Some(e) },
        }
    }
    fn unsubscribe(
        &self,
        sid: &str,
        env: Option<&cabby_core::common::SessionEnv>,
        ids: &str,
        channel: &str,
    ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
        match self.get() {
            Ok(c) => c.unsubscribe(sid, env, ids, channel),
            Err(e) => cabby_core::common::Outcome { result: None, error: Some(e) },
        }
    }
    fn perform(
        &self,
        sid: &str,
        env: Option<&cabby_core::common::SessionEnv>,
        ids: &str,
        channel: &str,
        data: &str,
    ) -> cabby_core::common::Outcome<cabby_core::common::CommandResult> {
        match self.get() {
            Ok(c) => c.perform(sid, env, ids, channel, data),
            Err(e) => cabby_core::common::Outcome { result: None, error: Some(e) },
        }
    }
    fn disconnect(
        &self,
        sid: &str,
        env: Option<&cabby_core::common::SessionEnv>,
        ids: &str,
        subscriptions: &[String],
    ) -> Result<(), cabby_core::common::ControllerError> {
        self.get()?.disconnect(sid, env, ids, subscriptions)
    }
}
