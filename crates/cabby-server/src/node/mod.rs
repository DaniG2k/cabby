//! Sessions, subscription state, the command flow and deferred disconnects
//! (upstream package `node`).
//!
//! Time is an explicit dependency ([`Clock`]): session timers (pings, cache and
//! presence keepalives) and the disconnect queue throttle read it, so tests drive
//! them with a controlled clock instead of sleeping (SPEC.md §6).

use std::sync::Arc;
use std::time::{Duration, Instant};

use cabby_core::broker::{Broker, Presenter};
use cabby_core::common::{
    CommandResult, ConnectResult, Message, PresenceEvent, RemoteCommandMessage, RemoteDisconnectMessage, StreamMessage,
};
use cabby_core::pending;

mod config;
mod deps;
mod disconnect_queue;
mod session;
mod subscription_state;

/// The canonical `node.Controller` (defined in `cabby_core::common` because
/// identity and streams, which live in `cabby-core`, implement or wrap it).
pub use cabby_core::common::Controller;
pub use config::{
    Config, DISCONNECT_MODE_ALWAYS, DISCONNECT_MODE_AUTO, DISCONNECT_MODE_NEVER, DISCONNECT_MODES,
    DisconnectQueueConfig,
};
pub use deps::{Hub, LegacySubscriber};
pub use disconnect_queue::{DisconnectQueue, Disconnector, InlineDisconnector, NoopDisconnectQueue};
pub use session::{Connection, ConnectionError, Executor, Session, SessionOption};
pub use subscription_state::SubscriptionState;

use crate::cli::SubscriberHandler;
use crate::metrics::Instrumenter;
use crate::rpc::Context;

/// A node operation failed (Go `error`), compared by message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct NodeError(pub String);

// --- Metric names (upstream unexported constants) ----------------------------

pub const METRICS_CLIENTS_NUM: &str = "clients_num";
pub const METRICS_UNIQ_CLIENTS_NUM: &str = "clients_uniq_num";
pub const METRICS_STREAMS_NUM: &str = "broadcast_streams_num";
pub const METRICS_DISCONNECT_QUEUE: &str = "disconnect_queue_size";
pub const METRICS_FAILED_AUTHS: &str = "failed_auths_total";
pub const METRICS_RECEIVED_MSG: &str = "client_msg_total";
pub const METRICS_FAILED_COMMAND_RECEIVED: &str = "failed_client_msg_total";
pub const METRICS_DISCONNECTED_SLOW_CLIENTS: &str = "disconnected_slow_clients_total";
pub const METRICS_DISCONNECT_DISCARDED: &str = "disconnect_queue_discarded_total";
pub const METRICS_PUBLICATIONS: &str = "publications_total";
pub const METRICS_BROADCAST_MSG: &str = "broadcast_msg_total";
pub const METRICS_REMOTE_COMMANDS: &str = "remote_commands_total";
pub const METRICS_UNKNOWN_BROADCAST: &str = "failed_broadcast_msg_total";
pub const METRICS_REJECTED_CLIENTS: &str = "rejected_clients_total";
pub const METRICS_SENT_MSG: &str = "server_msg_total";
pub const METRICS_FAILED_SENT: &str = "failed_server_msg_total";
pub const METRICS_WRITE_TIMEOUT: &str = "timeout_server_msg_total";
pub const METRICS_DATA_SENT: &str = "data_sent_total";
pub const METRICS_DATA_RECEIVED: &str = "data_rcvd_total";

// WebSocket close codes the node uses (upstream `ws.Close*`).
pub const CLOSE_NORMAL_CLOSURE: i32 = 1000;
pub const CLOSE_GOING_AWAY: i32 = 1001;
pub const CLOSE_ABNORMAL_CLOSURE: i32 = 1006;
pub const CLOSE_INTERNAL_SERVER_ERR: i32 = 1011;

// --- Time -------------------------------------------------------------------

/// A scheduled callback (upstream `*time.Timer`).
pub trait Timer: Send + Sync {
    /// Cancels the callback; `false` if it already ran or was stopped.
    fn stop(&self) -> bool;
}

/// A periodic tick source shared by several receivers (upstream `*time.Ticker`:
/// one tick is buffered, further ticks are dropped while nobody receives).
pub trait Ticker: Send + Sync {
    /// Blocks until the next tick.
    fn recv(&self);
    fn stop(&self);
}

/// Time source of sessions and the disconnect queue (upstream reads `time.Now`
/// and uses `time.AfterFunc` / `time.NewTicker`).
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    /// Runs `f` once after `delay` unless the returned timer is stopped first.
    fn after(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer>;
    fn ticker(&self, period: Duration) -> Arc<dyn Ticker>;
}

/// The operating system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        pending!("node::system_clock_now")
    }
    fn after(&self, _delay: Duration, _f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer> {
        pending!("node::system_clock_after")
    }
    fn ticker(&self, _period: Duration) -> Arc<dyn Ticker> {
        pending!("node::system_clock_ticker")
    }
}

// --- Node -------------------------------------------------------------------

/// Node construction options (upstream `NodeOption` functions `With*`).
pub enum NodeOption {
    /// `WithController`.
    Controller(Arc<dyn Controller>),
    /// `WithInstrumenter`.
    Instrumenter(Arc<dyn Instrumenter>),
    /// `WithID`.
    Id(String),
    /// The time source for sessions and the disconnect queue (default: [`SystemClock`]).
    Clock(Arc<dyn Clock>),
}

/// Authentication options (upstream `AuthOption` functions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOption {
    /// `WithDisconnectOnFailure` (default `true`).
    DisconnectOnFailure(bool),
}

/// The whole application: hub, broker, controller and disconnector (upstream
/// `node.Node`).
pub struct Node {
    _private: (),
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node").finish_non_exhaustive()
    }
}

impl Node {
    /// Upstream `NewNode(config, opts...)`: builds the hub and registers metrics.
    pub fn new(_config: Config, _opts: Vec<NodeOption>) -> Arc<Node> {
        pending!("node::new_node")
    }

    /// Starts the hub and the stats collector (upstream `Start`).
    pub fn start(self: &Arc<Self>) -> Result<(), NodeError> {
        pending!("node::start")
    }

    /// Disconnects active sessions, then stops the hub, the disconnector and the
    /// controller (upstream `Shutdown(ctx)`); fails when called twice.
    pub fn shutdown(&self, _ctx: &Context) -> Result<(), NodeError> {
        pending!("node::shutdown")
    }

    pub fn id(&self) -> String {
        pending!("node::id")
    }

    pub fn set_disconnector(&self, _d: Arc<dyn Disconnector>) {
        pending!("node::set_disconnector")
    }

    pub fn set_broker(&self, _b: Arc<dyn Broker>) {
        pending!("node::set_broker")
    }

    /// Replaces the controller (upstream tests assign the unexported `controller` field).
    pub fn set_controller(&self, _c: Arc<dyn Controller>) {
        pending!("node::set_controller")
    }

    pub fn instrumenter(&self) -> Arc<dyn Instrumenter> {
        pending!("node::instrumenter")
    }

    /// The node's broker (upstream unexported field `broker`).
    pub fn broker(&self) -> Arc<dyn Broker> {
        pending!("node::broker")
    }

    /// The node's hub (upstream unexported field `hub`).
    pub fn hub(&self) -> &Hub {
        pending!("node::hub")
    }

    /// The node's disconnector (upstream unexported field `disconnector`).
    pub fn disconnector(&self) -> Arc<dyn Disconnector> {
        pending!("node::disconnector")
    }

    pub fn is_shutting_down(&self) -> bool {
        pending!("node::is_shutting_down")
    }

    pub fn admit_new_client(&self) -> bool {
        pending!("node::admit_new_client")
    }

    /// Parses an application broadcast and passes it to the broker (upstream
    /// `HandleBroadcast`). Unparseable payloads are counted and ignored.
    pub fn handle_broadcast(&self, _raw: &[u8]) -> Result<(), NodeError> {
        pending!("node::handle_broadcast")
    }

    /// Parses a pub/sub message and delivers it locally, bypassing the broker
    /// (upstream `HandlePubSub`).
    pub fn handle_pub_sub(&self, _raw: &[u8]) {
        pending!("node::handle_pub_sub")
    }

    pub fn lookup_session(&self, _identifiers: &str) -> Option<Arc<Session>> {
        pending!("node::lookup_session")
    }

    /// Restores the session from the broker cache or calls the controller; on
    /// success registers the session with the hub (upstream `Authenticate`).
    /// `Ok(None)` is Go's `(nil, nil)` (e.g. the session closed meanwhile).
    pub fn authenticate(&self, _s: &Arc<Session>, _opts: &[AuthOption]) -> Result<Option<ConnectResult>, NodeError> {
        pending!("node::authenticate")
    }

    /// Marks the session authenticated and registers it with the hub unless it
    /// is already closed (upstream `Authenticated`).
    pub fn authenticated(&self, _s: &Arc<Session>, _identifiers: &str) {
        pending!("node::authenticated")
    }

    pub fn try_restore_session(&self, _s: &Arc<Session>) -> bool {
        pending!("node::try_restore_session")
    }

    pub fn subscribe(&self, _s: &Arc<Session>, _msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        pending!("node::subscribe")
    }

    pub fn unsubscribe(&self, _s: &Arc<Session>, _msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        pending!("node::unsubscribe")
    }

    pub fn perform(&self, _s: &Arc<Session>, _msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        pending!("node::perform")
    }

    /// Sends the requested backlog followed by `confirm_history`, or
    /// `reject_history` on broker errors (upstream `History`).
    pub fn history(&self, _s: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::history")
    }

    pub fn whisper(&self, _s: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::whisper")
    }

    pub fn presence(&self, _s: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::presence")
    }

    pub fn presence_join(&self, _s: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::presence_join")
    }

    pub fn presence_leave(&self, _s: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::presence_leave")
    }

    /// Delivers a stream message to local sessions (upstream `Broadcast`).
    pub fn broadcast(&self, _msg: &StreamMessage) {
        pending!("node::broadcast")
    }

    pub fn execute_remote_command(&self, _msg: &RemoteCommandMessage) {
        pending!("node::execute_remote_command")
    }

    /// Unregisters the session (hub, broker streams) and enqueues its Disconnect
    /// call if it is disconnectable (upstream `Disconnect`).
    pub fn disconnect(&self, _s: &Arc<Session>) -> Result<(), NodeError> {
        pending!("node::disconnect")
    }

    /// Calls the controller's Disconnect now (upstream `DisconnectNow`).
    pub fn disconnect_now(&self, _s: &Arc<Session>) -> Result<(), NodeError> {
        pending!("node::disconnect_now")
    }

    pub fn remote_disconnect(&self, _msg: &RemoteDisconnectMessage) {
        pending!("node::remote_disconnect")
    }

    /// Sessions registered with the hub.
    pub fn size(&self) -> usize {
        pending!("node::size")
    }

    /// Upstream `HandleJoin` / `HandleLeave` (also via [`Presenter`]).
    pub fn handle_join(&self, _stream: &str, _msg: &PresenceEvent) {
        pending!("node::handle_join")
    }

    pub fn handle_leave(&self, _stream: &str, _msg: &PresenceEvent) {
        pending!("node::handle_leave")
    }
}

impl Executor for Node {
    fn handle_command(&self, _session: &Arc<Session>, _msg: &Message) -> Result<(), NodeError> {
        pending!("node::handle_command")
    }

    fn disconnect(&self, _session: &Arc<Session>) -> Result<(), NodeError> {
        pending!("node::executor_disconnect")
    }
}

impl Presenter for Node {
    fn handle_join(&self, _stream: &str, _msg: &PresenceEvent) {
        pending!("node::handle_join")
    }

    fn handle_leave(&self, _stream: &str, _msg: &PresenceEvent) {
        pending!("node::handle_leave")
    }
}

/// The pub/sub side of the node (upstream `pubsub.Handler`, implemented by `*Node`).
impl SubscriberHandler for Node {
    fn broadcast(&self, _msg: &StreamMessage) {
        pending!("node::broadcast")
    }

    fn execute_remote_command(&self, _msg: &RemoteCommandMessage) {
        pending!("node::execute_remote_command")
    }
}

/// Rejects every connection and subscription (upstream `NullController`, used
/// when no RPC is configured).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullController;

impl Controller for NullController {
    fn start(&self) -> Result<(), cabby_core::common::ControllerError> {
        pending!("node::null_controller_start")
    }
    fn shutdown(&self) -> Result<(), cabby_core::common::ControllerError> {
        pending!("node::null_controller_shutdown")
    }
    fn authenticate(
        &self,
        _sid: &str,
        _env: Option<&cabby_core::common::SessionEnv>,
    ) -> cabby_core::common::Outcome<ConnectResult> {
        pending!("node::null_controller_authenticate")
    }
    fn subscribe(
        &self,
        _sid: &str,
        _env: Option<&cabby_core::common::SessionEnv>,
        _ids: &str,
        _channel: &str,
    ) -> cabby_core::common::Outcome<CommandResult> {
        pending!("node::null_controller_subscribe")
    }
    fn unsubscribe(
        &self,
        _sid: &str,
        _env: Option<&cabby_core::common::SessionEnv>,
        _ids: &str,
        _channel: &str,
    ) -> cabby_core::common::Outcome<CommandResult> {
        pending!("node::null_controller_unsubscribe")
    }
    fn perform(
        &self,
        _sid: &str,
        _env: Option<&cabby_core::common::SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> cabby_core::common::Outcome<CommandResult> {
        pending!("node::null_controller_perform")
    }
    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&cabby_core::common::SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), cabby_core::common::ControllerError> {
        pending!("node::null_controller_disconnect")
    }
}
