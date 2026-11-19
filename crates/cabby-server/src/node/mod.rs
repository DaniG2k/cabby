//! Sessions, subscription state, the command flow and deferred disconnects
//! (upstream package `node`).
//!
//! Time is an explicit dependency ([`Clock`]): session timers (pings, cache and
//! presence keepalives) and the disconnect queue throttle read it, so tests drive
//! them with a controlled clock instead of sleeping (SPEC.md §6).
//!
//! # Concurrency
//!
//! The node is shared (`Arc<Node>`) by transports, the hub, brokers and the
//! disconnect queue. Its collaborators (broker, controller, disconnector) sit
//! behind read-mostly locks that are only held to clone an `Arc`; no node lock
//! is ever held while a controller, broker or session is called. Per-session
//! ordering comes from the session itself (see [`Session`]): the registration
//! lock serializes hub registration against disconnects, and commands of one
//! session are executed in order by its reader.
//!
//! # Threads
//!
//! No thread is tied to a connection. Transports push client messages to a
//! [`SessionReader`], which executes them in order on the bounded command pool
//! ([`execute`]); session writes drain on a shared task pool; timers share one
//! timer thread.

mod app;
mod clock;
mod config;
mod deps;
mod disconnect_queue;
mod session;
mod subscription_state;
mod tasks;

/// The canonical `node.Controller` (defined in `cabby_core::common` because
/// identity and streams, which live in `cabby-core`, implement or wrap it).
pub use cabby_core::common::Controller;
pub use config::{
    Config, DISCONNECT_MODE_ALWAYS, DISCONNECT_MODE_AUTO, DISCONNECT_MODE_NEVER, DISCONNECT_MODES,
    DisconnectQueueConfig,
};
pub use deps::{Hub, LegacySubscriber};
pub use disconnect_queue::{DisconnectQueue, Disconnector, InlineDisconnector, NoopDisconnectQueue};
pub use session::{Connection, ConnectionError, Executor, ReadPermit, Session, SessionOption, SessionReader};
pub use subscription_state::SubscriptionState;
pub use tasks::{DEFAULT_COMMAND_CONCURRENCY, command_concurrency, execute, set_command_concurrency};

pub use app::{AuthOption, Clock, Node, NodeOption, NullController, SystemClock, Ticker, Timer};

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
