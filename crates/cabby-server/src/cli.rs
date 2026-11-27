//! Command-line interface and server runner (upstream package `cli`).

use std::sync::{Arc, Mutex};

use cabby_core::common::{Controller, RemoteCommandMessage, StreamMessage};

use crate::config::Config;

mod adapters;
mod flags;
/// Go `strconv.ParseInt(s, 0, 64)`, as the CLI parses integer settings.
pub(crate) use flags::parse_int as parse_go_int;
mod options;
mod runner;
mod session_options;
mod signals;
mod sqlite;

pub use options::{ConfigSearch, new_config_from_cli, new_config_from_cli_to, new_config_from_cli_with, parse_tags};
pub use runner::{Embedded, Runner};
pub use session_options::{
    ACTION_CABLE_V1_EXT_JSON, ACTION_CABLE_V1_JSON, action_cable_protocols, is_extended_action_cable_protocol,
};

/// A CLI or runner failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CliError(pub String);

/// A string flag added by an embedding application (upstream `cli.StringFlag`
/// passed through `WithCLICustomOptions`). The parsed value is stored in `destination`.
#[derive(Debug, Clone)]
pub struct StringFlag {
    pub name: String,
    /// Default value.
    pub value: String,
    pub required: bool,
    pub destination: Arc<Mutex<String>>,
}

/// Options of [`new_config_from_cli`] (upstream `cliOption`s).
#[derive(Debug, Clone)]
pub enum CliOption {
    Name(String),
    Version(String),
    UsageHeader(String),
    CustomOptions(Vec<StringFlag>),
}

/// Runs a server node with the configuration from [`new_config_from_cli`]
/// (upstream `main`: `NewRunner` with the default components, then `Run`).
///
/// Builds the Tokio runtime the server's I/O runs on (one worker per CPU, like
/// Go's `GOMAXPROCS`), then blocks until the server stops. The process is
/// meant to exit when this returns (the runtime is left running, see below).
pub fn run(config: Config) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("cabby-io")
        .enable_all()
        .build()
        .map_err(|e| CliError(format!("failed to start the runtime: {e}")))?;
    let result = {
        let _guard = runtime.enter();
        set_command_pool_size(&config);
        Runner::new(config, default_runner_options()).and_then(|mut runner| runner.run())
    };
    // Work still in progress ends with the process, as Go's goroutines do:
    // connections still open, and Disconnect calls of sessions that a second
    // signal or the shutdown timeout left in progress. Shutting the runtime
    // down instead would fail the timers of the threads still waiting in those
    // calls, each with a panic message.
    std::mem::forget(runtime);
    result
}

/// The runner options of [`run`]: the default components.
fn default_runner_options() -> Vec<RunnerOption> {
    vec![
        RunnerOption::Name("Cabby".into()),
        RunnerOption::DefaultRpcController,
        RunnerOption::DefaultBroker,
        RunnerOption::DefaultSubscriber,
        RunnerOption::DefaultBroadcaster,
    ]
}

/// Sizes the node's command pool: twice the RPC concurrency and at least the
/// default. The seats bound commands doing work; a command blocked in an RPC
/// call gives its seat up (see `node::tasks`), so the size does not limit how
/// many sessions wait for the RPC application.
fn set_command_pool_size(config: &Config) {
    let rpc = usize::try_from(config.rpc.concurrency).unwrap_or(0);
    crate::node::set_command_concurrency((rpc * 2).max(crate::node::DEFAULT_COMMAND_CONCURRENCY));
}

// --- Runner -------------------------------------------------------------------
//
// The runner's views of the upstream interfaces it composes (broadcast.Broadcaster,
// broadcast.Handler, pubsub.Subscriber, pubsub.Handler, broker.Broker): what
// custom components given as options implement. The default components
// implement the full interfaces of their modules and are adapted to these.

/// Upstream `cli.Shutdownable`.
pub trait Shutdownable: Send + Sync {
    fn shutdown(&self) -> Result<(), CliError>;
}

/// Upstream `broadcast.Handler`: receives publications from broadcasters.
pub trait BroadcastHandler: Send + Sync {
    /// A message for this node only (passed through the broker).
    fn handle_broadcast(&self, json: &[u8]) -> Result<(), CliError>;
    /// A message delivered to all nodes.
    fn handle_pubsub(&self, json: &[u8]);
}

/// Upstream `broadcast.Broadcaster`.
pub trait Broadcaster: Shutdownable {
    fn start(&self) -> Result<(), CliError>;
    fn is_fanout(&self) -> bool;
}

/// Upstream `pubsub.Handler`: receives messages from the pub/sub subscriber.
pub use cabby_core::pubsub::Handler as SubscriberHandler;

/// Upstream `pubsub.Subscriber`.
pub trait Subscriber: Shutdownable {
    fn start(&self) -> Result<(), CliError>;
    fn broadcast(&self, msg: &StreamMessage);
    fn broadcast_command(&self, msg: &RemoteCommandMessage);
    fn subscribe(&self, stream: &str);
    fn unsubscribe(&self, stream: &str);
    fn is_multi_node(&self) -> bool;
}

/// Upstream `broker.Broker` (the parts the runner uses).
pub trait Broker: Shutdownable {
    fn start(&self) -> Result<(), CliError>;
    fn announce(&self) -> String;
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), CliError>;
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), CliError>;
}

pub type BroadcastersFactory =
    Box<dyn FnOnce(Arc<dyn BroadcastHandler>, &Config) -> Result<Vec<Arc<dyn Broadcaster>>, CliError> + Send>;
pub type SubscriberFactory =
    Box<dyn FnOnce(Arc<dyn SubscriberHandler>, &Config) -> Result<Arc<dyn Subscriber>, CliError> + Send>;
/// Receives the subscriber the broker publishes through.
pub type BrokerFactory = Box<dyn FnOnce(Arc<dyn Subscriber>, &Config) -> Result<Arc<dyn Broker>, CliError> + Send>;
/// Builds the node's controller (upstream `controllerFactory`).
pub type ControllerFactory =
    Box<dyn FnOnce(Arc<crate::metrics::Metrics>, &Config) -> Result<Arc<dyn Controller>, CliError> + Send>;

/// Where runner logs go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSink {
    /// The process logger as it is (upstream `WithLogger(slog.Default())`).
    Default,
    /// Nothing is logged.
    Discard,
}

/// Runner options (upstream `cli.Option` constructors `With*`).
pub enum RunnerOption {
    /// `WithName`.
    Name(String),
    /// `WithLogger`: without it, the runner initializes the process logger from
    /// the configuration.
    Logger(LogSink),
    /// `WithDefaultRPCController`.
    DefaultRpcController,
    /// `WithController`.
    Controller(ControllerFactory),
    /// `WithShutdownable`.
    Shutdownable(Arc<dyn Shutdownable>),
    /// `WithSubscriber`.
    Subscriber(SubscriberFactory),
    /// `WithDefaultSubscriber`: legacy, Redis or NATS pub/sub from the configuration.
    DefaultSubscriber,
    /// `WithBroker`.
    Broker(BrokerFactory),
    /// `WithDefaultBroker`: legacy, memory, NATS or SQLite broker from the configuration.
    DefaultBroker,
    /// `WithBroadcasters`.
    Broadcasters(BroadcastersFactory),
    /// `WithDefaultBroadcaster`: the configured broadcast adapters.
    DefaultBroadcaster,
}
