//! Command-line interface and server runner (upstream package `cli`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cabby_core::common::{RemoteCommandMessage, StreamMessage};
use cabby_core::pending;

use crate::config::{Config, EnvMap};

/// A CLI or runner failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CliError(pub String);

/// Parses `--metrics_tags` (`key:value,key:value`; upstream unexported `parseTags`).
pub fn parse_tags(_s: &str) -> HashMap<String, String> {
    pending!("cli::parse_tags")
}

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

/// Builds the configuration from command-line `args` (`args[0]` is the program
/// name), configuration files and `env` (upstream `NewConfigFromCLI`).
///
/// `Ok((config, true))` means the run was interrupted (usage or version shown) and
/// no further action is required; upstream returns an empty config then.
pub fn new_config_from_cli(_args: &[&str], _env: &EnvMap, _opts: Vec<CliOption>) -> Result<(Config, bool), CliError> {
    pending!("cli::new_config_from_cli")
}

// --- Runner -------------------------------------------------------------------
//
// Minimal views of the upstream interfaces the runner composes (broadcast.Broadcaster,
// broadcast.Handler, pubsub.Subscriber, pubsub.Handler, broker.Broker).
// TODO(merge): replace with the traits of the broadcast/pubsub/broker modules.

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
pub trait SubscriberHandler: Send + Sync {
    fn broadcast(&self, msg: &StreamMessage);
    fn execute_remote_command(&self, msg: &RemoteCommandMessage);
}

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

/// Where runner logs go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogSink {
    Default,
    Discard,
}

/// Runner options (upstream `cli.Option` constructors `With*`).
pub enum RunnerOption {
    Name(String),
    Logger(LogSink),
    DefaultRpcController,
    Shutdownable(Arc<dyn Shutdownable>),
    Subscriber(SubscriberFactory),
    Broker(BrokerFactory),
    Broadcasters(BroadcastersFactory),
}

/// Assembles and runs a server node (upstream `cli.Runner`).
pub struct Runner {
    _config: Config,
}

impl Runner {
    pub fn new(_config: Config, _options: Vec<RunnerOption>) -> Result<Runner, CliError> {
        pending!("cli::new_runner")
    }

    /// Builds and starts the node and its components (upstream unexported `runNode`).
    pub fn run_node(&mut self) -> Result<(), CliError> {
        pending!("cli::run_node")
    }

    /// Components to shut down, in shutdown order (upstream unexported `shutdownables`).
    pub fn shutdownables(&self) -> Vec<Arc<dyn Shutdownable>> {
        pending!("cli::shutdownables")
    }
}
