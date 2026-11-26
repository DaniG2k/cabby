//! The server runner (upstream `cli/cli.go`, `cli/runner_options.go` and
//! `cli/embed.go`).
//!
//! # Threads and runtime
//!
//! The runner is synchronous: [`Runner::run`] blocks its thread until the server
//! stops. Network I/O (HTTP, WebSocket, RPC) runs on the Tokio runtime current
//! when the runner was created ([`crate::server::runtime_handle`]); node work
//! that may block runs on the node's pools (see [`crate::node`]). Shutdown runs
//! on a thread of its own, so blocking shutdown steps never hold a runtime
//! worker.

use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use cabby_core::broker as core_broker;
use cabby_core::common::Controller;
use cabby_core::identity::{IdentifiableController, Identifier, IdentifierPipeline, JwtIdentifier, PublicIdentifier};
use cabby_core::logger::{Attr, Level, LogValue};
use cabby_core::pubsub as core_pubsub;
use cabby_core::router::RouterController;
use cabby_core::streams::StreamsController;
use cabby_core::utils::all_pools;
use tokio::runtime::Handle;

use super::adapters::{
    CoreBroadcaster, CoreBroker, CoreSubscriber, DeferredController, Events, NodeBroker, NodeHandler, RpcController,
    ShutdownContext, ShutdownFn, SubscriberBroadcaster, block_on, forward_errors_verbatim, remaining, shut_down_server,
};
use super::session_options::default_websocket_handler;
use super::{
    BroadcastHandler, Broadcaster, BroadcastersFactory, Broker, BrokerFactory, CliError, ControllerFactory, LogSink,
    RunnerOption, Shutdownable, Subscriber, SubscriberFactory, SubscriberHandler,
};
use crate::config::{Config, EnvMap, StartupNote};
use crate::metrics::{Instrumenter, Metrics, StatsdWriter};
use crate::node::{DisconnectQueue, Disconnector, Node, NodeOption, NoopDisconnectQueue, NullController};
use crate::rpc::Context;
use crate::server::{self, HttpServer, ServerDefaults};

/// errorx `AssertionFailed.New(msg)`.
fn assertion_failed(msg: &str) -> CliError {
    CliError(format!("common.assertion_failed: {msg}"))
}

/// errorx `IllegalArgument.New(msg)`.
fn illegal_argument(msg: &str) -> CliError {
    CliError(format!("common.illegal_argument: {msg}"))
}

/// errorx `Decorate(err, msg)`.
fn decorate(err: impl std::fmt::Display, msg: &str) -> CliError {
    CliError(format!("{msg}, cause: {err}"))
}

fn log_info(msg: &str) {
    crate::metrics::log(Level::Info, msg, &[]);
}

enum SubscriberSource {
    Custom(SubscriberFactory),
    Default,
}

enum BrokerSource {
    Custom(BrokerFactory),
    Default,
}

enum BroadcastersSource {
    Custom(BroadcastersFactory),
    Default,
}

/// The broker as the runner sees it, and as the node uses it.
type BrokerPair = (Arc<dyn Broker>, Arc<dyn core_broker::Broker>);

/// The subscriber in both shapes the runner needs.
struct RunnerSubscriber {
    runner: Arc<dyn Subscriber>,
    broadcaster: Arc<dyn core_broker::Broadcaster>,
}

/// Assembles and runs a server node (upstream `cli.Runner`).
pub struct Runner {
    name: String,
    config: Config,
    controller_factory: Option<ControllerFactory>,
    subscriber_factory: Option<SubscriberSource>,
    broker_factory: Option<BrokerSource>,
    broadcasters_factory: Option<BroadcastersSource>,
    metrics: Arc<Metrics>,
    shutdownables: Vec<Arc<dyn Shutdownable>>,
    shutdown_ctx: ShutdownContext,
    events: Events,
    events_rx: Receiver<Result<(), CliError>>,
    runtime: Handle,
    node: Option<Arc<Node>>,
    node_broker: Option<Arc<dyn core_broker::Broker>>,
    /// The router route for the Pusher channel, attached once the broker exists.
    pusher_controller: Option<Arc<DeferredController>>,
    /// Shut down when [`Runner::run`] or [`Runner::run_node`] fails after it
    /// was created: the SQLite broker, so a failed start leaves its file clean
    /// (everything it assigned is persisted) and the next start keeps the epoch.
    startup_cleanup: Option<Arc<dyn Shutdownable>>,
    /// Whether the broadcast adapters were set (flag, variable or file)
    /// rather than left at the default or filled in by a preset.
    broadcast_adapters_set: bool,
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner").field("name", &self.name).finish_non_exhaustive()
    }
}

/// Replaces `copy` with `after` when it is still the copy of `before` (upstream
/// components share pointers to the main Redis/NATS sections, so presets that
/// change those sections reach them).
fn refresh_copy<T: Clone + PartialEq>(copy: &mut Option<T>, before: &T, after: &T) {
    if copy.as_ref() == Some(before) {
        *copy = Some(after.clone());
    }
}

impl Runner {
    /// Upstream `NewRunner(config, options)`: applies the options, initializes
    /// the default logger (unless one was given), loads the presets and creates
    /// the metrics.
    pub fn new(config: Config, options: Vec<RunnerOption>) -> Result<Runner, CliError> {
        let (events, events_rx) = mpsc::channel();
        let timeout = Duration::from_secs(u64::try_from(config.app.shutdown_timeout).unwrap_or(0));
        let mut r = Runner {
            name: String::new(),
            config,
            controller_factory: None,
            subscriber_factory: None,
            broker_factory: None,
            broadcasters_factory: None,
            metrics: Arc::new(Metrics::new(Vec::new(), 0)),
            shutdownables: Vec::new(),
            shutdown_ctx: ShutdownContext::new(timeout),
            events,
            events_rx,
            runtime: server::runtime_handle(),
            node: None,
            node_broker: None,
            pusher_controller: None,
            startup_cleanup: None,
            broadcast_adapters_set: false,
        };
        r.check_and_set_defaults(options)?;
        Ok(r)
    }

    fn check_and_set_defaults(&mut self, options: Vec<RunnerOption>) -> Result<(), CliError> {
        let mut logger = None;
        for opt in options {
            match opt {
                RunnerOption::Name(name) => self.name = name,
                RunnerOption::Logger(sink) => logger = Some(sink),
                RunnerOption::DefaultRpcController => self.set_controller(default_rpc_controller())?,
                RunnerOption::Controller(factory) => self.set_controller(factory)?,
                RunnerOption::Shutdownable(s) => self.shutdownables.push(s),
                RunnerOption::Subscriber(factory) => self.set_subscriber(SubscriberSource::Custom(factory))?,
                RunnerOption::DefaultSubscriber => self.set_subscriber(SubscriberSource::Default)?,
                RunnerOption::Broker(factory) => self.set_broker(BrokerSource::Custom(factory))?,
                RunnerOption::DefaultBroker => self.set_broker(BrokerSource::Default)?,
                RunnerOption::Broadcasters(factory) => {
                    self.broadcasters_factory = Some(BroadcastersSource::Custom(factory));
                }
                RunnerOption::DefaultBroadcaster => self.broadcasters_factory = Some(BroadcastersSource::Default),
            }
        }

        match logger {
            None => {
                crate::metrics::init_logger(&self.config.log.log_format, &self.config.log.log_level)
                    .map_err(|e| decorate(e, "failed to initialize default logger"))?;
                crate::metrics::set_log_attrs(vec![Attr::new("nodeid", LogValue::String(self.config.id.clone()))]);
            }
            Some(LogSink::Discard) => crate::metrics::discard_logs(),
            Some(LogSink::Default) => {}
        }

        let env: EnvMap = std::env::vars_os()
            .map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
            .collect();
        let presets = self.config.presets(&env);
        if !presets.is_empty() {
            crate::metrics::log(
                Level::Info,
                "load presets",
                &[
                    Attr::new("context", LogValue::String("config".into())),
                    Attr::new("presets", LogValue::String(presets.join(","))),
                ],
            );
        }
        self.broadcast_adapters_set = broadcast_adapters_set(&self.config);
        let c = &mut self.config;
        let (redis, nats) = (c.redis.clone(), c.nats.clone());
        c.load_presets(&env).map_err(|e| decorate(e, "failed to load configuration presets"))?;
        refresh_copy(&mut c.redis_broadcast.redis, &redis, &c.redis);
        refresh_copy(&mut c.legacy_redis_broadcast.redis, &redis, &c.redis);
        refresh_copy(&mut c.redis_pubsub.redis, &redis, &c.redis);
        refresh_copy(&mut c.nats_broadcast.nats, &nats, &c.nats);
        refresh_copy(&mut c.nats_pubsub.nats, &nats, &c.nats);

        server::set_defaults(ServerDefaults {
            host: c.server.host.clone(),
            ssl: c.server.ssl.available().then(|| c.server.ssl.clone()),
            max_conn: usize::try_from(c.server.max_conn).unwrap_or(0),
        });

        if self.name.is_empty() {
            return Err(assertion_failed("Name is blank, specify WithName()"));
        }
        if self.controller_factory.is_none() {
            return Err(assertion_failed("Controller is blank, specify WithController()"));
        }
        if self.broker_factory.is_none() {
            return Err(assertion_failed("Broker is blank, specify WithBroker()"));
        }
        if self.subscriber_factory.is_none() {
            return Err(assertion_failed("Subscriber is blank, specify WithSubscriber()"));
        }

        self.metrics = Arc::new(self.init_metrics().map_err(|e| decorate(e, "failed to initialize metrics writer"))?);
        Ok(())
    }

    fn set_controller(&mut self, factory: ControllerFactory) -> Result<(), CliError> {
        if self.controller_factory.is_some() {
            return Err(illegal_argument("Controller has been already assigned"));
        }
        self.controller_factory = Some(factory);
        Ok(())
    }

    fn set_subscriber(&mut self, source: SubscriberSource) -> Result<(), CliError> {
        if self.subscriber_factory.is_some() {
            return Err(illegal_argument("Subscriber has been already assigned"));
        }
        self.subscriber_factory = Some(source);
        Ok(())
    }

    fn set_broker(&mut self, source: BrokerSource) -> Result<(), CliError> {
        if self.broker_factory.is_some() {
            return Err(illegal_argument("Broker has been already assigned"));
        }
        self.broker_factory = Some(source);
        Ok(())
    }

    fn init_metrics(&self) -> Result<Metrics, crate::metrics::MetricsError> {
        let config = crate::metrics::Config::from(&self.config.metrics);
        let m = Metrics::from_config_with_server_host(&config, &self.config.server.host)?;
        if config.statsd.enabled() {
            m.register_writer(Arc::new(StatsdWriter::new(config.statsd.clone(), config.tags.clone())));
        }
        Ok(m)
    }

    /// The runner's metrics (upstream `Instrumenter()`).
    pub fn instrumenter(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// The node, once [`Runner::run_node`] has built it.
    pub fn node(&self) -> Option<Arc<Node>> {
        self.node.clone()
    }

    // --- Run --------------------------------------------------------------------

    /// Starts the server and blocks until it stops: a signal-initiated shutdown
    /// completed (`Ok`) or a component failed (upstream `Run`).
    pub fn run(&mut self) -> Result<(), CliError> {
        let result = self.serve();
        self.clean_up_after(&result);
        result
    }

    /// On a failed start (or a component failure), shuts down what must not be
    /// left half-open; a no-op after a completed shutdown.
    fn clean_up_after(&self, result: &Result<(), CliError>) {
        if result.is_err()
            && let Some(cleanup) = &self.startup_cleanup
        {
            let _ = cleanup.shutdown();
        }
    }

    fn serve(&mut self) -> Result<(), CliError> {
        // Before anything starts (D-066).
        check_pusher_secret(&self.config)?;
        let _guard = self.runtime.enter();
        let procs = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        self.announce_debug_mode();
        log_info(&format!(
            "Starting {} {}{} (pid: {}, open file limit: {}, gomaxprocs: {procs})",
            self.name,
            crate::version::label(),
            mruby_support(),
            std::process::id(),
            open_file_limit()
        ));

        if let Some(path) = &self.config.config_file_path {
            log_info(&format!("Using configuration from file: {}", path.display()));
            if !self.config.unknown_config_keys.is_empty() {
                crate::metrics::log(
                    Level::Warn,
                    &format!(
                        "Unknown keys in configuration file (ignored): {}",
                        self.config.unknown_config_keys.join(", ")
                    ),
                    &[],
                );
            }
        }
        for note in &self.config.startup_notes {
            let (level, message, attrs) = note_record(note);
            crate::metrics::log(level, &message, &attrs);
        }

        let context = || [Attr::new("context", LogValue::String("config".into()))];
        if let Some(warning) = redis_broadcast_warning(&self.config) {
            crate::metrics::log(Level::Warn, &warning, &context());
        }
        if let Some(warning) = live_only_broadcast_warning(&self.config) {
            crate::metrics::log(live_only_level(self.broadcast_adapters_set), &warning, &context());
        }

        if self.config.public_mode {
            crate::metrics::log(Level::Warn, "Server is running in the public mode", &[]);
        }

        if let Some(warning) = redis_tls_warning(&self.config) {
            crate::metrics::log(Level::Warn, warning, &[]);
        }
        if let Some((level, note)) = redis_sentinel_tls_note(&self.config) {
            crate::metrics::log(level, note, &[]);
        }

        let node = self.build_node()?;

        let c = self.config.clone();
        let ws_server = server::for_port(&c.server.port.to_string()).map_err(|e| {
            decorate(e, &format!("failed to initialize WebSocket server at {}:{}", c.server.host, c.server.port))
        })?;
        let metrics_server = self.mount_metrics()?;

        let ws_handler = default_websocket_handler(node.clone(), &c);
        for path in &c.ws.paths {
            ws_server.setup_handler(path, ws_handler.clone());
            log_info(&format!("Handle WebSocket connections at {}{path}", ws_server.address()));
        }

        if c.pusher.enabled() {
            self.mount_pusher(&node, &ws_server)?;
        }

        ws_server.setup_handler(&c.server.health_path, server::health_endpoint());
        log_info(&format!("Handle health requests at {}{}", ws_server.address(), c.server.health_path));

        if c.sse.enabled {
            log_info(&format!("Handle SSE requests at {}{}", ws_server.address(), c.sse.path));
            let extractor: Option<crate::sse::SharedHeadersExtractor> =
                Some(Arc::new(super::session_options::headers_extractor(&c)));
            let metrics: Option<Arc<dyn Instrumenter>> = Some(self.metrics.clone() as Arc<dyn Instrumenter>);
            ws_server.setup_handler(
                &c.sse.path,
                crate::sse::handler(node.clone(), metrics, ws_server.shutdown_token(), extractor, &c.sse),
            );
        }

        if c.ds.enabled {
            let ds_path = format!("{}/*", c.ds.path);
            log_info(&format!("Handle Durable Streams requests at {}{ds_path}", ws_server.address()));
            let extractor: Option<crate::ds::SharedHeadersExtractor> =
                Some(Arc::new(super::session_options::headers_extractor(&c)));
            let metrics: Option<Arc<dyn Instrumenter>> = Some(self.metrics.clone() as Arc<dyn Instrumenter>);
            let broker = self.node_broker.clone().ok_or_else(|| assertion_failed("broker is not initialized"))?;
            // Upstream reuses the runner's streams controller, or builds one from config.
            let streams = Some(Arc::new(StreamsController::for_config(&c.streams)));
            let handler = crate::ds::handler(
                node.clone(),
                broker,
                streams,
                metrics,
                ws_server.shutdown_token(),
                extractor,
                &c.ds,
            )
            .map_err(|e| decorate(CliError(e.to_string()), "failed to initialize Durable Streams handler"))?;
            ws_server.setup_handler(&ds_path, handler);
        }

        self.start_api_server(&node)?;
        self.start_metrics(metrics_server)?;

        ws_server
            .start_and_announce("WebSocket server")
            .map_err(|e| CliError(format!("WebSocket server at {} stopped: {e}", ws_server.address())))?;

        // We MUST first stop the server (stop accepting new connections), then
        // gracefully disconnect active clients.
        let ctx = self.shutdown_ctx.clone();
        let (srv, runtime) = (ws_server.clone(), self.runtime.clone());
        self.shutdownables.insert(
            0,
            Arc::new(ShutdownFn(move || {
                shut_down_server(&runtime, &ctx, |time_left| srv.shutdown_graceful(time_left))
            })),
        );

        if c.app.shutdown_delay > 0 {
            let delay = Duration::from_secs(u64::try_from(c.app.shutdown_delay).unwrap_or(0));
            let ctx = self.shutdown_ctx.clone();
            self.shutdownables.insert(
                0,
                Arc::new(ShutdownFn(move || {
                    let ctx = ctx.get();
                    let until = Instant::now() + delay.min(remaining(&ctx));
                    while Instant::now() < until && !ctx.cancel.is_cancelled() {
                        std::thread::sleep(until.saturating_duration_since(Instant::now()).min(SLEEPER_TICK));
                    }
                    Ok(())
                })),
            );
        }

        super::signals::listen(
            self.shutdownables.clone(),
            self.shutdown_ctx.clone(),
            Duration::from_secs(u64::try_from(c.app.shutdown_timeout).unwrap_or(0)),
            self.events.clone(),
        )?;

        // Wait for an error (or none).
        self.events_rx.recv().unwrap_or(Ok(()))
    }

    /// Builds and starts the node and its components (upstream unexported
    /// `runNode`); the node is then available from [`Runner::node`].
    pub fn run_node(&mut self) -> Result<(), CliError> {
        let result = self.build_node().map(drop);
        self.clean_up_after(&result);
        result
    }

    fn build_node(&mut self) -> Result<Arc<Node>, CliError> {
        let _guard = self.runtime.enter();
        let metrics = self.metrics.clone();
        {
            let (m, runtime) = (metrics.clone(), self.runtime.clone());
            self.shutdownables
                .push(Arc::new(ShutdownFn(move || block_on(&runtime, m.shutdown()).map_err(|e| CliError(e.0)))));
        }

        let controller = self.new_controller(metrics.clone())?;

        let node = Node::new(
            self.config.app.clone(),
            vec![
                NodeOption::Controller(controller.clone()),
                NodeOption::Instrumenter(metrics.clone() as Arc<dyn Instrumenter>),
                NodeOption::Id(self.config.id.clone()),
            ],
        );
        let mut node_shutdownables: Vec<Arc<dyn Shutdownable>> = {
            let (n, ctx) = (node.clone(), self.shutdown_ctx.clone());
            vec![Arc::new(ShutdownFn(move || n.shutdown(&ctx.get()).map_err(|e| CliError(e.0))))]
        };

        let subscriber = self.new_subscriber(&node).map_err(|e| decorate(e, "couldn't configure pub/sub"))?;

        let (runner_broker, node_broker) =
            self.new_broker(&node, &subscriber).map_err(|e| decorate(e, "failed to initialize broker"))?;
        log_info(&runner_broker.announce());
        node.set_broker(node_broker.clone());
        node.set_sessions_ttl(Duration::from_secs(u64::try_from(self.config.broker.sessions_ttl).unwrap_or(0)));
        self.node_broker = Some(node_broker);

        let disconnector: Arc<dyn Disconnector> = if self.config.disconnector_disabled {
            Arc::new(NoopDisconnectQueue)
        } else {
            Arc::new(DisconnectQueue::new(node.clone(), &self.config.disconnect_queue))
        };
        {
            let d = disconnector.clone();
            std::thread::Builder::new()
                .name("cabby-disconnector".into())
                .spawn(move || {
                    let _ = d.run();
                })
                .map_err(|e| decorate(e, "failed to initialize disconnector"))?;
        }
        node.set_disconnector(disconnector);

        if self.config.embedded_nats.enabled {
            let service = Arc::new(crate::enats::Service::new(&self.config.embedded_nats));
            // Upstream's server runs in-process: it cannot die on its own and
            // leave AnyCable running.
            let events = self.events.clone();
            service.on_exit(move |e| {
                let _ = events.send(Err(CliError(e.0)));
            });
            service.start().map_err(|e| decorate(e, "failed to start embedded NATS server"))?;
            log_info(&format!("Embedded NATS server started: {}", self.config.embedded_nats.service_addr));
            self.shutdownables.push(Arc::new(ShutdownFn(move || service.shutdown().map_err(|e| CliError(e.0)))));
        }

        node.start().map_err(|e| decorate(e, "failed to initialize application"))?;

        subscriber.runner.start().map_err(|e| decorate(e, "failed to start subscriber"))?;
        runner_broker.start().map_err(|e| decorate(e, "failed to start broker"))?;

        if let Some(source) = self.broadcasters_factory.take() {
            let handler = Arc::new(NodeHandler(node.clone()));
            let broadcasters = match source {
                BroadcastersSource::Custom(factory) => factory(handler as Arc<dyn BroadcastHandler>, &self.config),
                BroadcastersSource::Default => self.default_broadcasters(handler),
            }
            .map_err(|e| decorate(e, "couldn't configure broadcasters"))?;
            for broadcaster in broadcasters {
                broadcaster.start().map_err(|e| decorate(e, "failed to start broadcaster"))?;
                node_shutdownables.push(broadcaster);
            }
        }

        // Drain broadcasters before closing the broker, and keep pub/sub
        // available until both stop.
        node_shutdownables.push(runner_broker);
        node_shutdownables.push(subscriber.runner);

        controller.start().map_err(|e| decorate(e.message, "failed to initialize RPC controller"))?;

        // Shared services, such as embedded NATS, must outlive the components using them.
        node_shutdownables.append(&mut self.shutdownables);
        self.shutdownables = node_shutdownables;

        self.announce_go_pools();
        self.node = Some(node.clone());
        Ok(node)
    }

    /// Components to shut down, in shutdown order (upstream unexported `shutdownables`).
    pub fn shutdownables(&self) -> Vec<Arc<dyn Shutdownable>> {
        self.shutdownables.clone()
    }

    /// Upstream `Embed`: starts the node without the HTTP endpoints and signal
    /// handling, for embedding the server into another application.
    pub fn embed(mut self) -> Result<Embedded, CliError> {
        self.announce_debug_mode();
        log_info(&format!(
            "Starting embedded {} {}{} (open file limit: {})",
            self.name,
            crate::version::label(),
            mruby_support(),
            open_file_limit()
        ));
        let node = self.build_node()?;
        let metrics = self.metrics.clone();
        let events = self.events.clone();
        self.runtime.spawn(async move {
            if let Err(e) = metrics.run().await {
                let _ = events.send(Err(CliError(format!("!!! Metrics module failed to start !!!\n{e}"))));
            }
        });
        Ok(Embedded { node, runner: self })
    }

    fn announce_debug_mode(&self) {
        if self.config.log.debug {
            crate::metrics::log(Level::Debug, "🔧 🔧 🔧 Debug mode is on 🔧 🔧 🔧", &[]);
        }
    }

    fn announce_go_pools(&self) {
        let pools: Vec<String> = all_pools().iter().map(|p| format!("{}: {}", p.name, p.size)).collect();
        crate::metrics::log(Level::Debug, &format!("Go pools initialized ({})", pools.join(", ")), &[]);
    }

    // --- Components ---------------------------------------------------------------

    /// Upstream `newController`: the controller from the factory, wrapped with
    /// the identifiers (JWT, public) and the channels router.
    fn new_controller(&mut self, metrics: Arc<Metrics>) -> Result<Arc<dyn Controller>, CliError> {
        let factory = self.controller_factory.take().ok_or_else(|| assertion_failed("Controller is blank"))?;
        let mut controller =
            factory(metrics, &self.config).map_err(|e| decorate(e, "!!! Failed to initialize controller !!!"))?;
        let c = &self.config;

        let mut ids: Vec<Arc<dyn Identifier>> = Vec::new();
        if c.jwt.enabled() {
            ids.push(Arc::new(JwtIdentifier::new(&c.jwt)));
            log_info(&format!("JWT authentication is enabled (param: {}, enforced: {})", c.jwt.param, c.jwt.force));
        }
        if c.skip_auth {
            ids.push(Arc::new(PublicIdentifier::new()));
            log_info("connection authentication is disabled");
        }
        if ids.len() > 1 {
            let pipeline: Arc<dyn Identifier> = Arc::new(IdentifierPipeline::new(ids));
            controller = Arc::new(IdentifiableController::new(controller, pipeline));
        } else if let Some(id) = ids.pop() {
            controller = Arc::new(IdentifiableController::new(controller, id));
        }

        let mut router = RouterController::new(Arc::new(NullController));
        let mut routes = Vec::new();
        if !c.streams.pubsub_channel.is_empty() {
            let _ = router.route(&c.streams.pubsub_channel, Arc::new(StreamsController::for_config(&c.streams)));
            routes.push(c.streams.pubsub_channel.clone());
        }
        let turbo_secret = c.streams.turbo_secret();
        if c.streams.turbo && !turbo_secret.is_empty() {
            let _ = router.route("Turbo::StreamsChannel", Arc::new(StreamsController::turbo(&turbo_secret)));
            routes.push("Turbo::StreamsChannel".to_owned());
        }
        let cr_secret = c.streams.cable_ready_secret();
        if c.streams.cable_ready && !cr_secret.is_empty() {
            let _ = router.route("CableReady::Stream", Arc::new(StreamsController::cable_ready(&cr_secret)));
            routes.push("CableReady::Stream".to_owned());
        }
        // Upstream routes the Pusher channel after the node and broker exist; the
        // route is registered now and its controller attached in `run`. It is not
        // part of the "Using channels router" line, as upstream logs that earlier.
        if c.pusher.enabled() {
            let deferred = Arc::new(DeferredController::default());
            let _ = router.route(crate::pusher::CHANNEL_NAME, deferred.clone());
            self.pusher_controller = Some(deferred);
        }
        if !router.empty() {
            router.set_default(controller);
            if !routes.is_empty() {
                log_info(&format!("Using channels router: {}", routes.join(", ")));
            }
            controller = Arc::new(router);
        }

        Ok(controller)
    }

    fn new_subscriber(&mut self, node: &Arc<Node>) -> Result<RunnerSubscriber, CliError> {
        let handler: Arc<dyn SubscriberHandler> = node.clone();
        match self.subscriber_factory.take() {
            Some(SubscriberSource::Custom(factory)) => {
                let runner = factory(handler, &self.config)?;
                Ok(RunnerSubscriber { broadcaster: Arc::new(SubscriberBroadcaster(runner.clone())), runner })
            }
            Some(SubscriberSource::Default) | None => {
                // Upstream `WithDefaultSubscriber`.
                let c = &self.config;
                let inner: Arc<dyn core_pubsub::Subscriber> = match c.pubsub_adapter.as_str() {
                    "" | "none" => Arc::new(core_pubsub::LegacySubscriber::new(handler)),
                    "redis" => Arc::new(
                        crate::pubsub::RedisSubscriber::new(handler, &c.redis_pubsub).map_err(|e| CliError(e.0))?,
                    ),
                    "nats" => Arc::new(
                        crate::pubsub::NatsSubscriber::new(handler, &c.nats_pubsub).map_err(|e| CliError(e.0))?,
                    ),
                    // Cabby only (D-070).
                    "postgres" => Arc::new(
                        crate::postgres::PostgresSubscriber::new(
                            handler,
                            &c.postgres,
                            crate::postgres::Metrics::register_once(&self.metrics),
                        )
                        .map_err(|e| CliError(e.0))?,
                    ),
                    other => return Err(illegal_argument(&format!("Unsupported subscriber adapter: {other}"))),
                };
                let core = Arc::new(CoreSubscriber { inner, events: self.events.clone() });
                Ok(RunnerSubscriber { runner: core.clone(), broadcaster: Arc::new(SubscriberBroadcaster(core)) })
            }
        }
    }

    fn new_broker(&mut self, node: &Arc<Node>, subscriber: &RunnerSubscriber) -> Result<BrokerPair, CliError> {
        match self.broker_factory.take() {
            Some(BrokerSource::Custom(factory)) => {
                let broker = factory(subscriber.runner.clone(), &self.config)?;
                let node_broker = Arc::new(NodeBroker {
                    broker: broker.clone(),
                    legacy: core_broker::Legacy::new(subscriber.broadcaster.clone()),
                });
                Ok((broker, node_broker))
            }
            Some(BrokerSource::Default) | None => {
                if self.config.broker.adapter == "sqlite" {
                    return self.new_sqlite_broker(node, subscriber.broadcaster.clone());
                }
                // Upstream `WithDefaultBroker`.
                let br = subscriber.broadcaster.clone();
                let c = &mut self.config;
                let inner: Arc<dyn core_broker::Broker> = match c.broker.adapter.as_str() {
                    "" | "none" => Arc::new(core_broker::Legacy::new(br)),
                    "memory" => Arc::new(core_broker::Memory::new(
                        Some(br),
                        Some(node.clone() as Arc<dyn core_broker::Presenter>),
                        &c.broker,
                        Arc::new(core_broker::SystemClock),
                    )),
                    "nats" => {
                        // Upstream: JetStream is only needed when NATS is the broker.
                        c.embedded_nats.jetstream = true;
                        let nats = core_broker::Nats::new(Some(br), &c.broker, &c.nats);
                        // Upstream `Start(r.errChan)`: a JetStream that never
                        // becomes available stops the server.
                        nats.set_done(forward_errors_verbatim::<core_broker::BrokerError>(&self.events));
                        Arc::new(nats)
                    }
                    // Cabby only (D-069).
                    "redis" => Arc::new(
                        crate::broker::RedisBroker::new(
                            Some(br),
                            &c.broker,
                            &c.redis,
                            &c.redis_broker,
                            Some(self.metrics.clone() as Arc<dyn Instrumenter>),
                        )
                        .map_err(|e| CliError(e.message))?,
                    ),
                    // Cabby only (D-071).
                    "postgres" => Arc::new(
                        crate::postgres::PostgresBroker::new(
                            Some(br),
                            &c.broker,
                            &c.postgres,
                            crate::postgres::Metrics::register_once(&self.metrics),
                        )
                        .map_err(|e| CliError(e.message))?,
                    ),
                    other => return Err(illegal_argument(&format!("Unsupported broker adapter: {other}"))),
                };
                Ok((Arc::new(CoreBroker(inner.clone())), inner))
            }
        }
    }

    /// `--broker=sqlite` (Cabby only, D-065): the memory broker with a
    /// write-behind SQLite copy, loaded before the server accepts connections.
    fn new_sqlite_broker(
        &mut self,
        node: &Arc<Node>,
        broadcaster: Arc<dyn core_broker::Broadcaster>,
    ) -> Result<BrokerPair, CliError> {
        let options = super::sqlite::sqlite_options(&self.config)?;
        let observer = Arc::new(super::sqlite::MetricsObserver::new(self.metrics.clone() as Arc<dyn Instrumenter>));
        let broker = Arc::new(
            core_broker::SqliteBroker::open(
                Some(broadcaster),
                Some(node.clone() as Arc<dyn core_broker::Presenter>),
                &self.config.broker,
                &options,
                Arc::new(core_broker::SystemClock),
                observer,
            )
            .map_err(|e| CliError(e.message))?,
        );
        let runner =
            Arc::new(super::sqlite::SqliteRunnerBroker { broker: broker.clone(), ctx: self.shutdown_ctx.clone() });
        self.startup_cleanup = Some(runner.clone());
        let runner: Arc<dyn Broker> = runner;
        let node_broker: Arc<dyn core_broker::Broker> = broker;
        Ok((runner, node_broker))
    }

    /// Upstream `WithDefaultBroadcaster`: one broadcaster per configured adapter.
    fn default_broadcasters(&self, handler: Arc<NodeHandler>) -> Result<Vec<Arc<dyn Broadcaster>>, CliError> {
        let c = &self.config;
        let h: Arc<dyn cabby_core::broadcast::Handler> = handler;
        let mut out: Vec<Arc<dyn Broadcaster>> = Vec::new();
        for adapter in &c.broadcast_adapters {
            let inner: Arc<dyn cabby_core::broadcast::Broadcaster> = match adapter.as_str() {
                "http" => Arc::new(crate::broadcast::HttpBroadcaster::new(h.clone(), &c.http_broadcast)),
                "redis" => {
                    Arc::new(crate::broadcast::LegacyRedisBroadcaster::new(h.clone(), &c.legacy_redis_broadcast))
                }
                "redisx" => Arc::new(crate::broadcast::RedisBroadcaster::new(h.clone(), &c.redis_broadcast)),
                "nats" => Arc::new(crate::broadcast::LegacyNatsBroadcaster::new(h.clone(), &c.nats_broadcast)),
                // Cabby only (D-070).
                "postgres" => Arc::new(
                    crate::postgres::PostgresBroadcaster::new(
                        h.clone(),
                        &c.postgres,
                        crate::postgres::Metrics::register_once(&self.metrics),
                    )
                    .map_err(|e| CliError(e.0))?,
                ),
                other => return Err(illegal_argument(&format!("Unsupported broadcast adapter: {other}"))),
            };
            out.push(Arc::new(CoreBroadcaster { inner, events: self.events.clone() }));
        }
        Ok(out)
    }

    /// Upstream: the Pusher WebSocket endpoint, the `$pusher` channel controller
    /// and the Pusher REST API (on the main port, or its own when `api_port` is set).
    fn mount_pusher(&mut self, node: &Arc<Node>, ws_server: &server::HttpServer) -> Result<(), CliError> {
        let c = self.config.clone();
        let broker = self.node_broker.clone().ok_or_else(|| assertion_failed("broker is not initialized"))?;

        // The `$pusher` channel only accepts subscriptions from the Pusher endpoint.
        node.reserve_pusher_channel();

        let ws_path = c.pusher.websocket_path();
        let extractor: Option<crate::ws::SharedHeadersExtractor> =
            Some(Arc::new(super::session_options::headers_extractor(&c)));
        ws_server.setup_handler(
            &ws_path,
            crate::pusher::websocket_handler(node.clone(), &c.pusher, &c.ws, extractor, c.broker.presence_ttl),
        );
        if let Some(deferred) = &self.pusher_controller {
            deferred.attach(Arc::new(crate::pusher::Controller::new(Some(broker.clone()), c.pusher.clone())));
        }
        log_info(&format!("Handle Pusher WebSocket connections at {}{ws_path}", ws_server.address()));

        let handler: Arc<dyn cabby_core::broadcast::Handler> =
            Arc::new(crate::pusher::NodeBroadcastHandler(node.clone()));
        if c.pusher.api_port == 0 {
            let api_path = c.pusher.api_path();
            let rest = Arc::new(crate::pusher::RestApi::new(handler, broker, c.pusher.clone()));
            ws_server.setup_handler(&api_path, rest.into_handler());
            log_info(&format!("Handle Pusher API requests at {}{api_path}", ws_server.address()));
        } else {
            let api = Arc::new(crate::pusher::Server::new(handler, broker, c.pusher.clone()));
            api.start().map_err(|e| {
                decorate(
                    CliError(e.to_string()),
                    &format!("failed to initialize Pusher API server at port {}", c.pusher.api_port),
                )
            })?;
            api.run().map_err(|e| CliError(e.to_string()))?;
            let (srv, runtime, ctx) = (api.clone(), self.runtime.clone(), self.shutdown_ctx.clone());
            self.shutdownables.push(Arc::new(ShutdownFn(move || {
                shut_down_server(&runtime, &ctx, |time_left| srv.shutdown(time_left))
            })));
        }
        Ok(())
    }

    /// Upstream `startAPIServer`.
    fn start_api_server(&mut self, node: &Arc<Node>) -> Result<(), CliError> {
        let c = &mut self.config;
        let enabled = c.public_mode || c.api.port != 0 || !c.api.secret.is_empty() || !c.secret.is_empty();
        if !enabled {
            return Ok(());
        }
        if c.api.secret.is_empty() && !c.secret.is_empty() {
            c.api.secret_base = c.secret.clone();
        }
        if c.api.port == 0 {
            c.api.port = u16::try_from(c.server.port).unwrap_or(0);
        }

        let broker = self.node_broker.clone().unwrap_or_else(|| node.broker());
        let handler = Arc::new(NodeHandler(node.clone()));
        let api = Arc::new(
            crate::api::Server::new(&mut c.api, broker, handler)
                .map_err(|e| decorate(e, "failed to initialize API server"))?,
        );
        let port = c.api.port.to_string();
        let srv = if !c.api.host.is_empty() && c.api.host != c.server.host {
            let ssl = c.server.ssl.available().then(|| c.server.ssl.clone());
            HttpServer::new(&c.api.host, &port, ssl.as_ref(), usize::try_from(c.server.max_conn).unwrap_or(0))
        } else {
            server::for_port(&port)
        }
        .map_err(|e| decorate(format!("failed to initialize API server: {e}"), "failed to start API server"))?;
        api.start().map_err(|e| decorate(e, "failed to start API server"))?;
        srv.setup_handler(&format!("{}/*", c.api.path), server::api_handler(api.clone(), c.api.max_body_size));

        let mut verified_via =
            if api.authenticator().is_enabled() { "authorization required" } else { "no authorization" }.to_owned();
        if c.api.add_cors_headers {
            verified_via.push_str(", CORS enabled");
        }
        crate::metrics::log(
            Level::Info,
            &format!("Handle API requests at {}{} ({verified_via})", srv.address(), c.api.path),
            &[Attr::new("context", LogValue::String("api".into()))],
        );
        srv.start_and_announce("API server")
            .map_err(|e| CliError(format!("API HTTP server at {} stopped: {e}", srv.address())))?;

        let (ctx, runtime, a, s) = (self.shutdown_ctx.clone(), self.runtime.clone(), api.clone(), srv.clone());
        self.shutdownables.push(Arc::new(ShutdownFn(move || {
            let _ = a.shutdown();
            shut_down_server(&runtime, &ctx, |time_left| s.shutdown_graceful(time_left))
        })));

        if !api.authenticator().is_enabled() {
            log_info("API server is running without authentication");
        }
        Ok(())
    }

    /// Mounts the Prometheus endpoint on the server for its port, unless a
    /// dedicated metrics server (run by `Metrics::run`) serves it. This happens
    /// before any other route, as upstream mounts it in `NewRunner`, so a route
    /// mounted later at the same path (health, WebSocket, SSE, API) replaces it.
    fn mount_metrics(&self) -> Result<Option<(Arc<HttpServer>, String)>, CliError> {
        let Some(endpoint) = self.metrics.http_endpoint().filter(|e| e.dedicated_host.is_none()) else {
            return Ok(None);
        };
        let srv = server::for_port(&endpoint.port.to_string()).map_err(|e| CliError(e.0))?;
        srv.setup_handler(&endpoint.path, server::metrics_handler(self.metrics.clone()));
        Ok(Some((srv, endpoint.path.clone())))
    }

    /// Starts the server of the Prometheus endpoint mounted by
    /// [`Runner::mount_metrics`], if any, and the metrics task.
    fn start_metrics(&mut self, mounted: Option<(Arc<HttpServer>, String)>) -> Result<(), CliError> {
        if let Some((srv, path)) = mounted {
            crate::metrics::log(
                Level::Info,
                &format!("Serve metrics at {}{path}", srv.address()),
                &[Attr::new("context", LogValue::String("metrics".into()))],
            );
            srv.start_and_announce("Metrics server")
                .map_err(|e| CliError(format!("metrics HTTP server at {} stopped: {e}", srv.address())))?;
        }
        let metrics = self.metrics.clone();
        let events = self.events.clone();
        self.runtime.spawn(async move {
            if let Err(e) = metrics.run().await {
                let _ = events.send(Err(CliError(e.0)));
            }
        });
        Ok(())
    }
}

/// Polling step of the shutdown delay (so a second signal interrupts it).
const SLEEPER_TICK: Duration = Duration::from_millis(50);

/// The default controller (upstream `WithDefaultRPCController`): the RPC
/// controller, or a null one with `rpc.implementation = "none"`.
fn default_rpc_controller() -> ControllerFactory {
    Box::new(|metrics: Arc<Metrics>, c: &Config| {
        if c.rpc.implementation == "none" {
            return Ok(Arc::new(NullController) as Arc<dyn Controller>);
        }
        let controller = crate::rpc::Controller::new(metrics as Arc<dyn Instrumenter>, c.rpc.clone())
            .map_err(|e| CliError(e.to_string()))?;
        Ok(Arc::new(RpcController::new(controller, server::runtime_handle())) as Arc<dyn Controller>)
    })
}

/// A warning when connections to Redis use TLS without verification, or do not
/// use TLS although a URL asks for it (D-081).
fn redis_tls_warning(c: &Config) -> Option<&'static str> {
    if c.redis.tls_verification_skipped() {
        Some("Redis server TLS certificate is not verified; set --redis_tls_verify=true to enable verification")
    } else if !c.redis.tls_request_unused() {
        None
    } else if c.redis.is_sentinel() {
        Some("Redis connections do not use TLS; use rediss:// in the first of --redis_sentinels to enable it")
    } else {
        Some("Redis connections do not use TLS; use rediss:// in the first URL of --redis_url to enable it")
    }
}

/// An info record when the master is reached over TLS (the Sentinels' scheme
/// asks for it) and the Sentinel nodes are not, because `--redis_sentinel_tls`
/// is off (D-115). Cabby 0.4.2 and earlier used TLS for both; AnyCable, for the
/// master only. That is a valid configuration, so the record is not a warning.
fn redis_sentinel_tls_note(c: &Config) -> Option<(Level, &'static str)> {
    if !c.redis.sentinel_tls_off_for_tls_master() {
        return None;
    }
    let note = if c.redis.sentinels.is_empty() {
        "Redis Sentinel connections do not use TLS; rediss:// in --redis_url applies to the master only (set --redis_sentinel_tls=true to use TLS with the Sentinel nodes)"
    } else {
        "Redis Sentinel connections do not use TLS; rediss:// in --redis_sentinels applies to the master only (set --redis_sentinel_tls=true to use TLS with the Sentinel nodes)"
    };
    Some((Level::Info, note))
}

/// A warning when a Redis URL is configured (after presets) but no broadcast
/// adapter reads from Redis: the release image defaults to HTTP-only
/// broadcasts, so an AnyCable set-up that broadcasts through Redis would
/// silently receive nothing. With the Redis broker (D-069), Redis is used
/// whatever the URL, and the warning says that the broker does not read
/// broadcasts.
fn redis_broadcast_warning(c: &Config) -> Option<String> {
    let redis_broker = c.broker.adapter == "redis";
    let redis_configured = redis_broker || c.redis.url != Config::new().redis.url;
    let redis_broadcasts = c.broadcast_adapters.iter().any(|a| a == "redis" || a == "redisx");
    if !redis_configured || redis_broadcasts {
        return None;
    }
    let adapters = if c.broadcast_adapters == ["http"] { "HTTP".to_owned() } else { c.broadcast_adapters.join(",") };
    if redis_broker {
        return Some(format!(
            "broadcasts are {adapters} only; the Redis broker does not read broadcasts from Redis (set CABBY_BROADCAST_ADAPTER to add redisx)"
        ));
    }
    Some(format!(
        "Redis is configured but broadcasts are {adapters} only; set CABBY_BROADCAST_ADAPTER to add redis or redisx"
    ))
}

/// A warning when a broker that keeps history runs with broadcast adapters
/// whose messages skip it (D-069): the legacy `redis` (PUBLISH) and `nats`
/// adapters deliver to the node directly, as upstream does, so their
/// messages are delivered live but not stored in history.
fn live_only_broadcast_warning(c: &Config) -> Option<String> {
    if !matches!(c.broker.adapter.as_str(), "memory" | "sqlite" | "nats" | "redis" | "postgres") {
        return None;
    }
    let live: Vec<&str> =
        ["redis", "nats"].into_iter().filter(|a| c.broadcast_adapters.iter().any(|b| b == a)).collect();
    let adapters = match live.as_slice() {
        [] => return None,
        [one] => format!("the {one} adapter"),
        many => format!("the {} adapters", many.join(" and ")),
    };
    Some(format!(
        "broadcasts through {adapters} are delivered live and not stored in history; use http or redisx to keep history"
    ))
}

/// Whether the broadcast adapters differ from the default list (checked
/// before presets apply).
fn broadcast_adapters_set(c: &Config) -> bool {
    c.broadcast_adapters != Config::new().broadcast_adapters
}

/// The level of the live-only warning: a warning for adapters the user
/// chose, debug for the default list or a preset's.
fn live_only_level(adapters_set: bool) -> Level {
    if adapters_set { Level::Warn } else { Level::Debug }
}

/// D-086: the warning for the application secret given as `none`.
const NONE_SECRET_WARNING: &str = "the application secret is \"none\": treated as not set, and no other secret is \
derived from it. Remove the setting to silence this warning";

/// The log record of a start-up note (D-060, D-061, D-086). An environment conflict
/// names both variables and never their values, which may be secrets.
fn note_record(note: &StartupNote) -> (Level, String, Vec<Attr>) {
    let context = Attr::new("context", LogValue::String("config".into()));
    match note {
        StartupNote::IgnoredConfigFile { path, used } => (
            Level::Debug,
            "configuration file ignored".to_owned(),
            vec![
                Attr::new("path", LogValue::String(path.clone())),
                Attr::new("used", LogValue::String(used.clone())),
                context,
            ],
        ),
        StartupNote::EnvConflict { cabby, anycable } => {
            (Level::Warn, format!("both {cabby} and {anycable} are set; using {cabby}"), vec![context])
        }
        StartupNote::NoneSecret => (Level::Warn, NONE_SECRET_WARNING.to_owned(), vec![context]),
        StartupNote::NoneSecretBase { section } => (
            Level::Warn,
            format!(
                "SecretBase is \"none\" in the [{section}] section of the configuration file: treated as not set. \
Remove the key to silence this warning"
            ),
            vec![context],
        ),
    }
}

/// Upstream `initMRuby`: the mruby version suffix of the start line, e.g.
/// ` (with mruby 1.2.0 (2015-11-17))`; empty without mruby support.
fn mruby_support() -> String {
    if !crate::mrb::supported() {
        return String::new();
    }
    match crate::mrb::version() {
        Ok(v) => format!(" (with {v})"),
        Err(e) => {
            crate::metrics::log(Level::Error, &format!("mruby failed to initialize: {e}"), &[]);
            String::new()
        }
    }
}

/// Upstream `utils.OpenFileLimit`: the soft limit of open files (from
/// `/proc/self/limits`), or `unknown`.
fn open_file_limit() -> String {
    std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|limits| {
            limits
                .lines()
                .find(|l| l.starts_with("Max open files"))
                .and_then(|l| l.trim_start_matches("Max open files").split_whitespace().next().map(str::to_owned))
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

/// A started node without its HTTP server (upstream `cli.Embedded`).
pub struct Embedded {
    node: Arc<Node>,
    runner: Runner,
}

impl std::fmt::Debug for Embedded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Embedded").field("node", &self.node).finish_non_exhaustive()
    }
}

impl Embedded {
    /// The node.
    pub fn node(&self) -> Arc<Node> {
        self.node.clone()
    }

    /// The WebSocket endpoint, to mount on the embedding application's server
    /// (upstream `WebSocketHandler`).
    pub fn websocket_handler(&self) -> crate::ws::WebsocketHandler {
        default_websocket_handler(self.node.clone(), &self.runner.config)
    }

    /// The HTTP broadcast endpoint (upstream `HTTPBroadcastHandler`).
    pub fn http_broadcast_handler(&self) -> Result<impl server::Handler, CliError> {
        let broadcaster = crate::broadcast::HttpBroadcaster::new(
            Arc::new(NodeHandler(self.node.clone())),
            &self.runner.config.http_broadcast,
        );
        broadcaster.prepare().map_err(|e| CliError(e.0))?;
        Ok(broadcaster.http_handler())
    }

    /// Stops the node gracefully: every shutdownable in order, within `ctx`,
    /// stopping at the first error (upstream `Shutdown(ctx)`).
    pub fn shutdown(&self, ctx: &Context) -> Result<(), CliError> {
        self.runner.shutdown_ctx.set(ctx.clone());
        for shutdownable in &self.runner.shutdownables {
            shutdownable.shutdown()?;
        }
        Ok(())
    }
}

/// Pusher is mounted only with a secret, its own or the `--secret` fallback (D-066).
fn check_pusher_secret(c: &Config) -> Result<(), CliError> {
    if c.pusher.enabled() && c.pusher.secret.is_empty() {
        return Err(illegal_argument("Pusher requires a secret (--pusher_secret or --secret)"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Read as _, Write as _};

    use super::*;
    use crate::server::tests::support;

    fn cli_config(args: &[&str]) -> Config {
        let (c, _) = super::super::options::new_config_from_cli(args, &EnvMap::new(), Vec::new()).expect("valid flags");
        c
    }

    /// Pins D-066: Pusher without a secret stops the server at startup, before
    /// any component starts.
    #[test]
    fn deviation_d066_pusher_without_a_secret_stops_the_server() {
        let c = cli_config(&["cabby", "--pusher_app_id=app", "--pusher_app_key=key"]);
        assert!(c.pusher.enabled() && c.pusher.secret.is_empty());
        let mut runner = Runner::new(c, super::super::default_runner_options()).expect("a runner");

        let err = runner.run().expect_err("the server must not start");

        assert!(err.0.contains("Pusher requires a secret (--pusher_secret or --secret)"), "{}", err.0);
        assert!(runner.node.is_none(), "nothing was started");
        for args in [
            &["cabby", "--pusher_app_id=app", "--pusher_app_key=key", "--secret=s3cr3t"][..],
            &["cabby", "--pusher_app_id=app", "--pusher_app_key=key", "--pusher_secret=s3cr3t"],
            &["cabby"],
        ] {
            assert!(check_pusher_secret(&cli_config(args)).is_ok(), "{args:?}");
        }
    }

    /// Pins D-066: with `--secret` alone, Pusher is mounted with that secret.
    #[test]
    fn deviation_d066_the_secret_fallback_mounts_pusher() {
        let c = cli_config(&["cabby", "--pusher_app_id=app", "--pusher_app_key=key", "--secret=s3cr3t"]);
        assert_eq!("s3cr3t", c.pusher.secret);
        let mut runner = Runner::new(c, super::super::default_runner_options()).expect("a runner");
        let node = Node::new(crate::node::Config::new(), Vec::new());
        let broker: Arc<dyn core_broker::Broker> = Arc::new(core_broker::Memory::new(
            None,
            Some(node.clone()),
            &core_broker::Config::new(),
            Arc::new(core_broker::SystemClock),
        ));
        broker.start().expect("broker");
        node.set_broker(broker.clone());
        node.set_controller(Arc::new(NullController));
        node.start().expect("node");
        runner.node_broker = Some(broker);
        let server = HttpServer::new("127.0.0.1", "0", None, 0).expect("server");

        runner.mount_pusher(&node, &server).expect("Pusher is mounted");

        let addr = support::start(&server);
        let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let path = "/apps/app/channels/presence-room/users";
        let params = format!("auth_key=key&auth_timestamp={ts}&auth_version=1.0");
        let signature = crate::pusher::Verifier::new("key", "s3cr3t").sign(&format!("GET\n{path}\n{params}"));
        let mut stream = support::connect(addr);
        write!(stream, "GET {path}?{params}&auth_signature={signature} HTTP/1.1\r\nHost: {addr}\r\n\r\n").unwrap();
        let response = support::read_response(&mut stream);
        assert_eq!(200, response.status, "{}", String::from_utf8_lossy(&response.body));

        let mut stream = support::connect(addr);
        write!(stream, "GET {path}?{params}&auth_signature=00 HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut text = String::new();
        let _ = stream.read_to_string(&mut text);
        assert!(text.starts_with("HTTP/1.1 401"), "{text}");
    }

    /// Pins D-060: an ignored configuration file is logged at debug level.
    #[test]
    fn startup_notes_are_logged() {
        let (level, message, attrs) = note_record(&StartupNote::IgnoredConfigFile {
            path: "./anycable.toml".into(),
            used: "/etc/cabby/cabby.toml".into(),
        });
        assert_eq!((level, message.as_str()), (Level::Debug, "configuration file ignored"));
        assert_eq!(
            attrs,
            vec![
                Attr::new("path", LogValue::String("./anycable.toml".into())),
                Attr::new("used", LogValue::String("/etc/cabby/cabby.toml".into())),
                Attr::new("context", LogValue::String("config".into())),
            ]
        );
    }

    /// Pins D-061: a conflict names both variables and never their values.
    #[test]
    fn env_conflicts_are_warnings_without_values() {
        let (level, message, attrs) =
            note_record(&StartupNote::EnvConflict { cabby: "CABBY_SECRET".into(), anycable: "ANYCABLE_SECRET".into() });
        assert_eq!(level, Level::Warn);
        assert_eq!(message, "both CABBY_SECRET and ANYCABLE_SECRET are set; using CABBY_SECRET");
        assert_eq!(attrs, vec![Attr::new("context", LogValue::String("config".into()))]);
    }

    /// Pins D-086: an application secret or a secret base given as `none` is
    /// reported as a warning.
    #[test]
    fn deviation_d086_none_secrets_are_warnings() {
        let context = vec![Attr::new("context", LogValue::String("config".into()))];
        let (level, message, attrs) = note_record(&StartupNote::NoneSecret);
        assert_eq!((level, attrs), (Level::Warn, context.clone()));
        assert_eq!(
            message,
            "the application secret is \"none\": treated as not set, and no other secret is derived from it. \
Remove the setting to silence this warning"
        );
        let (level, message, attrs) = note_record(&StartupNote::NoneSecretBase { section: "rpc" });
        assert_eq!((level, attrs), (Level::Warn, context));
        assert_eq!(
            message,
            "SecretBase is \"none\" in the [rpc] section of the configuration file: treated as not set. Remove the \
key to silence this warning"
        );
    }

    /// Pins D-081: TLS without verification, and TLS asked for but not used, are
    /// reported.
    #[test]
    fn deviation_d081_redis_tls_settings_are_reported() {
        let warning = |url: &str, sentinels: &str| {
            let mut c = Config::new();
            c.redis.url = url.to_owned();
            c.redis.sentinels = sentinels.to_owned();
            redis_tls_warning(&c).unwrap_or_default()
        };
        assert_eq!("", warning("redis://redis", ""));
        assert!(warning("valkeys://redis", "").contains("set --redis_tls_verify=true"));
        assert!(warning("REDISS://redis", "").contains("set --redis_tls_verify=true"));
        assert!(warning("rediss://master", "redis://sentinel").contains("first of --redis_sentinels"));
        assert!(warning("redis://a,rediss://b", "").contains("first URL of --redis_url"));
    }

    /// Pins D-115: a master over TLS with plain Sentinel connections is
    /// reported once at startup, at info level, naming `--redis_sentinel_tls`.
    #[test]
    fn deviation_d115_master_only_tls_is_reported() {
        let note = |url: &str, sentinels: &str, sentinel_tls: bool, verify: bool| {
            let mut c = Config::new();
            c.redis.url = url.to_owned();
            c.redis.sentinels = sentinels.to_owned();
            c.redis.sentinel_tls = sentinel_tls;
            c.redis.tls_verify = verify;
            redis_sentinel_tls_note(&c)
        };
        let listed = "Redis Sentinel connections do not use TLS; rediss:// in --redis_sentinels applies to the master only (set --redis_sentinel_tls=true to use TLS with the Sentinel nodes)";
        let implicit = "Redis Sentinel connections do not use TLS; rediss:// in --redis_url applies to the master only (set --redis_sentinel_tls=true to use TLS with the Sentinel nodes)";
        for verify in [false, true] {
            assert_eq!(
                Some((Level::Info, listed)),
                note("redis://mymaster", "rediss://s1:26379,rediss://s2:26379", false, verify)
            );
            assert_eq!(Some((Level::Info, listed)), note("redis://mymaster", "valkeys://s1:26379", false, verify));
            assert_eq!(Some((Level::Info, implicit)), note("rediss://s1:26379?master_set=mymaster", "", false, verify));
            assert_eq!(None, note("redis://mymaster", "rediss://s1:26379", true, verify));
            assert_eq!(None, note("rediss://s1:26379?master_set=mymaster", "", true, verify));
            assert_eq!(None, note("redis://mymaster", "redis://s1:26379", false, verify));
            assert_eq!(None, note("redis://mymaster", "redis://s1:26379", true, verify));
            assert_eq!(None, note("rediss://mymaster", "redis://s1:26379", false, verify));
            assert_eq!(None, note("rediss://redis:6379", "", false, verify));
        }
    }

    /// A Redis URL without a Redis broadcast adapter is reported once: the
    /// release image defaults to HTTP-only broadcasts.
    #[test]
    fn redis_without_a_redis_broadcast_adapter_is_reported() {
        let config = |url: &str, adapters: &[&str]| {
            let mut c = Config::new();
            c.redis.url = url.to_owned();
            c.broadcast_adapters = adapters.iter().map(|a| (*a).to_owned()).collect();
            c
        };
        let default_url = Config::new().redis.url;
        assert_eq!(redis_broadcast_warning(&config(&default_url, &["http"])), None);
        assert_eq!(redis_broadcast_warning(&config("redis://redis:6379", &["http", "redis"])), None);
        assert_eq!(redis_broadcast_warning(&config("redis://redis:6379", &["redisx"])), None);
        let mut redis_broker = config(&default_url, &["http"]);
        redis_broker.broker.adapter = "redis".to_owned();
        assert_eq!(
            redis_broadcast_warning(&redis_broker).as_deref(),
            Some(
                "broadcasts are HTTP only; the Redis broker does not read broadcasts from Redis (set CABBY_BROADCAST_ADAPTER to add redisx)"
            ),
            "Pins D-069"
        );
        redis_broker.broadcast_adapters = vec!["http".to_owned(), "redisx".to_owned()];
        assert_eq!(redis_broadcast_warning(&redis_broker), None);
        assert_eq!(
            redis_broadcast_warning(&config("redis://redis:6379", &["http"])).as_deref(),
            Some(
                "Redis is configured but broadcasts are HTTP only; set CABBY_BROADCAST_ADAPTER to add redis or redisx"
            )
        );
        assert_eq!(
            redis_broadcast_warning(&config("redis://redis:6379", &["http", "nats"])).as_deref(),
            Some(
                "Redis is configured but broadcasts are http,nats only; set CABBY_BROADCAST_ADAPTER to add redis or redisx"
            )
        );
    }

    /// Pins D-069: with a broker that keeps history, broadcasts through the
    /// legacy `redis` and `nats` adapters are delivered live only (as
    /// upstream), which the server says at startup.
    #[test]
    fn live_only_broadcast_adapters_with_a_history_broker_are_reported() {
        let config = |broker: &str, adapters: &[&str]| {
            let mut c = Config::new();
            c.broker.adapter = broker.to_owned();
            c.broadcast_adapters = adapters.iter().map(|a| (*a).to_owned()).collect();
            c
        };
        assert_eq!(live_only_broadcast_warning(&config("", &["http", "redis"])), None, "no history");
        assert_eq!(live_only_broadcast_warning(&config("none", &["redis"])), None);
        assert_eq!(live_only_broadcast_warning(&config("memory", &["http", "redisx"])), None);
        assert_eq!(
            live_only_broadcast_warning(&config("memory", &["http", "redis"])).as_deref(),
            Some(
                "broadcasts through the redis adapter are delivered live and not stored in history; use http or redisx to keep history"
            )
        );
        for broker in ["sqlite", "nats", "redis", "postgres"] {
            assert!(live_only_broadcast_warning(&config(broker, &["redis"])).is_some(), "{broker}");
        }
        // The Postgres outbox goes through the broker (D-070).
        assert_eq!(live_only_broadcast_warning(&config("postgres", &["http", "postgres"])), None);
        assert_eq!(live_only_broadcast_warning(&config("memory", &["postgres"])), None);
        assert_eq!(
            live_only_broadcast_warning(&config("nats", &["http", "redis", "nats"])).as_deref(),
            Some(
                "broadcasts through the redis and nats adapters are delivered live and not stored in history; use http or redisx to keep history"
            )
        );
    }

    /// Pins D-069: the live-only warning is for broadcast adapters the user
    /// chose; the default list and a preset's list log it at debug level.
    #[test]
    fn live_only_warnings_are_for_chosen_broadcast_adapters() {
        let mut c = Config::new();
        assert!(!broadcast_adapters_set(&c), "the default list");
        c.broadcast_adapters = vec!["http".to_owned(), "redis".to_owned(), "nats".to_owned()];
        assert!(broadcast_adapters_set(&c));
        assert_eq!(Level::Warn, live_only_level(true));
        assert_eq!(Level::Debug, live_only_level(false));
    }
}
