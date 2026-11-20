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
    ShutdownContext, ShutdownFn, SubscriberBroadcaster, block_on, forward_errors_verbatim, remaining,
};
use super::session_options::default_websocket_handler;
use super::{
    BroadcastHandler, Broadcaster, BroadcastersFactory, Broker, BrokerFactory, CliError, ControllerFactory, LogSink,
    RunnerOption, Shutdownable, Subscriber, SubscriberFactory, SubscriberHandler,
};
use crate::config::{Config, EnvMap};
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
    telemetry: Option<crate::telemetry::Config>,
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
            telemetry: None,
            metrics: Arc::new(Metrics::new(Vec::new(), 0)),
            shutdownables: Vec::new(),
            shutdown_ctx: ShutdownContext::new(timeout),
            events,
            events_rx,
            runtime: server::runtime_handle(),
            node: None,
            node_broker: None,
            pusher_controller: None,
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
                RunnerOption::Telemetry(props) => self.set_telemetry(props)?,
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

    /// Upstream `WithTelemetry(props...)`.
    fn set_telemetry(&mut self, props: Vec<String>) -> Result<(), CliError> {
        let env = crate::telemetry::ProcessEnv;
        if crate::telemetry::EnvSource::var(&env, "ANYCABLE_DISABLE_TELEMETRY").as_deref() == Some("true") {
            self.telemetry = None;
            return Ok(());
        }
        let mut config = crate::telemetry::Config::new(&env);
        if let Some(url) = crate::telemetry::EnvSource::var(&env, "ANYCABLE_TELEMETRY_URL").filter(|u| !u.is_empty()) {
            config.endpoint = url;
        }
        if !props.len().is_multiple_of(2) {
            return Err(illegal_argument("telemetry properties should be passed as pairs of key and value"));
        }
        for pair in props.chunks(2) {
            config.custom_props.insert(pair[0].clone(), pair[1].clone());
        }
        self.telemetry = Some(config);
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
        let _guard = self.runtime.enter();
        let procs = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        self.announce_debug_mode();
        log_info(&format!(
            "Starting {} {}{} (pid: {}, open file limit: {}, gomaxprocs: {procs})",
            self.name,
            version_label(),
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

        if self.config.public_mode {
            crate::metrics::log(Level::Warn, "Server is running in the public mode", &[]);
        }

        if self.config.redis.tls_verification_skipped() {
            crate::metrics::log(
                Level::Warn,
                "Redis server TLS certificate is not verified; set --redis_tls_verify=true to enable verification",
                &[],
            );
        }

        let node = self.build_node()?;

        let c = self.config.clone();
        let ws_server = server::for_port(&c.server.port.to_string()).map_err(|e| {
            decorate(e, &format!("failed to initialize WebSocket server at {}:{}", c.server.host, c.server.port))
        })?;

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
        self.start_metrics()?;

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
                block_on(&runtime, srv.shutdown_graceful(remaining(&ctx.get()))).map_err(|e| CliError(e.0))
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
        self.build_node().map(drop)
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

        if let Some(tconfig) = &self.telemetry {
            let tracker = Arc::new(crate::telemetry::Tracker::new(
                metrics.clone(),
                &crate::telemetry::AppConfig::from_config(&self.config),
                tconfig,
                Arc::new(crate::telemetry::ProcessEnv),
                std::env::args().collect(),
            ));
            if tracker.enabled() {
                crate::metrics::log(
                    Level::Info,
                    &tracker.announce(),
                    &[Attr::new("context", LogValue::String("telemetry".into()))],
                );
                let collector = tracker.clone();
                self.runtime.spawn(async move { collector.collect().await });
                let runtime = self.runtime.clone();
                self.shutdownables
                    .push(Arc::new(ShutdownFn(move || block_on(&runtime, tracker.shutdown()).map_err(CliError))));
            }
        }

        let subscriber = self.new_subscriber(&node).map_err(|e| decorate(e, "couldn't configure pub/sub"))?;

        let (runner_broker, node_broker) =
            self.new_broker(&node, &subscriber).map_err(|e| decorate(e, "failed to initialize broker"))?;
        log_info(&runner_broker.announce());
        node.set_broker(node_broker.clone());
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
            version_label(),
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
                    other => return Err(illegal_argument(&format!("Unsupported broker adapter: {other}"))),
                };
                Ok((Arc::new(CoreBroker(inner.clone())), inner))
            }
        }
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

        // Only the Pusher endpoint verifies channel signatures (D-055).
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
                block_on(&runtime, srv.shutdown(remaining(&ctx.get()))).map_err(|e| CliError(e.to_string()))
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
            block_on(&runtime, s.shutdown_graceful(remaining(&ctx.get()))).map_err(|e| CliError(e.0))
        })));

        if !api.authenticator().is_enabled() {
            log_info("API server is running without authentication");
        }
        Ok(())
    }

    /// Mounts the Prometheus endpoint on the main server for its port (a
    /// dedicated metrics server is run by `Metrics::run`) and runs the metrics.
    fn start_metrics(&mut self) -> Result<(), CliError> {
        if let Some(endpoint) = self.metrics.http_endpoint().filter(|e| e.dedicated_host.is_none()).cloned() {
            let srv = server::for_port(&endpoint.port.to_string()).map_err(|e| CliError(e.0))?;
            srv.setup_handler(&endpoint.path, server::metrics_handler(self.metrics.clone()));
            crate::metrics::log(
                Level::Info,
                &format!("Serve metrics at {}{}", srv.address(), endpoint.path),
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

/// Upstream `initMRuby`: the mruby version suffix of the start line, e.g.
/// ` (with mruby 1.2.0 (2015-11-17))`; empty without mruby support.
/// Cabby's version and the AnyCable server version it is compatible with, e.g.
/// `0.1.0 (AnyCable 1.6.21 compatible)` (upstream prints only its own version).
fn version_label() -> String {
    format!("{} (AnyCable {} compatible)", env!("CARGO_PKG_VERSION"), crate::telemetry::version())
}

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

#[cfg(test)]
mod tests {
    /// Pins D-035.
    #[test]
    fn version_label_names_cabby_and_the_compatible_version() {
        assert_eq!(super::version_label(), format!("{} (AnyCable 1.6.21 compatible)", env!("CARGO_PKG_VERSION")));
    }
}
