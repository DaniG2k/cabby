//! Harness for the AnyT scenario ports (`anyt_core`, `anyt_ext`): the server
//! under test configured like one of the upstream Makefile `test-conformance*`
//! targets, the AnyT RPC application (`cabby_testkit::anyt_app`), the
//! broadcast adapter the target uses, and AnyT-style clients.
//!
//! Each target is a [`Variant`]; `anyt_<variant>.rs` instantiates every
//! scenario the target runs upstream. The flags and `ANYCABLE_*` variables are
//! the Makefile's (they reach both the Ruby app and the Go server upstream),
//! with ports made unique. Adaptations:
//! * `REDIS_URL` (set job-wide by upstream CI) is set only where Redis is part
//!   of the target (`test-conformance`, `test-conformance-broker-redis`).
//! * Redis and NATS are the shared services (`cabby_testkit::services`,
//!   `eval "$(compat/scripts/services.sh start)"`). Each test gets its own Redis
//!   ACL user and names: the broadcast channel/stream/subject
//!   (`--redis_channel`, `--redisx_stream`, `--nats_channel`) and a stream name
//!   prefix in the application (stream names are opaque to clients; AnyCable's
//!   Redis pub/sub subscribes to one channel per stream). The Redis pub/sub
//!   command channel (`__anycable_internal__`) has no flag and stays shared, so
//!   a remote disconnect of `uid=26` reaches every such server; only the
//!   remote-disconnect scenario uses that identifier.
//! * Embedded NATS gets a per-test `--enats_store_dir` (unique storage paths).
//! * HTTP broadcasts go to the SUT's `--http_broadcast_port`.
//! * `test-conformance-ssl` (TLS for RPC and WebSocket) and
//!   `test-conformance-embedded` (a Go program embedding the server) are not
//!   instantiated: the fake RPC has no TLS, and embedding is a Go API.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use cabby_testkit::anyt_app::{AnytApp, AppOptions, BARRIER_CHANNEL};
use cabby_testkit::cable_client::{CableClient, ClientError, Options, QUIET_WINDOW};
use cabby_testkit::ports::free_port;
use cabby_testkit::publish::{self, Target};
use cabby_testkit::services::{self, RedisScope, unique};
use cabby_testkit::sut::Sut;
use cabby_testkit::workspace_root;
use serde_json::{Value, json};

pub use cabby_testkit::anyt_app::{assert_includes_message, assert_message, channel, ruby_eq};

const READY_DEADLINE: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Variant {
    /// `test-conformance`: gRPC RPC, Redis (legacy pub/sub) broadcasts.
    Default,
    /// `test-conformance-http`: HTTP RPC and HTTP broadcasts, both with secrets.
    Http,
    /// `test-conformance-nats`: NATS broadcasts (external NATS).
    Nats,
    /// `test-conformance-nats-embedded`: NATS broadcasts via embedded NATS.
    NatsEmbedded,
    /// `test-conformance-broker-http`: memory broker, presence TTL 2 s, HTTP broadcasts.
    BrokerHttp,
    /// `test-conformance-broker-redis`: memory broker, presence TTL 2 s, `redisx` broadcasts.
    BrokerRedis,
    /// `test-conformance-broker-nats`: NATS broker and pub/sub (embedded), HTTP broadcasts.
    BrokerNats,
}

pub struct World {
    pub app: AnytApp,
    pub sut: Sut,
    /// AnyT `--target-url`.
    pub url: String,
    _redis: Option<RedisScope>,
    store_dir: Option<PathBuf>,
}

impl Drop for World {
    fn drop(&mut self) {
        if let Some(d) = &self.store_dir {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

fn store_dir() -> PathBuf {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let dir = workspace_root().join(format!(
        "target/tmp/anyt-enats-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("creating a NATS store dir");
    dir
}

impl World {
    pub fn start(variant: Variant) -> Self {
        let shared = matches!(variant, Variant::Default | Variant::Nats | Variant::BrokerRedis);
        let app = AnytApp::start(AppOptions {
            http_rpc_secret: (variant == Variant::Http).then(|| "rpc_secret".to_owned()),
            target_host: "127.0.0.1".into(),
            stream_prefix: if shared { format!("{}:", unique("anyt")) } else { String::new() },
        });
        let mut args: Vec<String> = vec!["--headers=cookie,x-api-token".into()];
        let mut env: Vec<(&str, String)> = Vec::new();
        let mut redis: Option<RedisScope> = None;
        let mut redis_name = String::new();
        let mut nats_subject = String::new();
        let mut store = None;
        let mut enats_port = None;
        if variant == Variant::Http {
            args.push(format!("--rpc_host={}/_anycable", app.http_url()));
        } else {
            args.push(format!("--rpc_host={}", app.grpc_target()));
        }
        let mut embed = |env: &mut Vec<(&str, String)>, args: &mut Vec<String>| {
            let port = free_port();
            env.push(("ANYCABLE_EMBED_NATS", "true".into()));
            env.push(("ANYCABLE_ENATS_ADDR", format!("nats://127.0.0.1:{port}")));
            let dir = store_dir();
            args.push(format!("--enats_store_dir={}", dir.display()));
            store = Some(dir);
            enats_port = Some(port);
            port
        };
        match variant {
            Variant::Default => {
                let scope = services::redis().scope("anyt");
                redis_name = scope.name("anycable");
                env.push(("REDIS_URL", scope.url()));
                args.push(format!("--redis_channel={redis_name}"));
                redis = Some(scope);
            }
            Variant::Http => {
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "http".into()));
                env.push(("ANYCABLE_HTTP_BROADCAST_SECRET", "any_secret".into()));
                env.push(("ANYCABLE_HTTP_RPC_SECRET", "rpc_secret".into()));
                env.push(("ANYCABLE_HTTP_RPC_MOUNT_PATH", "/_anycable".into()));
            }
            Variant::Nats => {
                let nats = services::nats();
                nats_subject = nats.prefix("anyt");
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "nats".into()));
                env.push(("ANYCABLE_NATS_SERVERS", nats.url.clone()));
                args.push(format!("--nats_channel={nats_subject}"));
            }
            Variant::NatsEmbedded => {
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "nats".into()));
                let port = embed(&mut env, &mut args);
                env.push(("ANYCABLE_NATS_SERVERS", format!("nats://127.0.0.1:{port}")));
            }
            Variant::BrokerHttp => {
                env.push(("ANYCABLE_PRESENCE_TTL", "2".into()));
                env.push(("ANYCABLE_BROKER", "memory".into()));
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "http".into()));
            }
            Variant::BrokerRedis => {
                let scope = services::redis().scope("anyt");
                redis_name = scope.name("anycable");
                env.push(("ANYCABLE_PRESENCE_TTL", "2".into()));
                env.push(("ANYCABLE_BROKER", "memory".into()));
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "redisx".into()));
                env.push(("ANYCABLE_HTTP_BROADCAST_SECRET", "any_secret".into()));
                env.push(("REDIS_URL", scope.url()));
                args.push(format!("--redisx_stream={redis_name}"));
                redis = Some(scope);
            }
            Variant::BrokerNats => {
                env.push(("ANYCABLE_BROKER", "nats".into()));
                embed(&mut env, &mut args);
                env.push(("ANYCABLE_PUBSUB", "nats".into()));
                env.push(("ANYCABLE_BROADCAST_ADAPTER", "http".into()));
            }
        }
        let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();
        let env_refs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let mut sut = Sut::try_start(&arg_refs, &env_refs).unwrap_or_else(|e| panic!("{e:#}"));
        let local = |p: u16| std::net::SocketAddr::from(([127, 0, 0, 1], p));
        let target = match variant {
            Variant::Default => Target::RedisPublish { addr: redis_addr(), channel: redis_name.clone() },
            Variant::Http => Target::Http {
                addr: local(sut.broadcast_port),
                path: "/_broadcast".into(),
                key: Some("any_secret".into()),
            },
            Variant::Nats => Target::Nats { addr: nats_addr(), subject: nats_subject.clone() },
            Variant::NatsEmbedded => {
                Target::Nats { addr: local(enats_port.expect("enats port")), subject: "__anycable__".into() }
            }
            Variant::BrokerHttp | Variant::BrokerNats => {
                Target::Http { addr: local(sut.broadcast_port), path: "/_broadcast".into(), key: None }
            }
            Variant::BrokerRedis => Target::RedisStream { addr: redis_addr(), stream: redis_name.clone() },
        };
        match &target {
            Target::Http { addr, .. } => wait(&mut sut, *addr),
            Target::Nats { addr, .. } if enats_port.is_some() => wait(&mut sut, *addr),
            _ => {}
        }
        if let Some(p) = enats_port
            && variant == Variant::BrokerNats
        {
            wait(&mut sut, local(p));
        }
        await_broadcast_subscriber(&sut, &target);
        app.set_broadcaster(target);
        let url = format!("ws://127.0.0.1:{}/cable", sut.port);
        Self { app, sut, url, _redis: redis, store_dir: store }
    }

    /// `build_client(**args)`.
    pub fn client(&self, opts: Options) -> CableClient {
        CableClient::connect(&self.url, opts).unwrap_or_else(|e| {
            self.sut.process.propagate_pending();
            panic!("connecting to {} failed: {e}\nSUT output:\n{}", self.url, self.sut.process.output())
        })
    }

    /// AnyT `let(:client) { build_client(ignore: %w[ping welcome]) }`.
    pub fn default_client(&self) -> CableClient {
        self.client(ignoring(&["ping", "welcome"]))
    }

    /// `client.receive` (5 s).
    pub fn recv(&self, c: &mut CableClient) -> Value {
        c.receive().unwrap_or_else(|e| {
            panic!("{e}\napp errors: {:?}\nSUT output:\n{}", self.app.errors(), self.sut.process.output())
        })
    }

    /// `ActionCable.server.broadcast(stream, payload)` through the target's adapter.
    pub fn broadcast(&self, stream: &str, payload: &Value) {
        self.app.broadcast(stream, payload);
    }

    /// Replaces AnyT's "ActionCable doesn't provide an unsubscription ack :(
    /// sleep 1": subscribes `c` to [`BARRIER_CHANNEL`] and waits for the
    /// confirmation. The server handles a session's commands in order, so
    /// earlier commands (an unsubscribe, a `stop_stream_from` action) have been
    /// applied once it arrives.
    pub fn barrier(&self, c: &mut CableClient) {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let id = json!({"channel": BARRIER_CHANNEL, "n": SEQ.fetch_add(1, Ordering::SeqCst)}).to_string();
        c.send(&json!({"command": "subscribe", "identifier": id}));
        let ack = self.recv(c);
        assert_message(&json!({"identifier": id, "type": "confirm_subscription"}), &ack);
    }
}

/// Host and port of `CABBY_REDIS_URL` (the default user publishes).
fn redis_addr() -> std::net::SocketAddr {
    services::redis();
    let url = std::env::var("CABBY_REDIS_URL").expect("CABBY_REDIS_URL");
    host_port(&url, "redis://")
}

fn nats_addr() -> std::net::SocketAddr {
    host_port(&services::nats().url, "nats://")
}

fn host_port(url: &str, scheme: &str) -> std::net::SocketAddr {
    use std::net::ToSocketAddrs;
    let rest = url.strip_prefix(scheme).unwrap_or(url);
    let authority = rest.split('/').next().unwrap_or(rest);
    let hp = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
    hp.to_socket_addrs().ok().and_then(|mut a| a.next()).unwrap_or_else(|| panic!("cannot resolve {url}"))
}

/// Subjects on the shared NATS server are per-test (`services::unique`).
fn is_shared_subject(subject: &str) -> bool {
    subject != "__anycable__"
}

fn wait(sut: &mut Sut, addr: std::net::SocketAddr) {
    sut.process.wait_tcp(addr, READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));
}

/// Waits until the server consumes the broadcast channel, so a broadcast cannot
/// be dropped because the server has not subscribed yet (Redis `PUBLISH` to a
/// channel nobody listens to, `XADD` before the consumer group exists, or a NATS
/// subject without subscription, observed through the monitoring endpoint).
/// HTTP is ready once the port listens; embedded NATS once its port listens.
fn await_broadcast_subscriber(sut: &Sut, target: &Target) {
    let ready = |t: &Target| -> bool {
        match t {
            Target::RedisPublish { addr, channel } => publish::redis_subscribers(*addr, channel) > 0,
            Target::RedisStream { addr, stream } => publish::redis(*addr, &["XINFO", "GROUPS", stream])
                .ok()
                .and_then(|l| l.strip_prefix('*').and_then(|n| n.parse::<i64>().ok()))
                .is_some_and(|n| n > 0),
            Target::Nats { subject, .. } if is_shared_subject(subject) => {
                services::nats().monitor.subscriptions(subject).is_ok_and(|n| n > 0)
            }
            _ => true,
        }
    };
    let deadline = std::time::Instant::now() + READY_DEADLINE;
    while !ready(target) {
        assert!(
            std::time::Instant::now() < deadline,
            "the server did not subscribe to {target:?} within {READY_DEADLINE:?}\nSUT output:\n{}",
            sut.process.output()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

pub fn ignoring(types: &[&str]) -> Options {
    Options { ignore: types.iter().map(|t| (*t).to_owned()).collect(), ..Default::default() }
}

/// `{channel: name}.to_json` and friends: keys in the given order.
pub fn identifier(pairs: &[(&str, Value)]) -> String {
    let body: Vec<String> = pairs.iter().map(|(k, v)| format!("{}:{v}", Value::String((*k).into()))).collect();
    format!("{{{}}}", body.join(","))
}

pub fn subscribe_request(identifier: &str) -> Value {
    json!({"command": "subscribe", "identifier": identifier})
}

pub fn ack(identifier: &str) -> Value {
    json!({"identifier": identifier, "type": "confirm_subscription"})
}

/// `assert_raises(Anyt::Client::TimeoutError) { client.receive(timeout: 0.5) }`:
/// the 0.5 s window is AnyT's `WAIT_WHEN_NOT_EXPECTING_EVENT`.
#[track_caller]
pub fn assert_receive_times_out(c: &mut CableClient) {
    match c.receive_within(QUIET_WINDOW) {
        Err(ClientError::Timeout) => {}
        other => panic!("expected Anyt::Client::TimeoutError, got {other:?}"),
    }
}
