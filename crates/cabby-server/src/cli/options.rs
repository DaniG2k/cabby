//! `NewConfigFromCLI` (upstream `cli/options.go`): flags, environment variables,
//! configuration files and the computed settings that depend on them.
//!
//! Precedence, as upstream gets it from urfave/cli:
//!
//! 1. defaults (`config.NewConfig`), then every flag's own default written to its
//!    destination when the flag is defined (flags without a default reset the
//!    setting to its zero value, as upstream's `withDefaults` does);
//! 2. the configuration file (`--config-path`, else the first of `./cabby.toml`,
//!    `/etc/cabby/cabby.toml`, `./anycable.toml` and `/etc/anycable/anycable.toml`,
//!    D-060), loaded in the `Before` hook;
//! 3. flags set on the command line or through their environment variables
//!    (`CABBY_<NAME>`, then `ANYCABLE_<NAME>`, then `PORT` or `REDIS_URL`, D-061;
//!    Cabby-only flags have no `ANYCABLE_*` name), applied by flag actions after
//!    the file; the command line wins over the environment.
//!
//! Presets are applied later, by the runner (`Config::load_presets`).

use std::io::Write;
use std::path::PathBuf;

use super::flags::{self, App, Binding, Flag, Setter, Val, env_conflict, env_var_names};
use super::{CliError, CliOption};
use crate::config::{Config, EnvMap, StartupNote};

/// Where configuration files are looked for when `--config-path` isn't given
/// (D-060). Tests point it at temporary directories.
#[derive(Debug, Clone)]
pub struct ConfigSearch {
    /// The working directory: `./cabby.toml`, `./anycable.toml`.
    pub cwd: PathBuf,
    /// The system configuration directory: `/etc/cabby/cabby.toml`,
    /// `/etc/anycable/anycable.toml`.
    pub etc: PathBuf,
}

impl Default for ConfigSearch {
    fn default() -> Self {
        Self { cwd: PathBuf::from("."), etc: PathBuf::from("/etc") }
    }
}

impl ConfigSearch {
    /// The candidates, highest rank first: Cabby's names, wherever they are,
    /// before AnyCable's.
    pub fn candidates(&self) -> [PathBuf; 4] {
        [
            self.cwd.join("cabby.toml"),
            self.etc.join("cabby").join("cabby.toml"),
            self.cwd.join("anycable.toml"),
            self.etc.join("anycable").join("anycable.toml"),
        ]
    }

    /// The first existing candidate, and the existing candidates ranked below it.
    pub fn find(&self) -> (Option<PathBuf>, Vec<PathBuf>) {
        let mut existing = self.candidates().into_iter().filter(|p| std::fs::metadata(p).is_ok());
        (existing.next(), existing.collect())
    }
}

/// Settings read from the environment outside flags, as `CABBY_<X>` then
/// `ANYCABLE_<X>` (D-061).
const NON_FLAG_ENV: &[&str] = &["FILTER_LOGS", "FLY_RPC_APP_NAME"];

/// The last line of `--help` (D-061). Cabby-only settings (D-065) have no
/// `ANYCABLE_*` name; a test checks that each one is named here.
const ENV_FOOTER: &str = "Every CABBY_* variable can also be set as ANYCABLE_* (CABBY_* takes precedence), except the Cabby-only CABBY_SQLITE_PATH, CABBY_REDIS_BROKER_KEY_PREFIX, CABBY_REDIS_BROKER_TIMEOUT and CABBY_POSTGRES_*.";

const SERVER: &str = "SERVER:";
const SSL: &str = "SSL:";
const BROADCAST: &str = "BROADCASTING:";
const REDIS: &str = "REDIS:";
const REDISX: &str = "REDIS X BROADCAST:";
const HTTP_BROADCAST: &str = "HTTP BROADCAST:";
const NATS: &str = "NATS:";
const RPC: &str = "RPC:";
const DISCONNECTOR: &str = "DISCONNECTOR:";
const LOG: &str = "LOG:";
const METRICS: &str = "METRICS:";
const WS: &str = "WEBSOCKETS:";
const PING: &str = "PING:";
const JWT: &str = "JWT:";
const SIGNED_STREAMS: &str = "SIGNED STREAMS:";
const STATSD: &str = "STATSD:";
const EMBEDDED_NATS: &str = "EMBEDDED NATS:";
const MISC: &str = "MISC:";
const BROKER: &str = "BROKER:";
const SSE: &str = "SERVER-SENT EVENTS:";
const PUSHER: &str = "PUSHER:";
const DS: &str = "DURABLE STREAMS:";
const API: &str = "API:";
const SQLITE: &str = "SQLITE:";
const REDIS_BROKER: &str = "REDIS BROKER:";
const POSTGRES: &str = "POSTGRES:";

/// The configuration being built plus upstream's local flag destinations.
#[derive(Default)]
pub(crate) struct State {
    pub c: Config,
    path: String,
    headers: String,
    cookie_filter: String,
    mtags: String,
    broadcast_adapters: String,
    metrics_filter: String,
    enats_routes: String,
    enats_gateways: String,
    presets: String,
    turbo_rails_key: String,
    cable_ready_key: String,
    turbo_rails_clear_text: bool,
    cable_ready_clear_text: bool,
    jwt_id_key: String,
    jwt_id_param: String,
    jwt_id_enforce: bool,
    no_rpc: bool,
    print_config: bool,
}

/// Parses `--metrics_tags` (`key:value,key:value`; upstream unexported `parseTags`).
///
/// # Panics
///
/// On an entry without a `:`, as upstream does (an index out of range there).
pub fn parse_tags(s: &str) -> std::collections::HashMap<String, String> {
    s.split(',')
        .map(|tag| {
            let mut parts = tag.split(':');
            let key = parts.next().unwrap_or_default();
            let value = parts.next().unwrap_or_else(|| panic!("invalid metrics tag {tag:?}: expected key:value"));
            (key.to_owned(), value.to_owned())
        })
        .collect()
}

/// Builds the configuration from command-line `args` (`args[0]` is the program
/// name), configuration files and `env` (upstream `NewConfigFromCLI`). Usage,
/// version, `--print-config` output and deprecation warnings go to stdout.
///
/// `Ok((config, true))` means the run was interrupted (usage, version or the
/// configuration shown) and no further action is required; upstream returns an
/// empty config for usage and version.
pub fn new_config_from_cli(args: &[&str], env: &EnvMap, opts: Vec<CliOption>) -> Result<(Config, bool), CliError> {
    new_config_from_cli_to(args, env, opts, &mut std::io::stdout().lock())
}

/// [`new_config_from_cli`] writing its output to `out`.
pub fn new_config_from_cli_to(
    args: &[&str],
    env: &EnvMap,
    opts: Vec<CliOption>,
    out: &mut dyn Write,
) -> Result<(Config, bool), CliError> {
    new_config_from_cli_with(args, env, opts, out, &ConfigSearch::default())
}

/// [`new_config_from_cli_to`] looking for configuration files where `search` says.
pub fn new_config_from_cli_with(
    args: &[&str],
    env: &EnvMap,
    opts: Vec<CliOption>,
    out: &mut dyn Write,
    search: &ConfigSearch,
) -> Result<(Config, bool), CliError> {
    let program = args.first().copied().unwrap_or_default();

    // A discovered configuration file comes first, so `--config-path` on the
    // command line (the last occurrence wins) overrides it.
    let (discovered, ranked_below) = search.find();
    let discovered = discovered.map(|p| p.display().to_string());
    let mut flag_args: Vec<&str> = Vec::with_capacity(args.len() + 2);
    if let Some(path) = &discovered {
        flag_args.extend(["--config-path", path.as_str()]);
    }
    flag_args.extend(args.iter().skip(1));

    let mut state = State { c: Config::new(), ..Default::default() };
    let mut flags = definitions(&state.c);
    flags::init_destinations(&flags, &mut state);

    let mut app = App {
        name: "cabby".to_owned(),
        version: crate::version::label(),
        usage: "Cabby, a real-time server for Rails and beyond".to_owned(),
        footer: ENV_FOOTER.to_owned(),
    };

    for opt in opts {
        match opt {
            CliOption::Name(name) => app.name = name,
            CliOption::Version(version) => app.version = version,
            CliOption::UsageHeader(usage) => app.usage = usage,
            CliOption::CustomOptions(custom) => flags.extend(custom.into_iter().map(|f| Flag {
                required: f.required,
                ..Flag::new(&f.name, Val::Str(f.value), "", Binding::Custom(f.destination))
            })),
        }
    }

    // urfave App.Setup
    if app.name.is_empty() {
        app.name = program.rsplit('/').next().unwrap_or_default().to_owned();
    }
    flags.push(Flag {
        aliases: vec!["h".to_owned()],
        no_default_text: true,
        ..Flag::new("help", Val::Bool(false), "show help", Binding::Unbound)
    });
    if !app.version.is_empty() {
        flags.push(Flag {
            aliases: vec!["v".to_owned()],
            no_default_text: true,
            ..Flag::new("version", Val::Bool(false), "print the version", Binding::Unbound)
        });
    }

    let mut emit = |text: &str| {
        // Upstream ignores write errors on its output too.
        let _ = out.write_all(text.as_bytes());
    };

    let usage_error = |emit: &mut dyn FnMut(&str), err: String| {
        emit(&format!("Incorrect Usage: {err}\n\n"));
        emit(&app.help(&flags));
        Err(CliError(err))
    };

    let mut parsed = match flags::apply_env(&flags, env, &mut state) {
        Ok(parsed) => parsed,
        Err(err) => return usage_error(&mut emit, err),
    };
    if let Err(err) = flags::parse_args(&flags, &flag_args, &mut parsed, &mut state) {
        return usage_error(&mut emit, err);
    }
    let non_flag_conflicts = NON_FLAG_ENV
        .iter()
        .filter_map(|name| env_conflict(&[format!("CABBY_{name}"), format!("ANYCABLE_{name}")], env));
    // A flag on the command line overrides both variables: no conflict to note.
    let env_notes: Vec<StartupNote> = parsed
        .env_conflicts
        .iter()
        .filter(|(i, _, _)| !parsed.on_cli[*i])
        .map(|(_, cabby, anycable)| (cabby.clone(), anycable.clone()))
        .chain(non_flag_conflicts)
        .map(|(cabby, anycable)| StartupNote::EnvConflict { cabby, anycable })
        .collect();

    if parsed.bool(&flags, "help") {
        emit(&app.help(&flags));
        return Ok((Config::default(), true));
    }
    if !app.version.is_empty() && parsed.bool(&flags, "version") {
        // `cabby 0.3.0 (AnyCable 1.6.21 compatible)` (D-059): the program name,
        // then the version (an embedder's `CliOption::Version` keeps its text).
        emit(&format!("{} {}\n", app.name, app.version));
        return Ok((Config::default(), true));
    }

    if let Err(err) = flags::check_required(&flags, &parsed) {
        emit(&app.help(&flags));
        return Err(CliError(err));
    }

    // Before: the configuration file.
    let mut file_notes: Vec<StartupNote> = Vec::new();
    if !parsed.bool(&flags, "ignore-config-path") {
        let path = parsed.string(&flags, "config-path");
        if !path.is_empty() {
            state.c.config_file_path = Some(path.clone().into());
            state.c.load_from_file().map_err(|e| CliError(e.0))?;
            if discovered.as_deref() == Some(path.as_str()) {
                file_notes.extend(
                    ranked_below
                        .iter()
                        .map(|p| StartupNote::IgnoredConfigFile { path: p.display().to_string(), used: path.clone() }),
                );
            }

            if parsed.bool(&flags, "strict-config") && !state.c.unknown_config_keys.is_empty() {
                return Err(CliError(format!(
                    "unknown keys in configuration file {path}: {}",
                    state.c.unknown_config_keys.join(", ")
                )));
            }
        }
    }

    flags::run_actions(&flags, &parsed, &mut state);

    let print_config = state.print_config;
    let mut c = finalize(state, &mut emit);
    c.startup_notes = file_notes.into_iter().chain(env_notes).collect();

    if print_config {
        emit(&c.to_toml());
        return Ok((c, true));
    }

    Ok((c, false))
}

const DEPRECATED_METRICS_LOG_INTERVAL: &str = "DEPRECATION WARNING: metrics_log_interval option is deprecated
and will be deleted in a future release of Cabby.
Use metrics_rotate_interval instead.\n";

fn deprecated(option: &str, replacement: &str) -> String {
    format!(
        "DEPRECATION WARNING: {option} option is deprecated\nand will be removed in a future release of Cabby.\n{replacement}\n"
    )
}

/// The settings computed from others once every source has been read.
fn finalize(state: State, emit: &mut dyn FnMut(&str)) -> Config {
    let State {
        mut c,
        path,
        headers,
        cookie_filter,
        mtags,
        broadcast_adapters,
        metrics_filter,
        enats_routes,
        enats_gateways,
        presets,
        turbo_rails_key,
        cable_ready_key,
        turbo_rails_clear_text,
        cable_ready_clear_text,
        jwt_id_key,
        jwt_id_param,
        jwt_id_enforce,
        no_rpc,
        print_config: _,
    } = state;

    let split = |s: &str, sep: char| s.split(sep).map(str::to_owned).collect::<Vec<_>>();

    if !broadcast_adapters.is_empty() {
        c.broadcast_adapters = split(&broadcast_adapters, ',');
    }

    if !path.is_empty() {
        c.ws.paths = split(&path, ',');
    }

    c.rpc.proxy_headers = split(&headers.to_lowercase(), ',');

    if !cookie_filter.is_empty() {
        c.rpc.proxy_cookies = split(&cookie_filter, ',');
    }

    if c.log.debug {
        c.log.log_level = "debug".to_owned();
    }

    if c.metrics.port == 0 {
        c.metrics.port = c.server.port;
    }

    if !mtags.is_empty() {
        c.metrics.tags = parse_tags(&mtags);
    }

    if c.metrics.log_interval > 0 {
        emit(DEPRECATED_METRICS_LOG_INTERVAL);

        if c.metrics.rotate_interval == 0 {
            c.metrics.rotate_interval = c.metrics.log_interval;
        }
    }

    if !metrics_filter.is_empty() {
        c.metrics.log_filter = Some(split(&metrics_filter, ','));
    }

    if !enats_routes.is_empty() {
        c.embedded_nats.routes = split(&enats_routes, ',');
    }

    if !enats_gateways.is_empty() {
        c.embedded_nats.gateways = split(&enats_gateways, ';');
    }

    if !presets.is_empty() {
        c.user_presets = Some(split(&presets, ','));
    }

    // Automatically set the URL of the embedded NATS as the pub/sub server URL
    if c.embedded_nats.enabled && c.nats.servers == crate::nats::DEFAULT_URL {
        c.nats.servers = c.embedded_nats.service_addr.clone();
    }

    if c.disconnector_disabled {
        emit(&deprecated("disable_disconnect", "Use disconnect_mode=never instead."));
        c.app.disconnect_mode = crate::node::DISCONNECT_MODE_NEVER.to_owned();
    }

    if c.disconnect_queue.shutdown_timeout > 0 {
        emit(&deprecated("disconnect_timeout", "Use shutdown_timeout instead."));
    }

    // Various computed/dependent configuration settings.
    let defaults = Config::new();

    // If REDIS_URL is available or redisx broadcast adapter or redis broker is used
    // and no pubsub configured, enable Redis pub/sub.
    if c.pubsub_adapter == defaults.pubsub_adapter
        && (c.redis.url != defaults.redis.url
            || c.broadcast_adapters.iter().any(|a| a == "redisx")
            || c.broker.adapter == "redis")
    {
        c.pubsub_adapter = "redis".to_owned();
    }

    let sse_ping_interval = if c.app.ping_interval != defaults.app.ping_interval {
        c.app.ping_interval
    } else {
        crate::sse::DEFAULT_PING_INTERVAL
    };

    if c.sse.ping_interval == 0 {
        c.sse.ping_interval = sse_ping_interval;
    }

    if c.ds.sse_ping_interval == 0 {
        c.ds.sse_ping_interval = sse_ping_interval;
    }

    // Propagate allowed origins to all the components
    c.ws.allowed_origins = c.server.allowed_origins.clone();
    c.sse.allowed_origins = c.server.allowed_origins.clone();
    c.ds.allowed_origins = c.server.allowed_origins.clone();
    c.http_broadcast.cors_hosts = c.server.allowed_origins.clone();

    c.pusher.add_cors_headers = c.http_broadcast.add_cors_headers;
    c.pusher.cors_hosts = c.http_broadcast.cors_hosts.clone();

    // Propagate Redis and NATS configs to components (upstream shares pointers to
    // the main sections; these are copies of their final values).
    if c.redis_broadcast.redis.is_none() {
        c.redis_broadcast.redis = Some(c.redis.clone());
    }

    if c.legacy_redis_broadcast.redis.is_none() {
        c.legacy_redis_broadcast.redis = Some(c.redis.clone());
    }

    if c.nats_broadcast.nats.is_none() {
        c.nats_broadcast.nats = Some(c.nats.clone());
    }

    if c.redis_pubsub.redis.is_none() {
        c.redis_pubsub.redis = Some(c.redis.clone());
    }

    // Ensure pubsub and broadcaster channels are not the same
    // when redis pub/sub broadcaster is enabled
    if c.broadcast_adapters.iter().any(|a| a == "redis") && c.redis_pubsub.channel == c.legacy_redis_broadcast.channel {
        emit(
            "WARNING: Same Redis channel is provided for broadcaster and pub/sub components, updating the latter one automatically\n",
        );
        c.redis_pubsub.channel = format!("{}-int", c.redis_pubsub.channel);
    }

    if c.nats_pubsub.nats.is_none() {
        c.nats_pubsub.nats = Some(c.nats.clone());
    }

    if !turbo_rails_key.is_empty() {
        emit(&deprecated("turbo_rails_key", "Use turbo_streams_secret instead."));
        c.streams.turbo_secret = turbo_rails_key;
        c.streams.turbo = true;
    }

    if turbo_rails_clear_text {
        emit(&deprecated("turbo_rails_cleartext", "It has no effect anymore, use public streams instead."));
    }

    if !cable_ready_key.is_empty() {
        emit(&deprecated("cable_ready_key", "Use cable_ready_secret instead."));
        c.streams.cable_ready_secret = cable_ready_key;
        c.streams.cable_ready = true;
    }

    if cable_ready_clear_text {
        emit(&deprecated("cable_ready_cleartext", "It has no effect anymore, use public streams instead."));
    }

    if !jwt_id_key.is_empty() {
        emit(&deprecated("jwt_id_key", "Use jwt_secret instead."));
        if c.jwt.secret.is_empty() {
            c.jwt.secret = jwt_id_key;
        }
    }

    if !jwt_id_param.is_empty() {
        emit(&deprecated("jwt_id_param", "Use jwt_param instead."));
        if c.jwt.param.is_empty() {
            c.jwt.param = jwt_id_param;
        }
    }

    if jwt_id_enforce {
        // Upstream's typo ("enfore_jwt") is part of the observable text.
        emit(&deprecated("jwt_id_enforce", "Use enfore_jwt instead."));
        c.jwt.force = true;
    }

    // Configure RPC
    if no_rpc {
        c.rpc.implementation = "none".to_owned();
    }

    // Legacy HTTP authentication stuff
    if !c.http_broadcast.secret.is_empty() {
        emit(&deprecated("http_broadcast_secret", "Use broadcast_key instead."));
    }

    if c.http_broadcast.secret.is_empty() {
        c.http_broadcast.secret = c.broadcast_key.clone();
    }

    // Fallback secrets
    if !c.secret.is_empty() {
        if c.streams.secret.is_empty() {
            c.streams.secret = c.secret.clone();
        }

        if c.jwt.secret.is_empty() {
            c.jwt.secret = c.secret.clone();
        }

        if c.http_broadcast.secret.is_empty() {
            c.http_broadcast.secret_base = c.secret.clone();
        }

        if c.rpc.secret.is_empty() {
            c.rpc.secret_base = c.secret.clone();
        }

        if c.pusher.secret.is_empty() {
            c.pusher.secret = c.secret.clone();
        }
    }

    // Nullify none secrets
    for secret in [
        &mut c.secret,
        &mut c.streams.secret,
        &mut c.jwt.secret,
        &mut c.rpc.secret,
        &mut c.http_broadcast.secret,
        &mut c.pusher.secret,
    ] {
        if secret == "none" {
            secret.clear();
        }
    }

    // Configure default HTTP port
    if c.http_broadcast.port == 0 {
        c.http_broadcast.port = if c.http_broadcast.is_secured() { c.server.port } else { 8090 };
    }

    // Configure public mode and other insecure features
    if c.public_mode {
        c.skip_auth = true;
        c.streams.public = true;
        // Ensure broadcasting is also public
        c.http_broadcast.secret.clear();
        c.http_broadcast.secret_base.clear();
    }

    c
}

// --- Flag definitions -----------------------------------------------------------

fn bool_flag(name: &str, default: bool, usage: &str, set: fn(&mut State, bool)) -> Flag {
    Flag::new(name, Val::Bool(default), usage, Binding::Action { init: true, set: Setter::Bool(set) })
}

fn int_flag(name: &str, default: i64, usage: &str, set: fn(&mut State, i64)) -> Flag {
    Flag::new(name, Val::Int(default), usage, Binding::Action { init: true, set: Setter::Int(set) })
}

fn str_flag(name: &str, default: &str, usage: &str, set: fn(&mut State, String)) -> Flag {
    Flag::new(name, Val::Str(default.to_owned()), usage, Binding::Action { init: true, set: Setter::Str(set) })
}

fn hidden(f: Flag) -> Flag {
    Flag { hidden: true, ..f }
}

fn with_env(f: Flag, env: &[&str]) -> Flag {
    Flag { env_vars: env.iter().map(|s| (*s).to_owned()).collect(), ..f }
}

/// Upstream `withDefaults`: the help category and, unless set, the
/// `CABBY_<NAME>` and `ANYCABLE_<NAME>` environment variables (D-061).
fn with_defaults(category: &'static str, flags: Vec<Flag>) -> Vec<Flag> {
    flags
        .into_iter()
        .map(|mut f| {
            f.category = category;
            if f.env_vars.is_empty() {
                f.env_vars = env_var_names(&f.name);
            }
            f
        })
        .collect()
}

/// Clamps Go `int` values into narrower Rust setting types.
fn clamp<T: TryFrom<i64> + Default>(v: i64, max: T) -> T {
    T::try_from(v).unwrap_or(if v < 0 { T::default() } else { max })
}

/// Every flag, in upstream order (help and version are appended by the app).
fn definitions(c: &Config) -> Vec<Flag> {
    let mut flags = vec![
        Flag::new("ignore-config-path", Val::Bool(false), "Ignore configuration files", Binding::Unbound),
        Flag::new("config-path", Val::Str(String::new()), "Path to the TOML configuration file", Binding::Unbound),
        with_env(
            Flag::new(
                "strict-config",
                Val::Bool(false),
                "Fail if the configuration file contains unknown keys",
                Binding::Unbound,
            ),
            &["CABBY_STRICT_CONFIG", "ANYCABLE_STRICT_CONFIG"],
        ),
        Flag::new(
            "print-config",
            Val::Bool(false),
            "Print configuration and exit",
            Binding::Plain(Setter::Bool(|s, v| s.print_config = v)),
        ),
    ];

    flags.extend(server_flags(c));
    flags.extend(ssl_flags());
    flags.extend(broadcast_flags(c));
    flags.extend(broker_flags(c));
    flags.extend(sqlite_flags(c));
    flags.extend(redis_broker_flags(c));
    flags.extend(postgres_flags(c));
    flags.extend(redis_flags(c));
    flags.extend(redis_broadcast_flags(c));
    flags.extend(http_broadcast_flags(c));
    flags.extend(nats_flags(c));
    flags.extend(rpc_flags(c));
    flags.extend(disconnector_flags(c));
    flags.extend(log_flags(c));
    flags.extend(metrics_flags(c));
    flags.extend(ws_flags(c));
    flags.extend(ping_flags(c));
    flags.extend(jwt_flags(c));
    flags.extend(signed_streams_flags(c));
    flags.extend(statsd_flags(c));
    flags.extend(embedded_nats_flags(c));
    flags.extend(sse_flags(c));
    flags.extend(pusher_flags());
    flags.extend(ds_flags(c));
    flags.extend(api_flags(c));
    flags.extend(misc_flags());

    flags
}

fn server_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        SERVER,
        vec![
            str_flag("host", &c.server.host, "Server host", |s, v| s.c.server.host = v),
            with_env(
                int_flag("port", c.server.port, "Server port", |s, v| s.c.server.port = v),
                &["CABBY_PORT", "ANYCABLE_PORT", "PORT"],
            ),
            str_flag(
                "allowed_origins",
                "",
                r#"Accept requests only from specified origins, e.g., "www.example.com,*example.io". No check is performed if empty"#,
                |s, v| s.c.server.allowed_origins = v,
            ),
            str_flag("secret", &c.secret, "A common secret key used by all features by default", |s, v| s.c.secret = v),
            str_flag("broadcast_key", &c.broadcast_key, "An authentication key for broadcast requests", |s, v| {
                s.c.broadcast_key = v
            }),
            bool_flag(
                "public",
                c.public_mode,
                "[DANGER ZONE] Run server in the public mode allowing all connections and stream subscriptions",
                |s, v| s.c.public_mode = v,
            ),
            bool_flag("noauth", c.skip_auth, "[DANGER ZONE] Disable client authentication over RPC", |s, v| {
                s.c.skip_auth = v
            }),
            int_flag("max-conn", 0, "Limit simultaneous server connections (0 – without limit)", |s, v| {
                s.c.server.max_conn = v
            }),
            str_flag(
                "path",
                &c.ws.paths.join(","),
                "WebSocket endpoint path (you can specify multiple paths using comma as separator)",
                |s, v| s.path = v,
            ),
            str_flag("health-path", &c.server.health_path, "HTTP health endpoint path", |s, v| {
                s.c.server.health_path = v
            }),
            int_flag("shutdown_timeout", c.app.shutdown_timeout, "Graceful shutdown timeout (in seconds)", |s, v| {
                s.c.app.shutdown_timeout = v
            }),
            hidden(int_flag(
                "shutdown_pool_size",
                c.app.shutdown_disconnect_pool_size,
                "The number of goroutines to use for disconnect calls on shutdown",
                |s, v| s.c.app.shutdown_disconnect_pool_size = v,
            )),
            int_flag("shutdown_delay", c.app.shutdown_delay, "Sleep time before shutting down (in seconds)", |s, v| {
                s.c.app.shutdown_delay = v
            }),
            hidden(Flag::new(
                "node_id",
                Val::Str(c.id.clone()),
                "Unique node identifier",
                Binding::Action {
                    init: false,
                    set: Setter::Str(|s, v| {
                        s.c.id = v;
                        s.c.user_provided_id = true;
                    }),
                },
            )),
        ],
    )
}

fn ssl_flags() -> Vec<Flag> {
    with_defaults(
        SSL,
        vec![
            str_flag("ssl_cert", "", "SSL certificate path", |s, v| s.c.server.ssl.cert_path = v),
            str_flag("ssl_key", "", "SSL private key path", |s, v| s.c.server.ssl.key_path = v),
        ],
    )
}

fn broadcast_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        BROADCAST,
        vec![
            str_flag(
                "broadcast_adapter",
                "",
                "Broadcasting adapter to use (http, redisx, redis, nats or postgres). You can specify multiple at once via a comma-separated list",
                |s, v| s.broadcast_adapters = v,
            ),
            str_flag(
                "broker",
                &c.broker.adapter,
                "Broker engine to use (memory, nats, sqlite, redis or postgres)",
                |s, v| s.c.broker.adapter = v,
            ),
            str_flag("pubsub", &c.pubsub_adapter, "Pub/Sub adapter to use (redis, nats or postgres)", |s, v| {
                s.c.pubsub_adapter = v
            }),
            str_flag("redis_channel", &c.legacy_redis_broadcast.channel, "Redis channel for broadcasts", |s, v| {
                s.c.legacy_redis_broadcast.channel = v
            }),
            str_flag("nats_channel", &c.nats_broadcast.channel, "NATS channel for broadcasts", |s, v| {
                s.c.nats_broadcast.channel = v
            }),
            hidden(int_flag(
                "hub_gopool_size",
                c.app.hub_gopool_size,
                "The size of the goroutines pool to broadcast messages",
                |s, v| s.c.app.hub_gopool_size = v,
            )),
        ],
    )
}

fn broker_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        BROKER,
        vec![
            int_flag(
                "history_limit",
                i64::try_from(c.broker.history_limit).unwrap_or(i64::MAX),
                "Max number of messages to keep in the stream's history",
                |s, v| s.c.broker.history_limit = clamp(v, usize::MAX),
            ),
            int_flag("history_ttl", c.broker.history_ttl, "TTL for messages in streams history (seconds)", |s, v| {
                s.c.broker.history_ttl = v
            }),
            int_flag(
                "sessions_ttl",
                c.broker.sessions_ttl,
                "TTL for expired/disconnected sessions (seconds)",
                |s, v| s.c.broker.sessions_ttl = v,
            ),
            int_flag("presence_ttl", c.broker.presence_ttl, "TTL for presence information (seconds)", |s, v| {
                s.c.broker.presence_ttl = v
            }),
        ],
    )
}

/// The SQLite broker (Cabby only, D-065): `CABBY_SQLITE_PATH`, no `ANYCABLE_*` alias.
fn sqlite_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        SQLITE,
        vec![with_env(
            str_flag("sqlite_path", &c.sqlite.path, "Database file for --broker=sqlite (Cabby only)", |s, v| {
                s.c.sqlite.path = v
            }),
            &["CABBY_SQLITE_PATH"],
        )],
    )
}

/// The Redis broker (Cabby only, D-069): `CABBY_REDIS_BROKER_*`, no `ANYCABLE_*` alias.
fn redis_broker_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        REDIS_BROKER,
        vec![
            with_env(
                str_flag(
                    "redis_broker_key_prefix",
                    &c.redis_broker.key_prefix,
                    "Prefix of the keys --broker=redis writes (Cabby only)",
                    |s, v| s.c.redis_broker.key_prefix = v,
                ),
                &["CABBY_REDIS_BROKER_KEY_PREFIX"],
            ),
            with_env(
                int_flag(
                    "redis_broker_timeout",
                    c.redis_broker.timeout,
                    "Timeout of one --broker=redis operation (seconds, Cabby only)",
                    |s, v| s.c.redis_broker.timeout = v,
                ),
                &["CABBY_REDIS_BROKER_TIMEOUT"],
            ),
        ],
    )
}

/// The Postgres adapters (Cabby only, D-070, D-071): `CABBY_POSTGRES_*`, no
/// `ANYCABLE_*` aliases.
fn postgres_flags(c: &Config) -> Vec<Flag> {
    let p = &c.postgres;
    with_defaults(
        POSTGRES,
        vec![
            with_env(
                str_flag(
                    "postgres_url",
                    &p.url,
                    "Postgres connection URL for the postgres adapters (Cabby only)",
                    |s, v| s.c.postgres.url = v,
                ),
                &["CABBY_POSTGRES_URL"],
            ),
            with_env(
                str_flag("postgres_schema", &p.schema, "Postgres schema for Cabby's tables", |s, v| {
                    s.c.postgres.schema = v
                }),
                &["CABBY_POSTGRES_SCHEMA"],
            ),
            with_env(
                str_flag(
                    "postgres_table_prefix",
                    &p.table_prefix,
                    "Prefix for Cabby's Postgres table names",
                    |s, v| s.c.postgres.table_prefix = v,
                ),
                &["CABBY_POSTGRES_TABLE_PREFIX"],
            ),
            with_env(
                str_flag("postgres_channel", &p.channel, "Base Postgres LISTEN/NOTIFY channel name", |s, v| {
                    s.c.postgres.channel = v
                }),
                &["CABBY_POSTGRES_CHANNEL"],
            ),
            with_env(
                bool_flag(
                    "postgres_auto_migrate",
                    p.auto_migrate,
                    "Create Cabby's Postgres tables at startup",
                    |s, v| s.c.postgres.auto_migrate = v,
                ),
                &["CABBY_POSTGRES_AUTO_MIGRATE"],
            ),
            with_env(
                int_flag(
                    "postgres_keepalive_interval",
                    p.keepalive_interval,
                    "Heartbeat interval of the Postgres LISTEN connection (seconds, 0 to disable)",
                    |s, v| s.c.postgres.keepalive_interval = v,
                ),
                &["CABBY_POSTGRES_KEEPALIVE_INTERVAL"],
            ),
            with_env(
                bool_flag(
                    "postgres_tls_verify",
                    p.tls_verify,
                    "Verify the Postgres server's TLS certificate",
                    |s, v| s.c.postgres.tls_verify = v,
                ),
                &["CABBY_POSTGRES_TLS_VERIFY"],
            ),
            with_env(
                str_flag(
                    "postgres_tls_ca_cert_path",
                    &p.tls_ca_cert_path,
                    "CA certificates (PEM file) to verify the Postgres server with",
                    |s, v| s.c.postgres.tls_ca_cert_path = v,
                ),
                &["CABBY_POSTGRES_TLS_CA_CERT_PATH"],
            ),
            with_env(
                bool_flag(
                    "postgres_unlogged",
                    p.unlogged,
                    "Create Cabby's ephemeral Postgres tables as UNLOGGED",
                    |s, v| s.c.postgres.unlogged = v,
                ),
                &["CABBY_POSTGRES_UNLOGGED"],
            ),
        ],
    )
}

fn redis_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        REDIS,
        vec![
            with_env(
                str_flag("redis_url", &c.redis.url, "Redis url", |s, v| s.c.redis.url = v),
                &["CABBY_REDIS_URL", "ANYCABLE_REDIS_URL", "REDIS_URL"],
            ),
            str_flag(
                "redis_sentinels",
                "",
                "Comma separated list of sentinel hosts, format: 'hostname:port,..'",
                |s, v| s.c.redis.sentinels = v,
            ),
            hidden(int_flag(
                "redis_sentinel_discovery_interval",
                c.redis.sentinel_discovery_interval,
                "Interval to rediscover sentinels in seconds",
                |s, v| s.c.redis.sentinel_discovery_interval = v,
            )),
            hidden(int_flag(
                "redis_keepalive_interval",
                c.redis.keepalive_ping_interval,
                "Interval to periodically ping Redis to make sure it's alive",
                |s, v| s.c.redis.keepalive_ping_interval = v,
            )),
            bool_flag(
                "redis_tls_verify",
                c.redis.tls_verify,
                "Verify Redis server TLS certificate (only if URL protocol is rediss://)",
                |s, v| s.c.redis.tls_verify = v,
            ),
            str_flag(
                "redis_tls_ca_cert_path",
                "",
                "Path to the CA certificate file to verify the Redis server certificate",
                |s, v| s.c.redis.tls_ca_cert_path = v,
            ),
            str_flag(
                "redis_tls_client_cert_path",
                "",
                "Path to the client TLS certificate file for mutual TLS with Redis",
                |s, v| s.c.redis.tls_client_cert_path = v,
            ),
            str_flag(
                "redis_tls_client_key_path",
                "",
                "Path to the client TLS private key file for mutual TLS with Redis",
                |s, v| s.c.redis.tls_client_key_path = v,
            ),
            hidden(bool_flag("redis_disable_cache", c.redis.disable_cache, "Disable client-side caching", |s, v| {
                s.c.redis.disable_cache = v
            })),
        ],
    )
}

fn redis_broadcast_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        REDISX,
        vec![
            str_flag("redisx_stream", &c.redis_broadcast.stream, "Redis X broadcaster stream name", |s, v| {
                s.c.redis_broadcast.stream = v
            }),
            hidden(str_flag(
                "redisx_group",
                &c.redis_broadcast.group,
                "Redis X broadcaster consumer group name",
                |s, v| s.c.redis_broadcast.group = v,
            )),
            hidden(int_flag(
                "redisx_read_block_milliseconds",
                c.redis_broadcast.stream_read_block_milliseconds,
                "Redis stream read wait time in milliseconds",
                |s, v| s.c.redis_broadcast.stream_read_block_milliseconds = v,
            )),
        ],
    )
}

fn http_broadcast_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        HTTP_BROADCAST,
        vec![
            int_flag("http_broadcast_port", c.http_broadcast.port, "HTTP pub/sub server port", |s, v| {
                s.c.http_broadcast.port = v
            }),
            str_flag("http_broadcast_path", &c.http_broadcast.path, "HTTP pub/sub endpoint path", |s, v| {
                s.c.http_broadcast.path = v
            }),
            hidden(str_flag("http_broadcast_secret", "", "[Deprecated] HTTP pub/sub authorization secret", |s, v| {
                s.c.http_broadcast.secret = v
            })),
            hidden(bool_flag("http_broadcast_cors", false, "", |s, v| s.c.http_broadcast.add_cors_headers = v)),
        ],
    )
}

fn nats_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        NATS,
        vec![
            str_flag("nats_servers", &c.nats.servers, "Comma separated list of NATS cluster servers", |s, v| {
                s.c.nats.servers = v
            }),
            hidden(bool_flag(
                "nats_dont_randomize_servers",
                false,
                "Pass this option to disable NATS servers randomization during (re-)connect",
                |s, v| s.c.nats.dont_randomize_servers = v,
            )),
        ],
    )
}

fn embedded_nats_flags(c: &Config) -> Vec<Flag> {
    let e = &c.embedded_nats;
    with_defaults(
        EMBEDDED_NATS,
        vec![
            bool_flag("embed_nats", e.enabled, "Enable embedded NATS server and use it for pub/sub", |s, v| {
                s.c.embedded_nats.enabled = v
            }),
            hidden(str_flag("enats_addr", &e.service_addr, "NATS server bind address", |s, v| {
                s.c.embedded_nats.service_addr = v
            })),
            hidden(str_flag("enats_cluster", &e.cluster_addr, "NATS cluster service bind address", |s, v| {
                s.c.embedded_nats.cluster_addr = v
            })),
            str_flag("enats_cluster_name", &e.cluster_name, "NATS cluster name", |s, v| {
                s.c.embedded_nats.cluster_name = v
            }),
            str_flag("enats_cluster_routes", "", "Comma-separated list of known cluster addresses", |s, v| {
                s.enats_routes = v
            }),
            hidden(str_flag("enats_gateway", &e.gateway_addr, "NATS gateway bind address", |s, v| {
                s.c.embedded_nats.gateway_addr = v
            })),
            str_flag(
                "enats_gateways",
                "",
                "Semicolon-separated list of known gateway configurations: name_a:gateway_1,gateway_2;name_b:gateway_4",
                |s, v| s.enats_gateways = v,
            ),
            str_flag("enats_gateway_advertise", &e.gateway_advertise, "NATS gateway advertise address", |s, v| {
                s.c.embedded_nats.gateway_advertise = v
            }),
            hidden(str_flag(
                "enats_store_dir",
                &e.store_dir,
                "Embedded NATS store directory (for JetStream)",
                |s, v| s.c.embedded_nats.store_dir = v,
            )),
            str_flag(
                "enats_server_name",
                &e.name,
                "Embedded NATS unique server name (required for JetStream), auto-generated by default",
                |s, v| s.c.embedded_nats.name = v,
            ),
            hidden(bool_flag("enats_debug", false, "Enable NATS server logs", |s, v| s.c.embedded_nats.debug = v)),
            hidden(bool_flag("enats_trace", false, "Enable NATS server protocol trace logs", |s, v| {
                s.c.embedded_nats.trace = v
            })),
            int_flag(
                "enats_max_payload",
                0,
                "Maximum message payload size in bytes (default: 1048576 = 1MB)",
                |s, v| s.c.embedded_nats.max_payload = v,
            ),
        ],
    )
}

fn rpc_flags(c: &Config) -> Vec<Flag> {
    let r = &c.rpc;
    with_defaults(
        RPC,
        vec![
            str_flag("rpc_host", &r.host, "RPC service address (full URL in case of HTTP RPC)", |s, v| {
                s.c.rpc.host = v
            }),
            bool_flag("norpc", false, "Disable RPC component and run server in the standalone mode", |s, v| {
                s.no_rpc = v
            }),
            int_flag(
                "rpc_concurrency",
                r.concurrency,
                "Max number of concurrent RPC request; should be slightly less than the RPC server concurrency",
                |s, v| s.c.rpc.concurrency = v,
            ),
            hidden(bool_flag("rpc_enable_tls", false, "Enable client-side TLS with the RPC server", |s, v| {
                s.c.rpc.enable_tls = v
            })),
            hidden(bool_flag("rpc_tls_verify", true, "Whether to verify the RPC server certificate", |s, v| {
                s.c.rpc.tls_verify = v
            })),
            hidden(str_flag(
                "rpc_tls_root_ca",
                "",
                "CA root certificate file path or contents in PEM format (if not set, system CAs will be used)",
                |s, v| s.c.rpc.tls_root_ca = v,
            )),
            hidden(int_flag(
                "rpc_max_call_recv_size",
                r.max_recv_size,
                "Override default MaxCallRecvMsgSize for the gRPC client and the HTTP RPC response body limit, 16 MiB by default (bytes)",
                |s, v| s.c.rpc.max_recv_size = v,
            )),
            hidden(int_flag(
                "rpc_max_call_send_size",
                r.max_send_size,
                "Override default MaxCallSendMsgSize for RPC client (bytes)",
                |s, v| s.c.rpc.max_send_size = v,
            )),
            int_flag("rpc_request_timeout", r.request_timeout, "RPC requests timeout (in ms)", |s, v| {
                s.c.rpc.request_timeout = v
            }),
            hidden(int_flag("rpc_command_timeout", r.command_timeout, "RPC commands timeout (in ms)", |s, v| {
                s.c.rpc.command_timeout = v
            })),
            int_flag(
                "rpc_wait_timeout",
                r.wait_timeout,
                "Max time to wait in an RPC queue (in ms); zero means no limit",
                |s, v| s.c.rpc.wait_timeout = v,
            ),
            str_flag("headers", &r.proxy_headers.join(","), "List of headers to proxy to RPC", |s, v| s.headers = v),
            str_flag("proxy-cookies", "", "Cookie keys to send to RPC, default is all", |s, v| s.cookie_filter = v),
            hidden(str_flag("rpc_impl", &r.implementation, "RPC implementation (grpc, http)", |s, v| {
                s.c.rpc.implementation = v
            })),
            str_flag("http_rpc_secret", &r.secret, "Authentication secret for RPC over HTTP", |s, v| {
                s.c.rpc.secret = v
            }),
            hidden(int_flag(
                "http_rpc_timeout",
                r.http_request_timeout,
                "[DEPRECATED] HTTP RPC timeout (in ms)",
                |s, v| s.c.rpc.http_request_timeout = v,
            )),
        ],
    )
}

fn disconnector_flags(c: &Config) -> Vec<Flag> {
    let q = &c.disconnect_queue;
    with_defaults(
        DISCONNECTOR,
        vec![
            str_flag(
                "disconnect_mode",
                &c.app.disconnect_mode,
                "Define when to call Disconnect callback (always, never, auto)",
                |s, v| s.c.app.disconnect_mode = v,
            ),
            hidden(int_flag("disconnect_rate", q.rate, "Max number of Disconnect calls per second", |s, v| {
                s.c.disconnect_queue.rate = v
            })),
            int_flag("disconnect_workers_num", q.workers_num, "The number of concurrent Disconnect calls", |s, v| {
                s.c.disconnect_queue.workers_num = v
            }),
            int_flag(
                "max_disconnect_backlog_size",
                q.max_backlog,
                "The max size of the disconnect requests queue; over limit discards new items",
                |s, v| s.c.disconnect_queue.max_backlog = v,
            ),
            hidden(int_flag(
                "disconnect_backlog_size",
                q.backlog,
                "DEPRECATED: The size of the channel's buffer for disconnect requests",
                |s, v| s.c.disconnect_queue.backlog = v,
            )),
            hidden(int_flag(
                "disconnect_timeout",
                q.shutdown_timeout,
                "[DEPRECATED] Graceful shutdown timeout (in seconds)",
                |s, v| s.c.disconnect_queue.shutdown_timeout = v,
            )),
            hidden(bool_flag("disable_disconnect", false, "Disable calling Disconnect callback", |s, v| {
                s.c.disconnector_disabled = v
            })),
        ],
    )
}

fn log_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        LOG,
        vec![
            str_flag("log_level", &c.log.log_level, "Set logging level (debug/info/warn/error)", |s, v| {
                s.c.log.log_level = v
            }),
            str_flag("log_format", &c.log.log_format, "Set logging format (text/json)", |s, v| s.c.log.log_format = v),
            bool_flag("debug", c.log.debug, "Enable debug mode (more verbose logging)", |s, v| s.c.log.debug = v),
        ],
    )
}

fn metrics_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        METRICS,
        vec![
            bool_flag("metrics_log", false, "Enable metrics logging (with info level)", |s, v| s.c.metrics.log = v),
            int_flag(
                "metrics_rotate_interval",
                c.metrics.rotate_interval,
                "Specify how often flush metrics to writers (logs, statsd) (in seconds)",
                |s, v| s.c.metrics.rotate_interval = v,
            ),
            hidden(int_flag(
                "metrics_log_interval",
                c.metrics.log_interval,
                "DEPRECATED. Specify how often flush metrics logs (in seconds)",
                |s, v| s.c.metrics.log_interval = v,
            )),
            str_flag(
                "metrics_log_filter",
                "",
                "Specify list of metrics to print to log (to reduce the output)",
                |s, v| s.metrics_filter = v,
            ),
            hidden(str_flag(
                "metrics_log_formatter",
                "",
                "Specify the path to custom Ruby formatter script (only supported on MacOS and Linux)",
                |s, v| s.c.metrics.log_formatter = v,
            )),
            str_flag("metrics_http", "", "Enable HTTP metrics endpoint at the specified path", |s, v| {
                s.c.metrics.http = v
            }),
            str_flag("metrics_host", "", "Server host for metrics endpoint", |s, v| s.c.metrics.host = v),
            int_flag(
                "metrics_port",
                0,
                "Server port for metrics endpoint, the same as for main server by default",
                |s, v| s.c.metrics.port = v,
            ),
            str_flag(
                "metrics_tags",
                "",
                "Comma-separated list of default (global) tags to add to every metric",
                |s, v| s.mtags = v,
            ),
            int_flag(
                "stats_refresh_interval",
                c.app.stats_refresh_interval,
                "How often to refresh the server stats (in seconds)",
                |s, v| s.c.app.stats_refresh_interval = v,
            ),
        ],
    )
}

fn ws_flags(c: &Config) -> Vec<Flag> {
    let w = &c.ws;
    let mut flags = with_defaults(
        WS,
        vec![
            hidden(int_flag(
                "read_buffer_size",
                w.read_buffer_size,
                "WebSocket connection read buffer size",
                |s, v| s.c.ws.read_buffer_size = v,
            )),
            hidden(int_flag(
                "write_buffer_size",
                w.write_buffer_size,
                "WebSocket connection write buffer size",
                |s, v| s.c.ws.write_buffer_size = v,
            )),
            hidden(int_flag("max_message_size", w.max_message_size, "Maximum size of a message in bytes", |s, v| {
                s.c.ws.max_message_size = v
            })),
            int_flag(
                "ws_write_timeout",
                w.write_timeout,
                "Maximum time to wait for a write operation to complete",
                |s, v| s.c.ws.write_timeout = v,
            ),
            hidden(bool_flag(
                "enable_ws_compression",
                false,
                "Enable experimental WebSocket per message compression",
                |s, v| s.c.ws.enable_compression = v,
            )),
        ],
    );
    // Upstream's `withDefaults` does not handle unsigned flags: this one has no
    // category and no environment variable, and keeps its plain destination
    // (written while parsing, so a configuration file overrides it).
    flags.insert(
        4,
        Flag::new(
            "ws_max_pending_size",
            Val::Uint(w.max_pending_size),
            "Maximum size (in bytes) of the write queue for a session before it's considered slow and disconnected (0 = unlimited)",
            Binding::Plain(Setter::Uint(|s, v| s.c.ws.max_pending_size = v)),
        ),
    );
    flags
}

fn ping_flags(c: &Config) -> Vec<Flag> {
    let a = &c.app;
    with_defaults(
        PING,
        vec![
            int_flag("ping_interval", a.ping_interval, "Action Cable ping interval (in seconds)", |s, v| {
                s.c.app.ping_interval = v
            }),
            hidden(str_flag(
                "ping_timestamp_precision",
                &a.ping_timestamp_precision,
                "Precision for timestamps in ping messages (s, ms, ns)",
                |s, v| s.c.app.ping_timestamp_precision = v,
            )),
            int_flag(
                "pong_timeout",
                a.pong_timeout,
                "How long to wait for a pong response before disconnecting the client (in seconds). Zero means no pongs required",
                |s, v| s.c.app.pong_timeout = v,
            ),
            bool_flag(
                "enable_native_pings",
                a.enable_native_ping,
                "Send native pings (e.g., WebSocket ping frames) along with application-level pings to keepalive clients using custom protocols",
                |s, v| s.c.app.enable_native_ping = v,
            ),
        ],
    )
}

fn jwt_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        JWT,
        vec![
            hidden(str_flag("jwt_id_key", "", "[Depracated]", |s, v| s.jwt_id_key = v)),
            str_flag("jwt_secret", "", "The encryption key used to verify JWT tokens", |s, v| s.c.jwt.secret = v),
            hidden(str_flag("jwt_id_param", "", "[Deprecated]", |s, v| s.jwt_id_param = v)),
            str_flag(
                "jwt_param",
                &c.jwt.param,
                "The name of a query string param or an HTTP header carrying a token",
                |s, v| s.c.jwt.param = v,
            ),
            hidden(bool_flag("jwt_id_enforce", false, "[Deprecated]", |s, v| s.jwt_id_enforce = v)),
            bool_flag("enforce_jwt", false, "Whether to enforce token presence for all connections", |s, v| {
                s.c.jwt.force = v
            }),
        ],
    )
}

fn signed_streams_flags(c: &Config) -> Vec<Flag> {
    let st = &c.streams;
    with_defaults(
        SIGNED_STREAMS,
        vec![
            str_flag("streams_secret", "", "Secret you use to sign stream names", |s, v| s.c.streams.secret = v),
            bool_flag("public_streams", false, "Enable public (unsigned) streams", |s, v| s.c.streams.public = v),
            bool_flag("streams_whisper", st.whisper, "Enable whispering for signed pub/sub streams", |s, v| {
                s.c.streams.whisper = v
            }),
            bool_flag("streams_presence", st.presence, "Enable presence for signed pub/sub streams", |s, v| {
                s.c.streams.presence = v
            }),
            bool_flag("turbo_streams", st.turbo, "Enable Turbo Streams support", |s, v| s.c.streams.turbo = v),
            bool_flag("cable_ready", st.cable_ready, "Enable Cable Ready support", |s, v| s.c.streams.cable_ready = v),
            hidden(str_flag("turbo_rails_key", "", "[Deprecated]", |s, v| s.turbo_rails_key = v)),
            str_flag("turbo_streams_secret", "", "A custom secret to verify Turbo Streams", |s, v| {
                s.c.streams.turbo_secret = v
            }),
            hidden(bool_flag(
                "turbo_rails_cleartext",
                false,
                "[DEPRECATED] Enable Turbo Streams fastlane without stream names signing",
                |s, v| s.turbo_rails_clear_text = v,
            )),
            hidden(str_flag("cable_ready_key", "", "[Deprecated]", |s, v| s.cable_ready_key = v)),
            str_flag("cable_ready_secret", "", "A custom secret to verify CableReady streams", |s, v| {
                s.c.streams.cable_ready_secret = v
            }),
            hidden(bool_flag(
                "cable_ready_cleartext",
                false,
                "[DEPRECATED] Enable Cable Ready fastlane without stream names signing",
                |s, v| s.cable_ready_clear_text = v,
            )),
        ],
    )
}

fn pusher_flags() -> Vec<Flag> {
    with_defaults(
        PUSHER,
        vec![
            str_flag("pusher_app_id", "", "Pusher application ID", |s, v| s.c.pusher.app_id = v),
            str_flag("pusher_secret", "", "Pusher secret", |s, v| s.c.pusher.secret = v),
            str_flag("pusher_app_key", "", "Pusher application key", |s, v| s.c.pusher.app_key = v),
            int_flag("pusher_api_port", 0, "Port for Pusher HTTP API (0 = use the main server port)", |s, v| {
                s.c.pusher.api_port = v
            }),
        ],
    )
}

fn statsd_flags(c: &Config) -> Vec<Flag> {
    let sd = &c.metrics.statsd;
    with_defaults(
        STATSD,
        vec![
            str_flag(
                "statsd_host",
                "",
                "Server host for metrics sent to statsd server in the format <host>:<port>",
                |s, v| s.c.metrics.statsd.host = v,
            ),
            str_flag("statsd_prefix", &sd.prefix, "Statsd metrics prefix", |s, v| s.c.metrics.statsd.prefix = v),
            hidden(int_flag(
                "statsd_max_packet_size",
                sd.max_packet_size,
                "Statsd client maximum UDP packet size",
                |s, v| s.c.metrics.statsd.max_packet_size = v,
            )),
            str_flag("statsd_tags_format", &sd.tag_format, r#"One of "datadog", "influxdb", or "graphite""#, |s, v| {
                s.c.metrics.statsd.tag_format = v
            }),
        ],
    )
}

fn sse_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        SSE,
        vec![
            bool_flag("sse", c.sse.enabled, "Enable SSE endpoint", |s, v| s.c.sse.enabled = v),
            str_flag("sse_path", &c.sse.path, "SSE endpoint path", |s, v| s.c.sse.path = v),
            bool_flag(
                "sse_comment_pings",
                c.sse.comment_pings,
                "Send pings as SSE comments instead of ping events",
                |s, v| s.c.sse.comment_pings = v,
            ),
        ],
    )
}

fn ds_flags(c: &Config) -> Vec<Flag> {
    let d = &c.ds;
    with_defaults(
        DS,
        vec![
            bool_flag("ds", d.enabled, "Enable Durable Streams endpoint", |s, v| s.c.ds.enabled = v),
            str_flag("ds_path", &d.path, "Durable Streams endpoint path", |s, v| s.c.ds.path = v),
            bool_flag(
                "ds_skip_auth",
                d.skip_auth,
                "Disable client authentication for Durable Streams (only authorize stream access)",
                |s, v| s.c.ds.skip_auth = v,
            ),
            int_flag("ds_poll_interval", d.poll_interval, "Durable Streams long polling interval (seconds)", |s, v| {
                s.c.ds.poll_interval = v
            }),
            int_flag("ds_sse_ttl", d.sse_ttl, "Durable Streams SSE connections time-to-live (seconds)", |s, v| {
                s.c.ds.sse_ttl = v
            }),
        ],
    )
}

fn api_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        API,
        vec![
            int_flag("api_port", i64::from(c.api.port), "API server port (0 = use main server port)", |s, v| {
                s.c.api.port = clamp(v, u16::MAX)
            }),
            str_flag("api_path", &c.api.path, "API endpoint base path", |s, v| s.c.api.path = v),
            str_flag("api_secret", "", "Secret token to authenticate API requests", |s, v| s.c.api.secret = v),
        ],
    )
}

fn misc_flags() -> Vec<Flag> {
    with_defaults(
        MISC,
        vec![str_flag(
            "presets",
            "",
            "Configuration presets, comma-separated (none, fly, heroku, broker). Inferred automatically",
            |s, v| s.presets = v,
        )],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(args: &[&str], env: &[(&str, &str)]) -> (Result<(Config, bool), CliError>, String) {
        let env: EnvMap = env.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let mut out = Vec::new();
        let mut full = vec!["cabby", "--ignore-config-path"];
        full.extend_from_slice(args);
        let res = new_config_from_cli_to(&full, &env, vec![], &mut out);
        (res, String::from_utf8(out).unwrap())
    }

    #[test]
    fn cli_overrides_env_and_env_overrides_defaults() {
        let (res, _) = run(&["--port=1"], &[("ANYCABLE_PORT", "2"), ("PORT", "3")]);
        assert_eq!(res.unwrap().0.server.port, 1);
        let (res, _) = run(&[], &[("PORT", "3")]);
        assert_eq!(res.unwrap().0.server.port, 3);
        let (res, _) = run(&[], &[("ANYCABLE_PORT", "2"), ("PORT", "3")]);
        assert_eq!(res.unwrap().0.server.port, 2);
        let (res, _) = run(&[], &[("CABBY_PORT", "4"), ("ANYCABLE_PORT", "2"), ("PORT", "3")]);
        assert_eq!(res.unwrap().0.server.port, 4);
        let (res, _) = run(&["--port=1"], &[("CABBY_PORT", "4")]);
        assert_eq!(res.unwrap().0.server.port, 1);
    }

    #[test]
    fn flag_syntax_follows_go() {
        let (res, _) = run(&["-port", "9", "-debug", "--host=h", "--", "--port=1"], &[]);
        let c = res.unwrap().0;
        assert_eq!((c.server.port, c.log.debug, c.server.host.as_str()), (9, true, "h"));
        assert_eq!(c.log.log_level, "debug");
    }

    #[test]
    fn unknown_flag_shows_usage() {
        let (res, out) = run(&["--bogus"], &[]);
        assert_eq!(res.unwrap_err().0, "flag provided but not defined: -bogus");
        assert!(out.starts_with("Incorrect Usage: flag provided but not defined: -bogus\n\nNAME:\n"), "{out}");
    }

    #[test]
    fn invalid_values_have_go_texts() {
        let (res, _) = run(&["--port=x"], &[]);
        assert_eq!(res.unwrap_err().0, "invalid value \"x\" for flag -port: parse error");
        let (res, _) = run(&["--debug=yes"], &[]);
        assert_eq!(res.unwrap_err().0, "invalid boolean value \"yes\" for -debug: parse error");
        let (res, _) = run(&["--host"], &[]);
        assert_eq!(res.unwrap_err().0, "flag needs an argument: -host");
        let (res, _) = run(&[], &[("ANYCABLE_PORT", "x")]);
        assert_eq!(
            res.unwrap_err().0,
            "could not parse \"x\" as int value from environment variable \"ANYCABLE_PORT\" for flag port: strconv.ParseInt: parsing \"x\": invalid syntax"
        );
    }

    #[test]
    fn empty_env_values() {
        // Empty string values are set; empty numeric values are ignored.
        let (res, _) = run(&[], &[("ANYCABLE_HOST", ""), ("ANYCABLE_PORT", "")]);
        let c = res.unwrap().0;
        assert_eq!((c.server.host.as_str(), c.server.port), ("", 8080));
    }

    /// Pins D-059: `--version`, `--help` and deprecation warnings name Cabby.
    #[test]
    fn deviation_d059_help_and_version_name_cabby() {
        let (res, out) = run(&["-v"], &[]);
        assert!(res.unwrap().1);
        assert_eq!(out, format!("cabby {} (AnyCable 1.6.21 compatible)\n", env!("CARGO_PKG_VERSION")));
        let (res, out) = run(&["--help"], &[]);
        assert!(res.unwrap().1);
        assert!(
            out.starts_with(
                "NAME:\n   cabby - Cabby, a real-time server for Rails and beyond\n\nUSAGE:\n   cabby [global options]\n"
            ),
            "{out}"
        );
        assert!(out.contains("\n   SERVER:\n\n"), "{out}");
        assert!(!out.to_lowercase().contains("anycable-go"), "{out}");
        let (res, _) = run(&["-h", "--help"], &[]);
        assert_eq!(res.unwrap_err().0, "Cannot use two forms of the same flag: h help");
        let (_, out) = run(&["--turbo_rails_key=k"], &[]);
        assert!(out.contains("and will be removed in a future release of Cabby.\n"), "{out}");
    }

    #[test]
    fn deprecated_flags_warn() {
        let (res, out) = run(&["--turbo_rails_key=k", "--disable_disconnect"], &[]);
        let c = res.unwrap().0;
        assert!(c.streams.turbo);
        assert_eq!(c.streams.turbo_secret, "k");
        assert_eq!(c.app.disconnect_mode, "never");
        assert!(out.contains("DEPRECATION WARNING: turbo_rails_key option is deprecated\n"), "{out}");
    }

    #[test]
    fn secrets_fall_back_and_none_is_empty() {
        let (res, _) = run(&["--secret=s", "--http_rpc_secret=none"], &[]);
        let c = res.unwrap().0;
        assert_eq!(c.jwt.secret, "s");
        assert_eq!(c.streams.secret, "s");
        assert_eq!(c.http_broadcast.secret_base, "s");
        assert_eq!(c.http_broadcast.port, 8080);
        assert_eq!(c.rpc.secret, "");
        // As upstream: "none" reaches the secret bases before secrets are nullified,
        // so HTTP broadcasting still counts as secured.
        let (res, _) = run(&["--secret=none"], &[]);
        let c = res.unwrap().0;
        assert_eq!((c.secret.as_str(), c.jwt.secret.as_str()), ("", ""));
        assert_eq!((c.http_broadcast.secret_base.as_str(), c.http_broadcast.port), ("none", 8080));
        let (res, _) = run(&[], &[]);
        assert_eq!(res.unwrap().0.http_broadcast.port, 8090);
    }

    #[test]
    fn headers_and_cookies() {
        let (res, _) = run(&["--headers=Cookie,X-Api-Key", "--proxy-cookies=a,b"], &[]);
        let c = res.unwrap().0;
        assert_eq!(c.rpc.proxy_headers, vec!["cookie", "x-api-key"]);
        assert_eq!(c.rpc.proxy_cookies, vec!["a", "b"]);
    }

    /// Pins D-019: values outside the narrower Rust setting types are clamped.
    #[test]
    fn deviation_d019_narrow_settings_are_clamped() {
        let (res, _) = run(&["--api_port=70000", "--history_limit=-1"], &[]);
        let c = res.unwrap().0;
        assert_eq!((c.api.port, c.broker.history_limit), (u16::MAX, 0));
        let (res, _) = run(&[], &[("ANYCABLE_API_PORT", "-5")]);
        assert_eq!(res.unwrap().0.api.port, 0);
    }

    /// Pins D-018: a tag without a value stops the program with a message.
    #[test]
    #[should_panic(expected = "invalid metrics tag \"env\": expected key:value")]
    fn deviation_d018_malformed_metrics_tag_panics_with_a_message() {
        parse_tags("env");
    }

    #[test]
    fn tags() {
        let tags = parse_tags("env:dev,rev:1.1");
        assert_eq!(tags.get("env").map(String::as_str), Some("dev"));
        assert_eq!(tags.get("rev").map(String::as_str), Some("1.1"));
    }

    #[test]
    fn sqlite_path_from_the_flag_and_the_cabby_env_var() {
        let (res, _) = run(&["--broker=sqlite", "--sqlite_path=/data/a.db"], &[]);
        let c = res.unwrap().0;
        assert_eq!(("sqlite", "/data/a.db"), (c.broker.adapter.as_str(), c.sqlite.path.as_str()));

        let (res, _) = run(&[], &[("CABBY_SQLITE_PATH", "/data/b.db")]);
        assert_eq!("/data/b.db", res.unwrap().0.sqlite.path);

        let (res, _) = run(&["--sqlite_path=/data/c.db"], &[("CABBY_SQLITE_PATH", "/data/b.db")]);
        assert_eq!("/data/c.db", res.unwrap().0.sqlite.path, "the command line wins");

        let (res, _) = run(&[], &[("ANYCABLE_SQLITE_PATH", "/data/d.db")]);
        assert_eq!("", res.unwrap().0.sqlite.path, "no ANYCABLE_* alias");
    }

    #[test]
    fn sqlite_path_from_the_config_file_unless_overridden() {
        let file = std::env::temp_dir().join(format!("cabby-sqlite-options-{}.toml", std::process::id()));
        std::fs::write(&file, "[sqlite]\npath = \"/data/file.db\"\n").unwrap();
        let path = file.to_str().unwrap();
        let mut out = Vec::new();

        let (c, _) =
            new_config_from_cli_to(&["cabby", "--config-path", path], &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!("/data/file.db", c.sqlite.path);

        let args = ["cabby", "--config-path", path, "--sqlite_path=/data/flag.db"];
        let (c, _) = new_config_from_cli_to(&args, &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!("/data/flag.db", c.sqlite.path);
        std::fs::remove_file(&file).unwrap();
    }

    #[test]
    fn sqlite_path_shows_in_help_with_its_env_var() {
        let (_, out) = run(&["--help"], &[]);
        assert!(out.contains("SQLITE:"), "{out}");
        assert!(out.contains("--sqlite_path value") && out.contains("[$CABBY_SQLITE_PATH]"), "{out}");
    }

    #[test]
    fn postgres_settings_from_flags_and_cabby_env_vars() {
        let (res, _) = run(
            &[
                "--pubsub=postgres",
                "--broadcast_adapter=http,postgres",
                "--broker=postgres",
                "--postgres_url=postgres://db/app",
                "--postgres_schema=rt",
                "--postgres_table_prefix=x_",
                "--postgres_channel=rt_chan",
                "--postgres_auto_migrate=false",
                "--postgres_keepalive_interval=5",
                "--postgres_tls_verify=false",
                "--postgres_tls_ca_cert_path=/ca.pem",
                "--postgres_unlogged=false",
            ],
            &[],
        );
        let c = res.unwrap().0;
        assert_eq!(("postgres", "postgres"), (c.pubsub_adapter.as_str(), c.broker.adapter.as_str()));
        assert_eq!(vec!["http".to_owned(), "postgres".to_owned()], c.broadcast_adapters);
        let p = &c.postgres;
        assert_eq!(
            ("postgres://db/app", "rt", "x_", "rt_chan"),
            (p.url.as_str(), p.schema.as_str(), p.table_prefix.as_str(), p.channel.as_str())
        );
        assert_eq!(
            (false, 5, false, "/ca.pem", false),
            (p.auto_migrate, p.keepalive_interval, p.tls_verify, p.tls_ca_cert_path.as_str(), p.unlogged)
        );

        let (res, _) =
            run(&[], &[("CABBY_POSTGRES_URL", "postgres://env/app"), ("CABBY_POSTGRES_AUTO_MIGRATE", "false")]);
        let c = res.unwrap().0;
        assert_eq!(("postgres://env/app", false), (c.postgres.url.as_str(), c.postgres.auto_migrate));

        let (res, _) = run(&[], &[("ANYCABLE_POSTGRES_URL", "postgres://env/app")]);
        assert_eq!("", res.unwrap().0.postgres.url, "no ANYCABLE_* alias");

        let (res, _) = run(&[], &[]);
        assert_eq!(crate::postgres::PostgresConfig::new(), res.unwrap().0.postgres, "defaults");
    }

    #[test]
    fn postgres_settings_from_the_config_file_unless_overridden() {
        let file = std::env::temp_dir().join(format!("cabby-postgres-options-{}.toml", std::process::id()));
        std::fs::write(&file, "[postgres]\nurl = \"postgres://file/app\"\nchannel = \"from_file\"\n").unwrap();
        let path = file.to_str().unwrap();
        let mut out = Vec::new();

        let (c, _) =
            new_config_from_cli_to(&["cabby", "--config-path", path], &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!(("postgres://file/app", "from_file"), (c.postgres.url.as_str(), c.postgres.channel.as_str()));

        let args = ["cabby", "--config-path", path, "--postgres_channel=from_flag"];
        let (c, _) = new_config_from_cli_to(&args, &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!("from_flag", c.postgres.channel);
        std::fs::remove_file(&file).unwrap();
    }

    #[test]
    fn postgres_settings_show_in_help() {
        let (_, out) = run(&["--help"], &[]);
        assert!(out.contains("POSTGRES:"), "{out}");
        assert!(out.contains("--postgres_url value") && out.contains("[$CABBY_POSTGRES_URL]"), "{out}");
        assert!(out.contains("Pub/Sub adapter to use (redis, nats or postgres)"), "{out}");
        assert!(out.contains("Broadcasting adapter to use (http, redisx, redis, nats or postgres)"), "{out}");
    }

    /// Pins D-070, D-071: `--presets=broker` keeps explicit Postgres adapters
    /// and picks nothing Postgres on its own.
    #[test]
    fn explicit_postgres_adapters_survive_the_broker_preset() {
        let (res, _) = run(
            &["--presets=broker", "--broker=postgres", "--pubsub=postgres", "--broadcast_adapter=postgres"],
            &[("CABBY_POSTGRES_URL", "postgres://db/app")],
        );
        let mut c = res.unwrap().0;
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!(("postgres", "postgres"), (c.broker.adapter.as_str(), c.pubsub_adapter.as_str()));
        assert_eq!(vec!["postgres".to_owned()], c.broadcast_adapters);

        let (res, _) = run(&["--presets=broker"], &[("CABBY_POSTGRES_URL", "postgres://db/app")]);
        let mut c = res.unwrap().0;
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!(
            ("memory", ""),
            (c.broker.adapter.as_str(), c.pubsub_adapter.as_str()),
            "a Postgres URL alone changes no preset"
        );
        assert_eq!(vec!["http".to_owned()], c.broadcast_adapters);
    }

    #[test]
    fn explicit_sqlite_broker_survives_the_broker_preset() {
        let (res, _) = run(&["--presets=broker", "--broker=sqlite", "--sqlite_path=/data/cabby.db"], &[]);
        let mut c = res.unwrap().0;
        assert_eq!(Some(vec!["broker".to_owned()]), c.user_presets);
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("sqlite", c.broker.adapter);
    }

    /// Pins D-069: `--broker=redis` without `--pubsub` selects Redis pub/sub,
    /// as upstream's option parsing does; an explicit `--pubsub` wins.
    #[test]
    fn deviation_d069_redis_broker_selects_redis_pubsub() {
        let (res, _) = run(&["--broker=redis"], &[]);
        let c = res.unwrap().0;
        assert_eq!(("redis", "redis"), (c.broker.adapter.as_str(), c.pubsub_adapter.as_str()));

        let (res, _) = run(&["--broker=redis", "--pubsub=nats"], &[]);
        assert_eq!("nats", res.unwrap().0.pubsub_adapter);

        let (res, _) = run(&[], &[("CABBY_BROKER", "redis")]);
        assert_eq!("redis", res.unwrap().0.pubsub_adapter);
    }

    /// Pins D-069: the Redis broker's settings are Cabby-only (no `ANYCABLE_*` alias).
    #[test]
    fn deviation_d069_redis_broker_flags_and_cabby_env() {
        let (res, _) = run(&["--redis_broker_key_prefix=app1", "--redis_broker_timeout=9"], &[]);
        let c = res.unwrap().0;
        assert_eq!(("app1", 9), (c.redis_broker.key_prefix.as_str(), c.redis_broker.timeout));

        let (res, _) = run(&[], &[]);
        let c = res.unwrap().0;
        assert_eq!(("__cabby__", 5), (c.redis_broker.key_prefix.as_str(), c.redis_broker.timeout));

        let env = [("CABBY_REDIS_BROKER_KEY_PREFIX", "app2"), ("CABBY_REDIS_BROKER_TIMEOUT", "3")];
        let (res, _) = run(&[], &env);
        let c = res.unwrap().0;
        assert_eq!(("app2", 3), (c.redis_broker.key_prefix.as_str(), c.redis_broker.timeout));

        let (res, _) = run(&["--redis_broker_key_prefix=app3"], &env);
        assert_eq!("app3", res.unwrap().0.redis_broker.key_prefix, "the command line wins");

        let (res, _) = run(&[], &[("ANYCABLE_REDIS_BROKER_KEY_PREFIX", "app4")]);
        assert_eq!("__cabby__", res.unwrap().0.redis_broker.key_prefix, "no ANYCABLE_* alias");
    }

    #[test]
    fn redis_broker_flags_show_in_help() {
        let (_, out) = run(&["--help"], &[]);
        assert!(out.contains("REDIS BROKER:"), "{out}");
        assert!(
            out.contains("--redis_broker_key_prefix value") && out.contains("[$CABBY_REDIS_BROKER_KEY_PREFIX]"),
            "{out}"
        );
        assert!(out.contains("--redis_broker_timeout value") && out.contains("[$CABBY_REDIS_BROKER_TIMEOUT]"), "{out}");
        assert!(out.contains("Broker engine to use (memory, nats, sqlite, redis or postgres)"), "{out}");
    }

    #[test]
    fn explicit_redis_broker_survives_the_broker_preset() {
        let (res, _) = run(&["--presets=broker", "--broker=redis"], &[]);
        let mut c = res.unwrap().0;
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("redis", c.broker.adapter);
    }

    /// Pins D-061: help lists Cabby's variable names and the unprefixed aliases,
    /// and explains the `ANYCABLE_*` fallback once.
    #[test]
    fn deviation_d061_help_lists_cabby_names() {
        let (_, out) = run(&["--help"], &[]);
        assert!(
            out.contains("   --port value              Server port (default: 8080) [$CABBY_PORT, $PORT]\n"),
            "{out}"
        );
        assert!(!out.contains("$ANYCABLE_"), "{out}");
        assert!(out.ends_with(&format!("\n{ENV_FOOTER}\n")), "{out}");
        assert!(
            ENV_FOOTER.starts_with("Every CABBY_* variable can also be set as ANYCABLE_* (CABBY_* takes precedence)")
        );
    }

    /// Pins D-061: the help footer names every Cabby-only variable, the flags
    /// that have no `ANYCABLE_*` name (D-065, D-069).
    #[test]
    fn deviation_d061_cabby_only_variables_are_named_in_help() {
        let cabby_only: Vec<String> = definitions(&Config::new())
            .into_iter()
            .filter(|f| !f.env_vars.iter().any(|v| v.starts_with("ANYCABLE_")))
            .flat_map(|f| f.env_vars.into_iter().filter(|v| v.starts_with("CABBY_")))
            .collect();
        let postgres = [
            "CABBY_POSTGRES_URL",
            "CABBY_POSTGRES_SCHEMA",
            "CABBY_POSTGRES_TABLE_PREFIX",
            "CABBY_POSTGRES_CHANNEL",
            "CABBY_POSTGRES_AUTO_MIGRATE",
            "CABBY_POSTGRES_KEEPALIVE_INTERVAL",
            "CABBY_POSTGRES_TLS_VERIFY",
            "CABBY_POSTGRES_TLS_CA_CERT_PATH",
            "CABBY_POSTGRES_UNLOGGED",
        ];
        let expected: Vec<&str> = ["CABBY_SQLITE_PATH", "CABBY_REDIS_BROKER_KEY_PREFIX", "CABBY_REDIS_BROKER_TIMEOUT"]
            .into_iter()
            .chain(postgres)
            .collect();
        assert_eq!(cabby_only, expected);
        for v in &cabby_only {
            let named = ENV_FOOTER.contains(v.as_str())
                || (v.starts_with("CABBY_POSTGRES_") && ENV_FOOTER.contains("CABBY_POSTGRES_*"));
            assert!(named, "{v} is not in the help footer: {ENV_FOOTER}");
        }
    }

    /// Pins D-061: `CABBY_*` wins over `ANYCABLE_*` and the unprefixed alias; two
    /// different prefixed values are noted once per setting, without the values.
    #[test]
    fn deviation_d061_cabby_env_wins_and_conflicts_are_noted() {
        let conflict = |name: &str| StartupNote::EnvConflict {
            cabby: format!("CABBY_{name}"),
            anycable: format!("ANYCABLE_{name}"),
        };
        let (res, _) = run(&[], &[("CABBY_PORT", "1"), ("ANYCABLE_PORT", "2"), ("PORT", "3")]);
        let c = res.unwrap().0;
        assert_eq!(c.server.port, 1);
        assert_eq!(c.startup_notes, vec![conflict("PORT")]);

        // Equal values, or an unprefixed alias: nothing to note.
        let (res, _) = run(&[], &[("CABBY_SECRET", "s"), ("ANYCABLE_SECRET", "s")]);
        let c = res.unwrap().0;
        assert_eq!((c.secret.as_str(), c.startup_notes.len()), ("s", 0));
        let (res, _) = run(&[], &[("CABBY_REDIS_URL", "redis://a:6379"), ("REDIS_URL", "redis://b:6379")]);
        let c = res.unwrap().0;
        assert_eq!((c.redis.url.as_str(), c.startup_notes.len()), ("redis://a:6379", 0));

        // Settings read outside flags are noted too.
        let (res, _) = run(&[], &[("CABBY_FILTER_LOGS", "false"), ("ANYCABLE_FILTER_LOGS", "true")]);
        assert_eq!(res.unwrap().0.startup_notes, vec![conflict("FILTER_LOGS")]);

        // The command line still wins over the environment, and then neither
        // variable is used: nothing to note.
        let (res, _) = run(&["--port=4"], &[("CABBY_PORT", "1")]);
        assert_eq!(res.unwrap().0.server.port, 4);
        let (res, _) = run(&["--port=4"], &[("CABBY_PORT", "1"), ("ANYCABLE_PORT", "2")]);
        let c = res.unwrap().0;
        assert_eq!((c.server.port, c.startup_notes.len()), (4, 0));

        // Values may be secrets: notes never carry them.
        let (res, _) = run(&[], &[("CABBY_SECRET", "cabby-secret-1"), ("ANYCABLE_SECRET", "anycable-secret-2")]);
        let notes = format!("{:?}", res.unwrap().0.startup_notes);
        assert!(!notes.contains("secret-1") && !notes.contains("secret-2"), "{notes}");
    }

    /// Pins D-061: with only `ANYCABLE_*` variables, nothing changes.
    #[test]
    fn deviation_d061_anycable_env_still_works() {
        let (res, _) = run(&[], &[("ANYCABLE_PORT", "2"), ("PORT", "3"), ("ANYCABLE_SECRET", "s")]);
        let c = res.unwrap().0;
        assert_eq!((c.server.port, c.secret.as_str()), (2, "s"));
        assert!(c.startup_notes.is_empty());
        let (res, _) = run(&[], &[("ANYCABLE_STRICT_CONFIG", "x")]);
        assert_eq!(
            res.unwrap_err().0,
            "could not parse \"x\" as bool value from environment variable \"ANYCABLE_STRICT_CONFIG\" for flag strict-config: strconv.ParseBool: parsing \"x\": invalid syntax"
        );
    }

    /// Pins D-061: no flag reads a variable the server or its test harness reserves.
    #[test]
    fn no_flag_reads_a_reserved_variable() {
        let reserved = [
            "CABBY_NATS_SERVER",
            "CABBY_NATS_SERVER_BIN",
            "CABBY_BACKEND",
            "CABBY_GO_BRIDGE",
            "CABBY_GO_MRUBY_BRIDGE",
            "CABBY_BRIDGE_LOG",
            "CABBY_BENCHI_SHIM",
        ];
        for f in definitions(&Config::new()) {
            for v in &f.env_vars {
                assert!(
                    !reserved.contains(&v.as_str()) && !v.starts_with("CABBY_TEST_") && !v.starts_with("CABBY_BENCH_"),
                    "flag {} reads the reserved variable {v}",
                    f.name
                );
            }
        }
    }

    /// A fresh directory for configuration-file tests, removed on drop.
    struct TempTree(PathBuf);

    impl TempTree {
        fn new(tag: &str) -> Self {
            static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let dir = std::env::temp_dir().join(format!("cabby-{tag}-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("temp dir");
            Self(dir)
        }

        fn search(&self) -> ConfigSearch {
            ConfigSearch { cwd: self.0.join("app"), etc: self.0.join("etc") }
        }

        /// Writes a configuration file whose `secret` names the file.
        fn write(&self, path: &std::path::Path) {
            std::fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
            std::fs::write(path, format!("secret = \"{}\"\n", path.display())).expect("config file");
        }
    }

    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Pins D-060: `./cabby.toml`, `/etc/cabby/cabby.toml`, `./anycable.toml`,
    /// `/etc/anycable/anycable.toml`; the first that exists is loaded and the
    /// others are noted as ignored. Every subset of the four files.
    #[test]
    fn deviation_d060_cabby_toml_is_found_first() {
        for subset in 0u8..16 {
            let tree = TempTree::new("d060");
            let search = tree.search();
            let candidates = search.candidates();
            let present: Vec<&PathBuf> =
                candidates.iter().enumerate().filter(|(i, _)| subset & (1 << i) != 0).map(|(_, p)| p).collect();
            for p in &present {
                tree.write(p);
            }

            let mut out = Vec::new();
            let (c, _) =
                new_config_from_cli_with(&["cabby"], &EnvMap::new(), vec![], &mut out, &search).expect("config");
            match present.split_first() {
                None => {
                    assert_eq!(c.config_file_path, None, "subset {subset:04b}");
                    assert!(c.startup_notes.is_empty(), "subset {subset:04b}");
                }
                Some((used, ignored)) => {
                    assert_eq!(c.secret, used.display().to_string(), "subset {subset:04b}");
                    let want: Vec<StartupNote> = ignored
                        .iter()
                        .map(|p| StartupNote::IgnoredConfigFile {
                            path: p.display().to_string(),
                            used: used.display().to_string(),
                        })
                        .collect();
                    assert_eq!(c.startup_notes, want, "subset {subset:04b}");
                }
            }
        }
    }

    /// Pins D-060: `--config-path` and `--ignore-config-path` are unchanged and
    /// leave nothing to note.
    #[test]
    fn deviation_d060_explicit_paths_win() {
        let tree = TempTree::new("d060-explicit");
        let search = tree.search();
        let [cabby, _, anycable, _] = search.candidates();
        tree.write(&cabby);
        tree.write(&anycable);
        let anycable_path = anycable.display().to_string();

        let mut out = Vec::new();
        let (c, _) = new_config_from_cli_with(
            &["cabby", "--config-path", &anycable_path],
            &EnvMap::new(),
            vec![],
            &mut out,
            &search,
        )
        .expect("config");
        assert_eq!((c.secret.as_str(), c.startup_notes.len()), (anycable_path.as_str(), 0));

        let (c, _) =
            new_config_from_cli_with(&["cabby", "--ignore-config-path"], &EnvMap::new(), vec![], &mut out, &search)
                .expect("config");
        assert_eq!((c.config_file_path, c.startup_notes.len()), (None, 0));
    }
}
