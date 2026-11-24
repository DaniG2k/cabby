//! PostgreSQL adapters, a Cabby addition (deviations D-070 and D-071): cross-node
//! pub/sub (`--pubsub=postgres`), broadcast ingestion from SQL
//! (`--broadcast_adapter=postgres`) and a shared durable broker
//! (`--broker=postgres`), so a multi-node deployment needs no Redis or NATS.
//!
//! - [`PostgresConfig`] is the `[postgres]` section (`--postgres_*`,
//!   `CABBY_POSTGRES_*`; Cabby only, so there are no `ANYCABLE_*` names).
//! - [`Names`] holds the validated schema, table prefix and channel: every
//!   identifier is checked against `^[A-Za-z_][A-Za-z0-9_]*$` and the 63-byte
//!   limit before it is written (quoted) into SQL or `LISTEN`; every value goes
//!   through a query parameter.
//! - [`schema`] creates the tables (`auto_migrate`) or checks that they exist.
//! - [`connect`] has the runtime, TLS, the command pool and the `LISTEN`
//!   connection (heartbeat, reconnect, re-`LISTEN`).
//!
//! Connection strings are never logged: log lines and errors describe the
//! server as `host:port/database` ([`connect::ConnectSpec::description`]).

use std::sync::Arc;
use std::time::Duration;

use cabby_core::toml_config::{self, go_quote};

use crate::metrics::Instrumenter;

pub mod broadcast;
pub mod broker;
pub(crate) mod connect;
pub mod pubsub;
pub(crate) mod schema;

pub use broadcast::PostgresBroadcaster;
pub use broker::PostgresBroker;
pub use pubsub::PostgresSubscriber;

/// The longest identifier PostgreSQL keeps (`NAMEDATALEN - 1`).
pub const MAX_IDENTIFIER: usize = 63;

/// The longest suffix appended to the table prefix (`pubsub_overflow_expires_at`,
/// an index name), so the prefix may use what is left of [`MAX_IDENTIFIER`].
const LONGEST_TABLE_SUFFIX: usize = "pubsub_overflow_expires_at".len();

/// The longest suffix appended to the channel (`_cmd`).
const LONGEST_CHANNEL_SUFFIX: usize = "_cmd".len();

/// Number of `LISTEN` channels streams are hashed into (`<channel>_0` ..
/// `<channel>_15`).
pub const SHARDS: usize = 16;

/// `[postgres]`: the PostgreSQL connection and the names Cabby uses in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresConfig {
    /// Connection URI (`postgres://user:password@host:5432/db?sslmode=require`)
    /// or libpq key/value string. Required by every Postgres adapter.
    pub url: String,
    /// Schema holding Cabby's tables.
    pub schema: String,
    /// Prefix of Cabby's table names.
    pub table_prefix: String,
    /// Base `LISTEN`/`NOTIFY` channel name.
    pub channel: String,
    /// Create the tables at startup; with `false` they must exist.
    pub auto_migrate: bool,
    /// Seconds between `SELECT 1` heartbeats on the `LISTEN` connection (0: none).
    pub keepalive_interval: i64,
    /// Verify the server certificate (and host name) when TLS is used.
    pub tls_verify: bool,
    /// PEM file with the CA certificates to trust (empty: the system roots).
    pub tls_ca_cert_path: String,
    /// Create the ephemeral tables (`meta`, `streams`, `history`, `sessions`,
    /// `pubsub_overflow`) as `UNLOGGED`.
    pub unlogged: bool,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self::new()
    }
}

cabby_core::toml_section!(PostgresConfig, "postgres.Config" {
    "url" => url,
    "schema" => schema,
    "table_prefix" => table_prefix,
    "channel" => channel,
    "auto_migrate" => auto_migrate,
    "keepalive_interval" => keepalive_interval,
    "tls_verify" => tls_verify,
    "tls_ca_cert_path" => tls_ca_cert_path,
    "unlogged" => unlogged,
});

impl PostgresConfig {
    pub fn new() -> Self {
        Self {
            url: String::new(),
            schema: "public".to_owned(),
            table_prefix: "cabby_".to_owned(),
            channel: "cabby_internal".to_owned(),
            auto_migrate: true,
            keepalive_interval: 30,
            tls_verify: true,
            tls_ca_cert_path: String::new(),
            unlogged: true,
        }
    }

    /// Renders the annotated TOML section. The URL is commented out when
    /// empty and printed as configured otherwise (`--print-config` prints
    /// every secret it is given, as it does for `redis_url`).
    pub fn to_toml(&self) -> String {
        let mut out = String::from(
            "# Connection URL for the Postgres adapters (--pubsub, --broadcast_adapter or --broker=postgres)\n",
        );
        if self.url.is_empty() {
            out.push_str("# url = \"postgres://cabby:secret@localhost:5432/app?sslmode=require\"\n");
        } else {
            out.push_str(&format!("url = {}\n", go_quote(&self.url)));
        }
        out.push_str("# Schema for Cabby's tables\n");
        out.push_str(&format!("schema = {}\n", go_quote(&self.schema)));
        out.push_str("# Prefix for Cabby's table names\n");
        out.push_str(&format!("table_prefix = {}\n", go_quote(&self.table_prefix)));
        out.push_str("# Base LISTEN/NOTIFY channel name\n");
        out.push_str(&format!("channel = {}\n", go_quote(&self.channel)));
        out.push_str("# Create Cabby's tables at startup (false: they must exist, see docs/postgres.md)\n");
        out.push_str(&format!("auto_migrate = {}\n", self.auto_migrate));
        out.push_str("# Heartbeat interval of the LISTEN connection (seconds, 0 to disable)\n");
        out.push_str(&format!("keepalive_interval = {}\n", self.keepalive_interval));
        out.push_str("# Verify the server's TLS certificate\n");
        out.push_str(&format!("tls_verify = {}\n", self.tls_verify));
        out.push_str("# CA certificates (PEM file) to verify the server with (default: system roots)\n");
        if self.tls_ca_cert_path.is_empty() {
            out.push_str("# tls_ca_cert_path = \"/etc/ssl/certs/postgres-ca.pem\"\n");
        } else {
            out.push_str(&format!("tls_ca_cert_path = {}\n", go_quote(&self.tls_ca_cert_path)));
        }
        out.push_str(
            "# Create the history, sessions and pub/sub tables as UNLOGGED (faster; emptied after a database crash)\n",
        );
        out.push_str(&format!("unlogged = {}\n", self.unlogged));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values.
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), String> {
        toml_config::decode(self, toml).map(drop).map_err(|e| e.0)
    }

    /// The heartbeat interval (`None`: disabled).
    pub(crate) fn keepalive(&self) -> Option<Duration> {
        u64::try_from(self.keepalive_interval).ok().filter(|s| *s > 0).map(Duration::from_secs)
    }
}

/// A Postgres adapter failure; the text never contains credentials.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PgError(pub String);

impl From<tokio_postgres::Error> for PgError {
    fn from(e: tokio_postgres::Error) -> Self {
        PgError(error_text(&e))
    }
}

/// A driver error as text: the server's message (with its detail) for
/// database errors, otherwise the driver's description and its cause.
pub(crate) fn error_text(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        return match db.detail() {
            Some(detail) => format!("{}: {} ({detail})", db.severity(), db.message()),
            None => format!("{}: {}", db.severity(), db.message()),
        };
    }
    match std::error::Error::source(e) {
        Some(cause) => format!("{e}: {cause}"),
        None => e.to_string(),
    }
}

/// Whether `value` is a valid identifier: `^[A-Za-z_][A-Za-z0-9_]*$`, at most
/// `max` bytes.
pub fn is_identifier(value: &str, max: usize) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else { return false };
    value.len() <= max
        && (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn invalid(flag: &str, value: &str, max: usize, allow_empty: bool) -> String {
    let empty = if allow_empty { " (or empty)" } else { "" };
    format!(
        "invalid --{flag} {}: must match ^[A-Za-z_][A-Za-z0-9_]*${empty} and be at most {max} bytes",
        go_quote(value)
    )
}

/// The validated names Cabby writes into SQL: schema, table prefix and channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Names {
    pub schema: String,
    pub prefix: String,
    pub channel: String,
    pub unlogged: bool,
}

/// Cabby's tables (without the prefix).
pub(crate) const META: &str = "meta";
pub(crate) const OVERFLOW: &str = "pubsub_overflow";
pub(crate) const BROADCASTS: &str = "broadcasts";
pub(crate) const STREAMS: &str = "streams";
pub(crate) const HISTORY: &str = "history";
pub(crate) const SESSIONS: &str = "sessions";

impl Names {
    /// Validates the configured names; errors name the flag.
    pub fn from_config(c: &PostgresConfig) -> Result<Self, String> {
        if !is_identifier(&c.schema, MAX_IDENTIFIER) {
            return Err(invalid("postgres_schema", &c.schema, MAX_IDENTIFIER, false));
        }
        let prefix_max = MAX_IDENTIFIER - LONGEST_TABLE_SUFFIX;
        if !c.table_prefix.is_empty() && !is_identifier(&c.table_prefix, prefix_max) {
            return Err(invalid("postgres_table_prefix", &c.table_prefix, prefix_max, true));
        }
        let channel_max = MAX_IDENTIFIER - LONGEST_CHANNEL_SUFFIX;
        if !is_identifier(&c.channel, channel_max) {
            return Err(invalid("postgres_channel", &c.channel, channel_max, false));
        }
        Ok(Self {
            schema: c.schema.clone(),
            prefix: c.table_prefix.clone(),
            channel: c.channel.clone(),
            unlogged: c.unlogged,
        })
    }

    /// `"schema"."<prefix><name>"`, for SQL.
    pub fn table(&self, name: &str) -> String {
        format!("{}.{}", quote(&self.schema), quote(&format!("{}{name}", self.prefix)))
    }

    /// `schema.<prefix><name>`, for messages.
    pub fn table_display(&self, name: &str) -> String {
        format!("{}.{}{name}", self.schema, self.prefix)
    }

    /// An object in the schema named `<prefix><name>` (an index, a function).
    pub fn object(&self, name: &str) -> String {
        self.table(name)
    }

    /// The `LISTEN` channel of stream shard `i`.
    pub fn shard_channel(&self, i: usize) -> String {
        format!("{}_{i}", self.channel)
    }

    /// The channel stream `name` is published on.
    pub fn stream_channel(&self, name: &str) -> String {
        self.shard_channel(shard(name))
    }

    /// The remote commands channel.
    pub fn command_channel(&self) -> String {
        format!("{}_cmd", self.channel)
    }

    /// The broadcast ingestion channel (the outbox trigger and direct `NOTIFY`).
    pub fn ingest_channel(&self) -> String {
        format!("{}_in", self.channel)
    }
}

/// Double-quotes a validated identifier (validation rules out `"`), so names
/// keep their case.
pub(crate) fn quote(ident: &str) -> String {
    debug_assert!(!ident.contains('"'));
    format!("\"{ident}\"")
}

/// The shard of a stream: FNV-1a (32 bits) of its name, modulo [`SHARDS`]. Every
/// node computes the same value.
pub fn shard(stream: &str) -> usize {
    let mut hash: u32 = 0x811c_9dc5;
    for b in stream.bytes() {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    (hash as usize) % SHARDS
}

pub(crate) const ERRORS: &str = "postgres_errors_total";
pub(crate) const QUERY_MS: &str = "postgres_query_ms";
pub(crate) const SPILLOVER: &str = "postgres_spillover_total";
pub(crate) const RECONNECTS: &str = "postgres_reconnects_total";
pub(crate) const CLAIMED: &str = "postgres_claimed_broadcasts_total";

/// The Postgres adapters' metrics (shared by every adapter of a server).
#[derive(Clone, Default)]
pub struct Metrics(Option<Arc<dyn Instrumenter>>);

impl Metrics {
    /// Registers the metrics on `metrics` (once per server: registering again
    /// resets them).
    pub fn register(metrics: Arc<dyn Instrumenter>) -> Self {
        metrics.register_counter(ERRORS, "The total number of failed Postgres operations");
        metrics.register_gauge(QUERY_MS, "The duration of the last Postgres operation (milliseconds)");
        metrics.register_counter(SPILLOVER, "The total number of pub/sub messages sent through the overflow table");
        metrics.register_counter(RECONNECTS, "The total number of Postgres LISTEN connection reconnects");
        metrics.register_counter(CLAIMED, "The total number of broadcasts claimed from the Postgres outbox table");
        Self(Some(metrics))
    }

    /// [`Metrics::register`], unless a Postgres adapter of this server
    /// registered them already.
    pub fn register_once(metrics: &Arc<crate::metrics::Metrics>) -> Self {
        let instrumenter: Arc<dyn Instrumenter> = metrics.clone();
        if metrics.counter(ERRORS).is_some() { Self(Some(instrumenter)) } else { Self::register(instrumenter) }
    }

    /// No metrics.
    pub fn none() -> Self {
        Self(None)
    }

    pub(crate) fn error(&self) {
        if let Some(m) = &self.0 {
            m.counter_increment(ERRORS);
        }
    }

    pub(crate) fn took(&self, d: Duration) {
        if let Some(m) = &self.0 {
            m.gauge_set(QUERY_MS, u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        }
    }

    pub(crate) fn spillover(&self) {
        if let Some(m) = &self.0 {
            m.counter_increment(SPILLOVER);
        }
    }

    pub(crate) fn reconnect(&self) {
        if let Some(m) = &self.0 {
            m.counter_increment(RECONNECTS);
        }
    }

    pub(crate) fn claimed(&self, n: usize) {
        if let Some(m) = &self.0 {
            m.counter_add(CLAIMED, u64::try_from(n).unwrap_or(u64::MAX));
        }
    }
}

/// Creates Cabby's tables for `config` (as `auto_migrate` does at startup).
pub fn migrate(config: &PostgresConfig) -> Result<(), PgError> {
    let names = Names::from_config(config).map_err(PgError)?;
    let spec = connect::ConnectSpec::from_config(config, "migrate").map_err(PgError)?;
    let log = logger("schema");
    connect::block_on(async move { schema::prepare(&spec, &names, true, &[], &log).await })
}

/// The adapters' logger (`context=<context> provider=postgres`).
pub(crate) fn logger(context: &str) -> cabby_core::logger::Logger {
    cabby_core::logger::Logger::default().with("context", context).with("provider", "postgres")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_validated() {
        for ok in ["public", "_x", "Cabby_1", &"a".repeat(63)] {
            assert!(is_identifier(ok, MAX_IDENTIFIER), "{ok}");
        }
        for bad in
            ["", "1abc", "a-b", "a b", "a\"b", "a;DROP TABLE x", "x'); DROP TABLE y; --", "caf\u{e9}", &"a".repeat(64)]
        {
            assert!(!is_identifier(bad, MAX_IDENTIFIER), "{bad:?}");
        }
    }

    #[test]
    fn names_reject_injection_and_long_identifiers() {
        let ok = Names::from_config(&PostgresConfig::new()).unwrap();
        assert_eq!(
            ("public", "cabby_", "cabby_internal"),
            (ok.schema.as_str(), ok.prefix.as_str(), ok.channel.as_str())
        );

        let with = |f: fn(&mut PostgresConfig)| {
            let mut c = PostgresConfig::new();
            f(&mut c);
            Names::from_config(&c)
        };
        assert_eq!(
            Err("invalid --postgres_schema \"public; DROP SCHEMA x\": must match ^[A-Za-z_][A-Za-z0-9_]*$ and be at most 63 bytes".to_owned()),
            with(|c| c.schema = "public; DROP SCHEMA x".into())
        );
        assert!(with(|c| c.schema = "a".repeat(64)).is_err());
        assert!(with(|c| c.table_prefix = "x\"y".into()).is_err());
        assert!(with(|c| c.table_prefix = "a".repeat(38)).is_err(), "prefix + longest suffix exceeds 63 bytes");
        assert!(with(|c| c.table_prefix = "a".repeat(37)).is_ok());
        assert!(with(|c| c.table_prefix = String::new()).is_ok(), "an empty prefix is allowed");
        assert!(with(|c| c.channel = "a".repeat(60)).is_err(), "channel + _cmd exceeds 63 bytes");
        assert!(with(|c| c.channel = "a".repeat(59)).is_ok());
        assert!(with(|c| c.channel = "chan'; NOTIFY x; --".into()).is_err());
    }

    #[test]
    fn names_quote_identifiers() {
        let mut c = PostgresConfig::new();
        c.schema = "App".into();
        let n = Names::from_config(&c).unwrap();
        assert_eq!("\"App\".\"cabby_history\"", n.table(HISTORY));
        assert_eq!("App.cabby_history", n.table_display(HISTORY));
        assert_eq!("cabby_internal_cmd", n.command_channel());
        assert_eq!("cabby_internal_in", n.ingest_channel());
        assert_eq!(format!("cabby_internal_{}", shard("chat")), n.stream_channel("chat"));
    }

    #[test]
    fn shards_are_stable_and_spread() {
        // FNV-1a 32 of "" is the offset basis 0x811c9dc5 = 2166136261.
        assert_eq!(2_166_136_261usize % SHARDS, shard(""));
        assert_eq!(shard("chat_42"), shard("chat_42"));
        let used: std::collections::HashSet<usize> = (0..200).map(|i| shard(&format!("stream-{i}"))).collect();
        assert_eq!(SHARDS, used.len());
    }

    #[test]
    fn toml_round_trip() {
        let printed = PostgresConfig::new().to_toml();
        assert!(printed.contains("# url = \"postgres://"), "{printed}");
        assert!(printed.contains("schema = \"public\"\n"), "{printed}");
        assert!(printed.contains("auto_migrate = true\n"), "{printed}");

        let c = PostgresConfig {
            url: "postgres://u:p@db:5432/app".into(),
            schema: "cabby".into(),
            table_prefix: "rt_".into(),
            channel: "rt".into(),
            auto_migrate: false,
            keepalive_interval: 7,
            tls_verify: false,
            tls_ca_cert_path: "/ca.pem".into(),
            unlogged: false,
        };
        let mut back = PostgresConfig::new();
        back.decode_toml(&c.to_toml()).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn keepalive_zero_disables_the_heartbeat() {
        let mut c = PostgresConfig::new();
        assert_eq!(Some(Duration::from_secs(30)), c.keepalive());
        c.keepalive_interval = 0;
        assert_eq!(None, c.keepalive());
        c.keepalive_interval = -1;
        assert_eq!(None, c.keepalive());
    }

    #[test]
    fn metrics_register_and_count() {
        let m = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        let pm = Metrics::register(m.clone());
        pm.error();
        pm.spillover();
        pm.spillover();
        pm.reconnect();
        pm.claimed(3);
        pm.took(Duration::from_millis(9));
        assert_eq!(1, m.counter(ERRORS).unwrap().value());
        assert_eq!(2, m.counter(SPILLOVER).unwrap().value());
        assert_eq!(1, m.counter(RECONNECTS).unwrap().value());
        assert_eq!(3, m.counter(CLAIMED).unwrap().value());
        assert_eq!(9, m.gauge(QUERY_MS).unwrap().value());
        Metrics::none().error();

        let again = Metrics::register_once(&m);
        again.error();
        assert_eq!(2, m.counter(ERRORS).unwrap().value(), "registering once keeps the counts");
    }
}
