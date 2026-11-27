//! PostgreSQL for the Postgres adapter tests (Cabby additions, D-070 and
//! D-071).
//!
//! `docs/translating-tests.md` ("Hard rules"): there is no silent skip. A test needs a server at
//! `CABBY_TEST_POSTGRES_URL` (`compat/scripts/services.sh start` runs one) and
//! panics with a message naming the variable when it is not set. Every test
//! works in its own scope: a unique schema (dropped afterwards) and a unique
//! base channel, so tests sharing the server never see each other's tables
//! or notifications.
//!
//! AnyCable has no Postgres adapters, so every scenario calls [`rust_only`]
//! first; on the Go backend the runner records the case as not observable.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cabby_server::postgres::PostgresConfig;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use crate::{Backend, backend};

/// Bound for every wait on the database.
pub const DEADLINE: Duration = Duration::from_secs(10);

/// Reports the case as not observable on the Go backend.
pub fn rust_only() {
    if backend() == Backend::Go {
        unimplemented!("not_observable: AnyCable has no Postgres adapters; they are a Cabby addition (D-070, D-071)");
    }
}

/// `CABBY_TEST_POSTGRES_URL`, required.
pub fn url() -> String {
    match std::env::var("CABBY_TEST_POSTGRES_URL") {
        Ok(v) if !v.trim().is_empty() => v.trim().to_owned(),
        _ => panic!(
            "CABBY_TEST_POSTGRES_URL is not set: this test needs a real PostgreSQL server (no silent \
             skips). Start one with `eval \"$(compat/scripts/services.sh start)\"`, see crates/cabby-testkit/src/postgres.rs"
        ),
    }
}

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("testkit-postgres")
            .enable_all()
            .build()
            .expect("building the test Postgres runtime")
    })
}

fn block<F>(fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    rx.recv_timeout(DEADLINE * 3).expect("a test database call timed out")
}

/// A test's schema and channel on the shared server.
pub struct PgScope {
    pub schema: String,
    pub channel: String,
    client: std::sync::Arc<Client>,
    roles: std::sync::Mutex<Vec<String>>,
}

/// `url` with `sslmode=disable`, for tests that start many adapters at once:
/// `sslmode=prefer` (the default) reads the system's CA certificates for each,
/// which macOS fails to list now and then under load.
pub fn without_tls(mut url: String) -> String {
    if !url.contains("sslmode=") {
        url.push_str(if url.contains('?') { "&sslmode=disable" } else { "?sslmode=disable" });
    }
    url
}

/// A plain connection to the test server.
pub fn connect() -> std::sync::Arc<Client> {
    let url = url();
    block(async move {
        let (client, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connecting to CABBY_TEST_POSTGRES_URL");
        tokio::spawn(conn);
        std::sync::Arc::new(client)
    })
}

impl PgScope {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos =
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
        let id = format!("{}_{n}_{nanos}", std::process::id());
        Self {
            schema: format!("t_{tag}_{id}"),
            channel: format!("c_{id}"),
            client: connect(),
            roles: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// The adapters' configuration for this scope.
    pub fn config(&self) -> PostgresConfig {
        PostgresConfig {
            url: url(),
            schema: self.schema.clone(),
            channel: self.channel.clone(),
            keepalive_interval: 1,
            ..PostgresConfig::new()
        }
    }

    /// [`PgScope::config`] without TLS (see [`without_tls`]).
    pub fn plain_config(&self) -> PostgresConfig {
        let mut config = self.config();
        config.url = without_tls(config.url);
        config
    }

    /// Creates the tables (what `auto_migrate` does when an adapter starts).
    pub fn migrate(&self) {
        cabby_server::postgres::migrate(&self.config()).expect("creating the Postgres tables");
    }

    /// `"schema"."cabby_<name>"`.
    pub fn table(&self, name: &str) -> String {
        format!("\"{}\".\"cabby_{name}\"", self.schema)
    }

    /// Runs SQL (one or more statements, simple protocol).
    pub fn execute(&self, sql: &str) {
        let (client, sql) = (self.client.clone(), sql.to_owned());
        block(async move { client.batch_execute(&sql).await }).unwrap_or_else(|e| panic!("SQL failed: {e:?}"));
    }

    /// Runs one query; every value as text (`None` for NULL).
    pub fn rows(&self, sql: &str) -> Vec<Vec<Option<String>>> {
        let (client, sql) = (self.client.clone(), sql.to_owned());
        let messages =
            block(async move { client.simple_query(&sql).await }).unwrap_or_else(|e| panic!("SQL failed: {e:?}"));
        messages
            .into_iter()
            .filter_map(|m| match m {
                SimpleQueryMessage::Row(r) => Some((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect()),
                _ => None,
            })
            .collect()
    }

    /// The first column of the first row.
    pub fn scalar(&self, sql: &str) -> Option<String> {
        self.rows(sql).into_iter().next().and_then(|r| r.into_iter().next().flatten())
    }

    /// `INSERT INTO <schema>.cabby_broadcasts (payload) VALUES ($1)`.
    pub fn insert_broadcast(&self, payload: &str) {
        let (client, sql, payload) = (
            self.client.clone(),
            format!("INSERT INTO {} (payload) VALUES ($1)", self.table("broadcasts")),
            payload.to_owned(),
        );
        block(async move { client.execute(&sql, &[&payload]).await }).expect("inserting a broadcast");
    }

    /// `pg_notify(channel, payload)`.
    pub fn notify(&self, channel: &str, payload: &str) {
        let (client, channel, payload) = (self.client.clone(), channel.to_owned(), payload.to_owned());
        block(async move { client.execute("SELECT pg_notify($1, $2)", &[&channel, &payload]).await }).expect("NOTIFY");
    }

    /// Backend PIDs of the connections named `app`.
    pub fn pids(&self, app: &str) -> Vec<i32> {
        let (client, app) = (self.client.clone(), app.to_owned());
        block(async move {
            client
                .query("SELECT pid FROM pg_stat_activity WHERE application_name = $1 ORDER BY pid", &[&app])
                .await
                .map(|rows| rows.iter().map(|r| r.get(0)).collect())
        })
        .expect("reading pg_stat_activity")
    }

    /// Terminates the connections named `app`; returns how many.
    pub fn terminate(&self, app: &str) -> usize {
        let (client, app) = (self.client.clone(), app.to_owned());
        block(async move {
            client
                .query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1", &[&app])
                .await
                .map(|rows| rows.len())
        })
        .expect("pg_terminate_backend")
    }

    /// Waits until a connection named `app` other than `old` has run a
    /// statement: a reconnected `LISTEN` connection's first statement is its
    /// `LISTEN` batch.
    pub fn wait_listening_again(&self, app: &str, old: &[i32]) {
        let (client, app2) = (self.client.clone(), app.to_owned());
        let old = old.to_vec();
        let deadline = Instant::now() + DEADLINE * 3;
        loop {
            let (client, app2, old) = (client.clone(), app2.clone(), old.clone());
            let ready: bool = block(async move {
                client
                    .query_one(
                        "SELECT count(*) > 0 FROM pg_stat_activity WHERE application_name = $1 AND query <> '' AND state = 'idle' \
                         AND NOT (pid = ANY($2))",
                        &[&app2, &old],
                    )
                    .await
                    .map(|r| r.get(0))
            })
            .expect("reading pg_stat_activity");
            if ready {
                return;
            }
            assert!(Instant::now() < deadline, "the {app} connection did not come back");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Creates a login role (unique to this scope, dropped with it) allowed
    /// to use this scope's tables, with `settings` applied to its sessions
    /// (`ALTER ROLE ... SET`), and returns a URL that logs in as it.
    pub fn role_url(&self, settings: &[(&str, &str)]) -> String {
        let role = format!("r_{}", self.channel);
        let mut sql = format!(
            "CREATE ROLE \"{role}\" LOGIN PASSWORD 'pw'; GRANT USAGE ON SCHEMA \"{s}\" TO \"{role}\"; \
             GRANT ALL ON ALL TABLES IN SCHEMA \"{s}\" TO \"{role}\"; GRANT ALL ON ALL SEQUENCES IN SCHEMA \"{s}\" TO \"{role}\";",
            s = self.schema
        );
        for (k, v) in settings {
            sql.push_str(&format!(" ALTER ROLE \"{role}\" SET {k} = '{v}';"));
        }
        self.execute(&sql);
        self.roles.lock().unwrap_or_else(|e| e.into_inner()).push(role.clone());
        let url = url();
        let (scheme, rest) = url.split_once("://").expect("a URL");
        let host = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
        format!("{scheme}://{role}:pw@{host}")
    }

    /// Another session on the test server (its own connection), for locks and
    /// transactions held across steps.
    pub fn other_session(&self) -> PgSession {
        PgSession(connect())
    }

    /// The test server's `host:port`.
    pub fn addr(&self) -> std::net::SocketAddr {
        let url = url();
        let rest = url.split_once("://").map_or(url.as_str(), |(_, r)| r);
        let host_port = rest.rsplit_once('@').map_or(rest, |(_, h)| h);
        let host_port = host_port.split(['/', '?']).next().unwrap_or(host_port);
        std::net::ToSocketAddrs::to_socket_addrs(host_port)
            .expect("resolving the test server")
            .next()
            .expect("an address")
    }

    /// The test URL pointing at `addr` (a proxy) instead.
    pub fn url_via(&self, addr: std::net::SocketAddr) -> String {
        let url = url();
        let (scheme, rest) = url.split_once("://").expect("a URL");
        let (user, host_and_rest) = rest.rsplit_once('@').map_or(("", rest), |(u, h)| (u, h));
        let tail = host_and_rest.find(['/', '?']).map_or("", |i| &host_and_rest[i..]);
        let user = if user.is_empty() { String::new() } else { format!("{user}@") };
        format!("{scheme}://{user}{addr}{tail}")
    }

    /// A database of this scope's own (its name is the scope's schema name),
    /// for what needs settings of a database such as its encoding; dropped
    /// with the returned value.
    pub fn database(&self, encoding: &str) -> PgDatabase {
        let name = self.schema.clone();
        self.execute(&format!(
            "CREATE DATABASE \"{name}\" ENCODING '{encoding}' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0"
        ));
        let url = url();
        let (scheme, rest) = url.split_once("://").expect("a URL");
        let (user, host_and_rest) = rest.rsplit_once('@').map_or(("", rest), |(u, h)| (u, h));
        let host = host_and_rest.split(['/', '?']).next().unwrap_or(host_and_rest);
        let query = host_and_rest.find('?').map_or("", |i| &host_and_rest[i..]);
        let user = if user.is_empty() { String::new() } else { format!("{user}@") };
        let db_url = format!("{scheme}://{user}{host}/{name}{query}");
        let target = db_url.clone();
        let client = block(async move {
            let (client, conn) =
                tokio_postgres::connect(&target, NoTls).await.expect("connecting to the scope's database");
            tokio::spawn(conn);
            std::sync::Arc::new(client)
        });
        PgDatabase { name, url: db_url, client, admin: self.client.clone() }
    }

    /// The name of a pub/sub `LISTEN` connection in this scope.
    pub fn pubsub_listener(&self) -> String {
        format!("cabby pubsub-listen {}", self.channel)
    }

    /// The name of a broadcaster `LISTEN` connection in this scope.
    pub fn broadcast_listener(&self) -> String {
        format!("cabby broadcast-listen {}", self.channel)
    }
}

/// A separate connection; dropping it closes the session (and releases its
/// locks).
pub struct PgSession(std::sync::Arc<Client>);

impl PgSession {
    pub fn execute(&self, sql: &str) {
        let (client, sql) = (self.0.clone(), sql.to_owned());
        block(async move { client.batch_execute(&sql).await }).unwrap_or_else(|e| panic!("SQL failed: {e:?}"));
    }
}

/// A database of a scope's own ([`PgScope::database`]).
pub struct PgDatabase {
    pub name: String,
    /// The test URL pointing at this database.
    pub url: String,
    client: std::sync::Arc<Client>,
    admin: std::sync::Arc<Client>,
}

impl PgDatabase {
    /// Runs SQL in this database (one or more statements, simple protocol).
    pub fn execute(&self, sql: &str) {
        let (client, sql) = (self.client.clone(), sql.to_owned());
        block(async move { client.batch_execute(&sql).await }).unwrap_or_else(|e| panic!("SQL failed: {e:?}"));
    }

    /// The first column of the first row, as text.
    pub fn scalar(&self, sql: &str) -> Option<String> {
        let (client, sql) = (self.client.clone(), sql.to_owned());
        let messages =
            block(async move { client.simple_query(&sql).await }).unwrap_or_else(|e| panic!("SQL failed: {e:?}"));
        messages.into_iter().find_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).map(str::to_owned)),
            _ => None,
        })?
    }
}

impl Drop for PgDatabase {
    fn drop(&mut self) {
        let sql = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", self.name);
        let client = self.admin.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        runtime().spawn(async move {
            let _ = tx.send(client.batch_execute(&sql).await);
        });
        let _ = rx.recv_timeout(DEADLINE);
    }
}

impl Drop for PgScope {
    fn drop(&mut self) {
        let mut sql = format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE;", self.schema);
        for role in self.roles.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            sql.push_str(&format!(
                " SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{role}'; DROP ROLE IF EXISTS \"{role}\";"
            ));
        }
        let client = self.client.clone();
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        runtime().spawn(async move {
            let _ = tx.send(client.batch_execute(&sql).await);
        });
        let _ = rx.recv_timeout(DEADLINE);
    }
}
