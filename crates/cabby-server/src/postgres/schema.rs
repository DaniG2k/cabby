//! Cabby's tables: created at startup (`auto_migrate`, inside an advisory
//! lock so that nodes starting together don't race) or checked when the
//! database role may not create them. `docs/postgres.md` prints the same DDL
//! for the default names (a test keeps them equal).
//!
//! Schema version 1:
//!
//! - `<prefix>meta (key, value)`: `schema_version` and the shared `epoch`;
//! - `<prefix>pubsub_overflow`: pub/sub payloads too large for `NOTIFY`;
//! - `<prefix>broadcasts`: the outbox applications insert broadcasts into,
//!   with a statement-level `AFTER INSERT` trigger notifying `<channel>_in`
//!   (one notification per insert statement, empty payload);
//! - `<prefix>streams`, `<prefix>history`, `<prefix>sessions`: the broker.
//!
//! With `unlogged`, every table but the outbox is `UNLOGGED`: PostgreSQL
//! empties such tables after a crash, the epoch goes with them, and the next
//! epoch is a new one, so clients resynchronize.
//!
//! Preparing the schema is bounded ([`Limits`]): a lock another session holds
//! fails the start with an error instead of hanging it. Existing tables are
//! not locked: an index that exists is left alone (`CREATE INDEX IF NOT
//! EXISTS` would take a `SHARE` lock on its table first, waiting for its
//! writers and making new ones wait behind it).

use std::time::Duration;

use cabby_core::log_warn;
use cabby_core::logger::Logger;
use tokio_postgres::error::SqlState;

use super::connect::ConnectSpec;
use super::{BROADCASTS, HISTORY, META, Names, OVERFLOW, PgError, SESSIONS, STREAMS, error_text, quote};

/// The schema version this Cabby creates and reads.
pub(crate) const SCHEMA_VERSION: i64 = 1;

/// The tables an adapter needs.
pub(crate) const PUBSUB_TABLES: &[&str] = &[OVERFLOW];
pub(crate) const BROADCAST_TABLES: &[&str] = &[BROADCASTS];
pub(crate) const BROKER_TABLES: &[&str] = &[META, STREAMS, HISTORY, SESSIONS];

/// The DDL of schema version 1 for `names`, one statement per entry.
pub(crate) fn ddl(n: &Names) -> Vec<String> {
    let unlogged = if n.unlogged { "UNLOGGED " } else { "" };
    let p = &n.prefix;
    let function = n.object("broadcasts_notify");
    vec![
        format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} (\n  key   TEXT PRIMARY KEY,\n  value TEXT NOT NULL\n)",
            n.table(META)
        ),
        format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} (\n  id         BIGSERIAL PRIMARY KEY,\n  payload    TEXT NOT NULL,\n  expires_at TIMESTAMPTZ NOT NULL\n)",
            n.table(OVERFLOW)
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (expires_at)",
            quote(&format!("{p}pubsub_overflow_expires_at")),
            n.table(OVERFLOW)
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n  id         BIGSERIAL PRIMARY KEY,\n  payload    TEXT NOT NULL,\n  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\n)",
            n.table(BROADCASTS)
        ),
        format!(
            "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$\nBEGIN\n  PERFORM pg_notify('{}', '');\n  RETURN NULL;\nEND\n$$",
            n.ingest_channel()
        ),
        format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} (\n  stream       TEXT PRIMARY KEY,\n  max_seq      BIGINT NOT NULL,\n  deadline_sec BIGINT NOT NULL,\n  last_ts_sec  BIGINT NOT NULL\n)",
            n.table(STREAMS)
        ),
        format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} (\n  stream TEXT NOT NULL,\n  seq    BIGINT NOT NULL,\n  ts_sec BIGINT NOT NULL,\n  data   TEXT NOT NULL,\n  PRIMARY KEY (stream, seq)\n)",
            n.table(HISTORY)
        ),
        format!("CREATE INDEX IF NOT EXISTS {} ON {} (ts_sec)", quote(&format!("{p}history_ts_sec")), n.table(HISTORY)),
        format!(
            "CREATE {unlogged}TABLE IF NOT EXISTS {} (\n  sid           TEXT PRIMARY KEY,\n  data          BYTEA NOT NULL,\n  expires_at_ms BIGINT NOT NULL\n)",
            n.table(SESSIONS)
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (expires_at_ms)",
            quote(&format!("{p}sessions_expires_at_ms")),
            n.table(SESSIONS)
        ),
    ]
}

/// The outbox trigger (created when missing).
pub(crate) fn trigger_ddl(n: &Names) -> String {
    format!(
        "CREATE TRIGGER {} AFTER INSERT ON {} FOR EACH STATEMENT EXECUTE FUNCTION {}()",
        quote(&trigger_name(n)),
        n.table(BROADCASTS),
        n.object("broadcasts_notify")
    )
}

fn trigger_name(n: &Names) -> String {
    format!("{}broadcasts_notify", n.prefix)
}

/// The schema version row.
pub(crate) fn version_sql(n: &Names) -> String {
    format!(
        "INSERT INTO {} (key, value) VALUES ('schema_version', '{SCHEMA_VERSION}') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        n.table(META)
    )
}

/// The advisory lock key of a schema and prefix (FNV-1a 64 bits).
fn lock_key(n: &Names) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in format!("cabby:{}.{}", n.schema, n.prefix).bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}

/// How long preparing the schema may take. The lock limits are well below
/// the adapters' query timeout ([`super::connect::QUERY_TIMEOUT`]): writes of
/// running nodes that queue behind a lock this start waits for are delayed,
/// not failed.
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// Waiting for another node's migration (each takes the table locks it
    /// needs in turn).
    migration: Duration,
    /// Waiting for any other lock.
    lock: Duration,
    /// One statement, lock waits included.
    statement: Duration,
    /// Everything, connecting included (a server that stops answering).
    total: Duration,
}

const LIMITS: Limits = Limits {
    migration: Duration::from_secs(15),
    lock: Duration::from_secs(5),
    statement: Duration::from_secs(30),
    total: Duration::from_secs(60),
};

fn db(e: tokio_postgres::Error) -> PgError {
    PgError(error_text(&e))
}

/// An error of the step `what`, naming it when a limit ended it.
fn step<'a>(what: &'a str, lock: Duration) -> impl FnOnce(tokio_postgres::Error) -> PgError + 'a {
    move |e| match e.code() {
        Some(c) if *c == SqlState::LOCK_NOT_AVAILABLE => PgError(format!(
            "timed out after {}s waiting for a lock to {what}: another session holds a conflicting lock (see pg_locks and pg_stat_activity)",
            lock.as_secs()
        )),
        Some(c) if *c == SqlState::QUERY_CANCELED => PgError(format!("failed to {what}, cause: {}", error_text(&e))),
        _ => db(e),
    }
}

/// What a DDL statement does, for errors: its first line, up to the column list.
fn summary(statement: &str) -> &str {
    let line = statement.lines().next().unwrap_or_default();
    line.split_once(" (").map_or(line, |(head, _)| head)
}

/// The index a statement of [`ddl`] creates (names are validated identifiers:
/// no quotes in them).
fn created_index(statement: &str) -> Option<&str> {
    statement.strip_prefix("CREATE INDEX IF NOT EXISTS \"")?.split('"').next()
}

/// Creates the tables (`auto_migrate`) or checks that `needed` exist.
pub(crate) async fn prepare(
    spec: &ConnectSpec,
    names: &Names,
    auto_migrate: bool,
    needed: &[&str],
    log: &Logger,
) -> Result<(), PgError> {
    prepare_within(spec, names, auto_migrate, needed, log, &LIMITS).await
}

async fn prepare_within(
    spec: &ConnectSpec,
    names: &Names,
    auto_migrate: bool,
    needed: &[&str],
    log: &Logger,
    limits: &Limits,
) -> Result<(), PgError> {
    let work = async {
        let mut client = spec.connect("cabby migrate").await?;
        if auto_migrate {
            migrate(&mut client, names, limits).await
        } else {
            check(&mut client, names, needed, log, limits).await
        }
    };
    match tokio::time::timeout(limits.total, work).await {
        Ok(result) => result,
        Err(_) => {
            Err(PgError(format!("timed out after {}s: no answer from {}", limits.total.as_secs(), spec.description)))
        }
    }
}

/// Begins a transaction in which `lock` bounds lock waits.
async fn begin<'a>(
    client: &'a mut tokio_postgres::Client,
    limits: &Limits,
    lock: Duration,
) -> Result<tokio_postgres::Transaction<'a>, PgError> {
    let tx = client.transaction().await.map_err(db)?;
    tx.batch_execute(&format!(
        "SET LOCAL statement_timeout = {}; {}",
        limits.statement.as_millis(),
        set_lock_timeout(lock)
    ))
    .await
    .map_err(db)?;
    Ok(tx)
}

fn set_lock_timeout(lock: Duration) -> String {
    format!("SET LOCAL lock_timeout = {}", lock.as_millis())
}

async fn stored_version(
    client: &impl tokio_postgres::GenericClient,
    n: &Names,
    limits: &Limits,
) -> Result<Option<i64>, PgError> {
    let sql = format!("SELECT value FROM {} WHERE key = 'schema_version'", n.table(META));
    let row = client.query_opt(&sql, &[]).await.map_err(step("read the schema version", limits.lock))?;
    Ok(row.and_then(|r| r.get::<_, String>(0).parse().ok()))
}

fn newer(n: &Names, version: i64) -> PgError {
    PgError(format!(
        "{} has schema version {version}, newer than this Cabby supports ({SCHEMA_VERSION})",
        n.table_display(META)
    ))
}

async fn migrate(client: &mut tokio_postgres::Client, n: &Names, limits: &Limits) -> Result<(), PgError> {
    let tx = begin(client, limits, limits.migration).await?;
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&lock_key(n)])
        .await
        .map_err(step("run the migration (one node at a time)", limits.migration))?;
    tx.batch_execute(&set_lock_timeout(limits.lock)).await.map_err(db)?;
    let has_schema =
        tx.query_opt("SELECT 1 FROM pg_namespace WHERE nspname = $1", &[&n.schema]).await.map_err(db)?.is_some();
    if !has_schema {
        let sql = format!("CREATE SCHEMA IF NOT EXISTS {}", quote(&n.schema));
        tx.batch_execute(&sql).await.map_err(step(&format!("run {sql}"), limits.lock))?;
    }
    for statement in ddl(n) {
        if let Some(index) = created_index(&statement)
            && relation_exists(&tx, &format!("{}.{}", quote(&n.schema), quote(index))).await?
        {
            continue;
        }
        tx.batch_execute(&statement).await.map_err(step(&format!("run {}", summary(&statement)), limits.lock))?;
    }
    if let Some(version) = stored_version(&tx, n, limits).await?
        && version > SCHEMA_VERSION
    {
        return Err(newer(n, version));
    }
    if !trigger_exists(&tx, n).await? {
        let sql = trigger_ddl(n);
        tx.batch_execute(&sql).await.map_err(step(&format!("run {}", summary(&sql)), limits.lock))?;
    }
    tx.batch_execute(&version_sql(n)).await.map_err(step("store the schema version", limits.lock))?;
    tx.commit().await.map_err(db)
}

/// Whether a relation (a table or an index) of this quoted name exists; takes
/// no lock on it.
async fn relation_exists(client: &impl tokio_postgres::GenericClient, name: &str) -> Result<bool, PgError> {
    Ok(client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&name]).await.map_err(db)?.get(0))
}

async fn trigger_exists(client: &impl tokio_postgres::GenericClient, n: &Names) -> Result<bool, PgError> {
    let row = client
        .query_opt(
            "SELECT 1 FROM pg_trigger WHERE tgrelid = to_regclass($1) AND tgname = $2",
            &[&n.table(BROADCASTS), &trigger_name(n)],
        )
        .await
        .map_err(db)?;
    Ok(row.is_some())
}

async fn check(
    client: &mut tokio_postgres::Client,
    n: &Names,
    needed: &[&str],
    log: &Logger,
    limits: &Limits,
) -> Result<(), PgError> {
    let tx = begin(client, limits, limits.lock).await?;
    for table in needed {
        if !relation_exists(&tx, &n.table(table)).await? {
            return Err(PgError(format!(
                "table {} is missing: create it with the DDL in docs/postgres.md or set --postgres_auto_migrate=true",
                n.table_display(table)
            )));
        }
    }
    if relation_exists(&tx, &n.table(META)).await?
        && let Some(version) = stored_version(&tx, n, limits).await?
        && version > SCHEMA_VERSION
    {
        return Err(newer(n, version));
    }
    if needed.contains(&BROADCASTS) && !trigger_exists(&tx, n).await? {
        log_warn!(
            log,
            &format!(
                "{} has no {} trigger: inserted broadcasts are picked up by polling only (every 5s)",
                n.table_display(BROADCASTS),
                trigger_name(n)
            )
        );
    }
    tx.commit().await.map_err(db)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::PostgresConfig;

    /// The DDL in docs/postgres.md is the DDL Cabby runs for the default names.
    #[test]
    fn the_documented_ddl_is_the_migration() {
        let names = Names::from_config(&PostgresConfig::new()).unwrap();
        let mut expected: Vec<String> = ddl(&names).into_iter().map(|s| format!("{s};")).collect();
        expected.push(format!("{};", trigger_ddl(&names)));
        expected.push(format!("{};", version_sql(&names)));
        let expected = expected.join("\n\n");

        let doc = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/postgres.md")).unwrap();
        let start = doc.find("<!-- ddl:start -->").expect("ddl:start marker") + "<!-- ddl:start -->".len();
        let end = doc.find("<!-- ddl:end -->").expect("ddl:end marker");
        let block = doc[start..end].trim().trim_start_matches("```sql").trim_end_matches("```").trim();
        assert_eq!(expected, block, "update the DDL in docs/postgres.md");
    }

    #[test]
    fn index_statements_are_recognized() {
        let names = Names::from_config(&PostgresConfig::new()).unwrap();
        let all = ddl(&names);
        let found: Vec<&str> = all.iter().filter_map(|s| created_index(s)).collect();
        assert_eq!(
            vec!["cabby_pubsub_overflow_expires_at", "cabby_history_ts_sec", "cabby_sessions_expires_at_ms"],
            found
        );
        assert_eq!(all.iter().filter(|s| s.contains("INDEX")).count(), found.len());
        assert_eq!("CREATE UNLOGGED TABLE IF NOT EXISTS \"public\".\"cabby_meta\"", summary(&all[0]));
        assert_eq!(
            "CREATE INDEX IF NOT EXISTS \"cabby_pubsub_overflow_expires_at\" ON \"public\".\"cabby_pubsub_overflow\"",
            summary(&all[2])
        );
    }

    /// A server that accepts the connection and then answers nothing more.
    fn silent_server() -> std::net::SocketAddr {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for mut conn in listener.incoming().flatten() {
                std::thread::spawn(move || {
                    let mut len = [0; 4];
                    conn.read_exact(&mut len)?;
                    let mut startup = vec![0; u32::from_be_bytes(len) as usize - 4];
                    conn.read_exact(&mut startup)?;
                    // AuthenticationOk, ReadyForQuery.
                    conn.write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")?;
                    std::io::copy(&mut conn, &mut std::io::sink())
                });
            }
        });
        addr
    }

    #[test]
    fn a_server_that_stops_answering_ends_the_start() {
        let addr = silent_server();
        let mut c = PostgresConfig::new();
        c.url = format!("postgres://cabby@{addr}/db?sslmode=disable");
        let limits = Limits { total: Duration::from_secs(1), ..LIMITS };
        for auto_migrate in [true, false] {
            let names = Names::from_config(&c).unwrap();
            let spec = ConnectSpec::from_config(&c, "test").unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            super::super::connect::runtime().spawn(async move {
                let log = Logger::default();
                let _ = tx.send(prepare_within(&spec, &names, auto_migrate, BROKER_TABLES, &log, &limits).await);
            });
            let result = rx.recv_timeout(Duration::from_secs(20)).expect("the start hangs");
            assert_eq!(Err(format!("timed out after 1s: no answer from {addr}/db")), result.map_err(|e| e.0));
        }
    }

    #[test]
    fn lock_waits_are_shorter_than_the_query_timeout() {
        assert!(LIMITS.lock * 2 <= super::super::connect::QUERY_TIMEOUT);
        assert!(LIMITS.migration + LIMITS.lock < LIMITS.statement);
        assert!(super::super::connect::CONNECT_TIMEOUT + LIMITS.migration + LIMITS.statement < LIMITS.total);
    }

    #[test]
    fn unlogged_applies_to_every_table_but_the_outbox() {
        let mut c = PostgresConfig::new();
        let names = Names::from_config(&c).unwrap();
        let all = ddl(&names).join("\n");
        assert_eq!(5, all.matches("CREATE UNLOGGED TABLE").count(), "{all}");
        assert!(all.contains("CREATE TABLE IF NOT EXISTS \"public\".\"cabby_broadcasts\""), "{all}");

        c.unlogged = false;
        let all = ddl(&Names::from_config(&c).unwrap()).join("\n");
        assert!(!all.contains("UNLOGGED"), "{all}");
    }

    #[test]
    fn lock_keys_differ_per_schema_and_prefix() {
        let mut c = PostgresConfig::new();
        let a = lock_key(&Names::from_config(&c).unwrap());
        c.table_prefix = "other_".into();
        assert_ne!(a, lock_key(&Names::from_config(&c).unwrap()));
    }
}
