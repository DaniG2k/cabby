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

use cabby_core::log_warn;
use cabby_core::logger::Logger;

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

fn db(e: tokio_postgres::Error) -> PgError {
    PgError(error_text(&e))
}

/// Creates the tables (`auto_migrate`) or checks that `needed` exist.
pub(crate) async fn prepare(
    spec: &ConnectSpec,
    names: &Names,
    auto_migrate: bool,
    needed: &[&str],
    log: &Logger,
) -> Result<(), PgError> {
    let mut client = spec.connect("cabby migrate").await?;
    if auto_migrate { migrate(&mut client, names).await } else { check(&client, names, needed, log).await }
}

async fn stored_version(client: &impl tokio_postgres::GenericClient, n: &Names) -> Result<Option<i64>, PgError> {
    let sql = format!("SELECT value FROM {} WHERE key = 'schema_version'", n.table(META));
    let row = client.query_opt(&sql, &[]).await.map_err(db)?;
    Ok(row.and_then(|r| r.get::<_, String>(0).parse().ok()))
}

fn newer(n: &Names, version: i64) -> PgError {
    PgError(format!(
        "{} has schema version {version}, newer than this Cabby supports ({SCHEMA_VERSION})",
        n.table_display(META)
    ))
}

async fn migrate(client: &mut tokio_postgres::Client, n: &Names) -> Result<(), PgError> {
    let tx = client.transaction().await.map_err(db)?;
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&lock_key(n)]).await.map_err(db)?;
    let has_schema =
        tx.query_opt("SELECT 1 FROM pg_namespace WHERE nspname = $1", &[&n.schema]).await.map_err(db)?.is_some();
    if !has_schema {
        tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {}", quote(&n.schema))).await.map_err(db)?;
    }
    for statement in ddl(n) {
        tx.batch_execute(&statement).await.map_err(db)?;
    }
    if let Some(version) = stored_version(&tx, n).await?
        && version > SCHEMA_VERSION
    {
        return Err(newer(n, version));
    }
    if !trigger_exists(&tx, n).await? {
        tx.batch_execute(&trigger_ddl(n)).await.map_err(db)?;
    }
    tx.batch_execute(&version_sql(n)).await.map_err(db)?;
    tx.commit().await.map_err(db)
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

async fn check(client: &tokio_postgres::Client, n: &Names, needed: &[&str], log: &Logger) -> Result<(), PgError> {
    for table in needed {
        let exists: bool =
            client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&n.table(table)]).await.map_err(db)?.get(0);
        if !exists {
            return Err(PgError(format!(
                "table {} is missing: create it with the DDL in docs/postgres.md or set --postgres_auto_migrate=true",
                n.table_display(table)
            )));
        }
    }
    let has_meta: bool =
        client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&n.table(META)]).await.map_err(db)?.get(0);
    if has_meta
        && let Some(version) = stored_version(client, n).await?
        && version > SCHEMA_VERSION
    {
        return Err(newer(n, version));
    }
    if needed.contains(&BROADCASTS) && !trigger_exists(client, n).await? {
        log_warn!(
            log,
            &format!(
                "{} has no {} trigger: inserted broadcasts are picked up by polling only (every 5s)",
                n.table_display(BROADCASTS),
                trigger_name(n)
            )
        );
    }
    Ok(())
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
