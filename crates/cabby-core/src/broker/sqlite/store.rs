//! The database file: connection settings, schema and migrations,
//! and the `meta` table.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, params};

use super::queue::{StreamMaxima, WriteOp};
use super::{log, units};
use crate::{log_info, log_warn};

/// How long a statement waits for a lock another connection holds.
pub(super) const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Schema migrations: entry `i` takes a file from version `i` to `i + 1`, so
/// the current version is `MIGRATIONS.len()`. Append new versions; never edit
/// a released one.
pub(super) const MIGRATIONS: &[&str] = &[SCHEMA_V1];

/// `meta` holds `schema_version`, `epoch` and `clean_shutdown`. It exists in
/// every version, so the version can be read before migrating.
const CREATE_META: &str = "CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID";

/// Version 1: the first schema, plus `streams_deadline` so pruning streams
/// by deadline uses an index.
const SCHEMA_V1: &str = "
-- One row per stream ever written, so a quiet stream keeps its offsets after
-- its entries expire (the memory broker keeps a stream for 10 x history_ttl).
CREATE TABLE streams (
  stream   TEXT PRIMARY KEY,
  max_seq  INTEGER NOT NULL,
  deadline INTEGER NOT NULL   -- unix seconds
) WITHOUT ROWID;
CREATE INDEX streams_deadline ON streams (deadline);

-- Rowid tables: payloads can be large.
CREATE TABLE history (
  stream TEXT    NOT NULL,
  seq    INTEGER NOT NULL,  -- the stream offset ('offset' is an SQL keyword)
  ts     INTEGER NOT NULL,  -- unix seconds
  data   TEXT    NOT NULL
);
CREATE UNIQUE INDEX history_stream_seq ON history (stream, seq);
CREATE INDEX history_ts ON history (ts);

CREATE TABLE sessions (
  sid           TEXT    PRIMARY KEY,
  data          BLOB    NOT NULL,  -- the Cacheable bytes commit_session receives
  expires_at_ms INTEGER NOT NULL   -- unix milliseconds
);
CREATE INDEX sessions_expires ON sessions (expires_at_ms);
";

/// A database failure, classified by what startup does with the file.
#[derive(Debug, thiserror::Error)]
pub(super) enum StoreError {
    /// `SQLITE_CORRUPT`, `SQLITE_NOTADB` or a failed `quick_check`: the only
    /// errors for which the file is moved aside.
    #[error("corrupt: {0}")]
    Corrupt(String),
    /// Written by a newer Cabby: refused, never moved aside.
    #[error(
        "schema version {found} is newer than this Cabby supports ({supported}); upgrade Cabby or move the file away"
    )]
    NewerSchema { found: i64, supported: i64 },
    /// Any other SQLite error (busy, permissions, I/O, disk full…).
    #[error("{0}")]
    Sqlite(rusqlite::Error),
    #[error("{0}")]
    Other(String),
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        match e.sqlite_error_code() {
            Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => StoreError::Corrupt(e.to_string()),
            _ => StoreError::Sqlite(e),
        }
    }
}

/// Opens (creating if needed) the database: incremental auto-vacuum for fresh
/// files, WAL, `synchronous=NORMAL`, a busy timeout, `quick_check`,
/// migrations up to the current version, then the one-time auto-vacuum
/// conversion of an older file (only once the file passed every check).
pub(super) fn open(path: &Path) -> Result<Connection, StoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let page_count: i64 = conn.pragma_query_value(None, "page_count", |r| r.get(0))?;
    if page_count == 0 {
        conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    }
    let mode: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::Other(format!("journal_mode is {mode}, the SQLite broker needs WAL")));
    }
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    quick_check(&conn)?;
    migrate(&conn, MIGRATIONS)?;
    convert_at_open(&conn, path);
    Ok(conn)
}

/// The auto-vacuum conversion at open, logged before it starts (it blocks
/// for a while on a large file) and after it ends, then checkpointed so the
/// space comes back at once. A failure is logged and the file is used
/// unconverted; the next start tries again.
fn convert_at_open(conn: &Connection, path: &Path) {
    let shown = path.display().to_string();
    let starting = |pages: i64| {
        log_info!(log(), "converting the SQLite database to incremental auto-vacuum (a one-time VACUUM)",
            "path" => shown.clone(), "pages" => pages);
    };
    match convert_to_incremental_auto_vacuum(conn, starting) {
        Ok(None) => {}
        Ok(Some((before, after))) => {
            log_info!(log(), "converted the SQLite database to incremental auto-vacuum",
                "path" => shown.clone(), "pages_before" => before, "pages_after" => after);
            if let Err(e) = checkpoint(conn) {
                log_warn!(log(), "failed to checkpoint the SQLite WAL after the conversion",
                    "path" => shown, "error" => e.to_string());
            }
        }
        Err(e) => log_warn!(log(), "failed to convert the SQLite database to incremental auto-vacuum",
            "path" => shown, "error" => e.to_string()),
    }
}

/// Switches a non-empty file that has no auto-vacuum (files written before
/// Cabby set it) to `auto_vacuum=INCREMENTAL`, which takes a full `VACUUM`;
/// `starting` gets the page count just before it. Returns the page counts
/// before and after, or `None` when there was nothing to convert.
pub(super) fn convert_to_incremental_auto_vacuum(
    conn: &Connection,
    starting: impl FnOnce(i64),
) -> Result<Option<(i64, i64)>, StoreError> {
    let auto_vacuum: i64 = conn.pragma_query_value(None, "auto_vacuum", |r| r.get(0))?;
    let before: i64 = conn.pragma_query_value(None, "page_count", |r| r.get(0))?;
    if auto_vacuum != 0 || before == 0 {
        return Ok(None);
    }
    starting(before);
    conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL; VACUUM;")?;
    let after: i64 = conn.pragma_query_value(None, "page_count", |r| r.get(0))?;
    Ok(Some((before, after)))
}

/// `PRAGMA quick_check`: anything but a single `ok` row is corruption.
pub(super) fn quick_check(conn: &Connection) -> Result<(), StoreError> {
    let mut stmt = conn.prepare("PRAGMA quick_check")?;
    let rows: Vec<String> = stmt.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
    if rows == ["ok"] { Ok(()) } else { Err(StoreError::Corrupt(format!("quick_check: {}", rows.join("; ")))) }
}

/// The stored schema version, 0 for a fresh file.
pub(super) fn schema_version(conn: &Connection) -> Result<i64, StoreError> {
    match meta_get(conn, "schema_version")? {
        None => Ok(0),
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            // Readable, but not a version Cabby wrote: refuse it, never move it aside.
            .ok_or_else(|| {
                StoreError::Other(format!(
                    "invalid schema_version {v:?} in its meta table (not written by Cabby?); fix it or move the file away"
                ))
            }),
    }
}

/// Brings the file up to `migrations.len()`, one transaction per version;
/// refuses a file whose version is newer.
pub(super) fn migrate(conn: &Connection, migrations: &[&str]) -> Result<(), StoreError> {
    conn.execute_batch(CREATE_META)?;
    let supported = i64::try_from(migrations.len()).unwrap_or(i64::MAX);
    let found = schema_version(conn)?;
    if found > supported {
        return Err(StoreError::NewerSchema { found, supported });
    }
    for (version, sql) in (1_i64..).zip(migrations).skip(usize::try_from(found).unwrap_or(0)) {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(sql)?;
        meta_set(&tx, "schema_version", &version.to_string())?;
        tx.commit()?;
    }
    Ok(())
}

pub(super) fn meta_get(conn: &Connection, key: &str) -> Result<Option<String>, StoreError> {
    Ok(conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0)).optional()?)
}

pub(super) fn meta_set(conn: &Connection, key: &str, value: &str) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [key, value],
    )?;
    Ok(())
}

/// Records the run's epoch and that it has not shut down cleanly yet, in one
/// transaction, before Cabby accepts connections. It must be
/// durable: with WAL and `synchronous=NORMAL` a commit isn't synced, and a
/// power cut that lost `clean_shutdown = '0'` would let the next start reuse
/// the epoch. So it checkpoints right away, which syncs the WAL and then the
/// database file.
pub(super) fn mark_running(conn: &mut Connection, epoch: &str) -> Result<(), StoreError> {
    let tx = conn.transaction()?;
    meta_set(&tx, "epoch", epoch)?;
    meta_set(&tx, "clean_shutdown", "0")?;
    tx.commit()?;
    checkpoint(conn)
}

/// The streams a batch wrote (pruning by limit visits only these).
pub(super) type Touched = BTreeSet<String>;

const INSERT_HISTORY: &str = "INSERT INTO history (stream, seq, ts, data) VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT (stream, seq) DO UPDATE SET ts = excluded.ts, data = excluded.data";
const UPSERT_SESSION: &str = "INSERT INTO sessions (sid, data, expires_at_ms) VALUES (?1, ?2, ?3)
  ON CONFLICT (sid) DO UPDATE SET data = excluded.data, expires_at_ms = excluded.expires_at_ms";
const TOUCH_SESSION: &str = "UPDATE sessions SET expires_at_ms = ?2 WHERE sid = ?1";
/// Keeps the highest offset (concurrent broadcasts may enqueue out of order),
/// unless the stored row's deadline has passed: the memory broker dropped that
/// stream and restarted its offsets, so the batch's offset is the stream's.
const UPSERT_STREAM: &str = "INSERT INTO streams (stream, max_seq, deadline) VALUES (?1, ?2, ?3)
  ON CONFLICT (stream) DO UPDATE SET
    max_seq = CASE WHEN streams.deadline < ?4 THEN excluded.max_seq
                   ELSE max(streams.max_seq, excluded.max_seq) END,
    deadline = max(streams.deadline, excluded.deadline)";

/// Writes `ops` in one transaction, in order, and upserts `streams` for every
/// stream they wrote and every stream in `dropped` (offsets whose history ops
/// were dropped from the queue: the stream offset still moves past them). With `clean_shutdown`, marks the shutdown clean in the
/// same transaction. `now_secs` decides whether a stored stream row
/// is stale. Nothing is written if any statement fails.
pub(super) fn write_batch(
    conn: &mut Connection,
    ops: &[WriteOp],
    dropped: &StreamMaxima,
    history_ttl: i64,
    now_secs: i64,
    clean_shutdown: bool,
) -> Result<Touched, StoreError> {
    let tx = conn.transaction()?;
    // The newest offset and timestamp of each stream in the batch.
    let mut newest: BTreeMap<&str, (u64, i64)> = dropped.iter().map(|(s, m)| (s.as_str(), *m)).collect();
    {
        let mut history = tx.prepare_cached(INSERT_HISTORY)?;
        let mut commit = tx.prepare_cached(UPSERT_SESSION)?;
        let mut touch = tx.prepare_cached(TOUCH_SESSION)?;
        for op in ops {
            match op {
                WriteOp::History { stream, offset, ts_secs, data } => {
                    history.execute(params![stream, units::offset_to_sql(*offset), ts_secs, data])?;
                    let entry = newest.entry(stream.as_str()).or_insert((*offset, *ts_secs));
                    *entry = (entry.0.max(*offset), entry.1.max(*ts_secs));
                }
                WriteOp::SessionCommit { sid, data, expires_at_ms } => {
                    commit.execute(params![sid, data, expires_at_ms])?;
                }
                WriteOp::SessionTouch { sid, expires_at_ms } => {
                    touch.execute(params![sid, expires_at_ms])?;
                }
            }
        }
        let mut streams = tx.prepare_cached(UPSERT_STREAM)?;
        for (stream, (offset, ts)) in &newest {
            streams.execute(params![
                stream,
                units::offset_to_sql(*offset),
                units::stream_deadline(*ts, history_ttl),
                now_secs
            ])?;
        }
    }
    if clean_shutdown {
        meta_set(&tx, "clean_shutdown", "1")?;
    }
    tx.commit()?;
    Ok(newest.into_keys().map(str::to_owned).collect())
}

/// History older than the TTL (index `history_ts`).
pub(super) const PRUNE_BY_TTL: &str = "DELETE FROM history WHERE ts < ?1";
/// One stream's entries beyond `history_limit`, measured from its stored
/// newest offset (index `history_stream_seq`).
pub(super) const PRUNE_BY_LIMIT: &str =
    "DELETE FROM history WHERE stream = ?1 AND seq <= (SELECT max_seq - ?2 FROM streams WHERE stream = ?1)";
/// Streams past their deadline (index `streams_deadline`).
pub(super) const PRUNE_STREAMS: &str = "DELETE FROM streams WHERE deadline < ?1";
/// Expired sessions (index `sessions_expires`).
pub(super) const PRUNE_SESSIONS: &str = "DELETE FROM sessions WHERE expires_at_ms < ?1";
/// Streams the memory broker would still keep (`Memory::expire` keeps
/// `deadline >= now`).
pub(super) const LOAD_STREAMS: &str = "SELECT stream, max_seq, deadline FROM streams WHERE deadline >= ?1";
/// A stream's newest entries at or below its stored offset, newest first.
pub(super) const LOAD_TAIL: &str =
    "SELECT seq, ts, data FROM history WHERE stream = ?1 AND seq <= ?2 ORDER BY seq DESC LIMIT ?3";
/// Sessions not yet expired (the memory broker expires at `deadline <= now`).
pub(super) const LOAD_SESSIONS: &str = "SELECT sid, data, expires_at_ms FROM sessions WHERE expires_at_ms > ?1";

/// What a prune pass removes, computed by the persister from its clock.
pub(super) struct Prune<'a> {
    /// History older than this (unix seconds) is expired.
    pub(super) history_cutoff: i64,
    /// Streams whose deadline is before this (unix seconds) are dropped.
    pub(super) now_secs: i64,
    /// Sessions expiring before this (unix milliseconds) are dropped.
    pub(super) now_ms: i64,
    pub(super) history_limit: i64,
    /// Streams written since the last prune.
    pub(super) streams: &'a Touched,
}

/// One prune pass, in one transaction.
pub(super) fn prune(conn: &mut Connection, p: &Prune<'_>) -> Result<(), StoreError> {
    let tx = conn.transaction()?;
    tx.execute(PRUNE_BY_TTL, [p.history_cutoff])?;
    {
        let mut by_limit = tx.prepare_cached(PRUNE_BY_LIMIT)?;
        for stream in p.streams {
            by_limit.execute(params![stream, p.history_limit])?;
        }
    }
    tx.execute(PRUNE_STREAMS, [p.now_secs])?;
    tx.execute(PRUNE_SESSIONS, [p.now_ms])?;
    tx.commit()?;
    Ok(())
}

/// Returns free pages to the file system (`PRAGMA incremental_vacuum`): at
/// most `pages` (all of them when `None`), and none once `deadline` has
/// passed. Each step of the pragma frees one page. Returns how many it freed.
pub(super) fn incremental_vacuum(
    conn: &Connection,
    pages: Option<u32>,
    deadline: Option<Instant>,
) -> Result<u64, StoreError> {
    let sql = match pages {
        Some(n) => format!("PRAGMA incremental_vacuum({n})"),
        None => "PRAGMA incremental_vacuum".to_owned(),
    };
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut freed = 0;
    while deadline.is_none_or(|d| Instant::now() < d) && rows.next()?.is_some() {
        freed += 1;
    }
    Ok(freed)
}

/// `PRAGMA wal_checkpoint(TRUNCATE)`: copies the WAL into the database file
/// and truncates it, so the file is self-contained for backups.
pub(super) fn checkpoint(conn: &Connection) -> Result<(), StoreError> {
    let blocked: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    if blocked != 0 {
        return Err(StoreError::Other("the WAL checkpoint was blocked by another connection".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::broker::sqlite::testing::{TempDir, commit, history, history_at, touch};
    use crate::broker::test_clock::T0;

    fn schema_objects(conn: &Connection) -> Vec<String> {
        let sql = "SELECT name FROM sqlite_schema WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' ORDER BY name";
        conn.prepare(sql).unwrap().query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    }

    #[test]
    fn a_fresh_file_gets_the_current_schema_in_wal_mode() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();

        assert_eq!(1, schema_version(&conn).unwrap());
        assert_eq!(
            vec![
                "history",
                "history_stream_seq",
                "history_ts",
                "meta",
                "sessions",
                "sessions_expires",
                "streams",
                "streams_deadline"
            ],
            schema_objects(&conn)
        );
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
        assert_eq!("wal", mode);
        let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0)).unwrap();
        assert_eq!(1, synchronous, "synchronous=NORMAL");
        let auto_vacuum: i64 = conn.pragma_query_value(None, "auto_vacuum", |r| r.get(0)).unwrap();
        assert_eq!(2, auto_vacuum, "auto_vacuum=INCREMENTAL");
    }

    #[test]
    fn reopening_keeps_the_data_and_the_version() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        meta_set(&open(&path).unwrap(), "epoch", "abcd").unwrap();

        let conn = open(&path).unwrap();
        assert_eq!(Some("abcd".to_owned()), meta_get(&conn, "epoch").unwrap());
        assert_eq!(1, schema_version(&conn).unwrap());
    }

    #[test]
    fn migrations_run_forward_from_the_stored_version() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();
        conn.execute("INSERT INTO streams (stream, max_seq, deadline) VALUES ('s', 3, 10)", []).unwrap();

        migrate(&conn, &[SCHEMA_V1, "CREATE TABLE extra (x INTEGER)"]).unwrap();

        assert_eq!(2, schema_version(&conn).unwrap());
        assert!(schema_objects(&conn).contains(&"extra".to_owned()));
        let max_seq: i64 = conn.query_row("SELECT max_seq FROM streams WHERE stream = 's'", [], |r| r.get(0)).unwrap();
        assert_eq!(3, max_seq, "rows survive a migration");
    }

    #[test]
    fn a_newer_schema_is_refused_and_not_treated_as_corruption() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        meta_set(&open(&path).unwrap(), "schema_version", "99").unwrap();

        match open(&path) {
            Err(StoreError::NewerSchema { found: 99, supported: 1 }) => {}
            other => panic!("expected NewerSchema, got {other:?}"),
        }
    }

    #[test]
    fn a_file_that_is_not_a_database_is_corrupt() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        std::fs::write(&path, vec![0x42_u8; 8192]).unwrap();

        assert!(matches!(open(&path), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn a_damaged_page_is_corrupt() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        {
            let conn = open(&path).unwrap();
            for i in 0..200 {
                conn.execute(
                    "INSERT INTO history (stream, seq, ts, data) VALUES ('s', ?1, 0, ?2)",
                    (i, "x".repeat(100)),
                )
                .unwrap();
            }
        } // The last connection closes: the WAL is checkpointed into the file and removed.
        let mut bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > 3 * 4096, "several pages");
        bytes[4096..3 * 4096].fill(0xFF);
        std::fs::write(&path, bytes).unwrap();

        assert!(matches!(open(&path), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn only_corruption_codes_count_as_corrupt() {
        use rusqlite::ffi;
        let classify = |code| StoreError::from(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));

        assert!(matches!(classify(ffi::SQLITE_CORRUPT), StoreError::Corrupt(_)));
        assert!(matches!(classify(ffi::SQLITE_NOTADB), StoreError::Corrupt(_)));
        for code in [ffi::SQLITE_BUSY, ffi::SQLITE_PERM, ffi::SQLITE_CANTOPEN, ffi::SQLITE_READONLY, ffi::SQLITE_FULL] {
            assert!(matches!(classify(code), StoreError::Sqlite(_)), "code {code} is not corruption");
        }
    }

    #[test]
    fn mark_running_records_the_epoch_and_an_unclean_state() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        meta_set(&conn, "clean_shutdown", "1").unwrap();

        mark_running(&mut conn, "e1").unwrap();

        assert_eq!(Some("e1".to_owned()), meta_get(&conn, "epoch").unwrap());
        assert_eq!(Some("0".to_owned()), meta_get(&conn, "clean_shutdown").unwrap());
    }

    fn history_rows(conn: &Connection) -> Vec<(String, i64, i64, String)> {
        let sql = "SELECT stream, seq, ts, data FROM history ORDER BY stream, seq";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn stream_rows(conn: &Connection) -> Vec<(String, i64, i64)> {
        let sql = "SELECT stream, max_seq, deadline FROM streams ORDER BY stream";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn session_rows(conn: &Connection) -> Vec<(String, Vec<u8>, i64)> {
        let sql = "SELECT sid, data, expires_at_ms FROM sessions ORDER BY sid";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_batch_writes_history_sessions_and_its_streams() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let ops = vec![
            history("s", 1),
            history("s", 2),
            history_at("t", 1, T0 + 5),
            commit("a", 10),
            touch("a", 20),
            touch("missing", 30),
        ];

        let touched = write_batch(&mut conn, &ops, &StreamMaxima::new(), 300, T0, false).unwrap();

        assert_eq!(BTreeSet::from(["s".to_owned(), "t".to_owned()]), touched);
        assert_eq!(
            vec![
                ("s".to_owned(), 1, T0, "m1".to_owned()),
                ("s".to_owned(), 2, T0, "m2".to_owned()),
                ("t".to_owned(), 1, T0 + 5, "m1".to_owned()),
            ],
            history_rows(&conn)
        );
        // Deadlines are ten history TTLs after each stream's newest message.
        assert_eq!(vec![("s".to_owned(), 2, T0 + 3000), ("t".to_owned(), 1, T0 + 5 + 3000)], stream_rows(&conn));
        // The commit, then its touch; a touch for an unknown session changes nothing.
        assert_eq!(vec![("a".to_owned(), b"a".to_vec(), 20)], session_rows(&conn));
        assert_eq!(None, meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn clean_shutdown_is_written_with_the_batch() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();

        write_batch(&mut conn, &[history("s", 1)], &StreamMaxima::new(), 300, T0, true).unwrap();

        assert_eq!(Some("1".to_owned()), meta_get(&conn, "clean_shutdown").unwrap());
        assert_eq!(1, history_rows(&conn).len());
    }

    #[test]
    fn a_failed_batch_leaves_nothing_behind() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let conn = open(&path).unwrap();
        let mut read_only = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();

        assert!(
            write_batch(&mut read_only, &[history("s", 1), commit("a", 1)], &StreamMaxima::new(), 300, T0, true)
                .is_err()
        );

        assert!(history_rows(&conn).is_empty());
        assert!(session_rows(&conn).is_empty());
        assert_eq!(None, meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn a_stream_keeps_its_highest_offset_when_ops_arrive_out_of_order() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();

        write_batch(&mut conn, &[history("s", 5)], &StreamMaxima::new(), 300, T0, false).unwrap();
        write_batch(&mut conn, &[history("s", 4)], &StreamMaxima::new(), 300, T0, false).unwrap();

        assert_eq!(vec![("s".to_owned(), 5, T0 + 3000)], stream_rows(&conn));
    }

    #[test]
    fn a_stream_past_its_deadline_restarts_its_offsets() {
        // The memory broker drops a stream ten TTLs after its last message;
        // the next broadcast starts again at offset 1.
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        write_batch(&mut conn, &[history_at("s", 9, T0)], &StreamMaxima::new(), 300, T0, false).unwrap();

        write_batch(&mut conn, &[history_at("s", 1, T0 + 4000)], &StreamMaxima::new(), 300, T0 + 4000, false).unwrap();

        assert_eq!(vec![("s".to_owned(), 1, T0 + 7000)], stream_rows(&conn));
    }

    /// The `EXPLAIN QUERY PLAN` details of `sql`, one line per step.
    fn plan(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> String {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        stmt.query_map(params, |r| r.get::<_, String>(3)).unwrap().map(Result::unwrap).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn prune_and_load_statements_use_indexes() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();
        let single: [(&str, &str); 5] = [
            (PRUNE_BY_TTL, "history_ts"),
            (PRUNE_STREAMS, "streams_deadline"),
            (PRUNE_SESSIONS, "sessions_expires"),
            (LOAD_STREAMS, "streams_deadline"),
            (LOAD_SESSIONS, "sessions_expires"),
        ];
        let mut plans: Vec<(&str, &str, String)> =
            single.iter().map(|(sql, index)| (*sql, *index, plan(&conn, sql, [0_i64]))).collect();
        plans.push((PRUNE_BY_LIMIT, "history_stream_seq", plan(&conn, PRUNE_BY_LIMIT, params!["s", 2_i64])));
        plans.push((LOAD_TAIL, "history_stream_seq", plan(&conn, LOAD_TAIL, params!["s", 9_i64, 100_i64])));

        for (sql, index, plan) in plans {
            assert!(plan.contains(&format!("INDEX {index}")), "{sql}\nuses {index}, plan:\n{plan}");
            assert!(!plan.contains("SCAN "), "{sql}\nscans, plan:\n{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{sql}\nsorts, plan:\n{plan}");
        }
    }

    #[test]
    fn pruning_drops_expired_history_entries_beyond_the_limit_stale_streams_and_expired_sessions() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let mut ops: Vec<WriteOp> = (1..=5).map(|offset| history("s", offset)).collect();
        ops.extend([history_at("old", 1, T0 - 100), commit("live", (T0 + 1) * 1000), commit("gone", T0 * 1000 - 1)]);
        write_batch(&mut conn, &ops, &StreamMaxima::new(), 300, T0, false).unwrap();
        conn.execute("INSERT INTO streams (stream, max_seq, deadline) VALUES ('stale', 1, ?1)", [T0 - 1]).unwrap();

        let streams = Touched::from(["s".to_owned()]);
        let prune_at =
            Prune { history_cutoff: T0 - 60, now_secs: T0, now_ms: T0 * 1000, history_limit: 2, streams: &streams };
        prune(&mut conn, &prune_at).unwrap();

        let history: Vec<(String, i64)> = history_rows(&conn).into_iter().map(|(s, seq, ..)| (s, seq)).collect();
        assert_eq!(vec![("s".to_owned(), 4), ("s".to_owned(), 5)], history, "TTL and limit");
        let streams: Vec<String> = stream_rows(&conn).into_iter().map(|(s, ..)| s).collect();
        assert_eq!(vec!["old", "s"], streams, "the stale stream row is gone; 'old' is within its deadline");
        let sessions: Vec<String> = session_rows(&conn).into_iter().map(|(sid, ..)| sid).collect();
        assert_eq!(vec!["live"], sessions);
    }

    #[test]
    fn pruning_by_limit_visits_only_the_streams_it_is_given() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let ops: Vec<WriteOp> = (1..=5).flat_map(|offset| [history("s", offset), history("t", offset)]).collect();
        write_batch(&mut conn, &ops, &StreamMaxima::new(), 300, T0, false).unwrap();

        let streams = Touched::from(["s".to_owned()]);
        let prune_at =
            Prune { history_cutoff: T0 - 60, now_secs: T0, now_ms: T0 * 1000, history_limit: 2, streams: &streams };
        prune(&mut conn, &prune_at).unwrap();

        let seqs = |stream: &str| -> Vec<i64> {
            history_rows(&conn).into_iter().filter(|(s, ..)| s == stream).map(|(_, seq, ..)| seq).collect()
        };
        assert_eq!(vec![4, 5], seqs("s"));
        assert_eq!(vec![1, 2, 3, 4, 5], seqs("t"));
    }

    #[test]
    fn checkpoint_empties_the_wal() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let mut conn = open(&path).unwrap();
        write_batch(&mut conn, &[history("s", 1)], &StreamMaxima::new(), 300, T0, false).unwrap();
        let wal = dir.file("cabby.db-wal");
        assert!(std::fs::metadata(&wal).unwrap().len() > 0);

        checkpoint(&conn).unwrap();

        assert_eq!(0, std::fs::metadata(&wal).unwrap().len());
    }

    #[test]
    fn dropped_offsets_still_move_their_streams_offset() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let dropped = StreamMaxima::from([("s".to_owned(), (3, T0)), ("q".to_owned(), (2, T0 + 5))]);

        let touched = write_batch(&mut conn, &[history("s", 1)], &dropped, 300, T0, false).unwrap();

        assert_eq!(BTreeSet::from(["q".to_owned(), "s".to_owned()]), touched);
        assert_eq!(vec![("q".to_owned(), 2, T0 + 5 + 3000), ("s".to_owned(), 3, T0 + 3000)], stream_rows(&conn));
        assert_eq!(vec![("s".to_owned(), 1, T0, "m1".to_owned())], history_rows(&conn), "only written ops have rows");
    }

    #[test]
    fn mark_running_reaches_the_database_file_itself() {
        // WAL with synchronous=NORMAL doesn't sync commits; mark_running
        // checkpoints, so the state is in the (synced) database file, not
        // only in the WAL.
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let mut conn = open(&path).unwrap();
        meta_set(&conn, "clean_shutdown", "1").unwrap();

        mark_running(&mut conn, "e1").unwrap();

        let copy = dir.file("copy.db");
        std::fs::copy(&path, &copy).unwrap();
        let copy = Connection::open(&copy).unwrap();
        assert_eq!(Some("e1".to_owned()), meta_get(&copy, "epoch").unwrap());
        assert_eq!(Some("0".to_owned()), meta_get(&copy, "clean_shutdown").unwrap());
    }

    #[test]
    fn a_non_numeric_schema_version_is_an_error_not_corruption() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        meta_set(&open(&path).unwrap(), "schema_version", "one").unwrap();

        match open(&path) {
            Err(StoreError::Other(why)) => assert!(why.contains("schema_version \"one\""), "{why}"),
            other => panic!("expected a startup error, got {other:?}"),
        }
    }

    fn pragma_i64(conn: &Connection, name: &str) -> i64 {
        conn.pragma_query_value(None, name, |r| r.get(0)).unwrap()
    }

    /// Writes `n` expired history entries of 4 KiB each to stream `s`.
    fn write_expired_history(conn: &mut Connection, n: u64) {
        let payload = "x".repeat(4096);
        let ops: Vec<WriteOp> = (1..=n)
            .map(|offset| WriteOp::History { stream: "s".into(), offset, ts_secs: T0 - 1000, data: payload.clone() })
            .collect();
        write_batch(conn, &ops, &StreamMaxima::new(), 300, T0, false).unwrap();
    }

    #[test]
    fn pruning_then_vacuuming_returns_the_freed_pages() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        write_expired_history(&mut conn, 80);
        let pages_before = pragma_i64(&conn, "page_count");
        assert!(pages_before > 40, "pages_before={pages_before}");

        let touched = Touched::from(["s".to_owned()]);
        let p = Prune { history_cutoff: T0, now_secs: T0, now_ms: T0 * 1000, history_limit: 100, streams: &touched };
        prune(&mut conn, &p).unwrap();
        let freed = pragma_i64(&conn, "freelist_count");
        assert!(freed > 40 && freed < 256, "freed={freed}");
        assert_eq!(u64::try_from(freed).unwrap(), incremental_vacuum(&conn, Some(256), None).unwrap());

        assert_eq!(0, pragma_i64(&conn, "freelist_count"));
        assert_eq!(pages_before - freed, pragma_i64(&conn, "page_count"));
    }

    #[test]
    fn incremental_vacuum_frees_the_requested_number_of_pages() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        write_expired_history(&mut conn, 80);
        conn.execute("DELETE FROM history", []).unwrap();
        let free = pragma_i64(&conn, "freelist_count");
        assert!(free > 20, "free={free}");

        assert_eq!(10, incremental_vacuum(&conn, Some(10), None).unwrap());
        assert_eq!(free - 10, pragma_i64(&conn, "freelist_count"));

        assert_eq!(u64::try_from(free - 10).unwrap(), incremental_vacuum(&conn, None, None).unwrap());
        assert_eq!(0, pragma_i64(&conn, "freelist_count"));
    }

    #[test]
    fn incremental_vacuum_stops_at_its_deadline() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        write_expired_history(&mut conn, 80);
        conn.execute("DELETE FROM history", []).unwrap();
        let free = pragma_i64(&conn, "freelist_count");

        assert_eq!(0, incremental_vacuum(&conn, None, Some(Instant::now())).unwrap());
        assert_eq!(free, pragma_i64(&conn, "freelist_count"), "a passed deadline frees nothing");

        let later = Instant::now() + Duration::from_secs(60);
        assert_eq!(u64::try_from(free).unwrap(), incremental_vacuum(&conn, None, Some(later)).unwrap());
        assert_eq!(0, pragma_i64(&conn, "freelist_count"));
    }

    #[test]
    fn a_file_without_auto_vacuum_is_converted_once_at_open() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        {
            // A file written before Cabby set auto_vacuum: the SQLite default (NONE), in WAL mode.
            let conn = Connection::open(&path).unwrap();
            let _: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0)).unwrap();
            migrate(&conn, MIGRATIONS).unwrap();
            let mut conn = conn;
            write_expired_history(&mut conn, 40);
            conn.execute("DELETE FROM history WHERE seq > 20", []).unwrap();
            meta_set(&conn, "epoch", "abcd").unwrap();
            assert_eq!(0, pragma_i64(&conn, "auto_vacuum"));
        }
        let conn = Connection::open(&path).unwrap();
        let pages_before = pragma_i64(&conn, "page_count");
        assert!(pragma_i64(&conn, "freelist_count") > 0);

        let mut announced = None;
        let converted = convert_to_incremental_auto_vacuum(&conn, |pages| {
            // Called before the VACUUM: the file is still unconverted.
            announced = Some((pages, pragma_i64(&conn, "auto_vacuum")));
        })
        .unwrap();

        assert_eq!(Some((pages_before, 0)), announced, "announced before converting, with the page count");
        let pages_after = pragma_i64(&conn, "page_count");
        assert_eq!(Some((pages_before, pages_after)), converted);
        assert!(pages_after < pages_before, "{pages_before} -> {pages_after}");
        assert_eq!(2, pragma_i64(&conn, "auto_vacuum"), "auto_vacuum=INCREMENTAL");
        let again = convert_to_incremental_auto_vacuum(&conn, |_| panic!("nothing to convert")).unwrap();
        assert_eq!(None, again, "converted only once");
        drop(conn);

        let conn = open(&path).unwrap();
        assert_eq!(2, pragma_i64(&conn, "auto_vacuum"));
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
        assert_eq!("wal", mode);
        assert_eq!(Some("abcd".to_owned()), meta_get(&conn, "epoch").unwrap(), "data survives");
        let rows: i64 = conn.query_row("SELECT COUNT(*) FROM history", [], |r| r.get(0)).unwrap();
        assert_eq!(20, rows);
    }

    #[test]
    fn open_converts_a_file_without_auto_vacuum() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        {
            let conn = Connection::open(&path).unwrap();
            let _: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0)).unwrap();
            migrate(&conn, MIGRATIONS).unwrap();
            assert_eq!(0, pragma_i64(&conn, "auto_vacuum"));
        }

        let conn = open(&path).unwrap();

        assert_eq!(2, pragma_i64(&conn, "auto_vacuum"));
    }

    /// A file written before Cabby set auto_vacuum: the SQLite default (NONE),
    /// in WAL mode, with `n` history entries of 4 KiB and its WAL checkpointed.
    fn unconverted_file(path: &Path, n: u64, configure: impl FnOnce(&Connection)) {
        let mut conn = Connection::open(path).unwrap();
        let _: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0)).unwrap();
        migrate(&conn, MIGRATIONS).unwrap();
        write_expired_history(&mut conn, n);
        configure(&conn);
        checkpoint(&conn).unwrap();
        assert_eq!(0, pragma_i64(&conn, "auto_vacuum"));
    }

    fn wal_len(path: &Path) -> u64 {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        std::fs::metadata(wal).map_or(0, |m| m.len())
    }

    #[test]
    fn the_conversion_at_open_is_checkpointed_at_once() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        unconverted_file(&path, 200, |conn| {
            conn.execute("DELETE FROM history WHERE seq > 100", []).unwrap();
        });
        let size_before = std::fs::metadata(&path).unwrap().len();
        assert!(size_before > 600 * 1024, "size_before={size_before}");

        let conn = open(&path).unwrap();

        assert_eq!(2, pragma_i64(&conn, "auto_vacuum"));
        assert!(wal_len(&path) < 64 * 1024, "the VACUUM copy is still in the WAL: {} bytes", wal_len(&path));
        let size_after = std::fs::metadata(&path).unwrap().len();
        assert!(size_after < size_before * 3 / 4, "the space comes back at once: {size_before} -> {size_after}");
    }

    #[test]
    fn a_file_that_open_refuses_is_never_converted() {
        for (version, newer) in [("99", true), ("one", false)] {
            let dir = TempDir::new();
            let path = dir.file("cabby.db");
            unconverted_file(&path, 20, |conn| {
                conn.execute("DELETE FROM history WHERE seq > 10", []).unwrap();
                meta_set(conn, "schema_version", version).unwrap();
            });
            let bytes_before = std::fs::read(&path).unwrap();

            match open(&path) {
                Err(StoreError::NewerSchema { found: 99, .. }) if newer => {}
                Err(StoreError::Other(why)) if !newer => assert!(why.contains("invalid schema_version"), "{why}"),
                other => panic!("schema_version {version}: expected a refusal, got {other:?}"),
            }

            let conn = Connection::open(&path).unwrap();
            assert_eq!(0, pragma_i64(&conn, "auto_vacuum"), "schema_version {version}: not converted");
            assert!(pragma_i64(&conn, "freelist_count") > 0, "schema_version {version}: not vacuumed");
            drop(conn);
            assert_eq!(bytes_before, std::fs::read(&path).unwrap(), "schema_version {version}: file unchanged");
        }
    }
}
