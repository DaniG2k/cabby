//! Startup (spec §5, §6, §7): the lock, opening and checking the file, the
//! epoch, and loading streams and sessions into the memory broker before
//! Cabby accepts connections.

use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{Connection, params};

use super::files::{self, LockError};
use super::persister::Database;
use super::store::{self, StoreError};
use super::{SqliteOptions, log, units};
use crate::broker::{BrokerError, Clock, Config, Entry, Memory, nanoid};
use crate::{log_info, log_warn};

/// How the previous run ended, as the loader found the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Previous {
    /// No epoch stored yet: a first start.
    NewDatabase,
    /// Shut down gracefully: the epoch is kept.
    Clean,
    /// Crashed, killed, or the final write failed: a new epoch.
    Unclean,
    /// The file was corrupt and moved to this path; a new database.
    Corrupt(PathBuf),
}

/// What the loader restored (spec §7's startup line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadReport {
    pub messages: usize,
    pub streams: usize,
    pub sessions: usize,
    pub epoch: String,
    pub previous: Previous,
    pub took: Duration,
}

impl LoadReport {
    /// E.g. `restored 4210 messages across 37 streams and 112 sessions (epoch
    /// abcd, clean shutdown) in 18 ms`.
    pub fn summary(&self) -> String {
        let how = match &self.previous {
            Previous::NewDatabase => "new database".to_owned(),
            Previous::Clean => "clean shutdown".to_owned(),
            Previous::Unclean => "new epoch after an unclean shutdown".to_owned(),
            Previous::Corrupt(moved) => format!("new database, the corrupt file was moved to {}", moved.display()),
        };
        format!(
            "restored {} messages across {} streams and {} sessions (epoch {}, {how}) in {} ms",
            self.messages,
            self.streams,
            self.sessions,
            self.epoch,
            self.took.as_millis()
        )
    }
}

/// The open database and what was loaded from it.
pub(super) struct Loaded {
    pub(super) db: Database,
    pub(super) report: LoadReport,
}

/// Opens `options.path` and fills `memory` from it.
pub(super) fn load(
    memory: &Memory,
    config: &Config,
    options: &SqliteOptions,
    clock: &dyn Clock,
) -> Result<Loaded, BrokerError> {
    let started = Instant::now();
    let path = options.path.as_path();
    files::prepare_dir(path).map_err(|e| error(path, format!("cannot create its directory: {e}")))?;
    let lock = files::acquire_lock(path, options.lock_timeout).map_err(|e| match e {
        LockError::Held => error(
            path,
            format!(
                "in use by another Cabby process (lock file {} still held after {} s)",
                files::lock_path(path).display(),
                options.lock_timeout.as_secs()
            ),
        ),
        LockError::Io(e) => error(path, format!("cannot open the lock file {}: {e}", files::lock_path(path).display())),
    })?;

    // Corruption can show at open (quick_check) or only while reading; either
    // way the file is moved aside once and a new one started (spec §6).
    // Nothing reaches the memory broker until the whole file was read.
    let (mut conn, mut moved) = match open_file(path) {
        Ok(conn) => (conn, None),
        Err(StoreError::Corrupt(why)) => {
            let moved = move_aside(path, &why, clock)?;
            (open_file(path).map_err(|e| error(path, e))?, Some(moved))
        }
        Err(e) => return Err(error(path, e)),
    };
    let contents = match read(&mut conn, moved.clone(), config, clock) {
        Ok(contents) => contents,
        Err(StoreError::Corrupt(why)) if moved.is_none() => {
            drop(conn);
            let to = move_aside(path, &why, clock)?;
            conn = open_file(path).map_err(|e| error(path, e))?;
            moved = Some(to);
            read(&mut conn, moved.clone(), config, clock).map_err(|e| error(path, e))?
        }
        Err(e) => return Err(error(path, e)),
    };

    let Contents { epoch, previous, streams, sessions } = contents;
    memory.set_epoch(&epoch);
    let messages = streams.iter().map(|s| s.entries.len()).sum();
    let stream_count = streams.len();
    for s in streams {
        memory.restore_stream(&s.name, s.max_offset, s.deadline, s.entries).map_err(|e| error(path, e.message))?;
    }
    let session_count = sessions.len();
    for (sid, data, expires_at_ms) in sessions {
        memory.restore_session_with_deadline(&sid, data, expires_at_ms);
    }
    let report = LoadReport {
        messages,
        streams: stream_count,
        sessions: session_count,
        epoch,
        previous,
        took: started.elapsed(),
    };
    log_info!(log(), &report.summary(), "path" => path.display().to_string());
    Ok(Loaded { db: Database { conn, lock, path: path.to_owned() }, report })
}

/// Renames a corrupt database (with its -wal and -shm) and logs where it went.
fn move_aside(path: &Path, why: &str, clock: &dyn Clock) -> Result<PathBuf, BrokerError> {
    let moved = files::move_aside(path, units::now_secs(clock))
        .map_err(|e| error(path, format!("is corrupt ({why}) and could not be moved aside: {e}")))?;
    log_warn!(log(), &format!("the SQLite database is corrupt ({why}); moved it to {} and starting a new one",
        moved.display()), "path" => path.display().to_string());
    Ok(moved)
}

/// A stream as stored: its offset, deadline and contiguous tail.
struct StoredStream {
    name: String,
    max_offset: u64,
    deadline: i64,
    entries: Vec<Entry>,
}

/// Everything the loader takes from the file.
struct Contents {
    epoch: String,
    previous: Previous,
    streams: Vec<StoredStream>,
    /// (sid, data, expires_at_ms)
    sessions: Vec<(String, Vec<u8>, i64)>,
}

/// Decides the epoch, marks the run, and reads the streams and sessions.
fn read(
    conn: &mut Connection,
    moved: Option<PathBuf>,
    config: &Config,
    clock: &dyn Clock,
) -> Result<Contents, StoreError> {
    let saved = store::meta_get(conn, "epoch")?.filter(|e| !e.is_empty());
    let clean = store::meta_get(conn, "clean_shutdown")?.as_deref() == Some("1");
    let (epoch, previous) = match (moved, saved) {
        (Some(moved), _) => (nanoid(4), Previous::Corrupt(moved)),
        (None, None) => (nanoid(4), Previous::NewDatabase),
        (None, Some(saved)) if clean => (saved, Previous::Clean),
        (None, Some(_)) => (nanoid(4), Previous::Unclean),
    };
    store::mark_running(conn, &epoch)?;
    let streams = read_streams(conn, config, clock)?;
    let sessions = read_sessions(conn, clock)?;
    Ok(Contents { epoch, previous, streams, sessions })
}

fn error(path: &Path, e: impl Display) -> BrokerError {
    BrokerError { message: format!("SQLite database {}: {e}", path.display()) }
}

/// Creates the file with mode 0600 if needed (SQLite gives its -wal and -shm
/// files the same mode), then opens and checks it.
fn open_file(path: &Path) -> Result<Connection, StoreError> {
    files::create_db_file(path).map_err(|e| StoreError::Other(format!("cannot create the file: {e}")))?;
    store::open(path)
}

/// Every stream still within its deadline, with its contiguous tail.
fn read_streams(conn: &Connection, config: &Config, clock: &dyn Clock) -> Result<Vec<StoredStream>, StoreError> {
    let now = units::now_secs(clock);
    let cutoff = units::history_cutoff(now, config.history_ttl);
    let limit = i64::try_from(config.history_limit).unwrap_or(i64::MAX);
    let rows: Vec<(String, i64, i64)> = conn
        .prepare(store::LOAD_STREAMS)?
        .query_map([now], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    let mut tail = conn.prepare(store::LOAD_TAIL)?;
    let mut streams = Vec::with_capacity(rows.len());
    for (name, max_seq, deadline) in rows {
        let Some(max_offset) = units::offset_from_sql(max_seq) else { continue };
        // Walk down from max_seq; stop at the first gap or expired entry.
        let mut expected = max_offset;
        let mut entries = Vec::new();
        let mut found = tail.query(params![name, max_seq, limit])?;
        while let Some(row) = found.next()? {
            let (seq, ts, data): (i64, i64, String) = (row.get(0)?, row.get(1)?, row.get(2)?);
            if expected == 0 || units::offset_from_sql(seq) != Some(expected) || ts < cutoff {
                break;
            }
            entries.push(Entry { offset: expected, timestamp: ts, data });
            expected -= 1;
        }
        entries.reverse();
        streams.push(StoredStream { name, max_offset, deadline, entries });
    }
    Ok(streams)
}

/// Every unexpired session with its stored expiry.
fn read_sessions(conn: &Connection, clock: &dyn Clock) -> Result<Vec<(String, Vec<u8>, i64)>, StoreError> {
    let mut stmt = conn.prepare(store::LOAD_SESSIONS)?;
    let rows = stmt.query_map([units::now_millis(clock)], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
    Ok(rows.collect::<Result<_, _>>()?)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    use super::*;
    use crate::broker::Broker;
    use crate::broker::sqlite::queue::{StreamMaxima, WriteOp};
    use crate::broker::sqlite::testing::{TempDir, commit, history, history_at};
    use crate::broker::test_clock::{T0, TestClock};

    /// Writes `ops` and `meta` into the database at `db`, as an earlier run would have.
    fn seed(db: &Path, history_ttl: i64, ops: &[WriteOp], meta: &[(&str, &str)]) {
        files::prepare_dir(db).unwrap();
        let mut conn = store::open(db).unwrap();
        store::write_batch(&mut conn, ops, &StreamMaxima::new(), history_ttl, T0, false).unwrap();
        for (key, value) in meta {
            store::meta_set(&conn, key, value).unwrap();
        }
    }

    fn options(dir: &TempDir) -> SqliteOptions {
        let mut options = SqliteOptions::new(dir.db());
        options.lock_timeout = Duration::from_millis(200);
        options
    }

    /// Loads `dir`'s database into a new memory broker on `clock`.
    fn load_into(dir: &TempDir, config: &Config, clock: &Arc<TestClock>) -> Result<(Memory, Loaded), BrokerError> {
        let memory = Memory::new(None, None, config, clock.clone());
        let loaded = load(&memory, config, &options(dir), clock.as_ref())?;
        Ok((memory, loaded))
    }

    fn data(memory: &Memory, stream: &str) -> Vec<String> {
        memory.history_since(stream, 0).unwrap().unwrap_or_default().into_iter().map(|m| m.data).collect()
    }

    fn corrupt_files(dir: &TempDir) -> Vec<String> {
        std::fs::read_dir(dir.db().parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt-"))
            .collect()
    }

    #[test]
    fn an_empty_directory_gives_a_new_database() {
        let dir = TempDir::new();
        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        let r = &loaded.report;
        assert_eq!((0, 0, 0, Previous::NewDatabase), (r.messages, r.streams, r.sessions, r.previous.clone()));
        assert_eq!(memory.epoch(), r.epoch);
        assert_eq!(4, r.epoch.len());
        assert_eq!(Some(r.epoch.clone()), store::meta_get(&loaded.db.conn, "epoch").unwrap());
        assert_eq!(Some("0".to_owned()), store::meta_get(&loaded.db.conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn history_is_loaded_within_the_limit_and_the_ttl() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_limit = 3;
        config.history_ttl = 60;
        let mut ops: Vec<WriteOp> = (1..=5).map(|offset| history("s", offset)).collect();
        ops.extend([history_at("t", 1, T0 - 120), history_at("t", 2, T0 - 120), history_at("t", 3, T0)]);
        seed(&dir.db(), 60, &ops, &[]);

        let (memory, loaded) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert_eq!(vec!["m3", "m4", "m5"], data(&memory, "s"), "the newest history_limit entries");
        assert_eq!(vec!["m3"], data(&memory, "t"), "entries beyond the TTL are not loaded");
        assert_eq!((4, 2), (loaded.report.messages, loaded.report.streams));
        assert_eq!(6, memory.add("s", "next"));
    }

    #[test]
    fn a_gap_loads_only_the_contiguous_tail() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[history("s", 1), history("s", 2), history("s", 4), history("s", 5)], &[]);

        let (memory, _) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!(vec!["m4", "m5"], data(&memory, "s"));
        assert_eq!(6, memory.add("s", "next"));
    }

    #[test]
    fn a_stream_whose_newest_row_is_missing_loads_empty_at_its_offset() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &(1..=5).map(|offset| history("s", offset)).collect::<Vec<_>>(), &[]);
        store::open(&dir.db()).unwrap().execute("UPDATE streams SET max_seq = 6", []).unwrap();

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!(Some(vec![]), memory.history_since("s", 0).unwrap(), "the stream exists, without entries");
        assert_eq!((0, 1), (loaded.report.messages, loaded.report.streams));
        assert_eq!(7, memory.add("s", "next"));
    }

    #[test]
    fn a_stream_whose_entries_all_expired_keeps_its_offset() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_ttl = 60;
        // Deadline T0 - 120 + 600: still ahead.
        seed(&dir.db(), 60, &[history_at("s", 1, T0 - 120), history_at("s", 2, T0 - 120)], &[]);

        let (memory, _) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert!(data(&memory, "s").is_empty());
        assert_eq!(3, memory.add("s", "next"), "max_seq + 1");
    }

    #[test]
    fn a_stream_past_its_deadline_is_not_loaded() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_ttl = 60;
        // Deadline T0 - 1000 + 600: passed.
        seed(&dir.db(), 60, &[history_at("s", 4, T0 - 1000)], &[]);

        let (memory, loaded) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert_eq!(None, memory.history_since("s", 0).unwrap());
        assert_eq!(0, loaded.report.streams);
        assert_eq!(1, memory.add("s", "next"));
    }

    #[test]
    fn unexpired_sessions_are_restored_with_their_original_expiry() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        seed(&dir.db(), 300, &[commit("live", (T0 + 10) * 1000), commit("gone", T0 * 1000)], &[]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &clock).unwrap();

        assert_eq!(1, loaded.report.sessions);
        assert_eq!(Some(b"live".to_vec()), memory.restore_session("live").unwrap());
        assert_eq!(None, memory.restore_session("gone").unwrap());
        clock.advance(Duration::from_millis(9_999));
        memory.expire();
        assert!(memory.restore_session("live").unwrap().is_some());
        clock.advance(Duration::from_millis(1));
        memory.expire();
        assert_eq!(None, memory.restore_session("live").unwrap(), "expired at its original deadline");
    }

    #[test]
    fn the_epoch_is_kept_after_a_clean_shutdown() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("epoch", "abcd"), ("clean_shutdown", "1")]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!("abcd", memory.epoch());
        assert_eq!(Previous::Clean, loaded.report.previous);
        assert_eq!(Some("0".to_owned()), store::meta_get(&loaded.db.conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn the_epoch_is_rotated_after_an_unclean_shutdown() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("epoch", "abcd"), ("clean_shutdown", "0")]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_ne!("abcd", memory.epoch());
        assert_eq!(Previous::Unclean, loaded.report.previous);
        assert_eq!(Some(memory.epoch()), store::meta_get(&loaded.db.conn, "epoch").unwrap());
    }

    #[test]
    fn a_corrupt_file_is_moved_aside_and_a_new_one_started() {
        let dir = TempDir::new();
        let db = dir.db();
        files::prepare_dir(&db).unwrap();
        std::fs::write(&db, vec![0x42_u8; 8192]).unwrap();

        let (_, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        let Previous::Corrupt(moved) = &loaded.report.previous else {
            panic!("expected Corrupt, got {:?}", loaded.report.previous)
        };
        assert_eq!(&files::sibling(&db, &format!(".corrupt-{T0}")), moved);
        assert_eq!(vec![0x42_u8; 8192], std::fs::read(moved).unwrap(), "the corrupt file is kept for inspection");
        assert_eq!(1, store::schema_version(&loaded.db.conn).unwrap(), "a new database");
    }

    #[test]
    fn a_newer_schema_is_refused_and_the_file_kept() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("schema_version", "99")]);

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");

        assert!(err.message.contains("schema version 99 is newer than this Cabby supports (1)"), "{}", err.message);
        assert!(dir.db().exists());
        assert!(corrupt_files(&dir).is_empty());
    }

    #[test]
    fn an_unreadable_file_is_an_error_not_corruption() {
        let dir = TempDir::new();
        let db = dir.db();
        seed(&db, 300, &[], &[]);
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&db).is_ok() {
            return; // Running as root: file permissions don't apply.
        }

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();

        assert!(err.message.contains("unable to open database file"), "{}", err.message);
        assert!(corrupt_files(&dir).is_empty());
    }

    #[test]
    fn a_database_in_use_is_refused() {
        let dir = TempDir::new();
        let db = dir.db();
        files::prepare_dir(&db).unwrap();
        let _held = files::acquire_lock(&db, Duration::ZERO).unwrap();

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");

        assert!(err.message.contains("in use by another Cabby process"), "{}", err.message);
        assert!(!db.exists(), "not even opened");
    }

    #[test]
    fn an_impossible_directory_is_an_error() {
        let dir = TempDir::new();
        std::fs::write(dir.file("blocker"), b"").unwrap();
        let clock = TestClock::new();
        let memory = Memory::new(None, None, &Config::new(), clock.clone());
        let options = SqliteOptions::new(dir.path().join("blocker").join("sub").join("cabby.db"));

        let err = load(&memory, &Config::new(), &options, clock.as_ref()).err().expect("refused");

        assert!(err.message.contains("cannot create its directory"), "{}", err.message);
    }

    #[test]
    fn the_database_its_wal_shm_lock_file_and_directory_are_private() {
        let dir = TempDir::new();
        let (_memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        let db = dir.db();
        assert_eq!(0o700, mode(db.parent().unwrap()));
        for path in [db.clone(), files::sibling(&db, "-wal"), files::sibling(&db, "-shm"), files::lock_path(&db)] {
            assert_eq!(0o600, mode(&path), "{}", path.display());
        }
        drop(loaded);
    }

    #[test]
    fn the_startup_line_says_what_was_restored() {
        let report = |previous| LoadReport {
            messages: 4210,
            streams: 37,
            sessions: 112,
            epoch: "abcd".into(),
            previous,
            took: Duration::from_millis(18),
        };
        let head = "restored 4210 messages across 37 streams and 112 sessions";
        assert_eq!(format!("{head} (epoch abcd, clean shutdown) in 18 ms"), report(Previous::Clean).summary());
        assert_eq!(
            format!("{head} (epoch abcd, new epoch after an unclean shutdown) in 18 ms"),
            report(Previous::Unclean).summary()
        );
        assert_eq!(format!("{head} (epoch abcd, new database) in 18 ms"), report(Previous::NewDatabase).summary());
        assert_eq!(
            format!("{head} (epoch abcd, new database, the corrupt file was moved to /d/cabby.db.corrupt-5) in 18 ms"),
            report(Previous::Corrupt("/d/cabby.db.corrupt-5".into())).summary()
        );
    }

    /// Swaps the root pages of `history_stream_seq` and a decoy index, so the
    /// file passes `quick_check` (it doesn't compare indexes with their
    /// tables) but reading a stream's tail through the index hits a row that
    /// doesn't exist: `SQLITE_CORRUPT`.
    fn corrupt_the_history_index(db: &Path) {
        let conn = store::open(db).unwrap();
        conn.execute_batch(
            "CREATE TABLE decoy (stream TEXT, seq INTEGER);
             CREATE UNIQUE INDEX decoy_idx ON decoy (stream, seq);
             INSERT INTO decoy (rowid, stream, seq) VALUES (1000, 's', 1), (1001, 's', 2);",
        )
        .unwrap();
        let root = |name: &str| -> i64 {
            conn.query_row("SELECT rootpage FROM sqlite_schema WHERE name = ?1", [name], |r| r.get(0)).unwrap()
        };
        let (history_root, decoy_root) = (root("history_stream_seq"), root("decoy_idx"));
        conn.execute_batch("PRAGMA writable_schema = ON").unwrap();
        conn.execute("UPDATE sqlite_schema SET rootpage = ?1 WHERE name = 'history_stream_seq'", [decoy_root]).unwrap();
        conn.execute("UPDATE sqlite_schema SET rootpage = ?1 WHERE name = 'decoy_idx'", [history_root]).unwrap();
        conn.execute_batch("PRAGMA writable_schema = OFF").unwrap();
        drop(conn);
        assert!(store::open(db).is_ok(), "the damage passes the checks at open");
    }

    #[test]
    fn corruption_found_while_loading_moves_the_file_aside_too() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[history("s", 1), history("s", 2), commit("live", (T0 + 10) * 1000)], &[]);
        corrupt_the_history_index(&dir.db());

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        let Previous::Corrupt(moved) = &loaded.report.previous else {
            panic!("expected Corrupt, got {:?}", loaded.report.previous)
        };
        assert!(moved.exists(), "the corrupt file is kept for inspection");
        let r = &loaded.report;
        assert_eq!((0, 0, 0), (r.messages, r.streams, r.sessions), "a new database");
        assert_eq!(None, memory.history_since("s", 0).unwrap(), "nothing from the corrupt file reached memory");
        assert_eq!(None, memory.restore_session("live").unwrap());
        assert_eq!(1, store::schema_version(&loaded.db.conn).unwrap());
    }

    #[test]
    fn a_non_numeric_schema_version_is_refused_and_the_file_kept() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("schema_version", "one")]);

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");

        assert!(err.message.contains("schema_version \"one\""), "{}", err.message);
        assert!(dir.db().exists());
        assert!(corrupt_files(&dir).is_empty());
    }
}
