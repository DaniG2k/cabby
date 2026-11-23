//! Files around the database (spec §2, §3, §6): the parent directory (mode
//! 0700), the database file (0600), the lock file and moving a corrupt
//! database aside.

use std::fs::{DirBuilder, File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(super) const DIR_MODE: u32 = 0o700;
pub(super) const FILE_MODE: u32 = 0o600;
/// How often a held lock is tried again (spec §5 step 1).
const LOCK_RETRY: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(super) enum LockError {
    /// Another process still holds the lock.
    Held,
    Io(std::io::Error),
}

/// `<db><suffix>`, e.g. `cabby.db.lock`, `cabby.db-wal`.
pub(super) fn sibling(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

pub(super) fn lock_path(db: &Path) -> PathBuf {
    sibling(db, ".lock")
}

/// Creates the database's parent directory, and missing ancestors, with mode
/// 0700. An existing directory is left as it is.
pub(super) fn prepare_dir(db: &Path) -> std::io::Result<()> {
    match db.parent() {
        Some(dir) if !dir.as_os_str().is_empty() && !dir.exists() => {
            DirBuilder::new().recursive(true).mode(DIR_MODE).create(dir)
        }
        _ => Ok(()),
    }
}

/// Creates the database file with mode 0600 unless it exists.
pub(super) fn create_db_file(db: &Path) -> std::io::Result<()> {
    match OpenOptions::new().write(true).create_new(true).mode(FILE_MODE).open(db) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

/// Takes the advisory lock on `<db>.lock`, trying every 100 ms for up to `timeout`.
pub(super) fn acquire_lock(db: &Path, timeout: Duration) -> Result<File, LockError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(FILE_MODE)
        .open(lock_path(db))
        .map_err(LockError::Io)?;
    let deadline = Instant::now() + timeout;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(LockError::Held);
                }
                std::thread::sleep(LOCK_RETRY.min(left));
            }
            Err(TryLockError::Error(e)) => return Err(LockError::Io(e)),
        }
    }
}

/// Renames the database and its `-wal` and `-shm` files to
/// `<db>.corrupt-<now_secs>` (`…-wal`, `…-shm`); returns the new database path.
pub(super) fn move_aside(db: &Path, now_secs: i64) -> std::io::Result<PathBuf> {
    let target = sibling(db, &format!(".corrupt-{now_secs}"));
    std::fs::rename(db, &target)?;
    for side in ["-wal", "-shm"] {
        let from = sibling(db, side);
        if from.exists() {
            std::fs::rename(&from, sibling(&target, side))?;
        }
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::broker::sqlite::testing::TempDir;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn the_lock_is_exclusive_until_released() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        let first = acquire_lock(&db, Duration::ZERO).unwrap();

        let started = Instant::now();
        assert!(matches!(acquire_lock(&db, Duration::from_millis(250)), Err(LockError::Held)));
        assert!(started.elapsed() >= Duration::from_millis(250), "retried until the timeout");

        drop(first);
        assert!(acquire_lock(&db, Duration::ZERO).is_ok(), "released with its file");
    }

    #[test]
    fn a_lock_released_while_waiting_is_taken() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        let first = acquire_lock(&db, Duration::ZERO).unwrap();
        let waiter = {
            let db = db.clone();
            std::thread::spawn(move || acquire_lock(&db, Duration::from_secs(10)).is_ok())
        };
        drop(first);
        assert!(waiter.join().unwrap());
    }

    #[test]
    fn created_directories_and_files_are_private() {
        let dir = TempDir::new();
        let db = dir.path().join("a").join("b").join("cabby.db");

        prepare_dir(&db).unwrap();
        create_db_file(&db).unwrap();
        let _lock = acquire_lock(&db, Duration::ZERO).unwrap();

        assert_eq!(DIR_MODE, mode(&dir.path().join("a")));
        assert_eq!(DIR_MODE, mode(&dir.path().join("a").join("b")));
        assert_eq!(FILE_MODE, mode(&db));
        assert_eq!(FILE_MODE, mode(&lock_path(&db)));
    }

    #[test]
    fn an_existing_database_file_is_kept() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        std::fs::write(&db, b"data").unwrap();

        create_db_file(&db).unwrap();

        assert_eq!(b"data".to_vec(), std::fs::read(&db).unwrap());
    }

    #[test]
    fn a_directory_under_a_file_cannot_be_prepared() {
        let dir = TempDir::new();
        std::fs::write(dir.file("blocker"), b"").unwrap();
        assert!(prepare_dir(&dir.path().join("blocker").join("sub").join("cabby.db")).is_err());
    }

    #[test]
    fn moving_aside_renames_the_database_and_its_wal_and_shm() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        for (path, content) in [(db.clone(), "db"), (sibling(&db, "-wal"), "wal"), (sibling(&db, "-shm"), "shm")] {
            std::fs::write(path, content).unwrap();
        }

        let moved = move_aside(&db, 1_700_000_000).unwrap();

        assert_eq!(dir.file("cabby.db.corrupt-1700000000"), moved);
        assert_eq!("db", std::fs::read_to_string(&moved).unwrap());
        assert_eq!("wal", std::fs::read_to_string(sibling(&moved, "-wal")).unwrap());
        assert_eq!("shm", std::fs::read_to_string(sibling(&moved, "-shm")).unwrap());
        assert!(!db.exists() && !sibling(&db, "-wal").exists() && !sibling(&db, "-shm").exists());
    }

    #[test]
    fn moving_aside_without_wal_and_shm() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        std::fs::write(&db, "db").unwrap();
        let moved = move_aside(&db, 5).unwrap();
        assert!(moved.exists() && !db.exists());
    }
}
