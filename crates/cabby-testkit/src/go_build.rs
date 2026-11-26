//! Go test binaries built under `target/` (reference bridges, `nats-server`)
//! for test processes and threads that may run at the same time: each output
//! is built at most once per process, a lock file next to it serializes
//! builds across processes, and the build writes a unique temporary file that
//! is then renamed into place, so nobody runs a partly written binary and a
//! running one is never overwritten.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Result, anyhow};

/// What to do with a binary that is already in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reuse {
    /// Use it (a server binary pinned by version).
    Existing,
    /// Build again once per process (the bridges, whose sources change; the
    /// Go build cache makes an unchanged rebuild quick).
    Rebuild,
}

/// How a build is run and reported.
pub struct Build<'a> {
    /// What is built, for messages ("the reference bridge").
    pub what: &'a str,
    /// Added to the message of a build that ran and failed.
    pub hint: &'a str,
    pub reuse: Reuse,
}

type Built = Arc<OnceLock<std::result::Result<PathBuf, String>>>;

/// Builds `out` with the command `command(tmp)` returns, which must write the
/// binary to `tmp`; once per process for each `out`. Threads asking for the
/// same `out` meanwhile wait for that build and get its result.
pub fn build(out: &Path, how: &Build<'_>, command: impl FnOnce(&Path) -> Command) -> Result<PathBuf> {
    static BUILT: OnceLock<Mutex<HashMap<PathBuf, Built>>> = OnceLock::new();
    let once = {
        let mut built = BUILT.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
        built.entry(out.to_path_buf()).or_default().clone()
    };
    once.get_or_init(|| build_locked(out, how, command).map(|()| out.to_path_buf()).map_err(|e| format!("{e:#}")))
        .clone()
        .map_err(|e| anyhow!(e))
}

/// One build of `out` under its lock file, without the per-process memo.
fn build_locked(out: &Path, how: &Build<'_>, command: impl FnOnce(&Path) -> Command) -> Result<()> {
    let name = out.file_name().ok_or_else(|| anyhow!("{} has no file name", out.display()))?.to_string_lossy();
    if let Some(dir) = out.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let lock =
        File::options().create(true).truncate(false).write(true).open(out.with_file_name(format!("{name}.lock")))?;
    lock.lock()?;
    if how.reuse == Reuse::Existing && out.exists() {
        return Ok(());
    }
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let tmp = out.with_file_name(format!(".{name}.tmp-{}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::Relaxed)));
    let mut cmd = command(&tmp);
    let status = cmd.status();
    let built = match status {
        Err(e) => Err(anyhow!("running {cmd:?} for {} (is the Go toolchain on PATH?): {e}", how.what)),
        Ok(s) if !s.success() => Err(anyhow!("building {} failed ({s}); {}", how.what, how.hint)),
        Ok(_) => std::fs::rename(&tmp, out).map_err(|e| anyhow!("moving {} into place: {e}", how.what)),
    };
    if built.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    built
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Duration;

    fn scratch() -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "cabby-go-build-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A stand-in for `go build -o <tmp>`: writes the binary in two steps
    /// with a pause between them, and logs its start and end to `log`.
    fn slow_build(tmp: &Path, log: &Path) -> Command {
        let mut cmd = Command::new("sh");
        cmd.args([
            "-c",
            r#"echo start >> "$2"; printf partial > "$1"; sleep 0.1; printf -- -done >> "$1"; echo end >> "$2""#,
            "sh",
        ])
        .arg(tmp)
        .arg(log);
        cmd
    }

    fn how(reuse: Reuse) -> Build<'static> {
        Build { what: "a stand-in", hint: "see the test", reuse }
    }

    fn lines(path: &Path) -> Vec<String> {
        std::fs::read_to_string(path).unwrap_or_default().lines().map(str::to_owned).collect()
    }

    /// Builds of one output from several threads, each with its own lock file
    /// handle as separate processes have: never two at a time, and a reader
    /// never sees a partly written binary.
    fn race(reuse: Reuse, builders: usize) -> (Vec<String>, PathBuf) {
        let dir = scratch();
        let (out, log) = (dir.join("bin"), dir.join("log"));
        let start = Barrier::new(builders + 1);
        let done = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            let reader = s.spawn(|| {
                start.wait();
                let mut seen = 0;
                while !done.load(Ordering::SeqCst) {
                    if let Ok(text) = std::fs::read_to_string(&out) {
                        assert_eq!("partial-done", text, "a reader saw a partial binary");
                        seen += 1;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                seen
            });
            let builders: Vec<_> = (0..builders)
                .map(|_| {
                    s.spawn(|| {
                        start.wait();
                        build_locked(&out, &how(reuse), |tmp| slow_build(tmp, &log)).unwrap();
                    })
                })
                .collect();
            for b in builders {
                b.join().unwrap();
            }
            done.store(true, Ordering::SeqCst);
            reader.join().unwrap();
        });
        assert_eq!("partial-done", std::fs::read_to_string(&out).unwrap());
        let mut left: Vec<String> =
            std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        left.sort();
        assert_eq!(vec!["bin", "bin.lock", "log"], left, "no temporary file is left");
        (lines(&log), dir)
    }

    #[test]
    fn concurrent_builds_of_an_existing_binary_run_once() {
        let (log, dir) = race(Reuse::Existing, 8);
        assert_eq!(vec!["start", "end"], log);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_rebuilds_take_turns() {
        let (log, dir) = race(Reuse::Rebuild, 4);
        assert_eq!(["start", "end"].repeat(4), log, "one build at a time");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failed_build_leaves_nothing_in_place() {
        let dir = scratch();
        let out = dir.join("bin");
        let e = build_locked(&out, &how(Reuse::Rebuild), |tmp| {
            let mut cmd = Command::new("sh");
            cmd.args(["-c", r#"printf partial > "$1"; exit 3"#, "sh"]).arg(tmp);
            cmd
        })
        .unwrap_err();
        assert!(e.to_string().starts_with("building a stand-in failed"), "{e}");
        assert!(e.to_string().ends_with("; see the test"), "{e}");
        let left: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().map(|e| e.file_name()).collect();
        assert_eq!(vec![std::ffi::OsString::from("bin.lock")], left);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_process_builds_each_output_once() {
        let dir = scratch();
        let (out, log) = (dir.join("bin"), dir.join("log"));
        for _ in 0..3 {
            assert_eq!(out, build(&out, &how(Reuse::Rebuild), |tmp| slow_build(tmp, &log)).unwrap());
        }
        assert_eq!(vec!["start", "end"], lines(&log));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
