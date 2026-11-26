//! Client for `compat/reference-adapters/bridge`, a Go program that exposes
//! exported functions of the pinned reference over JSON lines (protocol v1).
//!
//! One bridge process is shared per test binary; calls are serialized and each
//! has a deadline.

use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;
const CALL_DEADLINE: Duration = Duration::from_secs(30);

struct Bridge {
    _child: Child,
    stdin: ChildStdin,
    lines: Receiver<std::io::Result<String>>,
}

static BRIDGE: OnceLock<Mutex<Bridge>> = OnceLock::new();

/// Path to the bridge binary: `CABBY_GO_BRIDGE`, or built once with `go build`.
fn bridge_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("CABBY_GO_BRIDGE") {
        return Ok(PathBuf::from(p));
    }
    let root = crate::workspace_root();
    let how = crate::go_build::Build {
        what: "the reference bridge",
        hint: "run `cargo xtask reference-checkout` first",
        reuse: crate::go_build::Reuse::Rebuild,
    };
    crate::go_build::build(&root.join("target/reference-adapters/bridge"), &how, |tmp| {
        let mut cmd = Command::new("go");
        cmd.args(["build", "-o"]).arg(tmp).arg(".").current_dir(root.join("compat/reference-adapters/bridge"));
        cmd
    })
}

fn start() -> Result<Bridge> {
    spawn(Command::new(bridge_binary()?))
}

fn spawn(mut cmd: Command) -> Result<Bridge> {
    let mut child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning the reference bridge")?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    let mut bridge = Bridge { _child: child, stdin, lines: rx };
    let v = bridge.call_raw("Version", json!({}))?;
    if v != json!(PROTOCOL_VERSION) {
        bail!("bridge protocol version {v}, expected {PROTOCOL_VERSION}");
    }
    Ok(bridge)
}

impl Bridge {
    fn call_raw(&mut self, op: &str, args: Value) -> Result<Value> {
        let req = json!({"v": PROTOCOL_VERSION, "op": op, "args": args});
        writeln!(self.stdin, "{req}").context("writing to bridge")?;
        self.stdin.flush()?;
        let line = self
            .lines
            .recv_timeout(CALL_DEADLINE)
            .map_err(|_| anyhow!("bridge did not answer {op} within {CALL_DEADLINE:?}"))??;
        let mut resp: Value = serde_json::from_str(&line).with_context(|| format!("bridge reply: {line}"))?;
        if let Some(err) = resp.get("err").and_then(Value::as_str) {
            bail!("bridge op {op} failed: {err}");
        }
        Ok(resp.get_mut("ok").map(Value::take).unwrap_or(Value::Null))
    }
}

/// Calls a bridge op and decodes its result. Panics on bridge failures: those are
/// harness errors, never test outcomes.
pub fn call<A: Serialize, R: DeserializeOwned>(op: &str, args: A) -> R {
    let bridge = BRIDGE.get_or_init(|| Mutex::new(start().expect("starting the Go reference bridge")));
    let mut bridge = bridge.lock().unwrap_or_else(|e| e.into_inner());
    let args = serde_json::to_value(args).expect("serializable bridge args");
    let value = bridge.call_raw(op, args).unwrap_or_else(|e| panic!("{e:#}"));
    serde_json::from_value(value.clone()).unwrap_or_else(|e| panic!("decoding {op} result {value}: {e}"))
}

/// Calls a bridge op in a fresh bridge process with exactly `env` as its environment
/// (nothing is inherited) and a new empty working directory. For Go code that reads
/// the process environment or working directory (presets, CLI flags, log filtering),
/// so tests never mutate the shared bridge's or the test process's state.
pub fn call_isolated<A: Serialize, R: DeserializeOwned>(env: &[(&str, &str)], op: &str, args: A) -> R {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let cwd = std::env::temp_dir().join(format!("cabby-bridge-{}-{seq}", std::process::id()));
    std::fs::create_dir_all(&cwd).unwrap_or_else(|e| panic!("creating {}: {e}", cwd.display()));
    let result = (|| -> Result<Value> {
        let mut cmd = Command::new(bridge_binary()?);
        cmd.env_clear().envs(env.iter().copied()).current_dir(&cwd);
        let mut bridge = spawn(cmd)?;
        let args = serde_json::to_value(args).expect("serializable bridge args");
        let value = bridge.call_raw(op, args);
        let _ = bridge._child.kill();
        let _ = bridge._child.wait();
        value
    })();
    let _ = std::fs::remove_dir_all(&cwd);
    let value = result.unwrap_or_else(|e| panic!("{e:#}"));
    serde_json::from_value(value.clone()).unwrap_or_else(|e| panic!("decoding {op} result {value}: {e}"))
}
