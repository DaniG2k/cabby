//! Client for `compat/reference-adapters/benchi-bridge`, the Go reference adapter for
//! Benchi (`etc/benchi`, a separate Go module the shared bridge cannot import).
//!
//! Protocol v1 (JSON lines), see the bridge's `main.go`:
//! * requests carry an `id` and may be answered out of order, so blocking Go calls
//!   (`Client.Receive`, `ReconnectingClient.Run`) can run while other threads keep
//!   calling;
//! * Go objects live in a handle table on the Go side;
//! * Go func-typed parameters are callbacks: the bridge sends `{"cb","call","args"}`
//!   and this client answers `{"reply","ok"}` after running the registered closure
//!   on its own thread (closures may call back into the bridge).
//!
//! Each test gets its own bridge process ([`Session::current`]): Benchi supports one
//! embedded server per process, and a test's objects must share a handle table.
//! Dropping the last reference kills the process.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;
/// Deadline of an ordinary call.
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);

type Callback = Arc<dyn Fn(Value) -> Value + Send + Sync>;
type Waiters = Arc<Mutex<HashMap<u64, Sender<Result<Value, String>>>>>;

/// Directory of the Benchi Go artifacts (bridge and scenario binaries).
pub fn artifacts_dir() -> PathBuf {
    crate::workspace_root().join("target/reference-adapters/benchi")
}

fn go_build(out: &std::path::Path, package: &str) -> Result<()> {
    let root = crate::workspace_root();
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(out)
        .arg(package)
        .current_dir(root.join("compat/reference-adapters/benchi-bridge"))
        .status()
        .context("running `go build` for the Benchi bridge (is the Go toolchain on PATH?)")?;
    if !status.success() {
        bail!("building {package} failed; run `cargo xtask reference-checkout` first");
    }
    Ok(())
}

/// Path to the bridge binary: `CABBY_GO_BENCHI_BRIDGE`, or built once with `go build`.
fn bridge_binary() -> Result<PathBuf> {
    static BUILT: OnceLock<std::result::Result<PathBuf, String>> = OnceLock::new();
    if let Ok(p) = std::env::var("CABBY_GO_BENCHI_BRIDGE") {
        return Ok(PathBuf::from(p));
    }
    BUILT
        .get_or_init(|| {
            let out = artifacts_dir().join("bridge");
            go_build(&out, ".").map(|()| out).map_err(|e| format!("{e:#}"))
        })
        .clone()
        .map_err(anyhow::Error::msg)
}

/// Path to an upstream scenario binary (`stress_publications`, `reconnect_floods`):
/// `CABBY_GO_BENCHI_SCENARIOS/<name>`, or built once from the Benchi module.
pub fn scenario_binary(name: &str) -> PathBuf {
    static BUILT: OnceLock<Mutex<HashMap<String, PathBuf>>> = OnceLock::new();
    if let Ok(dir) = std::env::var("CABBY_GO_BENCHI_SCENARIOS") {
        return PathBuf::from(dir).join(name);
    }
    let built = BUILT.get_or_init(Default::default);
    let mut built = built.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(p) = built.get(name) {
        return p.clone();
    }
    let out = artifacts_dir().join(name);
    go_build(&out, &format!("github.com/anycable/anycable-go/etc/benchi/scenarios/{name}"))
        .unwrap_or_else(|e| panic!("{e:#}"));
    built.insert(name.to_owned(), out.clone());
    out
}

/// One bridge process.
pub struct Session {
    child: Mutex<Child>,
    stdin: Arc<Mutex<ChildStdin>>,
    waiters: Waiters,
    callbacks: Arc<Mutex<HashMap<u64, Callback>>>,
    next: AtomicU64,
    stderr: Arc<Mutex<Vec<u8>>>,
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<Session>>> = const { RefCell::new(None) };
}

impl Session {
    /// The calling test's session, started on first use. Helper threads must reuse the
    /// session of the objects they were given rather than call this.
    pub fn current() -> Arc<Session> {
        CURRENT.with(|c| {
            c.borrow_mut()
                .get_or_insert_with(|| Arc::new(Session::start().expect("starting the Go Benchi bridge")))
                .clone()
        })
    }

    fn start() -> Result<Session> {
        let mut child = Command::new(bridge_binary()?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("spawning the Benchi bridge")?;
        let stdin = Arc::new(Mutex::new(child.stdin.take().expect("piped stdin")));
        let stdout = child.stdout.take().expect("piped stdout");
        let mut stderr_pipe = child.stderr.take().expect("piped stderr");
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&stderr);
        std::thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(n @ 1..) = stderr_pipe.read(&mut buf) {
                sink.lock().unwrap_or_else(|e| e.into_inner()).extend_from_slice(&buf[..n]);
            }
        });
        let waiters: Waiters = Arc::default();
        let callbacks: Arc<Mutex<HashMap<u64, Callback>>> = Arc::default();
        {
            let waiters = Arc::clone(&waiters);
            let callbacks = Arc::clone(&callbacks);
            let stdin = Arc::clone(&stdin);
            std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    let Ok(mut msg) = serde_json::from_str::<Value>(&line) else { break };
                    if let Some(cb) = msg.get("cb").and_then(Value::as_u64) {
                        let call = msg["call"].as_u64().unwrap_or_default();
                        let args = msg["args"].take();
                        let f = callbacks.lock().unwrap_or_else(|e| e.into_inner()).get(&cb).cloned();
                        let stdin = Arc::clone(&stdin);
                        std::thread::spawn(move || {
                            let ok = f.map_or(Value::Null, |f| f(args));
                            let reply = json!({"v": PROTOCOL_VERSION, "reply": call, "ok": ok});
                            let mut w = stdin.lock().unwrap_or_else(|e| e.into_inner());
                            let _ = writeln!(w, "{reply}");
                            let _ = w.flush();
                        });
                        continue;
                    }
                    let Some(id) = msg.get("id").and_then(Value::as_u64) else { continue };
                    let result = match msg.get("err").and_then(Value::as_str) {
                        Some(e) => Err(e.to_owned()),
                        None => Ok(msg.get_mut("ok").map(Value::take).unwrap_or(Value::Null)),
                    };
                    if let Some(tx) = waiters.lock().unwrap_or_else(|e| e.into_inner()).remove(&id) {
                        let _ = tx.send(result);
                    }
                }
                // The bridge exited: fail every waiting call.
                for (_, tx) in waiters.lock().unwrap_or_else(|e| e.into_inner()).drain() {
                    let _ = tx.send(Err("bridge exited".into()));
                }
            });
        }
        let session = Session { child: Mutex::new(child), stdin, waiters, callbacks, next: AtomicU64::new(1), stderr };
        let v: Value = session.call("Version", json!({}));
        if v != json!(PROTOCOL_VERSION) {
            bail!("Benchi bridge protocol version {v}, expected {PROTOCOL_VERSION}");
        }
        Ok(session)
    }

    /// Calls `op`, waiting up to `deadline`. `Ok(None)` means the deadline passed (the
    /// Go call keeps running; its late answer is discarded).
    pub fn call_within(&self, op: &str, args: Value, deadline: Duration) -> Result<Option<Value>> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        self.waiters.lock().unwrap_or_else(|e| e.into_inner()).insert(id, tx);
        {
            let req = json!({"v": PROTOCOL_VERSION, "id": id, "op": op, "args": args});
            let mut w = self.stdin.lock().unwrap_or_else(|e| e.into_inner());
            writeln!(w, "{req}").context("writing to the Benchi bridge")?;
            w.flush()?;
        }
        match rx.recv_timeout(deadline) {
            Ok(Ok(v)) => Ok(Some(v)),
            Ok(Err(e)) => bail!("Benchi bridge op {op} failed: {e}"),
            Err(_) => {
                self.waiters.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
                Ok(None)
            }
        }
    }

    /// Calls `op` and decodes its result. Bridge failures panic: they are harness
    /// errors, never test outcomes.
    pub fn call<R: DeserializeOwned>(&self, op: &str, args: Value) -> R {
        let v = self
            .call_within(op, args, CALL_DEADLINE)
            .unwrap_or_else(|e| panic!("{e:#}"))
            .unwrap_or_else(|| panic!("Benchi bridge did not answer {op} within {CALL_DEADLINE:?}"));
        serde_json::from_value(v.clone()).unwrap_or_else(|e| panic!("decoding {op} result {v}: {e}"))
    }

    /// Registers a closure the bridge can call; returns its callback id.
    pub fn register_callback(&self, f: impl Fn(Value) -> Value + Send + Sync + 'static) -> u64 {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.callbacks.lock().unwrap_or_else(|e| e.into_inner()).insert(id, Arc::new(f));
        id
    }

    pub fn unregister_callback(&self, id: u64) {
        self.callbacks.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
    }

    /// Everything the bridge process wrote to stderr so far.
    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut child = self.child.lock().unwrap_or_else(|e| e.into_inner());
        let _ = child.kill();
        let _ = child.wait();
    }
}
