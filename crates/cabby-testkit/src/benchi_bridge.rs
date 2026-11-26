//! Client for `compat/reference-adapters/benchi-bridge`, the Go reference adapter for
//! Benchi (`etc/benchi`, a separate Go module the shared bridge cannot import).
//!
//! Protocol v1 (JSON lines), see `benchi-bridge/protocol.go`:
//! * requests carry an `id` and may be answered out of order, so blocking Go calls
//!   (`Client.Receive`, `ReconnectingClient.Run`) can run while other threads keep
//!   calling;
//! * Go objects live in a handle table on the Go side;
//! * Go func-typed parameters are callbacks: the bridge sends `{"cb","call","args"}`
//!   and this client answers `{"reply","ok"}` (or `{"reply","err"}`) after running
//!   the registered closure on its own thread (closures may call back into the
//!   bridge).
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;
/// Deadline of an ordinary call.
pub const CALL_DEADLINE: Duration = Duration::from_secs(30);

type Callback = Arc<dyn Fn(Value) -> Value + Send + Sync>;
type Waiters = Arc<Mutex<HashMap<u64, Sender<Result<Value, String>>>>>;

/// Directory of the Benchi Go artifacts (bridge and scenario shims).
pub fn artifacts_dir() -> PathBuf {
    crate::workspace_root().join("target/reference-adapters/benchi")
}

/// Path to the bridge binary: `CABBY_GO_BENCHI_BRIDGE`, or built once with `go build`.
fn bridge_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("CABBY_GO_BENCHI_BRIDGE") {
        return Ok(PathBuf::from(p));
    }
    let how = crate::go_build::Build {
        what: "the Benchi bridge",
        hint: "run `cargo xtask reference-checkout` first",
        reuse: crate::go_build::Reuse::Rebuild,
    };
    crate::go_build::build(&artifacts_dir().join("bridge"), &how, |tmp| {
        let mut cmd = Command::new("go");
        cmd.args(["build", "-o"])
            .arg(tmp)
            .arg(".")
            .current_dir(crate::workspace_root().join("compat/reference-adapters/benchi-bridge"));
        cmd
    })
}

/// Path to the test-only shim of an upstream scenario (`stress_publications`,
/// `reconnect_floods`): `CABBY_GO_BENCHI_SCENARIOS/<name>.shim`, or built once.
///
/// The shim is the scenario's own `go test -c` binary with two extra `_test.go` files
/// added through `-overlay` (nothing is written into the reference checkout):
/// `benchi-bridge/protocol.go` and `benchi-bridge/_scenario_shims/<name>_test.go`,
/// whose `TestMain` serves the adapter protocol when `CABBY_BENCHI_SHIM=1`. That
/// reaches the unexported `run`, `parseFlags`, `runWithConfig` and `commandStall` of
/// package main without patching upstream files.
pub fn shim_binary(name: &str) -> PathBuf {
    if let Ok(dir) = std::env::var("CABBY_GO_BENCHI_SCENARIOS") {
        return PathBuf::from(dir).join(format!("{name}.shim"));
    }
    build_shim(name).unwrap_or_else(|e| panic!("{e:#}"))
}

fn build_shim(name: &str) -> Result<PathBuf> {
    let root = crate::workspace_root();
    let adapters = root.join("compat/reference-adapters");
    // Overlay keys must be the paths `go` sees, so resolve a symlinked checkout.
    let benchi = root.join("compat/reference/etc/benchi").canonicalize().context("compat/reference/etc/benchi")?;
    let pkg = benchi.join("scenarios").join(name);
    let mut replace = serde_json::Map::new();
    replace.insert(
        pkg.join("zz_cabby_protocol_test.go").to_string_lossy().into_owned(),
        json!(adapters.join("benchi-bridge/protocol.go")),
    );
    replace.insert(
        pkg.join("zz_cabby_shim_test.go").to_string_lossy().into_owned(),
        json!(adapters.join(format!("benchi-bridge/_scenario_shims/{name}_test.go"))),
    );
    let overlay_path = artifacts_dir().join(format!("{name}.overlay.json"));
    let overlay = json!({ "Replace": replace }).to_string();
    let how = crate::go_build::Build {
        what: &format!("the {name} shim"),
        hint: "run `cargo xtask reference-checkout` first",
        reuse: crate::go_build::Reuse::Rebuild,
    };
    crate::go_build::build(&artifacts_dir().join(format!("{name}.shim")), &how, |tmp| {
        // Written under the build's lock, so no other process reads it half written.
        std::fs::write(&overlay_path, overlay).unwrap_or_else(|e| panic!("writing {}: {e}", overlay_path.display()));
        // Benchi's own go.mod is stale at the pinned commit; use the tidied copy
        // (compat/baseline/README.md item 1).
        let mut cmd = Command::new("go");
        cmd.args(["test", "-c"])
            .arg(format!("-modfile={}", adapters.join("benchi-modfile/go.mod").display()))
            .arg(format!("-overlay={}", overlay_path.display()))
            .arg("-o")
            .arg(tmp)
            .arg(format!("./scenarios/{name}"))
            .current_dir(&benchi);
        cmd
    })
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
        Session::start_with(Command::new(bridge_binary()?))
    }

    /// A fresh session served by the scenario shim `name` (see [`shim_binary`]).
    pub fn shim(name: &str) -> Arc<Session> {
        let mut cmd = Command::new(shim_binary(name));
        cmd.env("CABBY_BENCHI_SHIM", "1").env("ANYCABLE_DISABLE_TELEMETRY", "true");
        Arc::new(Session::start_with(cmd).unwrap_or_else(|e| panic!("starting the {name} shim: {e:#}")))
    }

    fn start_with(mut cmd: Command) -> Result<Session> {
        let mut child = cmd
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
            let stderr = Arc::clone(&stderr);
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
                            // Every reply is explicit: a missing callback, a panicking
                            // closure or a null result is an error the adapter treats as
                            // fatal, never a silent `false`.
                            let result = match f {
                                None => Err(format!("callback {cb} is not registered")),
                                Some(f) => match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(args))) {
                                    Ok(Value::Null) => Err(format!("callback {cb} returned null")),
                                    Ok(v) => Ok(v),
                                    Err(p) => Err(format!("callback {cb} panicked: {}", panic_text(p.as_ref()))),
                                },
                            };
                            let reply = match result {
                                Ok(ok) => json!({"v": PROTOCOL_VERSION, "reply": call, "ok": ok}),
                                Err(err) => json!({"v": PROTOCOL_VERSION, "reply": call, "err": err}),
                            };
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
                // The adapter exited: fail every waiting call with what it reported. The
                // stderr reader may still be draining, so give it a bounded moment.
                std::thread::sleep(Duration::from_millis(100));
                let text = String::from_utf8_lossy(&stderr.lock().unwrap_or_else(|e| e.into_inner())).into_owned();
                for (_, tx) in waiters.lock().unwrap_or_else(|e| e.into_inner()).drain() {
                    let _ = tx.send(Err(format!("adapter exited; stderr:\n{text}")));
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

    /// Everything the adapter wrote to stderr before this call, once the pipe has been
    /// drained up to now: a sentinel is written to stderr through the adapter and
    /// awaited, so earlier output cannot still be in flight.
    pub fn stderr_flushed(&self) -> String {
        let sentinel = format!("\u{1}cabby-stderr-sentinel-{}\u{1}", self.next.fetch_add(1, Ordering::Relaxed));
        let () = self.call("Stderr.Sentinel", json!({ "Text": sentinel }));
        let deadline = std::time::Instant::now() + CALL_DEADLINE;
        loop {
            let text = self.stderr();
            if let Some(i) = text.find(&sentinel) {
                return text[..i].to_owned();
            }
            assert!(std::time::Instant::now() < deadline, "stderr sentinel not seen within {CALL_DEADLINE:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Everything the bridge process wrote to stderr so far.
    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

fn panic_text(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<String>()
        .cloned()
        .or_else(|| p.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default()
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut child = self.child.lock().unwrap_or_else(|e| e.into_inner());
        let _ = child.kill();
        let _ = child.wait();
    }
}
