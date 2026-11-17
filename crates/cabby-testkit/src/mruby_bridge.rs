//! Client for `compat/reference-adapters/mruby-bridge`, the Go program that
//! exposes the vendored go-mruby binding and upstream package `mrb` over JSON
//! lines (mruby-bridge protocol v1, see its `main.go`).
//!
//! It is separate from [`crate::bridge`] because it links libmruby through cgo.
//! One process is shared per test binary. Each Rust thread is a bridge session
//! served by its own goroutine, so tests that run VMs on several threads run
//! them concurrently in Go too. Ruby methods defined from Rust are callbacks:
//! while the bridge runs Ruby code that calls one, it sends a callback message;
//! the calling thread runs the closure (which may make nested calls) and replies
//! with `Return`. Every wait for the bridge has a deadline.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

const PROTOCOL_VERSION: u64 = 1;
const CALL_DEADLINE: Duration = Duration::from_secs(30);

type Routes = Arc<Mutex<HashMap<u64, Sender<Value>>>>;

struct Shared {
    _child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    routes: Routes,
    next_session: AtomicU64,
}

static SHARED: OnceLock<Shared> = OnceLock::new();

/// Path to the bridge binary: `CABBY_GO_MRUBY_BRIDGE`, or built once with
/// `go build -tags mrb` (needs compat/reference/vendorlib/go-mruby/libmruby.a).
fn bridge_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("CABBY_GO_MRUBY_BRIDGE") {
        return Ok(PathBuf::from(p));
    }
    let root = crate::workspace_root();
    let out = root.join("target/reference-adapters/mruby-bridge");
    let status = Command::new("go")
        .args(["build", "-tags", "mrb", "-o"])
        .arg(&out)
        .arg(".")
        .current_dir(root.join("compat/reference-adapters/mruby-bridge"))
        .status()
        .context("running `go build` for the mruby bridge (is the Go toolchain on PATH?)")?;
    if !status.success() {
        bail!("building the mruby bridge failed; it needs compat/reference/vendorlib/go-mruby/libmruby.a");
    }
    Ok(out)
}

fn start() -> Result<Shared> {
    let mut child = Command::new(bridge_binary()?)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .context("spawning the mruby bridge")?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("piped stdout"));

    writeln!(stdin, "{}", json!({"v": PROTOCOL_VERSION, "s": 0, "op": "Version"}))?;
    stdin.flush()?;
    let mut line = String::new();
    stdout.read_line(&mut line).context("reading the mruby bridge version")?;
    let v: Value = serde_json::from_str(&line).with_context(|| format!("mruby bridge reply: {line}"))?;
    if v["ok"] != json!(PROTOCOL_VERSION) {
        bail!("mruby bridge protocol {line}, expected version {PROTOCOL_VERSION}");
    }

    let routes: Routes = Arc::default();
    let reader_routes = Arc::clone(&routes);
    std::thread::spawn(move || {
        for line in stdout.lines() {
            let Ok(line) = line else { break };
            let Ok(msg) = serde_json::from_str::<Value>(&line) else {
                eprintln!("mruby bridge: unparsable line {line}");
                continue;
            };
            let session = msg["s"].as_u64().unwrap_or(0);
            let route = reader_routes.lock().unwrap_or_else(|e| e.into_inner()).get(&session).cloned();
            if let Some(tx) = route {
                let _ = tx.send(msg);
            }
        }
        // Bridge exited: dropping every sender wakes all waiting sessions.
        reader_routes.lock().unwrap_or_else(|e| e.into_inner()).clear();
    });

    Ok(Shared { _child: Mutex::new(child), stdin: Mutex::new(stdin), routes, next_session: AtomicU64::new(1) })
}

fn shared() -> &'static Shared {
    SHARED.get_or_init(|| start().expect("starting the Go mruby bridge"))
}

fn send(msg: &Value) {
    let mut stdin = shared().stdin.lock().unwrap_or_else(|e| e.into_inner());
    writeln!(stdin, "{msg}").and_then(|()| stdin.flush()).expect("writing to the mruby bridge");
}

/// A callback run when Ruby calls a method defined from Rust. It receives the
/// callback arguments (`{"mrb": h, "self": h}`) and returns the `Return` args.
pub type Callback = Rc<dyn Fn(Value) -> Value>;

struct Session {
    id: u64,
    rx: Receiver<Value>,
    callbacks: RefCell<HashMap<u64, Callback>>,
    next_callback: Cell<u64>,
}

impl Session {
    fn new() -> Session {
        let shared = shared();
        let id = shared.next_session.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        shared.routes.lock().unwrap_or_else(|e| e.into_inner()).insert(id, tx);
        Session { id, rx, callbacks: RefCell::default(), next_callback: Cell::new(1) }
    }

    fn call_raw(&self, op: &str, args: Value) -> Result<Value> {
        send(&json!({"v": PROTOCOL_VERSION, "s": self.id, "op": op, "args": args}));
        loop {
            let mut msg = self
                .rx
                .recv_timeout(CALL_DEADLINE)
                .map_err(|e| anyhow!("mruby bridge did not answer {op} within {CALL_DEADLINE:?}: {e}"))?;
            if let Some(cb) = msg.get("cb").and_then(Value::as_u64) {
                let f = self.callbacks.borrow().get(&cb).cloned().ok_or_else(|| anyhow!("unknown callback {cb}"))?;
                let ret = f(msg.get_mut("args").map(Value::take).unwrap_or(Value::Null));
                send(&json!({"v": PROTOCOL_VERSION, "s": self.id, "op": "Return", "args": ret}));
                continue;
            }
            if let Some(err) = msg.get("err").and_then(Value::as_str) {
                bail!("mruby bridge op {op} failed: {err}");
            }
            return Ok(msg.get_mut("ok").map(Value::take).unwrap_or(Value::Null));
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(shared) = SHARED.get() {
            shared.routes.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
            let mut stdin = shared.stdin.lock().unwrap_or_else(|e| e.into_inner());
            let _ = writeln!(stdin, "{}", json!({"v": PROTOCOL_VERSION, "s": self.id, "op": "EndSession"}));
            let _ = stdin.flush();
        }
    }
}

thread_local! {
    static SESSION: Session = Session::new();
}

/// Calls a bridge op on this thread's session and decodes its result. Callbacks
/// that arrive meanwhile run on this thread. Panics on bridge failures: those
/// are harness errors, never test outcomes.
pub fn call<A: Serialize, R: DeserializeOwned>(op: &str, args: A) -> R {
    let args = serde_json::to_value(args).expect("serializable bridge args");
    let value = SESSION.with(|s| s.call_raw(op, args)).unwrap_or_else(|e| panic!("{e:#}"));
    serde_json::from_value(value.clone()).unwrap_or_else(|e| panic!("decoding {op} result {value}: {e}"))
}

/// Registers a callback for this thread's session and returns its id.
pub fn register_callback(f: Callback) -> u64 {
    SESSION.with(|s| {
        let id = s.next_callback.get();
        s.next_callback.set(id + 1);
        s.callbacks.borrow_mut().insert(id, f);
        id
    })
}
