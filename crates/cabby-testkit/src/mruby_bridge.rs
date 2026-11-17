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
//! with `Return`. Every wait for the bridge has a deadline. [`isolated`] gives a
//! thread a bridge process of its own.

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

/// One bridge process.
struct Shared {
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    routes: Routes,
    next_session: AtomicU64,
}

/// The bridge process shared by every thread of the test binary.
static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();

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

    Ok(Shared { child: Mutex::new(child), stdin: Mutex::new(stdin), routes, next_session: AtomicU64::new(1) })
}

fn global() -> Arc<Shared> {
    Arc::clone(SHARED.get_or_init(|| Arc::new(start().expect("starting the Go mruby bridge"))))
}

impl Shared {
    fn send(&self, msg: &Value) -> std::io::Result<()> {
        let mut stdin = self.stdin.lock().unwrap_or_else(|e| e.into_inner());
        writeln!(stdin, "{msg}").and_then(|()| stdin.flush())
    }
}

/// Only isolated processes are ever dropped (the shared one lives in a static).
impl Drop for Shared {
    fn drop(&mut self) {
        let child = self.child.get_mut().unwrap_or_else(|e| e.into_inner());
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// A callback run when Ruby calls a method defined from Rust. It receives the
/// callback arguments (`{"mrb": h, "self": h}`) and returns the `Return` args.
pub type Callback = Rc<dyn Fn(Value) -> Value>;

struct Session {
    shared: Arc<Shared>,
    id: u64,
    rx: Receiver<Value>,
    callbacks: RefCell<HashMap<u64, Callback>>,
    next_callback: Cell<u64>,
}

impl Session {
    fn new(shared: Arc<Shared>) -> Session {
        let id = shared.next_session.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = channel();
        shared.routes.lock().unwrap_or_else(|e| e.into_inner()).insert(id, tx);
        Session { shared, id, rx, callbacks: RefCell::default(), next_callback: Cell::new(1) }
    }

    fn send(&self, op: &str, args: Value) -> Result<()> {
        let msg = json!({"v": PROTOCOL_VERSION, "s": self.id, "op": op, "args": args});
        self.shared.send(&msg).context("writing to the mruby bridge")
    }

    fn call_raw(&self, op: &str, args: Value) -> Result<Value> {
        self.send(op, args)?;
        loop {
            let mut msg = self
                .rx
                .recv_timeout(CALL_DEADLINE)
                .map_err(|e| anyhow!("mruby bridge did not answer {op} within {CALL_DEADLINE:?}: {e}"))?;
            if let Some(cb) = msg.get("cb").and_then(Value::as_u64) {
                let f = self.callbacks.borrow().get(&cb).cloned().ok_or_else(|| anyhow!("unknown callback {cb}"))?;
                let ret = f(msg.get_mut("args").map(Value::take).unwrap_or(Value::Null));
                self.send("Return", ret)?;
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
        self.shared.routes.lock().unwrap_or_else(|e| e.into_inner()).remove(&self.id);
        let _ = self.send("EndSession", Value::Null);
    }
}

thread_local! {
    static SESSION: Session = Session::new(global());
    /// Set while this thread runs inside [`isolated`].
    static ISOLATED: RefCell<Option<Rc<Session>>> = const { RefCell::new(None) };
}

fn with_session<R>(f: impl FnOnce(&Session) -> R) -> R {
    match ISOLATED.with(|i| i.borrow().clone()) {
        Some(s) => f(&s),
        None => SESSION.with(f),
    }
}

/// Runs `f` with this thread's calls going to a fresh bridge process of its own,
/// which is stopped afterwards (also when `f` panics). Handles from that process
/// are only valid inside `f`. For cases whose Go behavior depends on no other VM
/// running in the same process (e.g. reading freed memory after `Close`).
pub fn isolated<R>(f: impl FnOnce() -> R) -> R {
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            // Dropping the session ends it; dropping the last Arc stops the process.
            let session = ISOLATED.with(|i| i.borrow_mut().take());
            drop(session);
        }
    }
    let shared = Arc::new(start().expect("starting an isolated Go mruby bridge"));
    let session = Rc::new(Session::new(shared));
    ISOLATED.with(|i| {
        let mut slot = i.borrow_mut();
        assert!(slot.is_none(), "nested mruby_bridge::isolated");
        *slot = Some(session);
    });
    let _restore = Restore;
    f()
}

/// Calls a bridge op on this thread's session and decodes its result. Callbacks
/// that arrive meanwhile run on this thread. Panics on bridge failures: those
/// are harness errors, never test outcomes.
pub fn call<A: Serialize, R: DeserializeOwned>(op: &str, args: A) -> R {
    let args = serde_json::to_value(args).expect("serializable bridge args");
    let value = with_session(|s| s.call_raw(op, args)).unwrap_or_else(|e| panic!("{e:#}"));
    serde_json::from_value(value.clone()).unwrap_or_else(|e| panic!("decoding {op} result {value}: {e}"))
}

/// Registers a callback for this thread's session and returns its id.
pub fn register_callback(f: Callback) -> u64 {
    with_session(|s| {
        let id = s.next_callback.get();
        s.next_callback.set(id + 1);
        s.callbacks.borrow_mut().insert(id, f);
        id
    })
}
