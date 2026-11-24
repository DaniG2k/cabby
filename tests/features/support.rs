//! Shared steps of the feature scenario ports: the `features/runner.rb` DSL
//! (`launch`, `run`, `wait_tcp`, `stop`, `stdout`, `retrying`) mapped onto
//! `cabby_testkit` processes, plus small wsdirector-style helpers.
//!
//! `launch :rpc, "bundle exec anyt --only-rpc"` is [`rpc_app`] (the AnyT
//! application reimplemented in `cabby_testkit::anyt_app`); upstream defaults
//! (`localhost:50051`, `localhost:8080`, broadcast port 8090) become isolated
//! ports passed explicitly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cabby_core::pending::{MARKER, pending_id};
use cabby_testkit::anyt_app::{AnytApp, AppOptions};
use cabby_testkit::sut::{Sut, sut_binary};
use cabby_testkit::workspace_root;
use cabby_testkit::wsdirector::{Client, Connection, Protocol, StepError};

/// `BenchRunner::RUN_TIMEOUT` (CI value).
pub const RUN_TIMEOUT: Duration = Duration::from_secs(120);
/// Bound for waits on observable readiness (log lines, files, ports).
pub const READY_DEADLINE: Duration = Duration::from_secs(20);

pub fn local(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// `launch :rpc, "bundle exec anyt --only-rpc"`.
pub fn rpc_app() -> AnytApp {
    rpc_app_with_prefix("")
}

/// [`rpc_app`] with a stream name prefix (`AppOptions::stream_prefix`), for
/// servers that share a Redis or NATS service with other tests.
pub fn rpc_app_with_prefix(prefix: &str) -> AnytApp {
    AnytApp::start(AppOptions {
        http_rpc_secret: None,
        target_host: "127.0.0.1".into(),
        stream_prefix: prefix.into(),
        ..Default::default()
    })
}

/// `launch :anycable, "./dist/anycable-go <args>"` + `wait_tcp`.
pub fn launch(args: &[&str], env: &[(&str, &str)]) -> Sut {
    Sut::try_start(args, env).unwrap_or_else(|e| panic!("{e:#}"))
}

pub fn ws_url(sut: &Sut, path: &str) -> String {
    format!("ws://127.0.0.1:{}{path}", sut.port)
}

/// Waits until the server's output satisfies `pred` (bounded), returning it.
pub fn wait_output(sut: &Sut, what: &str, deadline: Duration, pred: impl Fn(&str) -> bool) -> String {
    let until = Instant::now() + deadline;
    loop {
        let out = sut.process.output();
        if pred(&out) {
            return out;
        }
        sut.process.propagate_pending();
        assert!(Instant::now() < until, "{what} not observed within {deadline:?}. Output:\n{out}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `stop :anycable` (SIGTERM, then SIGKILL after `grace`) and `stdout(:anycable)`.
pub fn stop(sut: &mut Sut, grace: Duration) -> String {
    sut.process.stop(grace).unwrap_or_else(|e| panic!("stopping the server: {e:#}"));
    sut.process.output()
}

pub struct Output {
    pub stdout: String,
    pub stderr: String,
}

/// `run :name, cmd, env:, clean_env: true`: runs the SUT binary to completion
/// with the server configuration of the test process removed ([`cabby_testkit::sut::scrub_env`]; nothing injected),
/// within [`RUN_TIMEOUT`], draining both pipes concurrently (see
/// compat/baseline/README.md on the runner's pipe deadlock). A pending marker
/// from the Rust stub is re-raised.
pub fn run_sut(args: &[&str], env: &[(&str, &str)], dir: &std::path::Path) -> Output {
    let mut cmd = Command::new(sut_binary());
    cabby_testkit::sut::scrub_env(&mut cmd);
    cmd.envs(env.iter().copied()).args(args).current_dir(dir);
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawning {cmd:?}: {e}"));
    let drain = |mut r: Box<dyn std::io::Read + Send>| {
        std::thread::spawn(move || {
            let mut s = Vec::new();
            let _ = r.read_to_end(&mut s);
            String::from_utf8_lossy(&s).into_owned()
        })
    };
    let out = drain(Box::new(child.stdout.take().expect("stdout")));
    let err = drain(Box::new(child.stderr.take().expect("stderr")));
    let deadline = Instant::now() + RUN_TIMEOUT;
    let status = loop {
        if let Some(s) = child.try_wait().expect("polling the command") {
            break s;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("Command expected to finish in {RUN_TIMEOUT:?} but is still running: {cmd:?}");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = status;
    let output = Output { stdout: out.join().unwrap(), stderr: err.join().unwrap() };
    if let Some(id) = pending_id(&format!("{}{}", output.stdout, output.stderr)) {
        panic!("{MARKER}{id} (reported by {cmd:?})");
    }
    output
}

/// A fresh directory under `target/tmp`, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = workspace_root().join(format!(
            "target/tmp/features-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("creating a temp dir");
        Self(dir)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// wsdirector client for a scenario `url`.
pub fn wsd(url: &str, protocol: Protocol, conn: Connection) -> Result<Client, StepError> {
    Client::connect(url, protocol, &conn)
}

/// Groups of a wsdirector run and their outcomes ("Group <name>: N clients, F
/// failures"); fails unless every group has 0 failures.
#[track_caller]
pub fn assert_groups(results: &[(&str, Result<(), StepError>)]) {
    let failures: Vec<String> =
        results.iter().filter_map(|(g, r)| r.as_ref().err().map(|e| format!("Group {g}: {e}"))).collect();
    assert!(failures.is_empty(), "Unexpected scenario result:\n{}", failures.join("\n"));
}

/// `BenchRunner#retrying(delay:, attempts:)`: runs `f` up to `attempts` times,
/// sleeping `delay` after a failure, and fails with the last error.
pub fn retrying<T>(delay: Duration, attempts: usize, mut f: impl FnMut() -> Result<T, String>) -> T {
    let mut left = attempts;
    loop {
        match f() {
            Ok(v) => return v,
            Err(e) if left > 1 => {
                left -= 1;
                eprintln!("Retrying after error: {e}");
                std::thread::sleep(delay);
            }
            Err(e) => panic!("{e}"),
        }
    }
}

/// Polls `probe` until it succeeds (readiness), failing after `deadline`.
pub fn await_ready<T>(what: &str, deadline: Duration, mut probe: impl FnMut() -> Result<T, String>) -> T {
    let until = Instant::now() + deadline;
    loop {
        match probe() {
            Ok(v) => return v,
            Err(e) if Instant::now() < until => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(e) => panic!("{what} not ready within {deadline:?}: {e}"),
        }
    }
}

pub fn step<T>(r: Result<T, StepError>) -> Result<T, String> {
    r.map_err(|e| e.to_string())
}
