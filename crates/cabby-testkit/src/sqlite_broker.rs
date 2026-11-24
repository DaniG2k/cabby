//! The SQLite broker through the binary (a Cabby addition, deviation D-065):
//! data directories, servers started and stopped on one database file,
//! extended-protocol clients, and reading the file the server left behind.
//!
//! AnyCable rejects `--broker=sqlite`, so every scenario calls [`rust_only`]
//! first; on the Go backend the runner records the case as not observable.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::{Value, json};

use crate::cable_client::{CableClient, Options};
use crate::fake_rpc::{FakeRpc, Method, Reply};
use crate::raw_http;
use crate::sut::Sut;
use crate::{Backend, backend, workspace_root};

/// The extended Action Cable protocol (session recovery, offsets and epochs).
pub const EXT_PROTOCOL: &str = "actioncable-v1-ext-json";
/// Bound for every wait on the server, a client or the database.
pub const DEADLINE: Duration = Duration::from_secs(10);
/// From SIGTERM to exit: the default 30 s shutdown timeout plus a margin.
pub const STOP_GRACE: Duration = Duration::from_secs(40);

/// Reports the case as not observable on the Go backend.
pub fn rust_only() {
    if backend() == Backend::Go {
        unimplemented!(
            "not_observable: AnyCable rejects --broker=sqlite; the SQLite broker is a Cabby addition (D-065)"
        );
    }
}

/// A directory under `target/tmp` for one scenario's database, removed on drop.
pub struct DataDir(PathBuf);

impl DataDir {
    pub fn new(tag: &str) -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = workspace_root().join(format!(
            "target/tmp/sqlite-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("creating a data dir");
        Self(dir)
    }

    /// The database path. Its `data` directory doesn't exist yet: the server creates it.
    pub fn db(&self) -> PathBuf {
        self.0.join("data").join("cabby.db")
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `<db><suffix>`, e.g. the `-wal` file.
pub fn sibling(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// HTTP broadcasts and the SQLite broker on `db`, then `extra`.
pub fn server_args(db: &Path, extra: &[&str]) -> Vec<String> {
    let mut args = vec![
        "--broadcast_adapter=http".to_owned(),
        "--broker=sqlite".to_owned(),
        format!("--sqlite_path={}", db.display()),
    ];
    args.extend(extra.iter().map(|a| (*a).to_owned()));
    args
}

pub fn try_start(db: &Path, extra: &[&str]) -> anyhow::Result<Sut> {
    let args = server_args(db, extra);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Sut::try_start(&args, &[])
}

pub fn start(db: &Path, extra: &[&str]) -> Sut {
    try_start(db, extra).unwrap_or_else(|e| panic!("{e:#}"))
}

/// SIGTERM, then waits for a clean exit; returns the server's output.
pub fn stop_gracefully(sut: &mut Sut) -> String {
    let status = sut.process.stop(STOP_GRACE).expect("stopping the server");
    let output = sut.process.output();
    assert!(status.success(), "the server did not exit cleanly ({status}):\n{output}");
    output
}

/// SIGKILL (a crash), then waits for the process to go.
pub fn kill_hard(sut: &mut Sut) {
    let pid = sut.process.pid().to_string();
    let status = std::process::Command::new("kill").args(["-KILL", &pid]).status().expect("running kill");
    assert!(status.success(), "kill -KILL {pid}");
    sut.process.wait_exit(DEADLINE).expect("the killed server exits");
}

/// An application that authenticates every connection and welcomes it with
/// its session ID, as anycable-rails does.
pub fn session_app() -> FakeRpc {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(Reply::with(|call| {
        let sid = call.meta("sid").or_else(|| call.meta("x-anycable-meta-sid")).unwrap_or_default().to_owned();
        cabby_core::protos::ConnectionResponse {
            status: cabby_core::protos::Status::Success as i32,
            identifiers: r#"{"user":"u1"}"#.into(),
            transmissions: vec![json!({"type": "welcome", "sid": sid}).to_string()],
            ..Default::default()
        }
        .into()
    }));
    fake
}

/// Connects with the extended protocol, restoring `restore_sid` if given;
/// returns the client and its welcome.
pub fn connect(sut: &Sut, restore_sid: Option<&str>) -> (CableClient, Value) {
    let headers =
        restore_sid.map(|sid| vec![("X-ANYCABLE-RESTORE-SID".to_owned(), sid.to_owned())]).unwrap_or_default();
    let opts = Options { ignore: vec!["ping".into()], headers, protocol: EXT_PROTOCOL.into(), ..Options::default() };
    let mut client = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    let welcome = client.receive_within(DEADLINE).expect("welcome");
    assert_eq!(Some("welcome"), welcome["type"].as_str(), "unexpected first message {welcome}");
    (client, welcome)
}

/// The `$pubsub` identifier of `stream`.
pub fn identifier(stream: &str) -> String {
    json!({"channel": "$pubsub", "stream_name": stream}).to_string()
}

/// Subscribes to `stream` (public streams) and returns the identifier.
pub fn subscribe(client: &mut CableClient, stream: &str) -> String {
    let id = identifier(stream);
    client.send(&json!({"command": "subscribe", "identifier": id}));
    let ack = client.receive_within(DEADLINE).expect("subscription reply");
    assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
    id
}

/// Broadcasts the JSON string `text` to `stream` over HTTP.
pub fn broadcast(sut: &Sut, stream: &str, text: &str) {
    let body = json!({"stream": stream, "data": json!(text).to_string()}).to_string();
    let addr = SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let headers = [("Content-Type", "application/json")];
    let response =
        raw_http::request(addr, "POST", "/_broadcast", &headers, Some(body.as_bytes())).expect("broadcast request");
    assert_eq!(201, response.status, "broadcast rejected");
}

/// Waits for the broadcast whose message is `text`, skipping others; returns
/// it (with `stream_id`, `offset` and `epoch`).
pub fn receive_message(client: &mut CableClient, text: &str) -> Value {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match client.receive_within(deadline.saturating_duration_since(Instant::now())) {
            Ok(m) if m["message"] == text => return m,
            Ok(_) => {}
            Err(e) => panic!("no message {text:?} within {DEADLINE:?}: {e}"),
        }
    }
}

/// Asks for `stream`'s messages after `offset` in `epoch`; returns the replies
/// up to and including `confirm_history` or `reject_history`.
pub fn history_after(client: &mut CableClient, id: &str, stream: &str, offset: u64, epoch: &str) -> Vec<Value> {
    client.send(&json!({
        "command": "history",
        "identifier": id,
        "history": {"streams": {stream: {"offset": offset, "epoch": epoch}}},
    }));
    replies_until_verdict(client)
}

/// Asks for the messages since `since` (unix seconds).
pub fn history_since(client: &mut CableClient, id: &str, since: i64) -> Vec<Value> {
    client.send(&json!({"command": "history", "identifier": id, "history": {"since": since}}));
    replies_until_verdict(client)
}

fn replies_until_verdict(client: &mut CableClient) -> Vec<Value> {
    let mut replies = Vec::new();
    loop {
        let reply = client.receive_within(DEADLINE).expect("history reply");
        let verdict = matches!(reply["type"].as_str(), Some("confirm_history" | "reject_history"));
        replies.push(reply);
        if verdict {
            return replies;
        }
    }
}

/// The broadcasts among `replies`: (message, offset, epoch).
pub fn messages(replies: &[Value]) -> Vec<(String, u64, String)> {
    replies
        .iter()
        .filter(|m| m.get("message").is_some())
        .map(|m| {
            (
                m["message"].as_str().unwrap_or_default().to_owned(),
                m["offset"].as_u64().unwrap_or_default(),
                m["epoch"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

pub fn unix_now() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970");
    i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
}

pub fn unix_now_ms() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970");
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

/// Opens the database the server uses (WAL: it may be running).
pub fn open_db(db: &Path) -> Connection {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_WRITE).expect("opening the database");
    conn.busy_timeout(DEADLINE).expect("setting the busy timeout");
    conn
}

pub fn meta(db: &Path, key: &str) -> Option<String> {
    open_db(db).query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0)).optional().expect("meta")
}

pub fn session_expiry_ms(db: &Path, sid: &str) -> Option<i64> {
    open_db(db)
        .query_row("SELECT expires_at_ms FROM sessions WHERE sid = ?1", [sid], |r| r.get(0))
        .optional()
        .expect("sessions")
}

pub fn history_rows(db: &Path) -> i64 {
    open_db(db).query_row("SELECT count(*) FROM history", [], |r| r.get(0)).expect("history")
}

/// Waits until the server has written `rows` history rows (write-behind:
/// about 50 ms), observed in the file.
pub fn await_history_rows(db: &Path, rows: i64) {
    let deadline = Instant::now() + DEADLINE;
    while history_rows(db) < rows {
        assert!(Instant::now() < deadline, "{rows} history rows not written within {DEADLINE:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
