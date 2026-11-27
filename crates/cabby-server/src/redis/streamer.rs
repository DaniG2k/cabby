//! Redis Streams consumer-group reader with acknowledgements, pending-entry
//! auto-claim and reconnects (upstream `redis.Streamer`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cabby_core::logger::Logger;
use cabby_core::{log_debug, log_error, log_info};
use redis::aio::ConnectionLike;
use redis::{Cmd, RedisResult, Value};
use tokio_util::sync::CancellationToken;

use super::connect::{self, Commands, ConnectSpec, Signal};
use super::{RedisConfig, RedisError};

/// Handles one stream entry (field/value pairs).
pub type StreamHandler = Arc<dyn Fn(HashMap<String, String>) -> Result<(), RedisError> + Send + Sync>;

/// Upstream `StreamerOption`s.
#[derive(Clone, Default)]
pub struct StreamerOptions {
    pub block_ms: Option<i64>,
    pub consumer_name: Option<String>,
    pub handler: Option<StreamHandler>,
}

/// The text of rueidis `ErrClosing`, returned by commands on a closed client.
const CLIENT_CLOSED: &str = "rueidis client is closing or unable to connect redis";

/// How long `shutdown` waits for the reader beyond one blocking read.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Most entries one `XREADGROUP` takes (`COUNT`). Upstream passes no `COUNT`,
/// so after an outage one reply carries the whole backlog, which waits in this
/// consumer's pending list, where other consumers claim entries idle longer
/// than `block_ms` and handle them too (D-109). 1000 entries take about 0.2 s
/// on a local Redis; a read per 1000 adds 0.1% to the per-entry round trips.
const READ_COUNT: usize = 1000;

/// The streamer's current Redis client (upstream `Streamer.client`).
struct Client {
    conn: Commands,
    closed: AtomicBool,
}

impl Client {
    async fn query(&self, cmd: &Cmd) -> RedisResult<Value> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_error());
        }
        let mut conn = self.conn.clone();
        // An error reply comes back as a value: make it the command's error,
        // as rueidis' `res.Error()` does.
        match conn.req_packed_command(cmd).await? {
            Value::ServerError(e) => Err(e.into()),
            v => Ok(v),
        }
    }

    async fn query_pipeline(&self, pipe: &redis::Pipeline) -> RedisResult<Vec<Value>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(closed_error());
        }
        let mut conn = self.conn.clone();
        conn.req_packed_commands(pipe, 0, pipe.len()).await
    }
}

fn closed_error() -> redis::RedisError {
    redis::RedisError::from((redis::ErrorKind::Io, CLIENT_CLOSED))
}

struct Inner {
    log: Logger,
    stream: String,
    group: String,
    consumer: String,
    block_ms: i64,
    max_reconnect_attempts: i64,
    handler: StreamHandler,
    config: RedisConfig,
    spec: Mutex<Option<ConnectSpec>>,
    client: Mutex<Option<Arc<Client>>>,
    ready: Signal,
    finished: Signal,
    shutdown: CancellationToken,
}

/// Redis Streams consumer-group reader with pending-entry auto-claim (upstream
/// `redis.Streamer`).
pub struct Streamer {
    inner: Arc<Inner>,
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as i64)
}

impl Streamer {
    pub fn new(stream: &str, group: &str, config: &RedisConfig, options: StreamerOptions) -> Self {
        Self::with_log(stream, group, config, options, Logger::default().with("context", "redis"))
    }

    /// [`Streamer::new`] logging with `log`'s attributes.
    pub(crate) fn with_log(
        stream: &str,
        group: &str,
        config: &RedisConfig,
        options: StreamerOptions,
        log: Logger,
    ) -> Self {
        let handler: StreamHandler = options.handler.unwrap_or_else(|| Arc::new(|_| Ok(())));
        Self {
            inner: Arc::new(Inner {
                log,
                stream: stream.to_owned(),
                group: group.to_owned(),
                consumer: options.consumer_name.unwrap_or_else(connect::nanoid6),
                block_ms: options.block_ms.unwrap_or(2000),
                max_reconnect_attempts: config.max_reconnect_attempts,
                handler,
                config: config.clone(),
                spec: Mutex::new(None),
                client: Mutex::new(None),
                ready: Signal::default(),
                finished: Signal::default(),
                shutdown: CancellationToken::new(),
            }),
        }
    }

    pub fn start(&self) -> Result<(), RedisError> {
        let spec = self.inner.config.connect_spec()?;
        *self.inner.spec.lock().unwrap_or_else(|e| e.into_inner()) = Some(spec);
        let inner = Arc::clone(&self.inner);
        connect::runtime().spawn(async move { inner.run().await });
        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), RedisError> {
        let inner = &self.inner;
        inner.shutdown.cancel();
        let Some(client) = inner.current_client() else { return Ok(()) };

        log_debug!(inner.log, "shutting down Redis streamer");

        let block = Duration::from_millis(inner.block_ms.max(0) as u64);
        inner.finished.wait(block + SHUTDOWN_GRACE);

        let cmd = redis::cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(&inner.stream)
            .arg(&inner.group)
            .arg(&inner.consumer)
            .to_owned();
        let c = Arc::clone(&client);
        match connect::wait_on(SHUTDOWN_GRACE, async move { c.query(&cmd).await }) {
            Some(Ok(_)) => {}
            Some(Err(e)) => log_error!(inner.log, "failed to remove Redis stream consumer", "error" => e.to_string()),
            None => log_error!(inner.log, "failed to remove Redis stream consumer", "error" => "timeout"),
        }

        client.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Waits until the consumer group is set up and reading (upstream
    /// `<-streamer.Ready()`). Returns false if `timeout` elapses first.
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        self.inner.ready.wait(timeout)
    }

    /// Closes the streamer's Redis client (upstream `streamer.Client().Close()`):
    /// entries being handled can no longer be acknowledged, and the reader then
    /// reconnects up to `max_reconnect_attempts` times.
    pub fn close_client(&self) {
        if let Some(client) = self.inner.current_client() {
            client.closed.store(true, Ordering::SeqCst);
        }
    }
}

impl Drop for Streamer {
    fn drop(&mut self) {
        self.inner.shutdown.cancel();
    }
}

impl Inner {
    fn current_client(&self) -> Option<Arc<Client>> {
        self.client.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Upstream `initClient`: the current client, connecting if there is none.
    async fn init_client(&self) -> Result<Arc<Client>, redis::RedisError> {
        if let Some(c) = self.current_client() {
            return Ok(c);
        }
        let spec = self.spec.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let Some(spec) = spec else { return Err(closed_error()) };
        // A blocking read takes up to `block_ms`; a reply that takes much
        // longer means the connection is dead.
        let timeout = Duration::from_millis(self.block_ms.max(0) as u64) + Duration::from_secs(10);
        let conn = spec.commands(Some(timeout)).await?;
        let client = Arc::new(Client { conn, closed: AtomicBool::new(false) });
        *self.client.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&client));
        Ok(client)
    }

    async fn run(self: Arc<Self>) {
        let mut attempt: i64 = 0;
        'connect: loop {
            if self.shutdown.is_cancelled() {
                self.finished.set();
                return;
            }
            let client = match self.init_client().await {
                Ok(c) => c,
                Err(e) => {
                    log_error!(self.log, "failed to connect to Redis", "error" => e.to_string());
                    if self.maybe_reconnect(&mut attempt).await {
                        continue 'connect;
                    }
                    return;
                }
            };

            if attempt > 0 {
                log_info!(self.log, "reconnected to Redis");
            }
            attempt = 0;

            // First, create a consumer group for the stream
            let create = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&self.stream)
                .arg(&self.group)
                .arg("$")
                .arg("MKSTREAM")
                .to_owned();
            if let Err(e) = client.query(&create).await {
                // Upstream only reacts to Redis (server) errors here.
                if e.kind() != redis::ErrorKind::Io && e.code().is_some() {
                    if e.code() == Some("BUSYGROUP") {
                        log_debug!(self.log, "Redis consumer group already exists");
                    } else {
                        log_error!(self.log, "failed to create consumer group", "error" => e.to_string());
                        if self.maybe_reconnect(&mut attempt).await {
                            continue 'connect;
                        }
                        return;
                    }
                }
            }

            let block = self.block_ms;
            let mut last_claimed_at: i64 = 0;

            self.ready.set();

            loop {
                if self.shutdown.is_cancelled() {
                    log_debug!(self.log, "stop consuming stream");
                    self.finished.set();
                    return;
                }

                if last_claimed_at + block < now_ms() {
                    match self.autoclaim(&client).await {
                        Err(e) => {
                            log_error!(self.log, "failed to claim from Redis stream", "error" => e.to_string());
                            if self.maybe_reconnect(&mut attempt).await {
                                continue 'connect;
                            }
                            return;
                        }
                        Ok(reclaimed) => {
                            last_claimed_at = now_ms();
                            if !reclaimed.is_empty() {
                                log_debug!(self.log, "reclaimed messages", "size" => reclaimed.len());
                                self.handle_range(&client, reclaimed).await;
                            }
                        }
                    }
                }

                match self.read(&client).await {
                    Err(e) => {
                        log_error!(self.log, "failed to read from Redis stream", "error" => e.to_string());
                        if self.maybe_reconnect(&mut attempt).await {
                            continue 'connect;
                        }
                        return;
                    }
                    Ok(messages) => self.handle_range(&client, messages).await,
                }
            }
        }
    }

    async fn read(&self, client: &Client) -> RedisResult<Vec<Entry>> {
        let reply = client.query(&self.read_command()).await?;
        Ok(stream_entries(&reply, &self.stream))
    }

    fn read_command(&self) -> Cmd {
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(READ_COUNT)
            .arg("BLOCK")
            .arg(self.block_ms)
            .arg("STREAMS")
            .arg(&self.stream)
            .arg(">")
            .to_owned()
    }

    async fn autoclaim(&self, client: &Client) -> RedisResult<Vec<Entry>> {
        let cmd = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.consumer)
            .arg(self.block_ms.to_string())
            .arg("0-0")
            .to_owned();
        match client.query(&cmd).await? {
            Value::Nil => Ok(Vec::new()),
            Value::Array(items) => {
                if items.len() < 2 {
                    return Err(redis::RedisError::from((
                        redis::ErrorKind::Parse,
                        "autoclaim failed",
                        format!("got {} elements, wanted 2", items.len()),
                    )));
                }
                Ok(entries(&items[1]))
            }
            other => Err(redis::RedisError::from((
                redis::ErrorKind::Parse,
                "autoclaim failed",
                format!("unexpected reply {other:?}"),
            ))),
        }
    }

    async fn handle_range(&self, client: &Client, messages: Vec<Entry>) {
        for (id, fields) in messages {
            log_debug!(self.log, "received message");
            match (self.handler)(fields) {
                Ok(()) => {
                    let mut pipe = redis::pipe();
                    pipe.cmd("XACK").arg(&self.stream).arg(&self.group).arg(&id);
                    pipe.cmd("XDEL").arg(&self.stream).arg(&id);
                    match client.query_pipeline(&pipe).await {
                        Ok(replies) => {
                            if let Some(Value::ServerError(e)) = replies.first() {
                                log_error!(self.log, "failed to ack message", "error" => e.to_string());
                            }
                        }
                        Err(e) => log_error!(self.log, "failed to ack message", "error" => e.to_string()),
                    }
                }
                Err(e) => log_error!(self.log, "Redis stream handler failed", "error" => e.0),
            }
        }
    }

    /// Upstream `maybeReconnect`: false when the reader must stop (attempts
    /// exceeded or shutdown).
    async fn maybe_reconnect(&self, attempt: &mut i64) -> bool {
        if *attempt >= self.max_reconnect_attempts {
            self.finished.set();
            log_error!(self.log, "failed to reconnect to Redis: attempts exceeded");
            return false;
        }

        *attempt += 1;

        let delay = connect::next_retry((*attempt - 1) as u32);

        log_info!(self.log, &format!("next Redis reconnect attempt in {}", go_duration(delay)));
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = self.shutdown.cancelled() => {
                self.finished.set();
                return false;
            }
        }

        log_info!(self.log, "reconnecting to Redis...");

        if let Some(old) = self.client.lock().unwrap_or_else(|e| e.into_inner()).take() {
            old.closed.store(true, Ordering::SeqCst);
        }
        true
    }
}

/// Go's `time.Duration.String()` for the delays `next_retry` returns.
pub(crate) fn go_duration(d: Duration) -> String {
    if d < Duration::from_secs(1) { format!("{}ms", d.as_millis()) } else { format!("{}s", d.as_secs()) }
}

type Entry = (String, HashMap<String, String>);

fn text(v: &Value) -> Option<String> {
    match v {
        Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        Value::Okay => Some("OK".to_owned()),
        _ => None,
    }
}

/// `[[id, [field, value, ...]], ...]` (nil entries, deleted meanwhile, are skipped).
fn entries(v: &Value) -> Vec<Entry> {
    let Value::Array(items) = v else { return Vec::new() };
    items
        .iter()
        .filter_map(|item| {
            let Value::Array(pair) = item else { return None };
            let id = text(pair.first()?)?;
            let mut fields = HashMap::new();
            match pair.get(1) {
                Some(Value::Array(kv)) => {
                    for chunk in kv.chunks(2) {
                        if let [k, v] = chunk
                            && let (Some(k), Some(v)) = (text(k), text(v))
                        {
                            fields.insert(k, v);
                        }
                    }
                }
                Some(Value::Map(kv)) => {
                    for (k, v) in kv {
                        if let (Some(k), Some(v)) = (text(k), text(v)) {
                            fields.insert(k, v);
                        }
                    }
                }
                _ => return None,
            }
            Some((id, fields))
        })
        .collect()
}

/// The entries of `stream` in an `XREADGROUP` reply (RESP2 array or RESP3 map).
fn stream_entries(reply: &Value, stream: &str) -> Vec<Entry> {
    match reply {
        Value::Array(streams) => streams
            .iter()
            .find_map(|s| match s {
                Value::Array(pair) if pair.first().and_then(text).as_deref() == Some(stream) => {
                    pair.get(1).map(entries)
                }
                _ => None,
            })
            .unwrap_or_default(),
        Value::Map(streams) => streams
            .iter()
            .find_map(|(k, v)| (text(k).as_deref() == Some(stream)).then(|| entries(v)))
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn parses_xreadgroup_replies() {
        let reply = Value::Array(vec![Value::Array(vec![
            bulk("s"),
            Value::Array(vec![Value::Array(vec![bulk("1-0"), Value::Array(vec![bulk("payload"), bulk("x")])])]),
        ])]);
        let got = stream_entries(&reply, "s");
        assert_eq!(1, got.len());
        assert_eq!("1-0", got[0].0);
        assert_eq!(Some("x"), got[0].1.get("payload").map(String::as_str));
        assert!(stream_entries(&Value::Nil, "s").is_empty());
        assert!(stream_entries(&reply, "other").is_empty());
    }

    #[test]
    fn skips_deleted_entries() {
        let list = Value::Array(vec![Value::Nil, Value::Array(vec![bulk("2-0"), Value::Nil])]);
        assert!(entries(&list).is_empty());
    }

    fn args(cmd: &Cmd) -> Vec<String> {
        cmd.args_iter()
            .map(|a| match a {
                redis::Arg::Simple(b) => String::from_utf8_lossy(b).into_owned(),
                _ => "<cursor>".to_owned(),
            })
            .collect()
    }

    fn streamer() -> Streamer {
        let options = StreamerOptions { block_ms: Some(500), consumer_name: Some("c1".to_owned()), handler: None };
        Streamer::new("s", "g", &RedisConfig::new(), options)
    }

    /// Pins D-109: a read takes at most 1000 entries.
    #[test]
    fn deviation_d109_reads_are_bounded() {
        assert_eq!(
            ["XREADGROUP", "GROUP", "g", "c1", "COUNT", "1000", "BLOCK", "500", "STREAMS", "s", ">"],
            args(&streamer().inner.read_command()).as_slice()
        );
    }

    #[test]
    fn formats_delays_like_go() {
        assert_eq!("250ms", go_duration(Duration::from_millis(250)));
        assert_eq!("3s", go_duration(Duration::from_secs(3)));
    }
}
