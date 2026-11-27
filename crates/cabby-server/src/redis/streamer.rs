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
        let mut retries = Retries::default();
        'connect: loop {
            if self.shutdown.is_cancelled() {
                self.finished.set();
                return;
            }
            let client = match self.init_client().await {
                Ok(c) => c,
                Err(e) => {
                    log_error!(self.log, "failed to connect to Redis", "error" => e.to_string());
                    if self.maybe_reconnect(&mut retries).await {
                        continue 'connect;
                    }
                    return;
                }
            };

            if retries.attempt > 0 {
                log_info!(self.log, "reconnected to Redis");
            }
            retries.attempt = 0;

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
                        if self.maybe_reconnect(&mut retries).await {
                            continue 'connect;
                        }
                        return;
                    }
                }
            }

            let block = self.block_ms;
            let mut last_claimed_at: i64 = 0;
            // Where the next claim pass starts in the group's pending list.
            let mut claim_from = String::from(SCAN_START);

            self.ready.set();

            loop {
                if self.shutdown.is_cancelled() {
                    log_debug!(self.log, "stop consuming stream");
                    self.finished.set();
                    return;
                }

                if last_claimed_at + block < now_ms() {
                    match self.autoclaim(&client, &claim_from).await {
                        Err(e) => {
                            log_error!(self.log, "failed to claim from Redis stream", "error" => e.to_string());
                            if self.maybe_reconnect(&mut retries).await {
                                continue 'connect;
                            }
                            return;
                        }
                        Ok((next, reclaimed)) => {
                            last_claimed_at = now_ms();
                            claim_from = next;
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
                        if self.maybe_reconnect(&mut retries).await {
                            continue 'connect;
                        }
                        return;
                    }
                    Ok(messages) => {
                        retries.failures = 0;
                        self.handle_range(&client, messages).await;
                    }
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

    /// One claim pass from `start`; returns where the next pass starts and
    /// the entries claimed.
    async fn autoclaim(&self, client: &Client, start: &str) -> RedisResult<(String, Vec<Entry>)> {
        let cmd = redis::cmd("XAUTOCLAIM")
            .arg(&self.stream)
            .arg(&self.group)
            .arg(&self.consumer)
            .arg(self.block_ms.to_string())
            .arg(start)
            .to_owned();
        claimed(client.query(&cmd).await?)
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
    async fn maybe_reconnect(&self, retries: &mut Retries) -> bool {
        if retries.attempt >= self.max_reconnect_attempts {
            self.finished.set();
            log_error!(self.log, "failed to reconnect to Redis: attempts exceeded");
            return false;
        }

        retries.attempt += 1;
        retries.failures = retries.failures.saturating_add(1);

        let delay = connect::next_retry(retries.step());

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

/// The most [`connect::next_retry`] step for failures on a connection that
/// opens (8 to 16 s).
const MAX_FAILURE_STEP: u32 = 3;

/// The reader's reconnect counters.
#[derive(Debug, Default)]
struct Retries {
    /// Reconnect attempts since a connection last opened (upstream
    /// `reconnectAttempt`), limited by `max_reconnect_attempts`.
    attempt: i64,
    /// Failures in a row since a read last succeeded: failed connections,
    /// group creations, claims and reads. A read returns within the block
    /// time when the consumer works, so it is the one that resets them.
    failures: u32,
}

impl Retries {
    /// The [`connect::next_retry`] step before the next attempt: upstream's
    /// (`attempt - 1`), or more after failures on connections that open, up
    /// to [`MAX_FAILURE_STEP`].
    ///
    /// Upstream counts attempts from the last connection that opened, so when
    /// the connection opens and a command fails every time (a value of
    /// another type at the stream key, a command the Redis user may not
    /// run), it reconnects every 250 ms and logs four records each time,
    /// without end. Here the delay grows as it does while Redis can't be
    /// reached; the first retry after a read that worked is still 250 ms
    /// (D-111).
    fn step(&self) -> u32 {
        let since_connect = u32::try_from(self.attempt - 1).unwrap_or(0);
        since_connect.max(self.failures.saturating_sub(1).min(MAX_FAILURE_STEP))
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

/// The ID a claim pass starts from to scan the pending list from its start;
/// `XAUTOCLAIM` returns it as the cursor once it has scanned to the end.
const SCAN_START: &str = "0-0";

/// An `XAUTOCLAIM` reply: `[cursor, entries, deleted IDs]` (the third
/// element since Redis 7: entries no longer in the stream, which Redis has
/// already removed from the pending list). Returns the cursor, where the
/// next pass starts, and the claimed entries.
///
/// Upstream starts every pass at `0-0`: when the first 100 entries it claims
/// stay pending (their handling fails), every pass claims those again and
/// never reaches the entries after them (D-110).
fn claimed(reply: Value) -> RedisResult<(String, Vec<Entry>)> {
    match reply {
        Value::Nil => Ok((SCAN_START.to_owned(), Vec::new())),
        Value::Array(items) => {
            if items.len() < 2 {
                return Err(redis::RedisError::from((
                    redis::ErrorKind::Parse,
                    "autoclaim failed",
                    format!("got {} elements, wanted 2", items.len()),
                )));
            }
            let next = text(&items[0]).unwrap_or_else(|| SCAN_START.to_owned());
            Ok((next, entries(&items[1])))
        }
        other => Err(redis::RedisError::from((
            redis::ErrorKind::Parse,
            "autoclaim failed",
            format!("unexpected reply {other:?}"),
        ))),
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

    /// Pins D-110: the cursor of a claim pass is where the next one starts;
    /// deleted IDs (Redis 7) are not entries.
    #[test]
    fn deviation_d110_claim_passes_continue_at_the_cursor() {
        let entry = |id: &str| Value::Array(vec![bulk(id), Value::Array(vec![bulk("payload"), bulk("x")])]);
        let reply = Value::Array(vec![
            bulk("7-0"),
            Value::Array(vec![entry("5-0"), entry("6-0")]),
            Value::Array(vec![bulk("4-0")]),
        ]);
        let (next, got) = claimed(reply).unwrap();
        assert_eq!("7-0", next);
        assert_eq!(vec!["5-0", "6-0"], got.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>());

        // At the end of the pending list the cursor is `0-0`: the next pass starts over.
        let (next, got) = claimed(Value::Array(vec![bulk("0-0"), Value::Array(vec![]), Value::Array(vec![])])).unwrap();
        assert_eq!(("0-0", 0), (next.as_str(), got.len()));
        let (next, got) = claimed(Value::Nil).unwrap();
        assert_eq!(("0-0", 0), (next.as_str(), got.len()));
        assert!(claimed(Value::Array(vec![bulk("0-0")])).is_err());
    }

    /// Pins D-111: the retry step after `failures` in a row, when each
    /// reconnect opens a connection (`attempt` 1) and when none opens.
    #[test]
    fn deviation_d111_retry_steps() {
        let step = |attempt, failures| Retries { attempt, failures }.step();
        // A failure after a read that worked: 250 ms, as upstream.
        assert_eq!(0, step(1, 1));
        // Commands keep failing on connections that open: the delay grows up to 8-16 s.
        assert_eq!([0, 1, 2, 3, 3, 3], [1, 2, 3, 4, 5, 6].map(|f| step(1, f)));
        // Redis can't be reached: upstream's steps (both counters grow).
        assert_eq!([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 6].map(|n| step(n, n as u32)));
        // Connections fail after commands failed on one that opened.
        assert_eq!(3, step(2, 6));
        assert_eq!(4, step(5, 9));
    }

    #[test]
    fn formats_delays_like_go() {
        assert_eq!("250ms", go_duration(Duration::from_millis(250)));
        assert_eq!("3s", go_duration(Duration::from_secs(3)));
    }
}
