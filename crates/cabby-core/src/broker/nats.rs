//! The NATS JetStream broker (upstream `broker/nats.go`).
//!
//! History lives in one JetStream stream per broadcast stream (`_ac_<stream>`),
//! whose sequence numbers are the stream offsets; every subscribed stream has a
//! consumer copying its messages into a node-local [`Memory`] broker, which
//! answers history queries. Sessions are cached in a JetStream key-value
//! bucket (`_anycable_`, TTL = sessions TTL) and the epoch is shared through
//! another bucket (`_anycable_epoch_`) and watched for changes. TTLs are
//! enforced by the NATS server on its own clock.
//!
//! JetStream is initialized in the background after [`Broker::start`]
//! (upstream waits for a JetStream cluster to become ready); operations that
//! need it wait up to [`JETSTREAM_READY_TIMEOUT`]. Broadcasts made before it
//! is ready are kept in a backlog and published once it is.
//!
//! The [`Broker`] trait is synchronous; every NATS operation runs on the
//! shared NATS runtime and the calling thread waits for it
//! ([`crate::nats::client::block_on`], see that module for the rationale).

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

use async_nats::jetstream::{self, consumer, kv, stream};
use async_nats::{Client, jetstream::ErrorCode};
use serde_json::Value;
use tokio::task::AbortHandle;
use tokio_stream::StreamExt;

use super::{
    Broadcaster, Broker, BrokerError, Cacheable, Config, Memory, NatsConfig, PresenceInfoOptions, StreamsTracker,
    SystemClock, lock, nanoid, read, write,
};
use crate::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use crate::logger::Logger;
use crate::nats::client::{self, block_on, blocking, runtime};
use crate::{log_debug, log_error, log_info, log_warn};

const KV_BUCKET: &str = "_anycable_";
const EPOCH_BUCKET: &str = "_anycable_epoch_";
const EPOCH_KEY: &str = "_epoch_";
const SESSIONS_PREFIX: &str = "";
const STREAM_PREFIX: &str = "_ac_";

/// How long operations wait for JetStream to be initialized (upstream
/// `jetstreamReadyTimeout`).
pub const JETSTREAM_READY_TIMEOUT: Duration = Duration::from_secs(1);

/// How long a stream stays "being synced" after its last consumed message
/// (upstream `streamHistorySyncPeriod`).
const STREAM_HISTORY_SYNC_PERIOD: Duration = Duration::from_millis(50);
/// The longest a history read waits for a busy stream (upstream
/// `streamHistorySyncTimeout`).
const STREAM_HISTORY_SYNC_TIMEOUT: Duration = Duration::from_millis(200);

/// How long `Shutdown` waits for the connection to drain.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

fn err(message: impl Into<String>) -> BrokerError {
    BrokerError { message: message.into() }
}

/// errorx's `Decorate(err, msg)` rendering.
fn decorate(message: impl std::fmt::Display, cause: impl std::fmt::Display) -> BrokerError {
    err(format!("{message}, cause: {cause}"))
}

/// Upstream `shouldRetryOnError`: request timeouts and missing JetStream
/// responses are retried.
fn is_retryable(e: &(dyn std::error::Error + 'static)) -> bool {
    let text = e.to_string();
    text.contains("timed out") || text.contains("no responders")
}

/// The text of a JetStream API error as nats.go reports it: a server without
/// JetStream does not answer API requests, which nats.go reports as
/// `ErrJetStreamNotEnabled`; other errors keep async-nats' text.
fn js_error_text(e: &(dyn std::error::Error + 'static)) -> String {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        if let Some(account) = err.downcast_ref::<jetstream::context::AccountError>()
            && account.kind() == jetstream::context::AccountErrorKind::JetStreamUnavailable
        {
            return "nats: API error: code=503 err_code=10076 description=jetstream not enabled".to_owned();
        }
        cur = err.source();
    }
    e.to_string()
}

/// Whether a create failed because a stream with this name exists with a
/// different configuration (Go `jetstream.ErrStreamNameAlreadyInUse`).
fn stream_name_in_use(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(e) = cur {
        if let Some(cse) = e.downcast_ref::<jetstream::context::CreateStreamError>()
            && let jetstream::context::CreateStreamErrorKind::JetStream(js) = cse.kind()
            && js.error_code() == ErrorCode::STREAM_NAME_EXIST
        {
            return true;
        }
        cur = e.source();
    }
    false
}

/// The broker's logger (upstream `l.With("context", "broker").With("provider", "nats")`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "nats"))
}

/// Go's `time.Duration.String()` (`1h0m0s`, `5m0s`, `1.5s`, `500ms`, `0s`).
pub(crate) fn go_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    let fraction = |value: u128, unit: u128, suffix: &str| {
        let whole = value / unit;
        let rest = value % unit;
        if rest == 0 {
            return format!("{whole}{suffix}");
        }
        let width = unit.ilog10() as usize;
        let digits = format!("{rest:0width$}");
        format!("{whole}.{}{suffix}", digits.trim_end_matches('0'))
    };
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return fraction(nanos, 1_000, "\u{b5}s");
    }
    if nanos < 1_000_000_000 {
        return fraction(nanos, 1_000_000, "ms");
    }
    let secs = d.as_secs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    let seconds = fraction(u128::from(s) * 1_000_000_000 + u128::from(d.subsec_nanos()), 1_000_000_000, "s");
    match (h, m) {
        (0, 0) => seconds,
        (0, m) => format!("{m}m{seconds}"),
        (h, m) => format!("{h}h{m}m{seconds}"),
    }
}

/// Upstream `shouldRetryOnError`'s warning before a retry.
fn log_retry(cooldown: Duration, e: &dyn std::fmt::Display) {
    log_warn!(log(), &format!("operation failed, retrying in {}...", go_duration(cooldown)), "error" => e.to_string());
}

fn secs(n: i64) -> Duration {
    Duration::from_secs(u64::try_from(n).unwrap_or(0))
}

/// JetStream handles, available once initialized.
#[derive(Clone)]
struct Js {
    js: jetstream::Context,
    kv: kv::Store,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadyState {
    Pending,
    Ready,
    Failed(String),
}

/// Upstream `readyCtx`: resolved once JetStream initialization finishes.
struct Ready {
    state: Mutex<ReadyState>,
    cv: Condvar,
}

impl Ready {
    fn set(&self, state: ReadyState) {
        *lock(&self.state) = state;
        self.cv.notify_all();
    }

    fn state(&self) -> ReadyState {
        lock(&self.state).clone()
    }

    fn wait(&self, timeout: Duration) -> Result<(), BrokerError> {
        let state = blocking(|| {
            let guard = lock(&self.state);
            let (guard, _) = self
                .cv
                .wait_timeout_while(guard, timeout, |s| *s == ReadyState::Pending)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.clone()
        });
        match state {
            ReadyState::Ready => Ok(()),
            ReadyState::Pending => Err(err("timeout waiting for JetStream to be ready")),
            ReadyState::Failed(e) => Err(err(e)),
        }
    }
}

/// A JetStream consumer copying a stream into the local broker.
#[derive(Clone)]
struct ConsumerHandle {
    stream: String,
    name: String,
    task: AbortHandle,
}

/// Upstream `lru`: values created on first use and dropped (handed back to the
/// caller for cleanup) once unused for the TTL. Creation runs without the map
/// lock held; concurrent fetches of one key share a single creation.
struct Lru<T: Clone> {
    ttl: Duration,
    entries: Mutex<HashMap<String, Slot<T>>>,
}

struct Slot<T> {
    cell: Arc<tokio::sync::OnceCell<T>>,
    last_active: Instant,
}

impl<T: Clone> Lru<T> {
    fn new(ttl: Duration) -> Self {
        Self { ttl, entries: Mutex::new(HashMap::new()) }
    }

    fn expired(&self, slot: &Slot<T>, now: Instant) -> bool {
        slot.last_active + self.ttl < now
    }

    /// Returns the value for `key` (creating it when missing or expired) and
    /// the values evicted on the way, which the caller must clean up.
    async fn fetch<E, Fut>(&self, key: &str, create: impl FnOnce() -> Fut) -> (Result<T, E>, Vec<T>)
    where
        Fut: Future<Output = Result<T, E>>,
    {
        let mut evicted = Vec::new();
        let cell = {
            let mut entries = lock(&self.entries);
            let now = Instant::now();
            match entries.get_mut(key) {
                Some(slot) if !self.expired(slot, now) => {
                    slot.last_active = now;
                    slot.cell.clone()
                }
                _ => {
                    let cell = Arc::new(tokio::sync::OnceCell::new());
                    let old = entries.insert(key.to_owned(), Slot { cell: cell.clone(), last_active: now });
                    evicted.extend(old.and_then(|s| s.cell.get().cloned()));
                    cell
                }
            }
        };
        if let Some(v) = cell.get() {
            return (Ok(v.clone()), evicted);
        }
        match cell.get_or_try_init(create).await {
            Ok(v) => {
                let v = v.clone();
                evicted.extend(self.expire(key));
                (Ok(v), evicted)
            }
            Err(e) => {
                let mut entries = lock(&self.entries);
                if entries.get(key).is_some_and(|s| Arc::ptr_eq(&s.cell, &cell)) {
                    entries.remove(key);
                }
                (Err(e), evicted)
            }
        }
    }

    /// Removes expired, initialized entries other than `keep`.
    fn expire(&self, keep: &str) -> Vec<T> {
        let now = Instant::now();
        let mut entries = lock(&self.entries);
        let stale: Vec<String> = entries
            .iter()
            .filter(|(k, s)| k.as_str() != keep && s.cell.initialized() && self.expired(s, now))
            .map(|(k, _)| k.clone())
            .collect();
        stale.into_iter().filter_map(|k| entries.remove(&k).and_then(|s| s.cell.get().cloned())).collect()
    }

    fn drain(&self) -> Vec<T> {
        lock(&self.entries).drain().filter_map(|(_, s)| s.cell.get().cloned()).collect()
    }
}

#[derive(Debug)]
struct SyncState {
    active: bool,
    active_since: Instant,
    last_touch: Instant,
}

/// Upstream `streamSync`: a stream is "active" while messages keep being
/// consumed (each within [`STREAM_HISTORY_SYNC_PERIOD`] of the previous one);
/// history reads wait until it is idle. A message arriving after the stream
/// has been active for [`STREAM_HISTORY_SYNC_TIMEOUT`] makes it idle, so reads
/// never wait much longer than that.
struct StreamSync {
    state: Mutex<SyncState>,
    cv: Condvar,
}

impl StreamSync {
    fn new() -> Self {
        let now = Instant::now();
        Self { state: Mutex::new(SyncState { active: false, active_since: now, last_touch: now }), cv: Condvar::new() }
    }

    /// Waits until the stream is idle (blocking).
    fn sync(&self) {
        blocking(|| {
            let mut st = lock(&self.state);
            loop {
                if !st.active {
                    return;
                }
                let now = Instant::now();
                let idle_at = st.last_touch + STREAM_HISTORY_SYNC_PERIOD;
                if now >= idle_at {
                    st.active = false;
                    return;
                }
                st = self.cv.wait_timeout(st, idle_at - now).unwrap_or_else(std::sync::PoisonError::into_inner).0;
            }
        });
    }

    fn restart(&self) {
        let now = Instant::now();
        let mut st = lock(&self.state);
        if st.active && now < st.last_touch + STREAM_HISTORY_SYNC_PERIOD {
            if now > st.active_since + STREAM_HISTORY_SYNC_TIMEOUT {
                st.active = false;
                self.cv.notify_all();
            } else {
                st.last_touch = now;
            }
            return;
        }
        st.active = true;
        st.active_since = now;
        st.last_touch = now;
    }

    fn idle(&self) {
        lock(&self.state).active = false;
        self.cv.notify_all();
    }
}

/// Upstream `streamsSynchronizer`.
#[derive(Default)]
struct StreamsSynchronizer {
    entries: RwLock<HashMap<String, Arc<StreamSync>>>,
}

impl StreamsSynchronizer {
    fn sync(&self, stream: &str) {
        let syncer = read(&self.entries).get(stream).cloned();
        if let Some(s) = syncer {
            s.sync();
        }
    }

    fn touch(&self, stream: &str) {
        let syncer = read(&self.entries).get(stream).cloned();
        match syncer {
            Some(s) => s.restart(),
            None => {
                write(&self.entries).entry(stream.to_owned()).or_insert_with(|| Arc::new(StreamSync::new())).restart()
            }
        }
    }

    fn remove(&self, stream: &str) {
        if let Some(s) = write(&self.entries).remove(stream) {
            s.idle();
        }
    }
}

/// Broker backed by NATS JetStream (upstream `broker.NATS`). History and
/// session TTLs are enforced by the NATS server, on its own clock.
pub struct Nats {
    inner: Arc<Inner>,
}

struct Inner {
    broadcaster: Option<Arc<dyn Broadcaster>>,
    conf: Config,
    nconf: NatsConfig,
    tracker: StreamsTracker,
    /// Node-local copy of the subscribed streams (upstream `local`).
    local: Memory,
    epoch: RwLock<String>,
    ready: Ready,
    client: Mutex<Option<Client>>,
    js: RwLock<Option<Js>>,
    backlog: Mutex<Vec<StreamMessage>>,
    jstreams: Lru<()>,
    jconsumers: Lru<ConsumerHandle>,
    stream_sync: StreamsSynchronizer,
    /// Background tasks (initialization, epoch watcher), aborted on shutdown.
    tasks: Mutex<Vec<AbortHandle>>,
    closed: AtomicBool,
    /// Where a failed JetStream initialization is reported (upstream `done`).
    done: Mutex<Option<std::sync::mpsc::Sender<BrokerError>>>,
    /// Delay before JetStream initialization attempt `n + 1` (upstream `utils.NextRetry`).
    retry_delay: fn(u32) -> Duration,
}

impl Nats {
    /// Upstream `NewNATSBroker(broadcaster, config, natsConfig)`.
    pub fn new(broadcaster: Option<Arc<dyn Broadcaster>>, config: &Config, nats: &NatsConfig) -> Self {
        let history_ttl = secs(config.history_ttl);
        Self {
            inner: Arc::new(Inner {
                broadcaster,
                conf: config.clone(),
                nconf: nats.clone(),
                tracker: StreamsTracker::new(),
                local: Memory::new(None, None, config, Arc::new(SystemClock)),
                epoch: RwLock::new(String::new()),
                ready: Ready { state: Mutex::new(ReadyState::Pending), cv: Condvar::new() },
                client: Mutex::new(None),
                js: RwLock::new(None),
                backlog: Mutex::new(Vec::new()),
                jstreams: Lru::new(history_ttl),
                jconsumers: Lru::new(history_ttl),
                stream_sync: StreamsSynchronizer::default(),
                tasks: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
                done: Mutex::new(None),
                retry_delay: client::next_retry,
            }),
        }
    }

    /// Where to report a JetStream initialization that failed for good after
    /// [`Broker::start`] (upstream `Start(done)`; the runner exits with it).
    pub fn set_done(&self, done: std::sync::mpsc::Sender<BrokerError>) {
        *lock(&self.inner.done) = Some(done);
    }

    /// Waits until JetStream is initialized (upstream `Ready(timeout)`).
    pub fn ready(&self, timeout: Duration) -> Result<(), BrokerError> {
        self.inner.ready.wait(timeout)
    }

    /// Deletes all sessions and every stream with a single-token subject (the
    /// broker's history streams), as upstream `Reset` does.
    pub fn reset(&self) -> Result<(), BrokerError> {
        let js = self.inner.js_ready()?;
        block_on(async move {
            if let Ok(mut keys) = js.kv.keys().await {
                let mut all = Vec::new();
                while let Some(key) = keys.next().await {
                    match key {
                        Ok(k) => all.push(k),
                        Err(_) => break,
                    }
                }
                for key in all {
                    let _ = js.kv.delete(&key).await;
                }
            }
            for name in stream_names(&js.js, &format!("{SESSIONS_PREFIX}*")).await? {
                let _ = js.js.delete_stream(&name).await;
            }
            Ok(())
        })
    }

    pub fn epoch(&self) -> String {
        self.inner.epoch()
    }

    /// Stores a new epoch in the shared bucket; other brokers pick it up from
    /// their watchers.
    pub fn set_epoch(&self, epoch: &str) -> Result<(), BrokerError> {
        let js = self.inner.js_ready()?;
        let value = epoch.to_owned();
        block_on(async move {
            let bucket = js.js.get_key_value(EPOCH_BUCKET).await.map_err(|e| err(e.to_string()))?;
            bucket.put(EPOCH_KEY, value.into_bytes().into()).await.map_err(|e| err(e.to_string()))?;
            Ok::<_, BrokerError>(())
        })?;
        self.inner.write_epoch(epoch);
        Ok(())
    }

    /// Epoch of the node-local broker (upstream reads `n.local.GetEpoch()`).
    pub fn local_epoch(&self) -> String {
        self.inner.local.epoch()
    }
}

impl Drop for Nats {
    fn drop(&mut self) {
        self.inner.close();
    }
}

/// `$JS.API.STREAM.NAMES` filtered by subject (Go `ListStreams(WithStreamListSubject)`).
async fn stream_names(js: &jetstream::Context, subject: &str) -> Result<Vec<String>, BrokerError> {
    #[derive(serde::Deserialize)]
    struct Page {
        #[serde(default)]
        total: usize,
        #[serde(default)]
        streams: Option<Vec<String>>,
    }
    let mut names = Vec::new();
    loop {
        let req = serde_json::json!({"offset": names.len(), "subject": subject});
        let resp: jetstream::response::Response<Page> =
            js.request("STREAM.NAMES", &req).await.map_err(|e| err(e.to_string()))?;
        let page = match resp {
            jetstream::response::Response::Ok(p) => p,
            jetstream::response::Response::Err { error } => return Err(err(error.to_string())),
        };
        let batch = page.streams.unwrap_or_default();
        if batch.is_empty() {
            return Ok(names);
        }
        names.extend(batch);
        if names.len() >= page.total {
            return Ok(names);
        }
    }
}

impl Inner {
    fn epoch(&self) -> String {
        read(&self.epoch).clone()
    }

    fn write_epoch(&self, value: &str) {
        let mut epoch = write(&self.epoch);
        *epoch = value.to_owned();
        self.local.set_epoch(value);
    }

    fn track(&self, task: AbortHandle) {
        if self.closed.load(Ordering::SeqCst) {
            task.abort();
            return;
        }
        lock(&self.tasks).push(task);
    }

    /// Waits for JetStream (upstream `Ready(jetstreamReadyTimeout)`) and returns its handles.
    fn js_ready(&self) -> Result<Js, BrokerError> {
        self.ready.wait(JETSTREAM_READY_TIMEOUT)?;
        read(&self.js).clone().ok_or_else(|| err("NATS broker is shut down"))
    }

    fn js(&self) -> Option<Js> {
        read(&self.js).clone()
    }

    async fn init_jetstream_with_retry(self: &Arc<Self>, client: Client) -> Result<(), String> {
        let mut attempt = 0;
        loop {
            let e = match self.init_jetstream(client.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            let delay = (self.retry_delay)(attempt);
            attempt += 1;
            if attempt > 5 {
                return Err(format!("JetStream is unavailable, cause: {e}"));
            }
            log_warn!(log(), "JetStream initialization failed", "error" => e);
            log_info!(log(), &format!("next JetStream initialization attempt in {}", go_duration(delay)));
            tokio::time::sleep(delay).await;
            log_info!(log(), "re-initializing JetStream...");
        }
    }

    async fn init_jetstream(self: &Arc<Self>, client: Client) -> Result<(), String> {
        let js = jetstream::new(client);
        let kv = fetch_bucket_with_ttl(&js, KV_BUCKET, secs(self.conf.sessions_ttl))
            .await
            .map_err(|e| format!("failed to connect to JetStream KV, cause: {e}"))?;
        let (epoch, epoch_kv) =
            self.calculate_epoch(&js).await.map_err(|e| format!("failed to calculate epoch, cause: {e}"))?;
        self.write_epoch(&epoch);
        self.local.start().map_err(|e| format!("failed to start internal memory broker, cause: {e}"))?;
        *write(&self.js) = Some(Js { js, kv });
        // A failed watcher is not fatal.
        match epoch_kv.watch_with_history(EPOCH_KEY).await {
            Err(e) => log_warn!(log(), "failed to set up epoch watcher", "error" => e.to_string()),
            Ok(watch) => self.watch_epoch(watch),
        }
        log_info!(log(), "NATS broker is ready", "epoch" => epoch);
        Ok(())
    }

    /// Upstream `watchEpoch`'s goroutine: follows epoch changes made by other nodes.
    fn watch_epoch(self: &Arc<Self>, mut watch: kv::Watch) {
        let inner = Arc::downgrade(self);
        let task = runtime().spawn(async move {
            while let Some(entry) = watch.next().await {
                let Ok(entry) = entry else { continue };
                if entry.operation != kv::Operation::Put {
                    continue;
                }
                let Some(inner) = inner.upgrade() else { return };
                let value = String::from_utf8_lossy(&entry.value).into_owned();
                if inner.epoch() != value {
                    log_warn!(log(), "epoch updated", "epoch" => &value);
                    inner.write_epoch(&value);
                }
            }
        });
        self.track(task.abort_handle());
    }

    /// Upstream `calculateEpoch`: the epoch stored in the epoch bucket, or a new
    /// random one stored there if there is none.
    async fn calculate_epoch(&self, js: &jetstream::Context) -> Result<(String, kv::Store), String> {
        let mut attempts = 5;
        let candidate = nanoid(4);
        let ttl = secs(self.conf.history_ttl.max(self.conf.sessions_ttl)).saturating_mul(10);
        loop {
            let bucket = fetch_bucket_with_ttl(js, EPOCH_BUCKET, ttl)
                .await
                .map_err(|e| format!("failed to connect to JetStream KV, cause: {e}"))?;
            match bucket.create(EPOCH_KEY, candidate.clone().into_bytes().into()).await {
                Ok(_) => return Ok((candidate, bucket)),
                Err(e) if e.kind() == kv::CreateErrorKind::AlreadyExists => {
                    let value = bucket
                        .get(EPOCH_KEY)
                        .await
                        .map_err(|e| format!("failed to retrieve key: {EPOCH_KEY}, cause: {e}"))?
                        .ok_or_else(|| format!("failed to retrieve key: {EPOCH_KEY}"))?;
                    return Ok((String::from_utf8_lossy(&value).into_owned(), bucket));
                }
                Err(e) if attempts > 0 && is_retryable(&e) => {
                    attempts -= 1;
                    log_retry(Duration::from_secs(1), &e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(format!("failed to create key: {EPOCH_KEY}, cause: {e}")),
            }
        }
    }

    /// Upstream `ensureStreamExists`: creates the JetStream stream once per
    /// LRU period. An existing stream with another configuration is reused.
    async fn ensure_stream_exists(&self, js: &jetstream::Context, stream: &str) -> Result<(), BrokerError> {
        let name = format!("{STREAM_PREFIX}{stream}");
        let mut attempts = 5;
        loop {
            let (res, _) = self
                .jstreams
                .fetch(stream, || async {
                    let config = stream::Config {
                        name: name.clone(),
                        max_messages: i64::try_from(self.conf.history_limit).unwrap_or(i64::MAX),
                        max_age: secs(self.conf.history_ttl),
                        num_replicas: 1,
                        ..Default::default()
                    };
                    match js.create_stream(config).await {
                        Ok(_) => Ok(()),
                        Err(e) if stream_name_in_use(&e) => Ok(()),
                        Err(e) => Err(e),
                    }
                })
                .await;
            match res {
                Ok(()) => return Ok(()),
                Err(e) if attempts > 0 && is_retryable(&e) => {
                    attempts -= 1;
                    log_retry(Duration::from_millis(500), &e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(err(e.to_string())),
            }
        }
    }

    /// Upstream `add`: publishes to the stream's JetStream stream and returns
    /// the sequence number (the offset).
    async fn add(self: Arc<Self>, js: Js, stream: String, data: String) -> Result<u64, BrokerError> {
        self.ensure_stream_exists(&js.js, &stream)
            .await
            .map_err(|e| decorate("failed to create JetStream Stream", e))?;
        // Touch on publish so that a following history read sees this message.
        self.stream_sync.touch(&stream);
        let ack = async { js.js.publish(format!("{STREAM_PREFIX}{stream}"), data.into()).await?.await }
            .await
            .map_err(|e| decorate("failed to publish message to JetStream", e))?;
        Ok(ack.sequence)
    }

    /// Stores, publishes and broadcasts a message (JetStream must be ready).
    fn publish(self: &Arc<Self>, msg: &StreamMessage) -> Result<(), BrokerError> {
        let js = self.js().ok_or_else(|| err("NATS broker is shut down"))?;
        let offset = block_on(self.clone().add(js, msg.stream.clone(), msg.data.clone()))
            .map_err(|e| decorate(format!("failed to add message to JetStream Stream: stream={}", msg.stream), e))?;
        let msg = StreamMessage { epoch: self.epoch(), offset, ..msg.clone() };
        if let Some(b) = &self.broadcaster {
            b.broadcast(&msg);
        }
        Ok(())
    }

    fn backlog_add(self: &Arc<Self>, msg: StreamMessage) {
        let mut backlog = lock(&self.backlog);
        match self.ready.state() {
            ReadyState::Pending => backlog.push(msg),
            // JetStream became ready meanwhile (the backlog may already be flushed).
            ReadyState::Ready => {
                drop(backlog);
                let _ = self.publish(&msg);
            }
            // It never will be: nothing is kept (the runner exits with the error).
            ReadyState::Failed(_) => {}
        }
    }

    fn backlog_flush(self: &Arc<Self>) {
        let pending = std::mem::take(&mut *lock(&self.backlog));
        for msg in pending {
            let _ = self.publish(&msg);
        }
    }

    /// Upstream `addStreamConsumer`: makes sure the stream exists and a
    /// consumer copies it into the local broker.
    async fn add_stream_consumer(self: Arc<Self>, js: Js, stream: String) {
        if let Err(e) = self.ensure_stream_exists(&js.js, &stream).await {
            log_error!(log(), "failed to create JetStream stream", "stream" => &stream, "error" => e.message);
            return;
        }
        let mut attempts = 5;
        loop {
            let (res, evicted) = self.jconsumers.fetch(&stream, || self.create_consumer(&js, &stream)).await;
            for handle in evicted {
                self.drop_consumer(&js, handle).await;
            }
            match res {
                Ok(_) => return,
                Err(e) if attempts > 0 && is_retryable(e.as_ref()) => {
                    attempts -= 1;
                    log_retry(Duration::from_millis(500), &e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(_) => return,
            }
        }
    }

    async fn create_consumer(
        self: &Arc<Self>,
        js: &Js,
        stream: &str,
    ) -> Result<ConsumerHandle, Box<dyn std::error::Error + Send + Sync>> {
        let prefixed = format!("{STREAM_PREFIX}{stream}");
        let cons = js
            .js
            .create_consumer_on_stream(
                consumer::pull::Config { ack_policy: consumer::AckPolicy::None, ..Default::default() },
                &prefixed,
            )
            .await
            .inspect_err(|e| {
                log_error!(log(), "failed to create JetStream stream consumer", "stream" => stream, "error" => e.to_string());
            })?;
        log_debug!(log(), "created JetStream consumer", "consumer" => &cons.cached_info().name, "stream" => stream);
        self.stream_sync.touch(stream);

        let batch_size = if self.conf.history_limit == 0 { 100 } else { self.conf.history_limit };
        let mut batch = cons.fetch().max_messages(batch_size).messages().await.inspect_err(|e| {
            log_error!(log(), "failed to fetch initial messages from JetStream", "error" => e.to_string());
        })?;
        while let Some(msg) = batch.next().await {
            match msg {
                Ok(m) => self.consume_message(stream, &m),
                Err(_) => break,
            }
        }

        let mut messages = cons.messages().await?;
        let inner = Arc::downgrade(self);
        let name = stream.to_owned();
        let task = runtime().spawn(async move {
            while let Some(msg) = messages.next().await {
                let Some(inner) = inner.upgrade() else { return };
                if let Ok(m) = msg {
                    inner.consume_message(&name, &m);
                }
            }
        });
        Ok(ConsumerHandle {
            stream: stream.to_owned(),
            name: cons.cached_info().name.clone(),
            task: task.abort_handle(),
        })
    }

    async fn drop_consumer(&self, js: &Js, handle: ConsumerHandle) {
        log_debug!(log(), "deleting JetStream consumer", "consumer" => &handle.name, "stream" => &handle.stream);
        handle.task.abort();
        self.stream_sync.remove(&handle.stream);
        let _ = js.js.delete_consumer_from_stream(&handle.name, format!("{STREAM_PREFIX}{}", handle.stream)).await;
    }

    fn consume_message(&self, stream: &str, msg: &jetstream::Message) {
        self.stream_sync.touch(stream);
        let info = match msg.info() {
            Ok(info) => info,
            Err(e) => {
                log_error!(log(), "failed to get JetStream message metadata", "error" => e.to_string());
                return;
            }
        };
        let nanos = info.published.unix_timestamp_nanos();
        let ts = u64::try_from(nanos).map(|n| UNIX_EPOCH + Duration::from_nanos(n)).unwrap_or(UNIX_EPOCH);
        if let Err(e) = self.local.store(stream, &msg.payload, info.stream_sequence, Some(ts)) {
            log_error!(log(), "failed to store message in local broker", "error" => e.message);
        }
    }

    /// Stops background work and closes the connection (idempotent).
    fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
        for handle in self.jconsumers.drain() {
            handle.task.abort();
            self.stream_sync.remove(&handle.stream);
        }
        self.jstreams.drain();
        *write(&self.js) = None;
        if let Some(client) = lock(&self.client).take() {
            block_on(async move {
                let _ = tokio::time::timeout(DRAIN_TIMEOUT, client.drain()).await;
            });
        }
        let _ = self.local.shutdown();
        // Unblock anyone still waiting for JetStream.
        let mut state = lock(&self.ready.state);
        if *state == ReadyState::Pending {
            *state = ReadyState::Failed("NATS broker is shut down".into());
        }
        drop(state);
        self.ready.cv.notify_all();
    }
}

/// A session's value and the revision it was read at.
type Revisioned = (bytes::Bytes, u64);

/// The key-value operations of a session keepalive, so that its interleavings
/// with commits can be tested without a server.
trait SessionKv {
    /// The value of `key` and its revision; `None` when the key is missing or
    /// deleted.
    fn latest(&self, key: &str) -> impl Future<Output = Result<Option<Revisioned>, String>> + Send;

    /// Writes `value` to `key` if `revision` is still the key's latest
    /// revision (JetStream's expected last subject sequence); `Ok(false)` when
    /// it is not, including when the key has been deleted or has expired.
    fn update(
        &self,
        key: &str,
        value: bytes::Bytes,
        revision: u64,
    ) -> impl Future<Output = Result<bool, String>> + Send;
}

impl SessionKv for kv::Store {
    async fn latest(&self, key: &str) -> Result<Option<Revisioned>, String> {
        match self.entry(key).await.map_err(|e| e.to_string())? {
            Some(entry) if entry.operation == kv::Operation::Put => Ok(Some((entry.value, entry.revision))),
            _ => Ok(None),
        }
    }

    async fn update(&self, key: &str, value: bytes::Bytes, revision: u64) -> Result<bool, String> {
        match kv::Store::update(self, key, value, revision).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == kv::UpdateErrorKind::WrongLastRevision => Ok(false),
            // The publish error says more than the kind ("failed getting entry").
            Err(e) => Err(std::error::Error::source(&e).map_or_else(|| e.to_string(), ToString::to_string)),
        }
    }
}

/// Upstream `TouchSession`: writes the session's value back to restart its
/// TTL. AnyCable writes it back unconditionally; Cabby writes it only over the
/// revision it read (deviation D-088), so a commit that lands meanwhile stays
/// the latest value, and a key deleted or expired meanwhile is not created.
async fn refresh_session(kv: &impl SessionKv, key: &str) -> Result<(), BrokerError> {
    let (value, revision) = kv
        .latest(key)
        .await
        .map_err(|e| decorate("failed to restore session from NATS", e))?
        .ok_or_else(|| decorate("failed to restore session from NATS", "nats: key not found"))?;
    write_back_session(kv, key, value, revision).await
}

/// The write of [`refresh_session`]: `value` as read at `revision`. A newer
/// revision means a commit has written the key since, which restarted its TTL
/// too, so the touch has nothing left to do and succeeds.
async fn write_back_session(
    kv: &impl SessionKv,
    key: &str,
    value: bytes::Bytes,
    revision: u64,
) -> Result<(), BrokerError> {
    kv.update(key, value, revision).await.map(|_| ()).map_err(|e| decorate("failed to touch session in NATS", e))
}

/// Upstream `fetchBucketWithTTL`: creates the bucket, or reuses an existing
/// one, recreating it when its TTL differs from `ttl`.
async fn fetch_bucket_with_ttl(js: &jetstream::Context, key: &str, ttl: Duration) -> Result<kv::Store, BrokerError> {
    let mut attempts = 5;
    loop {
        let config = kv::Config { bucket: key.to_owned(), max_age: ttl, num_replicas: 1, ..Default::default() };
        match js.create_key_value(config).await {
            Ok(store) => return Ok(store),
            Err(e) if e.kind() == jetstream::context::CreateKeyValueErrorKind::TimedOut && attempts > 0 => {
                attempts -= 1;
                log_warn!(log(), "failed to retrieve bucket, retrying in 500ms...", "bucket" => key);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) if stream_name_in_use(&e) => {
                let bucket = js
                    .get_key_value(key)
                    .await
                    .map_err(|e| decorate(format!("failed to retrieve bucket: {key}"), e))?;
                let status = bucket
                    .status()
                    .await
                    .map_err(|e| decorate(format!("failed to retrieve bucket status: {key}"), e))?;
                if status.max_age() == ttl {
                    return Ok(bucket);
                }
                // The TTL changed: discard the bucket and create a new one.
                log_warn!(
                    log(),
                    "bucket TTL has been changed, recreating the bucket",
                    "bucket" => key,
                    "old_ttl" => go_duration(status.max_age()),
                    "ttl" => go_duration(ttl),
                );
                js.delete_key_value(key).await.map_err(|e| decorate(format!("failed to delete bucket: {key}"), e))?;
            }
            Err(e) => return Err(decorate(format!("failed to create bucket: {key}"), js_error_text(&e))),
        }
    }
}

impl Broker for Nats {
    /// Connects (in the background, retrying) and initializes JetStream asynchronously.
    fn start(&self) -> Result<(), BrokerError> {
        let nconf = self.inner.nconf.clone();
        let client = block_on(async move { client::connect(&nconf, log().clone()).await }).map_err(|e| err(e.0))?;
        *lock(&self.inner.client) = Some(client.clone());
        let inner = self.inner.clone();
        let task = runtime().spawn(async move {
            match inner.init_jetstream_with_retry(client).await {
                Ok(()) => {
                    inner.ready.set(ReadyState::Ready);
                    // Publishing blocks on the NATS runtime: flush from a plain thread.
                    let flusher = inner.clone();
                    let _ = std::thread::Builder::new()
                        .name("cabby-nats-backlog".into())
                        .spawn(move || flusher.backlog_flush());
                }
                Err(e) => {
                    inner.ready.set(ReadyState::Failed(e.clone()));
                    // Messages kept while waiting can no longer be published.
                    lock(&inner.backlog).clear();
                    if let Some(done) = lock(&inner.done).take() {
                        let _ = done.send(err(e));
                    }
                }
            }
        });
        self.inner.track(task.abort_handle());
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.inner.close();
        Ok(())
    }

    fn announce(&self) -> String {
        let c = &self.inner.conf;
        format!(
            "Using NATS broker: {} (history limit: {}, history ttl: {}s, sessions ttl: {}s)",
            self.inner.nconf.filtered_servers(),
            c.history_limit,
            c.history_ttl,
            c.sessions_ttl
        )
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        if msg.meta.as_ref().is_some_and(|m| m.transient) {
            if let Some(b) = &self.inner.broadcaster {
                b.broadcast(msg);
            }
            return Ok(());
        }
        if self.inner.ready.wait(JETSTREAM_READY_TIMEOUT).is_err() {
            log_debug!(log(), "JetStream is not ready yet to publish messages, add to backlog");
            self.inner.backlog_add(msg.clone());
            return Ok(());
        }
        self.inner.publish(msg)
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast_command(msg);
        }
        Ok(())
    }

    fn subscribe(&self, stream: &str) -> String {
        let is_new = self.inner.tracker.add(stream, &mut || {
            if let Some(b) = &self.inner.broadcaster {
                b.subscribe(stream);
            }
        });
        // Without JetStream there is no history to copy (upstream requires it).
        if is_new && let Ok(js) = self.inner.js_ready() {
            block_on(self.inner.clone().add_stream_consumer(js, stream.to_owned()));
        }
        stream.to_owned()
    }

    fn unsubscribe(&self, stream: &str) -> String {
        self.inner.tracker.remove(stream, &mut || {
            if let Some(b) = &self.inner.broadcaster {
                b.unsubscribe(stream);
            }
        });
        stream.to_owned()
    }

    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.inner.ready.wait(JETSTREAM_READY_TIMEOUT)?;
        self.inner.stream_sync.sync(stream);
        self.inner.local.history_from(stream, epoch, offset)
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.inner.ready.wait(JETSTREAM_READY_TIMEOUT)?;
        self.inner.stream_sync.sync(stream);
        self.inner.local.history_since(stream, ts)
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let js = self.inner.js_ready()?;
        let data = session.to_cache_entry().map_err(|e| decorate("failed to serialize session", e))?;
        let key = format!("{SESSIONS_PREFIX}{sid}");
        block_on(async move { js.kv.put(key, data.into()).await })
            .map_err(|e| decorate("failed to save session to NATS", e))?;
        Ok(())
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        let js = self.inner.js_ready()?;
        let key = format!("{SESSIONS_PREFIX}{sid}");
        block_on(async move { js.kv.get(key).await })
            .map(|v| v.map(|b| b.to_vec()))
            .map_err(|e| decorate("failed to restore session from NATS", e))
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        let js = self.inner.js_ready()?;
        let key = format!("{SESSIONS_PREFIX}{sid}");
        block_on(async move { refresh_session(&js.kv, &key).await })
    }

    fn supports_presence(&self) -> bool {
        false
    }

    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(err("presence not supported"))
    }

    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(err("presence not supported"))
    }

    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        Err(err("presence not supported"))
    }

    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_sync_waits_for_idle() {
        let s = StreamSync::new();
        let start = Instant::now();
        s.sync();
        assert!(start.elapsed() < STREAM_HISTORY_SYNC_PERIOD);

        s.restart();
        let start = Instant::now();
        s.sync();
        let waited = start.elapsed();
        assert!(waited >= STREAM_HISTORY_SYNC_PERIOD / 2, "{waited:?}");
        assert!(waited < STREAM_HISTORY_SYNC_TIMEOUT * 2, "{waited:?}");
    }

    #[test]
    fn stream_sync_idle_releases_waiters() {
        let s = Arc::new(StreamSync::new());
        s.restart();
        s.idle();
        let start = Instant::now();
        s.sync();
        assert!(start.elapsed() < STREAM_HISTORY_SYNC_PERIOD);
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<StreamMessage>>);

    impl Broadcaster for Recorder {
        fn broadcast(&self, msg: &StreamMessage) {
            lock(&self.0).push(msg.clone());
        }
        fn broadcast_command(&self, _msg: &RemoteCommandMessage) {}
        fn subscribe(&self, _stream: &str) {}
        fn unsubscribe(&self, _stream: &str) {}
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
    }

    fn nats_config(port: u16) -> NatsConfig {
        NatsConfig { servers: format!("nats://127.0.0.1:{port}"), ..NatsConfig::new() }
    }

    /// The `nats-server` for these tests: `CABBY_NATS_SERVER_BIN`, then
    /// `CABBY_NATS_SERVER` (as the server reads them), else the test-services
    /// binary (built by the test kit), if present.
    fn nats_server_binary() -> Option<std::path::PathBuf> {
        let built = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-services/nats-server");
        ["CABBY_NATS_SERVER_BIN", "CABBY_NATS_SERVER"]
            .iter()
            .find_map(|k| std::env::var_os(k).filter(|p| !p.is_empty()))
            .map(std::path::PathBuf::from)
            .or(Some(built))
            .filter(|p| p.is_file())
    }

    /// Before JetStream is initialized (here: never started), operations that
    /// need it fail after the ready timeout instead of panicking as upstream does
    /// (nil JetStream context); `Subscribe` still tracks the stream.
    #[test]
    fn deviation_d025_calls_before_jetstream_is_ready_fail() {
        let rec = Arc::new(Recorder::default());
        let broker = Nats::new(Some(rec.clone()), &Config::new(), &nats_config(free_port()));

        let timeout = "timeout waiting for JetStream to be ready";
        assert_eq!(timeout, broker.set_epoch("e").unwrap_err().message);
        assert_eq!(timeout, broker.reset().unwrap_err().message);
        assert_eq!(timeout, broker.restore_session("s").unwrap_err().message);
        assert_eq!("s", broker.subscribe("s"));
        assert!(broker.inner.tracker.has("s"));
        broker.shutdown().unwrap();
    }

    /// A JetStream that never becomes available (a NATS server without
    /// JetStream) is reported through `done` after the retries, as upstream
    /// reports it to the runner (which exits); broadcasts made afterwards are
    /// not kept (D-023: nothing grows once initialization has failed).
    #[test]
    fn jetstream_initialization_failure_is_reported_and_nothing_is_kept() {
        let Some(bin) = nats_server_binary() else { return };
        let port = free_port();
        let mut server = std::process::Command::new(bin)
            .args(["-a", "127.0.0.1", "-p", &port.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let rec = Arc::new(Recorder::default());
        let mut broker = Nats::new(Some(rec.clone()), &Config::new(), &nats_config(port));
        Arc::get_mut(&mut broker.inner).expect("not shared yet").retry_delay = |_| Duration::from_millis(10);
        let (done, failed) = std::sync::mpsc::channel();
        broker.set_done(done);
        broker.start().unwrap();

        let e = failed.recv_timeout(Duration::from_secs(30)).expect("the failure is reported");
        let msg = StreamMessage { stream: "s".into(), data: "a".into(), ..Default::default() };
        for _ in 0..100 {
            broker.handle_broadcast(&msg).unwrap();
        }
        let kept = lock(&broker.inner.backlog).len();
        broker.shutdown().unwrap();
        let _ = server.kill();
        let _ = server.wait();

        assert!(
            e.message.starts_with(
                "JetStream is unavailable, cause: failed to connect to JetStream KV, cause: failed to create bucket: _anycable_, cause: nats: API error: code=503 err_code=10076 description=jetstream not enabled"
            ),
            "{e}"
        );
        assert_eq!(0, kept);
        assert!(lock(&rec.0).is_empty());
    }

    /// Broadcasts made while JetStream is not ready are published once it is
    /// (upstream never publishes them: its backlog is only flushed when
    /// initialization fails, and that flush deadlocks).
    #[test]
    fn deviation_d023_backlog_is_published_once_jetstream_is_ready() {
        let Some(bin) = nats_server_binary() else { return };
        let port = free_port();
        let store = std::env::temp_dir().join(format!("cabby-broker-backlog-{port}"));
        let rec = Arc::new(Recorder::default());
        let broker = Nats::new(Some(rec.clone()), &Config::new(), &nats_config(port));
        broker.start().unwrap();

        // No server yet: the broadcast waits for the ready timeout and is kept.
        let msg = StreamMessage { stream: "backlog".into(), data: "a".into(), ..Default::default() };
        broker.handle_broadcast(&msg).unwrap();
        assert!(lock(&rec.0).is_empty());

        let mut server = std::process::Command::new(bin)
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(&store)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(30);
        while lock(&rec.0).is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(50));
        }
        let published = lock(&rec.0).clone();
        broker.shutdown().unwrap();
        let _ = server.kill();
        let _ = server.wait();
        let _ = std::fs::remove_dir_all(&store);

        assert_eq!(1, published.len(), "{published:?}");
        assert_eq!(("a", 1), (published[0].data.as_str(), published[0].offset));
        assert_eq!(broker.epoch(), published[0].epoch);
    }

    /// Go's `time.Duration.String()`, as logged in TTL and retry records.
    #[test]
    fn durations_print_like_go() {
        assert_eq!("0s", go_duration(Duration::ZERO));
        assert_eq!("500ms", go_duration(Duration::from_millis(500)));
        assert_eq!("1.5s", go_duration(Duration::from_millis(1500)));
        assert_eq!("5m0s", go_duration(Duration::from_secs(300)));
        assert_eq!("1h0m0s", go_duration(Duration::from_secs(3600)));
        assert_eq!("50h0m0s", go_duration(Duration::from_secs(180_000)));
        assert_eq!("1.5\u{b5}s", go_duration(Duration::from_nanos(1500)));
    }

    /// A key-value bucket in memory with revisions as JetStream counts them
    /// (one sequence for the whole bucket).
    #[derive(Default)]
    struct FakeKv {
        state: Mutex<(u64, HashMap<String, Revisioned>)>,
    }

    impl FakeKv {
        /// A commit (`commit_session`): an unconditional put.
        fn commit(&self, key: &str, value: &'static str) -> u64 {
            let mut st = lock(&self.state);
            st.0 += 1;
            let revision = st.0;
            st.1.insert(key.to_owned(), (bytes::Bytes::from_static(value.as_bytes()), revision));
            revision
        }

        /// A deletion, or an expiry by the bucket's TTL.
        fn remove(&self, key: &str) {
            let mut st = lock(&self.state);
            st.0 += 1;
            st.1.remove(key);
        }

        fn value(&self, key: &str) -> Option<(String, u64)> {
            lock(&self.state).1.get(key).map(|(v, r)| (String::from_utf8_lossy(v).into_owned(), *r))
        }
    }

    impl SessionKv for FakeKv {
        async fn latest(&self, key: &str) -> Result<Option<Revisioned>, String> {
            Ok(lock(&self.state).1.get(key).cloned())
        }

        async fn update(&self, key: &str, value: bytes::Bytes, revision: u64) -> Result<bool, String> {
            let mut st = lock(&self.state);
            // A deleted or expired key matches no revision that was read
            // (JetStream compares with its delete marker, or with 0).
            if st.1.get(key).map_or(0, |(_, r)| *r) != revision {
                return Ok(false);
            }
            st.0 += 1;
            let next = st.0;
            st.1.insert(key.to_owned(), (value, next));
            Ok(true)
        }
    }

    fn run<F: Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(fut)
    }

    /// A touch writes the value back as a new revision, which restarts the
    /// key's TTL.
    #[test]
    fn deviation_d088_a_touch_writes_the_session_back() {
        let kv = FakeKv::default();
        kv.commit("sid", "a");

        run(refresh_session(&kv, "sid")).unwrap();

        assert_eq!(Some(("a".to_owned(), 2)), kv.value("sid"));
    }

    /// A commit that lands while a touch waits for JetStream stays the latest
    /// value: the touch's write is conditional on the revision it read, and
    /// a newer revision counts as refreshed.
    #[test]
    fn deviation_d088_a_touch_does_not_overwrite_a_newer_commit() {
        let kv = FakeKv::default();
        kv.commit("sid", "a");
        let (value, revision) = run(kv.latest("sid")).unwrap().unwrap();
        let newer = kv.commit("sid", "b");

        run(write_back_session(&kv, "sid", value, revision)).unwrap();

        assert_eq!(Some(("b".to_owned(), newer)), kv.value("sid"));
    }

    /// A touch of a missing key fails, as in AnyCable, and a key that expires
    /// or is deleted while a touch waits is not written back.
    #[test]
    fn deviation_d088_a_touch_does_not_create_a_session() {
        let kv = FakeKv::default();
        assert_eq!(
            "failed to restore session from NATS, cause: nats: key not found",
            run(refresh_session(&kv, "sid")).unwrap_err().message
        );
        assert_eq!(None, kv.value("sid"));

        kv.commit("sid", "a");
        let (value, revision) = run(kv.latest("sid")).unwrap().unwrap();
        kv.remove("sid");

        run(write_back_session(&kv, "sid", value, revision)).unwrap();

        assert_eq!(None, kv.value("sid"));
    }

    /// The same interleavings on a JetStream bucket (needs `nats-server`).
    #[test]
    fn deviation_d088_touches_on_jetstream_keep_newer_commits_and_create_nothing() {
        let Some(bin) = nats_server_binary() else { return };
        let port = free_port();
        let store = std::env::temp_dir().join(format!("cabby-broker-touch-{port}"));
        let mut server = std::process::Command::new(bin)
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(&store)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();

        let outcome = block_on(async move {
            let s = |e: &dyn std::fmt::Display| e.to_string();
            let deadline = Instant::now() + Duration::from_secs(30);
            let client = loop {
                match async_nats::connect(format!("nats://127.0.0.1:{port}")).await {
                    Ok(c) => break c,
                    Err(e) if Instant::now() > deadline => return Err(s(&e)),
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            };
            let kv = jetstream::new(client)
                .create_key_value(kv::Config { bucket: "touch".into(), ..Default::default() })
                .await
                .map_err(|e| s(&e))?;

            // A plain touch writes a new revision.
            let first = kv.put("sid", "a".into()).await.map_err(|e| s(&e))?;
            refresh_session(&kv, "sid").await.map_err(|e| e.message)?;
            let touched: Option<Revisioned> = kv.latest("sid").await?;

            // A commit between the touch's read and its write stays the latest.
            let (value, revision) = kv.latest("sid").await?.ok_or("no value")?;
            let newer = kv.put("sid", "b".into()).await.map_err(|e| s(&e))?;
            write_back_session(&kv, "sid", value, revision).await.map_err(|e| e.message)?;
            let kept: Option<Revisioned> = kv.latest("sid").await?;

            // A missing key, and one deleted meanwhile, are not created.
            let missing = refresh_session(&kv, "none").await.err().map(|e| e.message);
            let none: Option<Revisioned> = kv.latest("none").await?;
            kv.put("gone", "a".into()).await.map_err(|e| s(&e))?;
            let (value, revision) = kv.latest("gone").await?.ok_or("no value")?;
            kv.delete("gone").await.map_err(|e| s(&e))?;
            write_back_session(&kv, "gone", value, revision).await.map_err(|e| e.message)?;
            let gone = kv.get("gone").await.map_err(|e| s(&e))?;
            Ok::<_, String>((first, touched, newer, kept, missing, none, gone))
        });
        let _ = server.kill();
        let _ = server.wait();
        let _ = std::fs::remove_dir_all(&store);

        let (first, touched, newer, kept, missing, none, gone) = outcome.unwrap();
        let (value, revision) = touched.unwrap();
        assert_eq!(("a", true), (std::str::from_utf8(&value).unwrap(), revision > first));
        assert_eq!(Some((bytes::Bytes::from_static(b"b"), newer)), kept);
        assert_eq!(Some("failed to restore session from NATS, cause: nats: key not found".to_owned()), missing);
        assert_eq!((None, None), (none, gone));
    }

    #[test]
    fn lru_reuses_values_and_evicts_expired_ones() {
        let lru = Lru::<u32>::new(Duration::from_millis(30));
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        rt.block_on(async {
            let (v, ev) = lru.fetch("a", || async { Ok::<_, ()>(1) }).await;
            assert_eq!((Ok(1), vec![]), (v, ev));
            let (v, _) = lru.fetch("a", || async { Ok::<_, ()>(2) }).await;
            assert_eq!(Ok(1), v);
            tokio::time::sleep(Duration::from_millis(50)).await;
            let (v, ev) = lru.fetch("b", || async { Ok::<_, ()>(3) }).await;
            assert_eq!((Ok(3), vec![1]), (v, ev));
            let (v, _) = lru.fetch("c", || async { Err::<u32, _>("boom") }).await;
            assert_eq!(Err("boom"), v);
            assert!(!lock(&lru.entries).contains_key("c"));
        });
    }
}
