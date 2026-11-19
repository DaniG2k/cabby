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
use std::sync::{Arc, Condvar, Mutex, RwLock};
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
use crate::nats::client::{self, block_on, blocking, runtime};

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

    fn is_ready(&self) -> bool {
        *lock(&self.state) == ReadyState::Ready
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
            }),
        }
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
            let delay = client::next_retry(attempt);
            attempt += 1;
            if attempt > 5 {
                return Err(format!("JetStream is unavailable, cause: {e}"));
            }
            tokio::time::sleep(delay).await;
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
        // A failed watcher is not fatal (upstream logs it).
        if let Ok(mut watch) = epoch_kv.watch_with_history(EPOCH_KEY).await {
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
                        inner.write_epoch(&value);
                    }
                }
            });
            self.track(task.abort_handle());
        }
        Ok(())
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
        if !self.ready.is_ready() {
            backlog.push(msg);
            return;
        }
        // JetStream became ready meanwhile (the backlog may already be flushed).
        drop(backlog);
        let _ = self.publish(&msg);
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
        if self.ensure_stream_exists(&js.js, &stream).await.is_err() {
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
            .await?;
        self.stream_sync.touch(stream);

        let batch_size = if self.conf.history_limit == 0 { 100 } else { self.conf.history_limit };
        let mut batch = cons.fetch().max_messages(batch_size).messages().await?;
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
        handle.task.abort();
        self.stream_sync.remove(&handle.stream);
        let _ = js.js.delete_consumer_from_stream(&handle.name, format!("{STREAM_PREFIX}{}", handle.stream)).await;
    }

    fn consume_message(&self, stream: &str, msg: &jetstream::Message) {
        self.stream_sync.touch(stream);
        let Ok(info) = msg.info() else { return };
        let nanos = info.published.unix_timestamp_nanos();
        let ts = u64::try_from(nanos).map(|n| UNIX_EPOCH + Duration::from_nanos(n)).unwrap_or(UNIX_EPOCH);
        let _ = self.local.store(stream, &msg.payload, info.stream_sequence, Some(ts));
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
                js.delete_key_value(key).await.map_err(|e| decorate(format!("failed to delete bucket: {key}"), e))?;
            }
            Err(e) => return Err(decorate(format!("failed to create bucket: {key}"), e)),
        }
    }
}

impl Broker for Nats {
    /// Connects (in the background, retrying) and initializes JetStream asynchronously.
    fn start(&self) -> Result<(), BrokerError> {
        let nconf = self.inner.nconf.clone();
        let client = block_on(async move { client::connect(&nconf).await }).map_err(|e| err(e.0))?;
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
                    inner.ready.set(ReadyState::Failed(e));
                    // Messages kept while waiting can no longer be published.
                    lock(&inner.backlog).clear();
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
        block_on(async move {
            let value = js
                .kv
                .get(key.clone())
                .await
                .map_err(|e| decorate("failed to restore session from NATS", e))?
                .ok_or_else(|| decorate("failed to restore session from NATS", "nats: key not found"))?;
            js.kv.put(key, value).await.map_err(|e| decorate("failed to touch session in NATS", e))?;
            Ok(())
        })
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

    /// The test-services `nats-server` (built by the test kit), if present.
    fn nats_server_binary() -> Option<std::path::PathBuf> {
        let built = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-services/nats-server");
        std::env::var_os("CABBY_NATS_SERVER").map(std::path::PathBuf::from).or(Some(built)).filter(|p| p.is_file())
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
