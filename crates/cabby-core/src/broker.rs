//! Streams history, presence and session cache (upstream package `broker`).
//!
//! Time is an explicit dependency ([`Clock`]): the memory broker reads
//! timestamps, deadlines and TTL expiry from it, so tests drive expiry by
//! advancing a controlled clock instead of sleeping (`docs/translating-tests.md`, "Hard rules").

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::common::{
    PRESENCE_INFO_TYPE, PRESENCE_JOIN_TYPE, PRESENCE_LEAVE_TYPE, PresenceEvent, PresenceInfo, RemoteCommandMessage,
    StreamMessage,
};
use crate::utils::{ItemId, PriorityQueue};

/// A broker operation failed (Go `error`), compared by message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct BrokerError {
    pub message: String,
}

/// Invalid TOML or a type mismatch while decoding a config.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ConfigError {
    pub message: String,
}

/// Source of wall-clock time for brokers.
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// The operating system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Broker settings (upstream `broker.Config`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub adapter: String,
    /// Seconds to keep streams history.
    pub history_ttl: i64,
    /// Messages to keep per stream.
    pub history_limit: usize,
    /// Seconds to keep session state after disconnect.
    pub sessions_ttl: i64,
    /// Seconds to keep presence after disconnect.
    pub presence_ttl: i64,
}

/// The largest TTL setting, in seconds (upstream `MaxTTL`, a year).
pub const MAX_TTL: i64 = 365 * 24 * 60 * 60;

crate::toml_section!(Config, "broker.Config" {
    "adapter" => adapter,
    "history_ttl" => history_ttl,
    "history_limit" => history_limit,
    "sessions_ttl" => sessions_ttl,
    "presence_ttl" => presence_ttl,
});

impl Config {
    /// Upstream `Config.Validate`: `history_ttl`, `sessions_ttl` and
    /// `presence_ttl` (checked in this order) must be between 0 and
    /// [`MAX_TTL`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (name, val) in [
            ("history_ttl", self.history_ttl),
            ("sessions_ttl", self.sessions_ttl),
            ("presence_ttl", self.presence_ttl),
        ] {
            if !(0..=MAX_TTL).contains(&val) {
                return Err(ConfigError {
                    message: format!("{name} must be between 0 and {MAX_TTL} seconds, got {val}"),
                });
            }
        }
        Ok(())
    }

    /// Upstream `NewConfig()`.
    pub fn new() -> Self {
        Self {
            adapter: String::new(),
            // 5 minutes
            history_ttl: 5 * 60,
            history_limit: 100,
            // 5 minutes
            sessions_ttl: 5 * 60,
            presence_ttl: 15,
        }
    }

    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Broker backend adapter\n");
        if self.adapter.is_empty() {
            out.push_str("# adapter = \"memory\"\n");
        } else {
            out.push_str(&format!("adapter = {}\n", crate::toml_config::toml_quote(&self.adapter)));
        }
        out.push_str("# For how long to keep streams history (seconds)\n");
        out.push_str(&format!("history_ttl = {}\n", self.history_ttl));
        out.push_str("# Max number of messages to keep in a stream history\n");
        out.push_str(&format!("history_limit = {}\n", self.history_limit));
        out.push_str("# For how long to store sessions state for resumeability (seconds)\n");
        out.push_str(&format!("sessions_ttl = {}\n", self.sessions_ttl));
        out.push_str("# For how long to keep presence information after session disconnect (seconds)\n");
        out.push_str(&format!("presence_ttl = {}\n", self.presence_ttl));
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn merge_toml(&mut self, toml: &str) -> Result<(), ConfigError> {
        crate::toml_config::decode(self, toml).map_err(|e| ConfigError { message: e.0 })?;
        Ok(())
    }
}

/// Fans out messages to stream clients and other nodes (upstream `Broadcaster`).
pub trait Broadcaster: Send + Sync {
    fn broadcast(&self, msg: &StreamMessage);
    fn broadcast_command(&self, msg: &RemoteCommandMessage);
    fn subscribe(&self, stream: &str);
    fn unsubscribe(&self, stream: &str);
}

/// Session state that can be cached for recovery (upstream `Cacheable`).
pub trait Cacheable {
    fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError>;
}

/// Handles presence join/leave events (upstream `Presenter`).
pub trait Presenter: Send + Sync {
    fn handle_join(&self, stream: &str, msg: &PresenceEvent);
    fn handle_leave(&self, stream: &str, msg: &PresenceEvent);
}

/// Presence read options (upstream `PresenceInfoOptions`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PresenceInfoOptions {
    pub return_records: bool,
}

impl PresenceInfoOptions {
    /// Upstream `NewPresenceInfoOptions()`.
    pub fn new() -> Self {
        Self { return_records: true }
    }
}

/// History, presence, session cache and cross-node distribution (upstream `Broker`).
pub trait Broker: Send + Sync {
    fn start(&self) -> Result<(), BrokerError>;
    fn shutdown(&self) -> Result<(), BrokerError>;
    fn announce(&self) -> String;
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError>;
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError>;
    /// Registers the stream and returns its unique identifier.
    fn subscribe(&self, stream: &str) -> String;
    fn unsubscribe(&self, stream: &str) -> String;
    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError>;
    /// History since a unix timestamp (seconds). `Ok(None)` is Go's nil slice.
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError>;
    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError>;
    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError>;
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError>;
    /// Removes the session's state from the cache (upstream `DeleteSession`).
    fn delete_session(&self, _sid: &str) -> Result<(), BrokerError> {
        crate::pending!("broker::Broker::DeleteSession")
    }
    fn supports_presence(&self) -> bool;
    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError>;
    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError>;
    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError>;
    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError>;
    /// Current stream position, `None` when the stream is unknown or empty.
    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError>;
}

/// Reference-counted stream registrations (upstream `StreamsTracker`).
///
/// Callbacks run while the tracker lock is held, as upstream does, so a
/// subscribe/unsubscribe pair for the same stream reaches the broadcaster in
/// the order the references changed.
#[derive(Debug, Default)]
pub struct StreamsTracker {
    store: Mutex<HashMap<String, u64>>,
}

impl StreamsTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a reference; calls `on_new` and returns true for the first one.
    pub fn add(&self, name: &str, on_new: &mut dyn FnMut()) -> bool {
        let mut store = lock(&self.store);
        if let Some(count) = store.get_mut(name) {
            *count += 1;
            return false;
        }
        store.insert(name.to_owned(), 1);
        on_new();
        true
    }

    pub fn has(&self, name: &str) -> bool {
        lock(&self.store).contains_key(name)
    }

    /// Drops a reference; calls `on_last` and returns true for the last one.
    pub fn remove(&self, name: &str, on_last: &mut dyn FnMut()) -> bool {
        let mut store = lock(&self.store);
        match store.get_mut(name) {
            None => false,
            Some(count) if *count > 1 => {
                *count -= 1;
                false
            }
            Some(_) => {
                store.remove(name);
                on_last();
                true
            }
        }
    }
}

/// Locks a mutex, recovering the data if a panicking thread poisoned it (the
/// broker state stays consistent: every critical section is panic-free).
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(PoisonError::into_inner)
}

/// Unix seconds of `t` (Go `t.Unix()`: floor division).
fn unix_secs(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => {
            let d = e.duration();
            -(d.as_secs() as i64) - i64::from(d.subsec_nanos() > 0)
        }
    }
}

/// Unix milliseconds of `t` (Go `t.UnixMilli()`: truncation toward zero).
fn unix_millis(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

/// A random URL-friendly ID (go-nanoid `Nanoid(size)`). Upstream ignores
/// generator errors (the ID is then empty); so does this.
fn nanoid(size: usize) -> String {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut bytes = vec![0u8; size];
    if getrandom::fill(&mut bytes).is_err() {
        return String::new();
    }
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

/// A stored history entry (upstream unexported `entry`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Unix seconds.
    pub timestamp: i64,
    pub offset: u64,
    pub data: String,
}

/// Where [`Memory::handle_broadcast_recorded`] stored a message: the offset and
/// timestamp assigned in the same critical section, so concurrent broadcasts
/// can't be mixed up (a later `peak()` could return another message's offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub offset: u64,
    /// Unix seconds, as [`Entry::timestamp`].
    pub timestamp: i64,
}

/// One stream's in-memory history (upstream unexported `memstream`).
///
/// Entries live in a ring buffer ordered by offset: appending, trimming to the
/// limit and expiring from the front are O(1) per entry, and an offset lookup
/// is index arithmetic from the lowest stored offset (upstream's `low`, which
/// is always the front entry's offset, or 0 when the stream is empty).
///
/// A stream in a [`Memory`] broker is written without the broker's map lock:
/// the expiry pass marks a stream it drops as removed while it holds the
/// stream's lock, and a writer that finds the mark looks the stream up again
/// (deviation D-087). Locks are always taken map first, then stream.
pub struct Memstream {
    state: RwLock<MemstreamState>,
    ttl: i64,
    limit: usize,
    clock: Arc<dyn Clock>,
}

struct MemstreamState {
    /// Last assigned offset; kept when entries expire, so offsets never repeat.
    offset: u64,
    /// Unix seconds after which the whole stream may be dropped.
    deadline: i64,
    data: VecDeque<Entry>,
    /// Set when the expiry pass has dropped the stream from its broker's map;
    /// nothing is written to it after that.
    removed: bool,
}

impl MemstreamState {
    fn low(&self) -> u64 {
        self.data.front().map_or(0, |e| e.offset)
    }
}

impl Memstream {
    pub fn new(ttl: i64, limit: usize, clock: Arc<dyn Clock>) -> Self {
        Self {
            state: RwLock::new(MemstreamState { offset: 0, deadline: 0, data: VecDeque::new(), removed: false }),
            ttl,
            limit,
            clock,
        }
    }

    pub fn add(&self, data: &str) -> u64 {
        let now = self.clock.now();
        self.add_locked(&mut write(&self.state), data, now).offset
    }

    /// [`Memstream::add`] for a stream of a broker's map, returning the
    /// assigned offset and timestamp; `None` when the stream was removed.
    fn add_recorded(&self, data: &str) -> Option<Recorded> {
        let now = self.clock.now();
        let mut st = write(&self.state);
        (!st.removed).then(|| self.add_locked(&mut st, data, now))
    }

    fn add_locked(&self, st: &mut MemstreamState, data: &str, now: SystemTime) -> Recorded {
        st.offset += 1;
        let recorded = Recorded { offset: st.offset, timestamp: unix_secs(now) };
        let entry = Entry { offset: recorded.offset, timestamp: recorded.timestamp, data: data.to_owned() };
        self.append(st, entry, now);
        recorded
    }

    /// Stores an entry at an explicit offset (upstream `insert`); `ts: None` is
    /// "now". `None` when the stream was removed.
    fn insert(&self, data: &str, offset: u64, ts: Option<SystemTime>) -> Option<Result<u64, BrokerError>> {
        let now = self.clock.now();
        let ts = ts.unwrap_or(now);
        let mut st = write(&self.state);
        if st.removed {
            return None;
        }
        if st.offset >= offset {
            return Some(Err(BrokerError { message: format!("offset {offset} is already taken") }));
        }
        st.offset = offset;
        self.append(&mut st, Entry { offset, timestamp: unix_secs(ts), data: data.to_owned() }, now);
        Some(Ok(offset))
    }

    /// The latest stored offset, 0 when the stream holds no entries.
    fn peak(&self) -> u64 {
        read(&self.state).data.back().map_or(0, |e| e.offset)
    }

    /// Installs restored state: the last assigned offset, the deadline (unix
    /// seconds) and the newest entries in ascending offsets, at most `limit`.
    /// `None`, with `entries` left in place, when the stream was removed.
    fn restore(&self, offset: u64, deadline: i64, entries: &mut Vec<Entry>) -> Option<()> {
        let mut st = write(&self.state);
        if st.removed {
            return None;
        }
        st.offset = offset;
        st.deadline = deadline;
        st.data = VecDeque::from(std::mem::take(entries));
        while st.data.len() > self.limit {
            st.data.pop_front();
        }
        Some(())
    }

    fn append(&self, st: &mut MemstreamState, entry: Entry, now: SystemTime) {
        st.data.push_back(entry);
        while st.data.len() > self.limit {
            st.data.pop_front();
        }
        // The stream outlives its entries 10 times the TTL, so a stream that
        // becomes active again keeps its offsets.
        st.deadline = unix_secs(now).saturating_add(self.ttl.saturating_mul(10));
    }

    pub fn expire(&self) {
        let now = unix_secs(self.clock.now());
        Self::expire_entries(&mut write(&self.state), now - self.ttl);
    }

    fn expire_entries(st: &mut MemstreamState, cutoff: i64) {
        while st.data.front().is_some_and(|e| e.timestamp < cutoff) {
            st.data.pop_front();
        }
    }

    /// One expiry pass over a stream of a broker's map (the caller holds the
    /// map's write lock): expires entries and, when the stream's deadline is
    /// before `now` (unix seconds), marks it removed in the same critical
    /// section. Returns whether the map keeps it.
    fn sweep(&self, now: i64) -> bool {
        let mut st = write(&self.state);
        Self::expire_entries(&mut st, now - self.ttl);
        st.removed = st.deadline < now;
        !st.removed
    }

    /// Calls `callback` for every entry after `offset`.
    pub fn filter_by_offset(&self, offset: u64, callback: &mut dyn FnMut(&Entry)) -> Result<(), BrokerError> {
        let st = read(&self.state);
        let low = st.low();
        if low > offset {
            return Err(BrokerError {
                message: format!("requested offset couldn't be found: {offset}, lowest: {low}"),
            });
        }
        let Some(latest) = st.data.back() else {
            return Err(BrokerError { message: "stream is empty".into() });
        };
        let start = offset - low + 1;
        if start > st.data.len() as u64 {
            return Err(BrokerError {
                message: format!("requested offset couldn't be found: {offset}, latest: {}", latest.offset),
            });
        }
        st.data.range(start as usize..).for_each(callback);
        Ok(())
    }

    /// Calls `callback` for every entry stored at or after `since` (unix seconds).
    fn filter_by_time(&self, since: i64, callback: &mut dyn FnMut(&Entry)) {
        // Stored timestamps need not be monotonic (`store` takes any), so every
        // entry is checked, as upstream does.
        read(&self.state).data.iter().filter(|e| e.timestamp >= since).for_each(callback);
    }
}

/// Cached session states and their expiration queue.
struct SessionsState {
    entries: HashMap<String, SessionEntry>,
    /// Session IDs by expiration deadline (unix milliseconds).
    expiration: PriorityQueue<String, i64>,
}

struct SessionEntry {
    data: Vec<u8>,
    expiration: ItemId,
}

/// One presence record: a presence ID within a stream and its sessions.
struct PresenceEntry {
    info: Value,
    id: String,
    sessions: Vec<String>,
}

impl PresenceEntry {
    /// Removes the session; true when it was the record's last one.
    fn remove(&mut self, sid: &str) -> bool {
        let Some(i) = self.sessions.iter().position(|s| s == sid) else {
            return false;
        };
        self.sessions.remove(i);
        self.sessions.is_empty()
    }

    fn add(&mut self, sid: &str, info: Value) {
        if !self.sessions.iter().any(|s| s == sid) {
            self.sessions.push(sid.to_owned());
        }
        self.info = info;
    }
}

struct PresenceSession {
    /// stream -> pid
    streams: HashMap<String, String>,
    /// Unix milliseconds.
    deadline: i64,
}

#[derive(Default)]
struct PresenceState {
    /// stream -> pid -> record
    streams: HashMap<String, HashMap<String, PresenceEntry>>,
    sessions: HashMap<String, PresenceSession>,
}

/// Go's `interface{}` info: JSON `null` is a nil interface, which presence
/// events omit.
fn event_info(info: &Value) -> Option<Value> {
    (!info.is_null()).then(|| info.clone())
}

/// State shared by a [`Memory`] broker and its expiration thread.
struct MemoryInner {
    broadcaster: Option<Arc<dyn Broadcaster>>,
    presenter: Option<Arc<dyn Presenter>>,
    config: Config,
    clock: Arc<dyn Clock>,
    tracker: StreamsTracker,
    epoch: RwLock<String>,
    streams: RwLock<HashMap<String, Arc<Memstream>>>,
    sessions: Mutex<SessionsState>,
    presence: Mutex<PresenceState>,
}

/// Stops the expiration thread (on shutdown or drop).
#[derive(Default)]
struct StopSignal {
    stopped: Mutex<bool>,
    cv: Condvar,
}

impl StopSignal {
    fn stop(&self) {
        *lock(&self.stopped) = true;
        self.cv.notify_all();
    }

    /// Waits up to `d`; true when stopped.
    fn wait(&self, d: Duration) -> bool {
        let guard = lock(&self.stopped);
        let (guard, _) =
            self.cv.wait_timeout_while(guard, d, |stopped| !*stopped).unwrap_or_else(PoisonError::into_inner);
        *guard
    }
}

/// Single-node broker keeping everything in memory (upstream `broker.Memory`).
///
/// `add` and `expire` are unexported upstream; they are public here because
/// the translated tests drive them directly.
pub struct Memory {
    inner: Arc<MemoryInner>,
    stop: Arc<StopSignal>,
}

/// How often the expiration thread runs (upstream `expireLoop`).
const EXPIRE_INTERVAL: Duration = Duration::from_secs(1);

impl Memory {
    /// Upstream `NewMemoryBroker(broadcaster, presenter, config)`, plus the clock.
    pub fn new(
        broadcaster: Option<Arc<dyn Broadcaster>>,
        presenter: Option<Arc<dyn Presenter>>,
        config: &Config,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(MemoryInner {
                broadcaster,
                presenter,
                config: config.clone(),
                clock,
                tracker: StreamsTracker::new(),
                epoch: RwLock::new(nanoid(4)),
                streams: RwLock::new(HashMap::new()),
                sessions: Mutex::new(SessionsState { entries: HashMap::new(), expiration: PriorityQueue::new() }),
                presence: Mutex::new(PresenceState::default()),
            }),
            stop: Arc::new(StopSignal::default()),
        }
    }

    pub fn epoch(&self) -> String {
        read(&self.inner.epoch).clone()
    }

    pub fn set_epoch(&self, epoch: &str) {
        *write(&self.inner.epoch) = epoch.to_owned();
    }

    /// Appends a message and returns its offset.
    pub fn add(&self, stream: &str, data: &str) -> u64 {
        self.inner.on_stream(stream, |s| s.add_recorded(data)).offset
    }

    /// Stores a message at an explicit offset; `ts: None` means "now".
    pub fn store(&self, stream: &str, data: &[u8], offset: u64, ts: Option<SystemTime>) -> Result<u64, BrokerError> {
        // Go converts the bytes with `string(data)`, which keeps invalid UTF-8;
        // history data is text here, so invalid sequences become U+FFFD.
        let data = String::from_utf8_lossy(data);
        self.inner.on_stream(stream, |s| s.insert(&data, offset, ts))
    }

    /// One expiration pass over history, sessions and presence.
    pub fn expire(&self) {
        self.inner.expire();
    }

    /// [`Broker::handle_broadcast`], returning where a stored message landed
    /// (`None` for a transient one, which is only fanned out).
    pub fn handle_broadcast_recorded(&self, msg: &StreamMessage) -> Result<Option<Recorded>, BrokerError> {
        if msg.meta.as_ref().is_some_and(|m| m.transient) {
            if let Some(b) = &self.inner.broadcaster {
                b.broadcast(msg);
            }
            return Ok(None);
        }
        let recorded = self.inner.on_stream(&msg.stream, |s| s.add_recorded(&msg.data));
        let msg = StreamMessage { epoch: self.epoch(), offset: recorded.offset, ..msg.clone() };
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast(&msg);
        }
        Ok(Some(recorded))
    }

    /// Installs a stream loaded from storage: `max_offset` is its last assigned
    /// offset, `deadline` (unix seconds) when it may be dropped, and `entries`
    /// its newest entries, in ascending and contiguous offsets ending at
    /// `max_offset` (or none).
    pub fn restore_stream(
        &self,
        name: &str,
        max_offset: u64,
        deadline: i64,
        mut entries: Vec<Entry>,
    ) -> Result<(), BrokerError> {
        let contiguous = entries.windows(2).all(|w| w[1].offset == w[0].offset + 1);
        let anchored = entries.last().is_none_or(|e| e.offset == max_offset);
        if !contiguous || !anchored {
            return Err(BrokerError {
                message: format!("restored entries of {name} must be contiguous and end at offset {max_offset}"),
            });
        }
        self.inner.on_stream(name, |s| s.restore(max_offset, deadline, &mut entries));
        Ok(())
    }

    /// Caches session state with an explicit expiry (unix milliseconds), as
    /// loaded from storage.
    pub fn restore_session_with_deadline(&self, sid: &str, data: Vec<u8>, expires_at_ms: i64) {
        self.inner.cache_session(sid, data, expires_at_ms);
    }

    /// [`Broker::commit_session`] for serialized state; returns the expiry it
    /// set (unix milliseconds).
    pub fn commit_session_data(&self, sid: &str, data: Vec<u8>) -> i64 {
        let deadline = self.inner.session_deadline();
        self.inner.cache_session(sid, data, deadline);
        deadline
    }

    /// Cached sessions waiting to expire (upstream unexported `expireSessions.Len()`).
    pub fn expiring_sessions(&self) -> usize {
        crate::pending!("broker::Memory::expireSessions")
    }

    /// [`Broker::touch_session`]; returns the new expiry (unix milliseconds), or
    /// `None` when nothing is cached for `sid`.
    pub fn touch_session_deadline(&self, sid: &str) -> Option<i64> {
        let deadline = self.inner.session_deadline();
        let mut guard = lock(&self.inner.sessions);
        let st = &mut *guard;
        let entry = st.entries.get(sid)?;
        st.expiration.update(entry.expiration, deadline);
        Some(deadline)
    }
}

impl MemoryInner {
    fn stream_or_create(&self, name: &str) -> Arc<Memstream> {
        if let Some(s) = read(&self.streams).get(name) {
            return s.clone();
        }
        write(&self.streams)
            .entry(name.to_owned())
            .or_insert_with(|| {
                Arc::new(Memstream::new(self.config.history_ttl, self.config.history_limit, self.clock.clone()))
            })
            .clone()
    }

    /// Runs `write` on the stream `name`, created if missing. `write` returns
    /// `None` when the expiry pass removed the stream after it was looked up;
    /// the stream is then looked up again, so every write lands in a stream
    /// the map holds. A retry needs a pass to drop this very stream between
    /// the lookup and the write, so it is rare and a second one rarer still.
    fn on_stream<R>(&self, name: &str, mut write: impl FnMut(&Memstream) -> Option<R>) -> R {
        loop {
            if let Some(done) = write(&self.stream_or_create(name)) {
                return done;
            }
        }
    }

    fn get(&self, name: &str) -> Option<Arc<Memstream>> {
        read(&self.streams).get(name).cloned()
    }

    fn now_millis(&self) -> i64 {
        unix_millis(self.clock.now())
    }

    /// When a session committed or touched now expires (unix milliseconds).
    fn session_deadline(&self) -> i64 {
        self.now_millis() + self.config.sessions_ttl * 1000
    }

    /// Caches `data` for `sid` until `deadline` (unix milliseconds).
    fn cache_session(&self, sid: &str, data: Vec<u8>, deadline: i64) {
        let mut guard = lock(&self.sessions);
        let st = &mut *guard;
        let expiration = match st.entries.get(sid) {
            Some(entry) => {
                st.expiration.update(entry.expiration, deadline);
                entry.expiration
            }
            None => st.expiration.push_item(sid.to_owned(), deadline),
        };
        st.entries.insert(sid.to_owned(), SessionEntry { data, expiration });
    }

    fn expire(&self) {
        {
            let now = unix_secs(self.clock.now());
            // Lock order: map, then each stream (`Memstream::sweep`).
            write(&self.streams).retain(|_, stream| stream.sweep(now));
        }
        self.expire_sessions_cache();
        self.expire_presence();
    }

    fn expire_sessions_cache(&self) {
        let now = self.now_millis();
        let mut guard = lock(&self.sessions);
        let st = &mut *guard;
        while st.expiration.peek().is_some_and(|item| item.priority <= now) {
            if let Some(item) = st.expiration.pop_item() {
                st.entries.remove(&item.value);
            }
        }
    }

    fn expire_presence(&self) {
        let now = self.now_millis();
        let mut leave_events = Vec::new();
        {
            let mut guard = lock(&self.presence);
            let st = &mut *guard;
            let expired: Vec<String> = st
                .sessions
                .iter()
                .filter(|(_, sp)| sp.deadline > 0 && sp.deadline < now)
                .map(|(sid, _)| sid.clone())
                .collect();
            for sid in expired {
                let Some(session) = st.sessions.remove(&sid) else { continue };
                for (stream, pid) in session.streams {
                    let Some(stream_presence) = st.streams.get_mut(&stream) else { continue };
                    let Some(record) = stream_presence.get_mut(&pid) else { continue };
                    if record.remove(&sid) {
                        stream_presence.remove(&pid);
                        if stream_presence.is_empty() {
                            st.streams.remove(&stream);
                        }
                        leave_events
                            .push((stream, PresenceEvent { kind: PRESENCE_LEAVE_TYPE.into(), id: pid, info: None }));
                    }
                }
            }
        }
        if let Some(presenter) = &self.presenter {
            for (stream, ev) in &leave_events {
                presenter.handle_leave(stream, ev);
            }
        }
    }

    fn history_message(&self, name: &str, epoch: &str, entry: &Entry) -> StreamMessage {
        StreamMessage {
            stream: name.to_owned(),
            data: entry.data.clone(),
            offset: entry.offset,
            epoch: epoch.to_owned(),
            meta: None,
        }
    }
}

impl Drop for Memory {
    fn drop(&mut self) {
        self.stop.stop();
    }
}

impl Broker for Memory {
    /// Starts the expiration loop (one pass per second) on its own thread.
    fn start(&self) -> Result<(), BrokerError> {
        let inner = Arc::downgrade(&self.inner);
        let stop = self.stop.clone();
        std::thread::Builder::new()
            .name("cabby-broker-expire".into())
            .spawn(move || {
                while !stop.wait(EXPIRE_INTERVAL) {
                    match inner.upgrade() {
                        Some(inner) => inner.expire(),
                        None => break,
                    }
                }
            })
            .map_err(|e| BrokerError { message: format!("failed to start expiration loop: {e}") })?;
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.stop.stop();
        Ok(())
    }

    fn announce(&self) -> String {
        let c = &self.inner.config;
        format!(
            "Using in-memory broker (epoch: {}, history limit: {}, history ttl: {}s, sessions ttl: {}s, presence ttl: {}s)",
            self.epoch(),
            c.history_limit,
            c.history_ttl,
            c.sessions_ttl,
            c.presence_ttl,
        )
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.handle_broadcast_recorded(msg).map(drop)
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast_command(msg);
        }
        Ok(())
    }

    fn subscribe(&self, stream: &str) -> String {
        self.inner.tracker.add(stream, &mut || {
            if let Some(b) = &self.inner.broadcaster {
                b.subscribe(stream);
            }
        });
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

    fn history_from(&self, name: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        let bepoch = self.epoch();
        if bepoch != epoch {
            return Err(BrokerError { message: format!("unknown epoch: {epoch}, current: {bepoch}") });
        }
        let stream = self.inner.get(name).ok_or_else(|| BrokerError { message: "stream not found".into() })?;
        let mut history = Vec::new();
        stream.filter_by_offset(offset, &mut |e| history.push(self.inner.history_message(name, &bepoch, e)))?;
        Ok(history)
    }

    fn history_since(&self, name: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        let Some(stream) = self.inner.get(name) else {
            return Ok(None);
        };
        let bepoch = self.epoch();
        let mut history = Vec::new();
        stream.filter_by_time(ts, &mut |e| history.push(self.inner.history_message(name, &bepoch, e)));
        Ok(Some(history))
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry()?;
        self.commit_session_data(sid, data);
        Ok(())
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        Ok(lock(&self.inner.sessions).entries.get(sid).map(|e| e.data.clone()))
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        self.touch_session_deadline(sid);
        Ok(())
    }

    fn supports_presence(&self) -> bool {
        true
    }

    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        let deadline = self.inner.now_millis() + self.inner.config.presence_ttl * 1000;
        let is_new = {
            let mut guard = lock(&self.inner.presence);
            let st = &mut *guard;
            // Upstream registers the stream and the session before checking
            // for a presence ID mismatch; keep that order.
            let stream_presence = st.streams.entry(stream.to_owned()).or_default();
            let session = st
                .sessions
                .entry(sid.to_owned())
                .or_insert_with(|| PresenceSession { streams: HashMap::new(), deadline });
            if session.streams.get(stream).is_some_and(|old| old != pid) {
                return Err(BrokerError { message: "presence ID mismatch".into() });
            }
            session.streams.insert(stream.to_owned(), pid.to_owned());

            let mut is_new = false;
            let record = stream_presence.entry(pid.to_owned()).or_insert_with(|| {
                is_new = true;
                PresenceEntry { info: Value::Null, id: pid.to_owned(), sessions: Vec::new() }
            });
            record.add(sid, info.clone());
            is_new
        };

        if !is_new {
            return Ok(None);
        }
        let ev = PresenceEvent { kind: PRESENCE_JOIN_TYPE.into(), id: pid.to_owned(), info: event_info(&info) };
        if let Some(p) = &self.inner.presenter {
            p.handle_join(stream, &ev);
        }
        Ok(Some(ev))
    }

    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        let (pid, empty) = {
            let mut guard = lock(&self.inner.presence);
            let st = &mut *guard;
            if !st.streams.contains_key(stream) {
                return Err(BrokerError { message: "stream not found".into() });
            }

            let mut pid = String::new();
            if let Some(session) = st.sessions.get_mut(sid) {
                match session.streams.remove(stream) {
                    Some(id) => pid = id,
                    None => return Err(BrokerError { message: "presence info not found".into() }),
                }
                if session.streams.is_empty() {
                    st.sessions.remove(sid);
                }
            }

            let Some(stream_presence) = st.streams.get_mut(stream) else {
                unreachable!("checked above, under the same lock");
            };
            let Some(record) = stream_presence.get_mut(&pid) else {
                return Err(BrokerError { message: "presence record not found".into() });
            };
            let empty = record.remove(sid);
            if empty {
                stream_presence.remove(&pid);
            }
            if stream_presence.is_empty() {
                st.streams.remove(stream);
            }
            (pid, empty)
        };

        if !empty {
            return Ok(None);
        }
        let ev = PresenceEvent { kind: PRESENCE_LEAVE_TYPE.into(), id: pid, info: None };
        if let Some(p) = &self.inner.presenter {
            p.handle_leave(stream, &ev);
        }
        Ok(Some(ev))
    }

    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        let return_records = opts.map_or_else(|| PresenceInfoOptions::new().return_records, |o| o.return_records);
        let st = lock(&self.inner.presence);
        let mut info = PresenceInfo { kind: PRESENCE_INFO_TYPE.into(), ..Default::default() };
        if let Some(stream_presence) = st.streams.get(stream) {
            info.total = stream_presence.len();
            if return_records {
                info.records = stream_presence
                    .values()
                    .map(|e| PresenceEvent { kind: String::new(), id: e.id.clone(), info: event_info(&e.info) })
                    .collect();
            }
        }
        Ok(info)
    }

    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        let deadline = self.inner.now_millis() + self.inner.config.presence_ttl * 1000;
        if let Some(session) = lock(&self.inner.presence).sessions.get_mut(sid) {
            session.deadline = deadline;
        }
        Ok(())
    }

    fn peak(&self, name: &str) -> Result<Option<StreamMessage>, BrokerError> {
        let Some(stream) = self.inner.get(name) else {
            return Ok(None);
        };
        match stream.peak() {
            0 => Ok(None),
            offset => Ok(Some(StreamMessage { offset, epoch: self.epoch(), ..Default::default() })),
        }
    }
}

/// NATS connection settings (upstream `nats.NATSConfig`, used by the NATS broker).
pub use crate::nats::NatsConfig;

mod nats;

pub use nats::{JETSTREAM_READY_TIMEOUT, Nats};

mod sqlite;

pub use sqlite::{
    LOCK_TIMEOUT, LoadReport, NoopObserver, OpKind, Previous, QUEUE_CAPACITY, SHUTDOWN_BUDGET, SqliteBroker,
    SqliteObserver, SqliteOptions,
};

/// Broker without history, presence or session cache: every call goes straight
/// to the broadcaster (upstream `broker.NewLegacyBroker(broadcaster)`, used by
/// single-node setups and by node tests).
pub struct Legacy {
    broadcaster: Arc<dyn Broadcaster>,
    tracker: StreamsTracker,
}

impl Legacy {
    pub fn new(broadcaster: Arc<dyn Broadcaster>) -> Self {
        Self { broadcaster, tracker: StreamsTracker::new() }
    }
}

fn unsupported(what: &str) -> BrokerError {
    BrokerError { message: format!("{what} not supported") }
}

impl Broker for Legacy {
    fn start(&self) -> Result<(), BrokerError> {
        Ok(())
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        Ok(())
    }
    fn announce(&self) -> String {
        "Using no-op (legacy) broker".into()
    }
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.broadcaster.broadcast(msg);
        Ok(())
    }
    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.broadcaster.broadcast_command(msg);
        Ok(())
    }
    fn subscribe(&self, stream: &str) -> String {
        self.tracker.add(stream, &mut || self.broadcaster.subscribe(stream));
        stream.to_owned()
    }
    fn unsubscribe(&self, stream: &str) -> String {
        self.tracker.remove(stream, &mut || self.broadcaster.unsubscribe(stream));
        stream.to_owned()
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        Err(unsupported("history"))
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        Err(unsupported("history"))
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        Ok(())
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        Ok(None)
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
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
        Err(unsupported("presence"))
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(unsupported("presence"))
    }
    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        Err(unsupported("presence"))
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        Err(unsupported("history"))
    }
}

/// A clock tests move by hand (shared by the memory and SQLite broker tests).
#[cfg(test)]
pub(crate) mod test_clock {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{Clock, lock};

    /// Where every [`TestClock`] starts, in unix seconds.
    pub(crate) const T0: i64 = 1_700_000_000;

    pub(crate) struct TestClock(Mutex<SystemTime>);

    impl TestClock {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self(Mutex::new(UNIX_EPOCH + Duration::from_secs(T0.unsigned_abs()))))
        }

        pub(crate) fn advance(&self, d: Duration) {
            *lock(&self.0) += d;
        }
    }

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            *lock(&self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::StreamMessageMetadata;

    use super::test_clock::TestClock;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<String>>);

    impl Broadcaster for Recorder {
        fn broadcast(&self, msg: &StreamMessage) {
            lock(&self.0).push(format!("broadcast {} {} {}", msg.stream, msg.offset, msg.epoch));
        }
        fn broadcast_command(&self, msg: &RemoteCommandMessage) {
            lock(&self.0).push(format!("command {}", msg.command));
        }
        fn subscribe(&self, stream: &str) {
            lock(&self.0).push(format!("subscribe {stream}"));
        }
        fn unsubscribe(&self, stream: &str) {
            lock(&self.0).push(format!("unsubscribe {stream}"));
        }
    }

    fn msg(stream: &str, transient: bool) -> StreamMessage {
        StreamMessage {
            stream: stream.into(),
            data: "x".into(),
            meta: transient.then(|| StreamMessageMetadata { transient: true, ..Default::default() }),
            ..Default::default()
        }
    }

    #[test]
    fn memory_handle_broadcast_stores_unless_transient() {
        let rec = Arc::new(Recorder::default());
        let broker = Memory::new(Some(rec.clone()), None, &Config::new(), TestClock::new());
        broker.set_epoch("e1");

        broker.handle_broadcast(&msg("s", false)).unwrap();
        broker.handle_broadcast(&msg("s", true)).unwrap();
        broker.handle_broadcast(&msg("s", false)).unwrap();

        assert_eq!(*lock(&rec.0), ["broadcast s 1 e1", "broadcast s 0 ", "broadcast s 2 e1"]);
        assert_eq!(2, broker.history_since("s", 0).unwrap().unwrap_or_default().len());
    }

    #[test]
    fn memory_handle_broadcast_recorded_reports_offset_and_timestamp() {
        let clock = TestClock::new();
        let rec = Arc::new(Recorder::default());
        let broker = Memory::new(Some(rec.clone()), None, &Config::new(), clock.clone());
        broker.set_epoch("e1");

        assert_eq!(
            Some(Recorded { offset: 1, timestamp: 1_700_000_000 }),
            broker.handle_broadcast_recorded(&msg("s", false)).unwrap()
        );
        clock.advance(Duration::from_secs(2));
        assert_eq!(None, broker.handle_broadcast_recorded(&msg("s", true)).unwrap());
        assert_eq!(
            Some(Recorded { offset: 2, timestamp: 1_700_000_002 }),
            broker.handle_broadcast_recorded(&msg("s", false)).unwrap()
        );

        // Fan-out and storage are exactly handle_broadcast's.
        assert_eq!(*lock(&rec.0), ["broadcast s 1 e1", "broadcast s 0 ", "broadcast s 2 e1"]);
        assert_eq!(2, broker.history_since("s", 0).unwrap().unwrap_or_default().len());
    }

    #[test]
    fn memory_concurrent_recorded_broadcasts_get_distinct_offsets() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let offsets: Vec<u64> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn(|| {
                        (0..50)
                            .map(|_| broker.handle_broadcast_recorded(&msg("s", false)).unwrap().unwrap().offset)
                            .collect::<Vec<u64>>()
                    })
                })
                .collect();
            workers.into_iter().flat_map(|w| w.join().unwrap()).collect()
        });

        let mut unique = offsets.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(offsets.len(), unique.len(), "every broadcast got its own offset");
        assert_eq!((1..=400).collect::<Vec<u64>>(), unique);
    }

    #[test]
    fn memory_restore_stream_installs_offset_and_entries() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let epoch = broker.epoch();
        let entries = vec![
            Entry { offset: 7, timestamp: 1_700_000_000, data: "g".into() },
            Entry { offset: 8, timestamp: 1_700_000_000, data: "h".into() },
        ];
        broker.restore_stream("s", 8, 1_700_003_000, entries).unwrap();

        let stored: Vec<(u64, String)> =
            broker.history_since("s", 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect();
        assert_eq!(vec![(7, "g".to_owned()), (8, "h".to_owned())], stored);
        // A client that saw offset 7 gets 8 (filter_by_offset needs the offset within the stored range).
        let after: Vec<u64> = broker.history_from("s", &epoch, 7).unwrap().into_iter().map(|m| m.offset).collect();
        assert_eq!(vec![8], after);
        assert_eq!(Some(8), broker.peak("s").unwrap().map(|m| m.offset));
        assert_eq!(9, broker.add("s", "i"), "the next broadcast continues after the restored offset");
    }

    #[test]
    fn memory_restored_empty_stream_keeps_its_offset_until_its_deadline() {
        let clock = TestClock::new();
        let broker = Memory::new(None, None, &Config::new(), clock.clone());
        broker.restore_stream("s", 5, 1_700_000_010, Vec::new()).unwrap();

        broker.expire();
        assert_eq!(Some(vec![]), broker.history_since("s", 0).unwrap(), "kept before its deadline");
        assert_eq!(None, broker.peak("s").unwrap(), "no entries");

        clock.advance(Duration::from_secs(11));
        broker.expire();
        assert_eq!(None, broker.history_since("s", 0).unwrap(), "dropped after its deadline");
        assert_eq!(1, broker.add("s", "x"));
    }

    #[test]
    fn memory_restored_empty_stream_continues_after_its_offset() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        broker.restore_stream("s", 5, 1_700_000_010, Vec::new()).unwrap();
        assert_eq!(6, broker.add("s", "x"));
    }

    #[test]
    fn memory_restore_stream_rejects_gaps_and_unanchored_tails() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let e = |offset| Entry { offset, timestamp: 1_700_000_000, data: String::new() };

        assert!(broker.restore_stream("gap", 3, 0, vec![e(1), e(3)]).is_err());
        assert!(broker.restore_stream("unanchored", 4, 0, vec![e(2), e(3)]).is_err());
        assert!(broker.restore_stream("ok", 3, 0, vec![e(2), e(3)]).is_ok());
        assert_eq!(None, broker.history_since("gap", 0).unwrap(), "a refused stream is not created");
    }

    #[test]
    fn memory_restored_session_keeps_its_original_expiry() {
        let clock = TestClock::new();
        let broker = Memory::new(None, None, &Config::new(), clock.clone());
        // Expires 2 s from now, not sessions_ttl (300 s) from now.
        broker.restore_session_with_deadline("sid", b"state".to_vec(), 1_700_000_002_000);
        assert_eq!(Some(b"state".to_vec()), broker.restore_session("sid").unwrap());

        clock.advance(Duration::from_millis(1_999));
        broker.expire();
        assert!(broker.restore_session("sid").unwrap().is_some(), "still cached 1 ms before its expiry");

        clock.advance(Duration::from_millis(1));
        broker.expire();
        assert_eq!(None, broker.restore_session("sid").unwrap(), "expired at its original deadline");
    }

    #[test]
    fn memory_session_commit_and_touch_report_their_expiry() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.sessions_ttl = 10;
        let broker = Memory::new(None, None, &config, clock.clone());

        assert_eq!(None, broker.touch_session_deadline("sid"), "nothing cached yet");
        assert_eq!(1_700_000_010_000, broker.commit_session_data("sid", b"a".to_vec()));
        clock.advance(Duration::from_secs(3));
        assert_eq!(Some(1_700_000_013_000), broker.touch_session_deadline("sid"));
        assert_eq!(Some(b"a".to_vec()), broker.restore_session("sid").unwrap());
    }

    #[test]
    fn memory_commit_and_touch_session_still_expire_after_the_ttl() {
        struct State;
        impl Cacheable for State {
            fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError> {
                Ok(b"state".to_vec())
            }
        }
        let clock = TestClock::new();
        let mut config = Config::new();
        config.sessions_ttl = 1;
        let broker = Memory::new(None, None, &config, clock.clone());

        broker.commit_session("sid", &State).unwrap();
        clock.advance(Duration::from_millis(600));
        broker.touch_session("sid").unwrap();
        clock.advance(Duration::from_millis(600));
        broker.expire();
        assert!(broker.restore_session("sid").unwrap().is_some(), "the touch moved the expiry");
        clock.advance(Duration::from_millis(400));
        broker.expire();
        assert_eq!(None, broker.restore_session("sid").unwrap());
    }

    #[test]
    fn memory_subscribe_is_reference_counted() {
        let rec = Arc::new(Recorder::default());
        let broker = Memory::new(Some(rec.clone()), None, &Config::new(), TestClock::new());

        assert_eq!("s", broker.subscribe("s"));
        broker.subscribe("s");
        broker.unsubscribe("s");
        broker.unsubscribe("s");

        assert_eq!(*lock(&rec.0), ["subscribe s", "unsubscribe s"]);
    }

    #[test]
    fn memory_streams_outlive_history_ten_times_the_ttl() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 1;
        let broker = Memory::new(None, None, &config, clock.clone());

        broker.add("s", "a");
        clock.advance(Duration::from_secs(5));
        broker.expire();
        // Entries are gone; the stream (and its offset) is kept.
        assert_eq!(Some(vec![]), broker.history_since("s", 0).unwrap());
        assert_eq!(2, broker.add("s", "b"));

        clock.advance(Duration::from_secs(11));
        broker.expire();
        assert_eq!(None, broker.history_since("s", 0).unwrap());
        assert_eq!(1, broker.add("s", "c"));
    }

    /// Polls `done` until it holds or `limit` passes.
    fn eventually(limit: Duration, what: &str, done: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + limit;
        while !done() {
            assert!(std::time::Instant::now() < deadline, "{what} within {limit:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The expiration loop `start` runs (upstream `expireLoop`, one pass a
    /// second) expires history, cached sessions and presence on its own, and
    /// stops with `shutdown`. The other tests call `expire` directly.
    #[test]
    fn memory_expiry_loop_runs_until_shutdown() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 1;
        config.sessions_ttl = 1;
        config.presence_ttl = 1;
        let broker = Memory::new(None, None, &config, clock.clone());
        broker.start().unwrap();

        broker.add("s", "a");
        struct State;
        impl Cacheable for State {
            fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError> {
                Ok(b"state".to_vec())
            }
        }
        broker.commit_session("sid", &State).unwrap();
        broker.presence_add("s", "sid", "pid", Value::Null).unwrap();
        assert_eq!(1, broker.history_since("s", 0).unwrap().unwrap_or_default().len());
        assert!(broker.restore_session("sid").unwrap().is_some());

        clock.advance(Duration::from_secs(5));
        let limit = EXPIRE_INTERVAL * 3;
        eventually(limit, "history expires", || broker.history_since("s", 0).unwrap() == Some(vec![]));
        eventually(limit, "the cached session expires", || broker.restore_session("sid").unwrap().is_none());
        eventually(limit, "presence expires", || broker.presence_info("s", None).unwrap().total == 0);
        // The stream itself is dropped ten history TTLs after its last message.
        clock.advance(Duration::from_secs(11));
        eventually(limit, "the stream expires", || broker.history_since("s", 0).unwrap().is_none());

        broker.shutdown().unwrap();
        // Give the loop time to observe the stop, then make everything stale:
        // nothing expires any more.
        std::thread::sleep(Duration::from_millis(100));
        broker.add("t", "b");
        clock.advance(Duration::from_secs(60));
        std::thread::sleep(EXPIRE_INTERVAL * 2 + Duration::from_millis(200));
        assert_eq!(1, broker.history_since("t", 0).unwrap().unwrap_or_default().len());
    }

    #[test]
    fn memory_history_from_reports_offsets_out_of_range() {
        let mut config = Config::new();
        config.history_limit = 2;
        let broker = Memory::new(None, None, &config, TestClock::new());
        let epoch = broker.epoch();
        assert_eq!(4, epoch.len());

        assert_eq!("stream not found", broker.history_from("s", &epoch, 0).unwrap_err().message);
        for d in ["a", "b", "c"] {
            broker.add("s", d);
        }
        assert_eq!(
            "requested offset couldn't be found: 1, lowest: 2",
            broker.history_from("s", &epoch, 1).unwrap_err().message
        );
        assert_eq!(
            "requested offset couldn't be found: 5, latest: 3",
            broker.history_from("s", &epoch, 5).unwrap_err().message
        );
        assert_eq!(Vec::<StreamMessage>::new(), broker.history_from("s", &epoch, 3).unwrap());
    }

    #[test]
    fn memory_presence_remove_errors() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        assert_eq!("stream not found", broker.presence_remove("a", "s1").unwrap_err().message);

        broker.presence_add("a", "s1", "u1", Value::Null).unwrap();
        broker.presence_add("b", "s2", "u2", Value::Null).unwrap();
        assert_eq!("presence info not found", broker.presence_remove("a", "s2").unwrap_err().message);
        assert_eq!("presence record not found", broker.presence_remove("a", "s3").unwrap_err().message);

        let info = broker.presence_info("a", Some(&PresenceInfoOptions { return_records: false })).unwrap();
        assert_eq!((PRESENCE_INFO_TYPE, 1, 0), (info.kind.as_str(), info.total, info.records.len()));
    }

    #[test]
    fn memory_presence_expiry_reports_leaves() {
        struct Leaves(Mutex<Vec<(String, String)>>);
        impl Presenter for Leaves {
            fn handle_join(&self, _: &str, _: &PresenceEvent) {}
            fn handle_leave(&self, stream: &str, msg: &PresenceEvent) {
                lock(&self.0).push((stream.into(), msg.id.clone()));
            }
        }

        let clock = TestClock::new();
        let leaves = Arc::new(Leaves(Mutex::new(Vec::new())));
        let mut config = Config::new();
        config.presence_ttl = 1;
        let broker = Memory::new(None, Some(leaves.clone()), &config, clock.clone());

        broker.presence_add("a", "s1", "u1", Value::Null).unwrap();
        clock.advance(Duration::from_millis(1001));
        broker.expire();

        assert_eq!(*lock(&leaves.0), [("a".to_owned(), "u1".to_owned())]);
        assert_eq!(0, broker.presence_info("a", None).unwrap().total);
    }

    #[test]
    fn legacy_broker_has_no_history_or_presence() {
        let rec = Arc::new(Recorder::default());
        let broker = Legacy::new(rec.clone());

        broker.subscribe("s");
        broker.subscribe("s");
        broker.handle_broadcast(&msg("s", false)).unwrap();
        broker.unsubscribe("s");
        broker.unsubscribe("s");

        assert_eq!(*lock(&rec.0), ["subscribe s", "broadcast s 0 ", "unsubscribe s"]);
        assert_eq!("history not supported", broker.history_from("s", "", 0).unwrap_err().message);
        assert_eq!("presence not supported", broker.presence_info("s", None).unwrap_err().message);
        assert!(!broker.supports_presence());
        assert_eq!(None, broker.restore_session("x").unwrap());
    }

    #[test]
    fn memory_shutdown_stops_the_expiration_thread() {
        let broker = Memory::new(None, None, &Config::new(), Arc::new(SystemClock));
        broker.start().unwrap();
        broker.shutdown().unwrap();
        assert!(broker.stop.wait(Duration::ZERO));
    }

    /// A [`TestClock`] that stops the first `now()` call made on the thread
    /// named `paused` until [`PausingClock::resume`]: a writer reads the clock
    /// after it has looked up its stream and before it locks it. Every wait
    /// has a deadline.
    struct PausingClock {
        clock: Arc<TestClock>,
        paused: &'static str,
        armed: Mutex<bool>,
        reached: Channel,
        resumed: Channel,
    }

    struct Channel {
        tx: Mutex<std::sync::mpsc::Sender<()>>,
        rx: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl Channel {
        fn new() -> Self {
            let (tx, rx) = std::sync::mpsc::channel();
            Self { tx: Mutex::new(tx), rx: Mutex::new(rx) }
        }

        fn send(&self) {
            lock(&self.tx).send(()).unwrap();
        }

        fn recv(&self, what: &str) {
            lock(&self.rx).recv_timeout(Duration::from_secs(10)).unwrap_or_else(|e| panic!("{what}: {e}"));
        }
    }

    impl PausingClock {
        fn new(paused: &'static str) -> Arc<Self> {
            Arc::new(Self {
                clock: TestClock::new(),
                paused,
                armed: Mutex::new(true),
                reached: Channel::new(),
                resumed: Channel::new(),
            })
        }

        /// Returns once the paused thread is waiting in `now()`.
        fn wait_paused(&self) {
            self.reached.recv("the writer reads the clock");
        }

        fn resume(&self) {
            self.resumed.send();
        }
    }

    impl Clock for PausingClock {
        fn now(&self) -> SystemTime {
            let pause = std::thread::current().name() == Some(self.paused) && std::mem::take(&mut *lock(&self.armed));
            if pause {
                self.reached.send();
                self.resumed.recv("the test resumes the writer");
            }
            self.clock.now()
        }
    }

    /// Runs `write` on a thread named `writer` that stops in its first clock
    /// read, runs one expiry pass meanwhile, then lets the writer finish.
    fn write_across_an_expiry_pass<R: Send + 'static>(
        broker: &Arc<Memory>,
        clock: &PausingClock,
        write: impl FnOnce(&Memory) -> R + Send + 'static,
    ) -> R {
        let b = broker.clone();
        let writer = std::thread::Builder::new().name("writer".into()).spawn(move || write(&b)).unwrap();
        clock.wait_paused();
        broker.expire();
        clock.resume();
        writer.join().unwrap()
    }

    /// A new stream has no deadline yet, so an expiry pass between its
    /// creation and the first append removes it; the append still lands in
    /// the stream the broker holds (D-087).
    #[test]
    fn deviation_d087_an_append_to_a_new_stream_is_not_lost_to_an_expiry_pass() {
        let clock = PausingClock::new("writer");
        let broker = Arc::new(Memory::new(None, None, &Config::new(), clock.clone()));

        let recorded = write_across_an_expiry_pass(&broker, &clock, |b| {
            b.handle_broadcast_recorded(&msg("s", false)).unwrap().unwrap().offset
        });

        assert_eq!(1, recorded);
        let stored: Vec<(u64, String)> =
            broker.history_since("s", 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect();
        assert_eq!(vec![(1, "x".to_owned())], stored, "the message is in history");
        assert_eq!(Some(1), broker.peak("s").unwrap().map(|m| m.offset));
        assert_eq!(2, broker.add("s", "y"), "the next offset follows it");
    }

    /// A stream past its deadline is removed by the pass even when a writer
    /// already looked it up: the writer's message starts a new stream, as a
    /// message after the pass does (D-087).
    #[test]
    fn deviation_d087_an_append_to_an_expired_stream_is_not_lost_to_an_expiry_pass() {
        let clock = PausingClock::new("writer");
        let mut config = Config::new();
        config.history_ttl = 1;
        let broker = Arc::new(Memory::new(None, None, &config, clock.clone()));
        broker.add("s", "a");
        broker.add("s", "b");
        // Ten TTLs after the last message, the stream may be dropped.
        clock.clock.advance(Duration::from_secs(11));

        let offset = write_across_an_expiry_pass(&broker, &clock, |b| b.add("s", "c"));

        assert_eq!(1, offset, "the pass dropped the old stream and its offsets");
        let stored: Vec<(u64, String)> =
            broker.history_since("s", 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect();
        assert_eq!(vec![(1, "c".to_owned())], stored, "the message is in history");
        assert_eq!(2, broker.add("s", "d"), "the next offset follows it");
    }

    /// The NATS broker copies messages into its local broker with `store`;
    /// the same pass must not lose them (D-087).
    #[test]
    fn deviation_d087_a_stored_message_is_not_lost_to_an_expiry_pass() {
        let clock = PausingClock::new("writer");
        let broker = Arc::new(Memory::new(None, None, &Config::new(), clock.clone()));

        let stored = write_across_an_expiry_pass(&broker, &clock, |b| b.store("s", b"m", 5, None));

        assert_eq!(5, stored.unwrap());
        assert_eq!(Some(5), broker.peak("s").unwrap().map(|m| m.offset), "the message is in history");
        assert!(broker.store("s", b"m", 5, None).is_err(), "the offset stays taken");
    }

    /// Concurrent appends to new streams while expiry passes run back to
    /// back: every append is kept and each stream's offsets are 1..=n. Which
    /// appends meet a pass depends on scheduling (without the removed mark
    /// most runs lose some); the tests above pin the interleaving itself.
    /// The sweeper runs its first pass before the writers start (a barrier),
    /// so the passes counted are never zero.
    #[test]
    fn deviation_d087_concurrent_appends_and_expiry_passes_keep_offsets_contiguous() {
        const WRITERS: usize = 4;
        const STREAMS: usize = 1000;
        let mut config = Config::new();
        config.history_limit = WRITERS;
        let broker = Memory::new(None, None, &config, TestClock::new());
        let done = std::sync::atomic::AtomicBool::new(false);
        let start = std::sync::Barrier::new(WRITERS + 1);

        let offsets: Vec<Vec<u64>> = std::thread::scope(|s| {
            let sweeper = s.spawn(|| {
                broker.expire();
                let mut passes = 1u64;
                start.wait();
                while !done.load(std::sync::atomic::Ordering::Relaxed) {
                    broker.expire();
                    passes += 1;
                }
                passes
            });
            let writers: Vec<_> = (0..WRITERS)
                .map(|w| {
                    let (broker, start) = (&broker, &start);
                    s.spawn(move || {
                        start.wait();
                        (0..STREAMS).map(|i| broker.add(&format!("s{i}"), &format!("w{w}"))).collect::<Vec<u64>>()
                    })
                })
                .collect();
            let offsets = writers.into_iter().map(|w| w.join().unwrap()).collect();
            done.store(true, std::sync::atomic::Ordering::Relaxed);
            assert!(sweeper.join().unwrap() > 0);
            offsets
        });

        for i in 0..STREAMS {
            let mut got: Vec<u64> = offsets.iter().map(|w| w[i]).collect();
            got.sort_unstable();
            assert_eq!((1..=WRITERS as u64).collect::<Vec<u64>>(), got, "offsets of s{i}");
            let mut stored: Vec<(u64, String)> = broker
                .history_since(&format!("s{i}"), 0)
                .unwrap()
                .unwrap_or_default()
                .into_iter()
                .map(|m| (m.offset, m.data))
                .collect();
            stored.sort_unstable();
            assert_eq!(WRITERS, stored.len(), "history of s{i}: {stored:?}");
            assert_eq!(got, stored.iter().map(|(o, _)| *o).collect::<Vec<u64>>(), "history of s{i}");
        }
    }

    /// A restore that finds its stream removed by the expiry pass installs
    /// nothing and leaves the entries with the caller, so that
    /// `restore_stream` can install them in the stream the map holds (D-087).
    #[test]
    fn deviation_d087_a_restore_into_a_removed_stream_returns_none_and_keeps_the_entries() {
        let stream = Memstream::new(1, 10, TestClock::new());
        // A new stream has no deadline yet, so a pass removes it.
        assert!(!stream.sweep(unix_secs(stream.clock.now())));
        let entry = |offset| Entry { timestamp: 1, offset, data: format!("m{offset}") };
        let mut entries = vec![entry(2), entry(3)];

        assert_eq!(None, stream.restore(3, i64::MAX, &mut entries));

        assert_eq!(vec![entry(2), entry(3)], entries, "the entries stay with the caller");
        let st = read(&stream.state);
        assert_eq!((0, 0, 0), (st.offset, st.deadline, st.data.len()), "nothing is installed");
    }
}
