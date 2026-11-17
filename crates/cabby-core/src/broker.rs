//! Streams history, presence and session cache (upstream package `broker`).
//!
//! Time is an explicit dependency ([`Clock`]): the memory broker reads
//! timestamps, deadlines and TTL expiry from it, so tests drive expiry by
//! advancing a controlled clock instead of sleeping (SPEC.md §6).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::Value;

use crate::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use crate::pending;

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
        pending!("broker::system_clock_now")
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

crate::toml_section!(Config, "broker.Config" {
    "adapter" => adapter,
    "history_ttl" => history_ttl,
    "history_limit" => history_limit,
    "sessions_ttl" => sessions_ttl,
    "presence_ttl" => presence_ttl,
});

impl Config {
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
            out.push_str(&format!("adapter = \"{}\"\n", self.adapter));
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
        pending!("broker::new_presence_info_options")
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
#[derive(Debug, Default)]
pub struct StreamsTracker {
    _private: (),
}

impl StreamsTracker {
    pub fn new() -> Self {
        pending!("broker::new_streams_tracker")
    }

    /// Adds a reference; calls `on_new` and returns true for the first one.
    pub fn add(&self, _name: &str, _on_new: &mut dyn FnMut()) -> bool {
        pending!("broker::streams_tracker_add")
    }

    pub fn has(&self, _name: &str) -> bool {
        pending!("broker::streams_tracker_has")
    }

    /// Drops a reference; calls `on_last` and returns true for the last one.
    pub fn remove(&self, _name: &str, _on_last: &mut dyn FnMut()) -> bool {
        pending!("broker::streams_tracker_remove")
    }
}

/// A stored history entry (upstream unexported `entry`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Unix seconds.
    pub timestamp: i64,
    pub offset: u64,
    pub data: String,
}

/// One stream's in-memory history (upstream unexported `memstream`).
pub struct Memstream {
    _private: (),
}

impl Memstream {
    pub fn new(_ttl: i64, _limit: usize, _clock: Arc<dyn Clock>) -> Self {
        pending!("broker::new_memstream")
    }

    pub fn add(&self, _data: &str) -> u64 {
        pending!("broker::memstream_add")
    }

    pub fn expire(&self) {
        pending!("broker::memstream_expire")
    }

    /// Calls `callback` for every entry after `offset`.
    pub fn filter_by_offset(&self, _offset: u64, _callback: &mut dyn FnMut(&Entry)) -> Result<(), BrokerError> {
        pending!("broker::memstream_filter_by_offset")
    }
}

/// Single-node broker keeping everything in memory (upstream `broker.Memory`).
///
/// `add` and `expire` are unexported upstream; they are public here because
/// the translated tests drive them directly.
pub struct Memory {
    _private: (),
}

impl Memory {
    /// Upstream `NewMemoryBroker(broadcaster, presenter, config)`, plus the clock.
    pub fn new(
        _broadcaster: Option<Arc<dyn Broadcaster>>,
        _presenter: Option<Arc<dyn Presenter>>,
        _config: &Config,
        _clock: Arc<dyn Clock>,
    ) -> Self {
        pending!("broker::new_memory_broker")
    }

    pub fn epoch(&self) -> String {
        pending!("broker::memory_get_epoch")
    }

    pub fn set_epoch(&self, _epoch: &str) {
        pending!("broker::memory_set_epoch")
    }

    /// Appends a message and returns its offset.
    pub fn add(&self, _stream: &str, _data: &str) -> u64 {
        pending!("broker::memory_add")
    }

    /// Stores a message at an explicit offset; `ts: None` means "now".
    pub fn store(
        &self,
        _stream: &str,
        _data: &[u8],
        _offset: u64,
        _ts: Option<SystemTime>,
    ) -> Result<u64, BrokerError> {
        pending!("broker::memory_store")
    }

    /// One expiration pass over history, sessions and presence.
    pub fn expire(&self) {
        pending!("broker::memory_expire")
    }
}

impl Broker for Memory {
    fn start(&self) -> Result<(), BrokerError> {
        pending!("broker::memory_start")
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        pending!("broker::memory_shutdown")
    }
    fn announce(&self) -> String {
        pending!("broker::memory_announce")
    }
    fn handle_broadcast(&self, _msg: &StreamMessage) -> Result<(), BrokerError> {
        pending!("broker::memory_handle_broadcast")
    }
    fn handle_command(&self, _msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        pending!("broker::memory_handle_command")
    }
    fn subscribe(&self, _stream: &str) -> String {
        pending!("broker::memory_subscribe")
    }
    fn unsubscribe(&self, _stream: &str) -> String {
        pending!("broker::memory_unsubscribe")
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        pending!("broker::memory_history_from")
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        pending!("broker::memory_history_since")
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        pending!("broker::memory_commit_session")
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        pending!("broker::memory_restore_session")
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::memory_touch_session")
    }
    fn supports_presence(&self) -> bool {
        pending!("broker::memory_supports_presence")
    }
    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::memory_presence_add")
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::memory_presence_remove")
    }
    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        pending!("broker::memory_presence_info")
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::memory_touch_presence")
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        pending!("broker::memory_peak")
    }
}

/// NATS connection settings (upstream `nats.NATSConfig`, used by the NATS broker).
pub use crate::nats::NatsConfig;

/// Broker backed by NATS JetStream (upstream `broker.NATS`). History and
/// session TTLs are enforced by the NATS server, on its own clock.
pub struct Nats {
    _private: (),
}

impl Nats {
    /// Upstream `NewNATSBroker(broadcaster, config, natsConfig)`.
    pub fn new(_broadcaster: Option<Arc<dyn Broadcaster>>, _config: &Config, _nats: &NatsConfig) -> Self {
        pending!("broker::new_nats_broker")
    }

    /// Waits until JetStream is initialized.
    pub fn ready(&self, _timeout: Duration) -> Result<(), BrokerError> {
        pending!("broker::nats_ready")
    }

    /// Deletes all streams and buckets the broker owns.
    pub fn reset(&self) -> Result<(), BrokerError> {
        pending!("broker::nats_reset")
    }

    pub fn epoch(&self) -> String {
        pending!("broker::nats_epoch")
    }

    pub fn set_epoch(&self, _epoch: &str) -> Result<(), BrokerError> {
        pending!("broker::nats_set_epoch")
    }

    /// Epoch of the node-local broker (upstream reads `n.local.GetEpoch()`).
    pub fn local_epoch(&self) -> String {
        pending!("broker::nats_local_epoch")
    }
}

impl Broker for Nats {
    fn start(&self) -> Result<(), BrokerError> {
        pending!("broker::nats_start")
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        pending!("broker::nats_shutdown")
    }
    fn announce(&self) -> String {
        pending!("broker::nats_announce")
    }
    fn handle_broadcast(&self, _msg: &StreamMessage) -> Result<(), BrokerError> {
        pending!("broker::nats_handle_broadcast")
    }
    fn handle_command(&self, _msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        pending!("broker::nats_handle_command")
    }
    fn subscribe(&self, _stream: &str) -> String {
        pending!("broker::nats_subscribe")
    }
    fn unsubscribe(&self, _stream: &str) -> String {
        pending!("broker::nats_unsubscribe")
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        pending!("broker::nats_history_from")
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        pending!("broker::nats_history_since")
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        pending!("broker::nats_commit_session")
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        pending!("broker::nats_restore_session")
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::nats_touch_session")
    }
    fn supports_presence(&self) -> bool {
        pending!("broker::nats_supports_presence")
    }
    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::nats_presence_add")
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::nats_presence_remove")
    }
    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        pending!("broker::nats_presence_info")
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::nats_touch_presence")
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        pending!("broker::nats_peak")
    }
}

/// Broker without history, presence or session cache: every call goes straight
/// to the broadcaster (upstream `broker.NewLegacyBroker(broadcaster)`, used by
/// single-node setups and by node tests).
pub struct Legacy {
    _broadcaster: Arc<dyn Broadcaster>,
}

impl Legacy {
    pub fn new(broadcaster: Arc<dyn Broadcaster>) -> Self {
        Self { _broadcaster: broadcaster }
    }
}

impl Broker for Legacy {
    fn start(&self) -> Result<(), BrokerError> {
        pending!("broker::legacy_start")
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        pending!("broker::legacy_shutdown")
    }
    fn announce(&self) -> String {
        pending!("broker::legacy_announce")
    }
    fn handle_broadcast(&self, _msg: &StreamMessage) -> Result<(), BrokerError> {
        pending!("broker::legacy_handle_broadcast")
    }
    fn handle_command(&self, _msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        pending!("broker::legacy_handle_command")
    }
    fn subscribe(&self, _stream: &str) -> String {
        pending!("broker::legacy_subscribe")
    }
    fn unsubscribe(&self, _stream: &str) -> String {
        pending!("broker::legacy_unsubscribe")
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        pending!("broker::legacy_history_from")
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        pending!("broker::legacy_history_since")
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        pending!("broker::legacy_commit_session")
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        pending!("broker::legacy_restore_session")
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::legacy_touch_session")
    }
    fn supports_presence(&self) -> bool {
        pending!("broker::legacy_supports_presence")
    }
    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::legacy_presence_add")
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        pending!("broker::legacy_presence_remove")
    }
    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        pending!("broker::legacy_presence_info")
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        pending!("broker::legacy_touch_presence")
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        pending!("broker::legacy_peak")
    }
}
