//! Redis pub/sub subscriptions that stay alive (deviation D-003, AnyCable issue 275).
//!
//! One engine owns one dedicated RESP3 subscriber connection and keeps the set of
//! channels Redis holds for it equal to the *desired* set (the channels with
//! local subscribers):
//!
//! - Every subscription change Redis reports (`subscribe`/`unsubscribe` pushes,
//!   solicited or not) is applied to the *confirmed* set; a channel whose state
//!   differs from the desired one is fixed right away. An unsolicited
//!   `unsubscribe` for a channel with local subscribers is therefore followed by
//!   a re-`SUBSCRIBE` (AnyCable issue 275), and a channel without local subscribers is always
//!   released (D-004, AnyCable issue 263).
//! - Each push carries Redis' count of the connection's subscriptions; a count
//!   that differs from the confirmed set means Redis changed something silently,
//!   and every desired channel is subscribed again (`SUBSCRIBE` is idempotent).
//! - Every [`RECONCILE_INTERVAL`] the engine re-subscribes the primary channel
//!   to obtain a fresh count and fixes any difference between the sets.
//! - Every [`HEARTBEAT_INTERVAL`] it sends `PING`; a reply that does not arrive
//!   within [`RESPONSE_TIMEOUT`] (or any lost connection) makes it reconnect and
//!   subscribe to every desired channel again (half-open connections).
//!
//! Commands on the connection are sent one at a time, and replies are never
//! used for state (pushes are): an unsolicited push can be taken by the client
//! library as the reply of the command in flight, which is harmless here.
//!
//! While a connection is up, a channel that becomes desired is reported to the
//! owner's [`Activations`] until Redis confirms it (or the `SUBSCRIBE` fails,
//! the channel is no longer desired, or the connection is lost), so the node
//! can wait for it before confirming a subscription (D-105).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cabby_core::log_at;
use cabby_core::logger::{Attr, Level, Logger};
use cabby_core::pubsub::Activations;
use redis::aio::MultiplexedConnection;
use redis::{PushInfo, PushKind, Value};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::connect::{self, ConnectSpec, Signal};
use super::streamer::go_duration;

/// How often the subscriber connection is checked with `PING`.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long any command on the subscriber connection may take.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the subscription set is reconciled with Redis.
pub(crate) const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
/// Delay before re-subscribing a channel Redis dropped on its own: avoids a
/// tight loop if Redis keeps dropping it, and well within D-003's 15 s bound.
pub(crate) const RESUBSCRIBE_DELAY: Duration = Duration::from_secs(1);
/// Upstream `batchSubscribeSize`.
const BATCH_SIZE: usize = 256;

/// Receives what the subscriber connection delivers.
pub(crate) trait Sink: Send + Sync + 'static {
    fn message(&self, channel: &str, payload: &[u8]);
}

/// Static settings of an engine.
pub(crate) struct Settings {
    /// The channel whose confirmation means "connected" (resets reconnect attempts).
    pub primary: String,
    pub max_reconnect_attempts: i64,
    /// Logged when the primary channel is (re)subscribed.
    pub connected_msg: &'static str,
    pub reconnected_msg: &'static str,
    /// Reported to `on_fatal` when reconnect attempts are exhausted.
    pub exhausted_error: &'static str,
    /// Logged (with the error) when a `SUBSCRIBE` fails on a live connection.
    pub subscribe_failed: &'static str,
    /// Logged (with the error) when the connection is lost.
    pub lost: &'static [(Level, &'static str)],
    /// The owner's logger (upstream `l.With("context", ...)...`).
    pub log: Logger,
    /// Where channels not confirmed yet are reported (D-105); `None`: not tracked.
    pub activations: Option<Arc<Activations>>,
}

/// A handle to a subscription engine.
pub(crate) struct Subscriptions {
    settings: Settings,
    desired: Mutex<HashSet<String>>,
    /// A connection is up and subscribes new desired channels; written with
    /// the `desired` lock held.
    live: AtomicBool,
    tx: UnboundedSender<String>,
    rx: Mutex<Option<UnboundedReceiver<String>>>,
    stop: CancellationToken,
    finished: Signal,
}

type Fatal = Box<dyn Fn(String) + Send + Sync>;

impl Subscriptions {
    pub(crate) fn new(settings: Settings) -> Arc<Self> {
        let (tx, rx) = unbounded_channel();
        Arc::new(Self {
            settings,
            desired: Mutex::new(HashSet::new()),
            live: AtomicBool::new(false),
            tx,
            rx: Mutex::new(Some(rx)),
            stop: CancellationToken::new(),
            finished: Signal::default(),
        })
    }

    fn desired(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.desired.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn is_desired(&self, channel: &str) -> bool {
        self.desired().contains(channel)
    }

    /// Wants `channel` subscribed (idempotent); false if it already was.
    pub(crate) fn subscribe(&self, channel: &str) -> bool {
        let mut desired = self.desired();
        let added = desired.insert(channel.to_owned());
        if added {
            // Without a connection nothing is coming soon: the channel is
            // subscribed with every other one once connected.
            if self.live.load(Ordering::Acquire) {
                self.requested(channel);
            }
            let _ = self.tx.send(channel.to_owned());
        }
        added
    }

    /// Wants `channel` released (idempotent); false if it was not wanted.
    pub(crate) fn unsubscribe(&self, channel: &str) -> bool {
        let removed = self.desired().remove(channel);
        if removed {
            self.settled(channel);
            let _ = self.tx.send(channel.to_owned());
        }
        removed
    }

    fn requested(&self, channel: &str) {
        if let Some(a) = &self.settings.activations {
            a.requested(channel);
        }
    }

    fn settled(&self, channel: &str) {
        if let Some(a) = &self.settings.activations {
            a.settled(channel);
        }
    }

    /// A connection is up: every desired channel, to subscribe now, is
    /// pending until Redis confirms it.
    fn go_live(&self) -> Vec<String> {
        let desired = self.desired();
        self.live.store(true, Ordering::Release);
        if let Some(a) = &self.settings.activations {
            desired.iter().for_each(|c| a.requested(c));
        }
        desired.iter().cloned().collect()
    }

    /// The connection is gone: nothing pending will be confirmed on it.
    fn go_offline(&self) {
        let _desired = self.desired();
        self.live.store(false, Ordering::Release);
        if let Some(a) = &self.settings.activations {
            a.settle_all();
        }
    }

    /// Starts the engine (once); `on_fatal` gets the error that stops it.
    pub(crate) fn start(self: &Arc<Self>, spec: ConnectSpec, sink: Arc<dyn Sink>, on_fatal: Fatal) {
        let Some(rx) = self.rx.lock().unwrap_or_else(|e| e.into_inner()).take() else { return };
        let engine = Engine { shared: Arc::clone(self), spec, sink, on_fatal, rx, attempt: 0 };
        connect::runtime().spawn(engine.run());
    }

    /// Stops the engine and waits (up to `timeout`) until its connection is closed.
    pub(crate) fn shutdown(&self, timeout: Duration) {
        self.stop.cancel();
        if self.rx.lock().unwrap_or_else(|e| e.into_inner()).is_none() {
            self.finished.wait(timeout);
        }
    }

    fn log(&self, level: Level, msg: &str, channel: Option<&str>) {
        if !cabby_core::logger::enabled(level) {
            return;
        }
        match channel {
            Some(c) => self.settings.log.log(level, msg, &[Attr::new("channel", c.into())]),
            None => self.settings.log.log(level, msg, &[]),
        }
    }

    /// A record with an `error` attribute (upstream `log.Error(msg, "error", err)`).
    fn log_error(&self, level: Level, msg: &str, err: impl std::fmt::Display) {
        log_at!(self.settings.log, level, msg, "error" => err.to_string());
    }
}

impl Drop for Subscriptions {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

struct Engine {
    shared: Arc<Subscriptions>,
    spec: ConnectSpec,
    sink: Arc<dyn Sink>,
    on_fatal: Fatal,
    rx: UnboundedReceiver<String>,
    attempt: i64,
}

/// Why a connection was given up.
enum End {
    Stop,
    Lost(String),
}

impl Engine {
    async fn run(mut self) {
        loop {
            if self.shared.stop.is_cancelled() {
                break;
            }
            let (push_tx, push_rx) = unbounded_channel();
            let conn = tokio::select! {
                () = self.shared.stop.cancelled() => break,
                conn = self.spec.subscriber(push_tx, RESPONSE_TIMEOUT) => conn,
            };
            match conn {
                Err(e) => {
                    self.shared.log_error(Level::Error, "failed to connect to Redis", e);
                }
                Ok(conn) => {
                    self.shared.log(Level::Debug, "initialized pub/sub client", None);
                    let mut session = Session::new(conn, push_rx);
                    let end = session.run(&mut self).await;
                    self.shared.go_offline();
                    match end {
                        End::Stop => {
                            self.shared.log(Level::Debug, "close pub/sub channel", None);
                            break;
                        }
                        End::Lost(reason) => {
                            for (level, msg) in self.shared.settings.lost {
                                self.shared.log_error(*level, msg, &reason);
                            }
                        }
                    }
                }
            }
            if !self.backoff().await {
                break;
            }
        }
        self.shared.finished.set();
    }

    /// Upstream `maybeReconnect`: false when the engine must stop.
    async fn backoff(&mut self) -> bool {
        if self.attempt >= self.shared.settings.max_reconnect_attempts {
            (self.on_fatal)(self.shared.settings.exhausted_error.to_owned());
            return false;
        }
        self.attempt += 1;
        let delay = connect::next_retry((self.attempt - 1) as u32);
        self.shared.log(Level::Info, &format!("next Redis reconnect attempt in {}", go_duration(delay)), None);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = self.shared.stop.cancelled() => return false,
        }
        self.shared.log(Level::Info, "reconnecting to Redis...", None);
        true
    }
}

/// One connection's state.
struct Session {
    conn: MultiplexedConnection,
    pushes: UnboundedReceiver<PushInfo>,
    /// Channels Redis reported as subscribed on this connection.
    confirmed: HashSet<String>,
    /// Unsubscribe confirmations still expected for our own `UNSUBSCRIBE`s.
    expected_unsub: HashMap<String, u32>,
    /// Redis' subscription count disagreed with `confirmed`.
    mismatch: bool,
    /// Channels Redis dropped on its own, re-subscribed at `dropped_at`.
    dropped: HashSet<String>,
    dropped_at: Option<tokio::time::Instant>,
    /// The primary channel's confirmation was logged on this connection.
    announced: bool,
}

fn interval(period: Duration) -> Interval {
    let mut i = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    i.set_missed_tick_behavior(MissedTickBehavior::Delay);
    i
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::BulkString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        Value::SimpleString(s) => Some(s.clone()),
        _ => None,
    }
}

/// Server errors (e.g. `NOPERM`) leave the connection usable; anything else
/// (I/O, timeout, closed connection) means it is gone.
fn is_fatal(e: &redis::RedisError) -> bool {
    e.code().is_none() || e.is_io_error() || e.is_timeout() || e.is_connection_dropped()
}

impl Session {
    fn new(conn: MultiplexedConnection, pushes: UnboundedReceiver<PushInfo>) -> Self {
        Self {
            conn,
            pushes,
            confirmed: HashSet::new(),
            expected_unsub: HashMap::new(),
            mismatch: false,
            dropped: HashSet::new(),
            dropped_at: None,
            announced: false,
        }
    }

    async fn run(&mut self, engine: &mut Engine) -> End {
        let shared = Arc::clone(&engine.shared);
        // Upstream `resubscribe`: everything desired, in batches.
        let all = shared.go_live();
        if let Err(end) = self.subscribe(engine, all).await {
            return end;
        }

        let mut heartbeat = interval(HEARTBEAT_INTERVAL);
        let mut reconcile = interval(RECONCILE_INTERVAL);

        loop {
            let dropped_at = self.dropped_at;
            let resubscribe_dropped = async move {
                match dropped_at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            let result = tokio::select! {
                biased;
                () = shared.stop.cancelled() => return End::Stop,
                () = resubscribe_dropped => {
                    self.dropped_at = None;
                    let channels = std::mem::take(&mut self.dropped);
                    self.sync(engine, channels).await
                },
                push = self.pushes.recv() => match push {
                    None => Err(End::Lost("connection closed".to_owned())),
                    Some(push) => {
                        let mut touched = HashSet::new();
                        match self.on_push(engine, push, &mut touched) {
                            Err(end) => Err(end),
                            Ok(()) => self.sync(engine, touched).await,
                        }
                    }
                },
                channel = engine.rx.recv() => {
                    let mut touched: HashSet<String> = channel.into_iter().collect();
                    while let Ok(c) = engine.rx.try_recv() {
                        touched.insert(c);
                    }
                    self.sync(engine, touched).await
                },
                _ = heartbeat.tick() => self.ping(engine).await,
                _ = reconcile.tick() => self.reconcile(engine).await,
            };
            if let Err(end) = result {
                return end;
            }
        }
    }

    /// Applies one push; channels whose state may now differ from the desired
    /// state are added to `touched`.
    fn on_push(&mut self, engine: &mut Engine, push: PushInfo, touched: &mut HashSet<String>) -> Result<(), End> {
        let shared = &engine.shared;
        match push.kind {
            PushKind::Message => {
                if let (Some(channel), Some(Value::BulkString(payload))) =
                    (push.data.first().and_then(text), push.data.get(1))
                {
                    engine.sink.message(&channel, payload);
                }
            }
            PushKind::Subscribe => {
                let Some(channel) = push.data.first().and_then(text) else { return Ok(()) };
                // Upstream subscribes once per connection and logs each
                // confirmation of the primary channel; the periodic
                // reconciliation re-subscribes it, so only the first one counts.
                if channel == shared.settings.primary && !self.announced {
                    self.announced = true;
                    let msg = if engine.attempt > 0 {
                        shared.settings.reconnected_msg
                    } else {
                        shared.settings.connected_msg
                    };
                    shared.log(Level::Info, msg, Some(&channel));
                    engine.attempt = 0;
                }
                // Confirmations of the reconciliation's re-subscriptions (D-003)
                // are not logged: upstream subscribes a channel once.
                if self.confirmed.insert(channel.clone()) {
                    shared.log(Level::Debug, "subscribe", Some(&channel));
                }
                // Active, unless an UNSUBSCRIBE of ours is still to be applied.
                if !self.expected_unsub.contains_key(&channel) {
                    shared.settled(&channel);
                }
                self.check_count(&push.data);
                touched.insert(channel);
            }
            PushKind::Unsubscribe => {
                let Some(channel) = push.data.first().and_then(text) else { return Ok(()) };
                shared.log(Level::Debug, "unsubscribe", Some(&channel));
                let solicited = match self.expected_unsub.get_mut(&channel) {
                    Some(n) if *n > 0 => {
                        *n -= 1;
                        if *n == 0 {
                            self.expected_unsub.remove(&channel);
                        }
                        true
                    }
                    _ => false,
                };
                self.confirmed.remove(&channel);
                self.check_count(&push.data);
                if !solicited && shared.is_desired(&channel) {
                    shared.log(Level::Warn, "Redis dropped the subscription, resubscribing", Some(&channel));
                    self.dropped.insert(channel);
                    self.dropped_at.get_or_insert_with(|| tokio::time::Instant::now() + RESUBSCRIBE_DELAY);
                } else {
                    touched.insert(channel);
                }
            }
            PushKind::Disconnection => return Err(End::Lost("connection closed".to_owned())),
            _ => {}
        }
        Ok(())
    }

    /// Compares Redis' subscription count (the last push element) with ours.
    fn check_count(&mut self, data: &[Value]) {
        if let Some(Value::Int(count)) = data.last()
            && *count as usize != self.confirmed.len()
        {
            self.mismatch = true;
        }
    }

    /// Applies the pushes already received (without waiting for more).
    fn drain(&mut self, engine: &mut Engine, touched: &mut HashSet<String>) -> Result<(), End> {
        while let Ok(push) = self.pushes.try_recv() {
            self.on_push(engine, push, touched)?;
        }
        Ok(())
    }

    /// Brings `channels` to their desired state, and keeps going while the
    /// confirmations change the state of further channels.
    async fn sync(&mut self, engine: &mut Engine, mut channels: HashSet<String>) -> Result<(), End> {
        let mut rounds = 0;
        loop {
            self.drain(engine, &mut channels)?;
            if self.mismatch {
                self.resync(engine).await?;
                return Ok(());
            }
            let (to_sub, to_unsub): (Vec<String>, Vec<String>) = {
                let desired = engine.shared.desired();
                let to_sub = channels
                    .iter()
                    .filter(|c| desired.contains(*c) && !self.confirmed.contains(*c) && !self.dropped.contains(*c));
                let to_unsub = channels.iter().filter(|c| !desired.contains(*c) && self.confirmed.contains(*c));
                (to_sub.cloned().collect(), to_unsub.cloned().collect())
            };
            if (to_sub.is_empty() && to_unsub.is_empty()) || rounds >= 3 {
                return Ok(());
            }
            rounds += 1;
            self.subscribe(engine, to_sub).await?;
            self.unsubscribe(engine, to_unsub).await?;
            channels = HashSet::new();
        }
    }

    /// Subscribes to every desired channel again and releases the others.
    async fn resync(&mut self, engine: &mut Engine) -> Result<(), End> {
        self.mismatch = false;
        engine.shared.log(Level::Warn, "Redis pub/sub subscriptions out of sync, resubscribing", None);
        let all: Vec<String> = engine.shared.desired().iter().cloned().collect();
        self.subscribe(engine, all).await?;
        let mut touched = HashSet::new();
        self.drain(engine, &mut touched)?;
        let extra: Vec<String> = {
            let desired = engine.shared.desired();
            self.confirmed.iter().filter(|c| !desired.contains(*c)).cloned().collect()
        };
        self.unsubscribe(engine, extra).await
    }

    async fn subscribe(&mut self, engine: &mut Engine, channels: Vec<String>) -> Result<(), End> {
        for batch in channels.chunks(BATCH_SIZE) {
            if let Err(e) = self.conn.subscribe(batch).await {
                if is_fatal(&e) {
                    return Err(End::Lost(e.to_string()));
                }
                engine.shared.log_error(Level::Error, engine.shared.settings.subscribe_failed, e);
                // No confirmation is coming: nobody waits for these.
                batch.iter().for_each(|c| engine.shared.settled(c));
            }
        }
        Ok(())
    }

    async fn unsubscribe(&mut self, engine: &mut Engine, channels: Vec<String>) -> Result<(), End> {
        for batch in channels.chunks(BATCH_SIZE) {
            for c in batch {
                *self.expected_unsub.entry(c.clone()).or_default() += 1;
            }
            if let Err(e) = self.conn.unsubscribe(batch).await {
                if is_fatal(&e) {
                    return Err(End::Lost(e.to_string()));
                }
                engine.shared.log_error(Level::Error, "failed to unsubscribe", e);
            }
        }
        Ok(())
    }

    /// Heartbeat: any reply within the response timeout proves the connection
    /// is alive (it may even be an unsolicited push, see the module docs).
    async fn ping(&mut self, engine: &mut Engine) -> Result<(), End> {
        let reply: redis::RedisResult<Value> = redis::cmd("PING").query_async(&mut self.conn).await;
        match reply {
            Err(e) if is_fatal(&e) => Err(End::Lost(format!("heartbeat failed: {e}"))),
            _ => {
                let mut touched = HashSet::new();
                self.drain(engine, &mut touched)?;
                self.sync(engine, touched).await
            }
        }
    }

    /// Re-subscribes the primary channel (its confirmation carries Redis'
    /// current count), then fixes every difference between the sets.
    async fn reconcile(&mut self, engine: &mut Engine) -> Result<(), End> {
        let primary = engine.shared.settings.primary.clone();
        if engine.shared.is_desired(&primary) {
            self.subscribe(engine, vec![primary]).await?;
        }
        let mut touched = HashSet::new();
        self.drain(engine, &mut touched)?;
        let all: HashSet<String> = {
            let desired = engine.shared.desired();
            desired.iter().chain(self.confirmed.iter()).cloned().collect()
        };
        self.sync(engine, all).await
    }
}
