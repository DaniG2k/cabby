//! Action Cable WebSocket client (upstream `etc/benchi/client`, package `anycable`, a
//! hard fork of `cinemast/anycable-go-client`).
//!
//! One reader per connection routes server messages to per-subscription queues;
//! `ping` is answered with `pong`, `disconnect` closes the client, and
//! confirm/reject resolve the pending subscription.
//!
//! The connection core ([`Conn`]) is async and runs on the harness runtime; the
//! public [`Client`] blocks on it. [`crate::client::Client`] uses the same core but
//! routes every subscription straight into its merged queue (one hop per message
//! instead of upstream's subscription queue → forwarder → merged queue).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tungstenite::Message as WsMessage;
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;

use crate::BenchError;
use crate::gojson;
use crate::net::{self, Ws};
use crate::pusher::{self, Wire};
use crate::runtime::{block_on, now_ns, rt};

/// `activity_timeout` assumed when `pusher:connection_established` has none.
const PUSHER_ACTIVITY_DEFAULT: u64 = 30;

/// Per-subscription queue capacity when [`Client::set_subscription_capacity`] was
/// not called (upstream `DefaultSubscriptionCapacity`).
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 1000;

/// A channel identifier, rendered as the JSON string sent in `identifier`.
pub trait ChannelIdentifier {
    fn identifier(&self) -> String;
}

impl ChannelIdentifier for String {
    fn identifier(&self) -> String {
        self.clone()
    }
}

/// A client command (`{"command":..,"identifier":..,"data":..,"action":..}`);
/// absent fields are omitted, as with Go's `omitempty` pointers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Command {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
}

impl Command {
    /// The command as Go's `encoding/json` encodes it (HTML-safe escaping).
    pub(crate) fn encode(&self) -> String {
        let mut out = String::from("{\"command\":");
        gojson::push_str(&mut out, &self.command);
        for (key, value) in [("identifier", &self.identifier), ("data", &self.data), ("action", &self.action)] {
            if let Some(v) = value {
                out.push_str(",\"");
                out.push_str(key);
                out.push_str("\":");
                gojson::push_str(&mut out, v);
            }
        }
        out.push('}');
        out
    }

    fn new(command: &str, identifier: Option<&str>) -> Self {
        Self { command: command.to_owned(), identifier: identifier.map(str::to_owned), ..Default::default() }
    }
}

/// A server message. `message` keeps the raw JSON value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub message: Option<serde_json::Value>,
    #[serde(default)]
    pub identifier: Option<String>,
}

/// A server message as the reader decodes it: `message` stays unparsed.
#[derive(Deserialize)]
struct RawEvent<'a> {
    #[serde(rename = "type", default, borrow)]
    kind: Cow<'a, str>,
    #[serde(default, borrow)]
    message: Option<&'a RawValue>,
    #[serde(default, borrow)]
    identifier: Option<Cow<'a, str>>,
}

/// A message payload as the benchi client sees it: string payloads are decoded,
/// other JSON is kept as raw text.
fn payload(message: Option<&RawValue>) -> String {
    match message {
        Some(m) => serde_json::from_str::<String>(m.get()).unwrap_or_else(|_| m.get().to_owned()),
        None => String::new(),
    }
}

/// Where the reader delivers a subscription's messages.
pub(crate) enum Sink {
    /// A bounded per-subscription queue of events (upstream `Subscription.Messages`).
    Queue(mpsc::Sender<Event>),
    /// A client's merged queue, tagged with the subscription's stream.
    Merged { tx: mpsc::Sender<crate::client::Message>, stream: Arc<str> },
    /// Counted and reconciled right in the reader (pool clients): no queue and no
    /// task hand-off per message.
    Record { recorder: Arc<crate::pool::Recorder>, stream: Arc<str> },
    /// Handed to a callback in the reader, payload decoded as for [`Sink::Merged`]
    /// (the soak scenario's per-subscription reconciliation).
    Callback(Arc<dyn Fn(&str) + Send + Sync>),
}

impl Clone for Sink {
    fn clone(&self) -> Self {
        match self {
            Sink::Queue(tx) => Sink::Queue(tx.clone()),
            Sink::Merged { tx, stream } => Sink::Merged { tx: tx.clone(), stream: Arc::clone(stream) },
            Sink::Record { recorder, stream } => {
                Sink::Record { recorder: Arc::clone(recorder), stream: Arc::clone(stream) }
            }
            Sink::Callback(f) => Sink::Callback(Arc::clone(f)),
        }
    }
}

impl Sink {
    /// Delivers `ev`, waiting while the queue is full (backpressure reaches the
    /// server through the socket, as upstream's blocking channel send does).
    async fn deliver(&self, ev: RawEvent<'_>) {
        match self {
            Sink::Queue(tx) => {
                let message = ev.message.and_then(|m| serde_json::from_str(m.get()).ok());
                let event =
                    Event { kind: ev.kind.into_owned(), message, identifier: ev.identifier.map(Cow::into_owned) };
                let _ = tx.send(event).await;
            }
            Sink::Merged { tx, stream } => {
                let data = payload(ev.message);
                let _ = tx.send(crate::client::Message { stream: stream.to_string(), data }).await;
            }
            Sink::Record { recorder, stream } => recorder.record(stream, &payload(ev.message)),
            Sink::Callback(f) => f(&payload(ev.message)),
        }
    }
}

/// Resolution state of one subscription.
pub(crate) struct SubFlags {
    subscribed: AtomicBool,
    rejected: AtomicBool,
    done: watch::Sender<bool>,
}

impl SubFlags {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            subscribed: AtomicBool::new(false),
            rejected: AtomicBool::new(false),
            done: watch::channel(false).0,
        })
    }

    fn signal_done(&self) {
        self.done.send_replace(true);
    }

    pub(crate) fn subscribed(&self) -> bool {
        self.subscribed.load(Ordering::SeqCst)
    }

    pub(crate) fn rejected(&self) -> bool {
        self.rejected.load(Ordering::SeqCst)
    }

    /// Waits until the server confirmed or rejected the subscription, or the
    /// connection was lost.
    pub(crate) async fn wait_done(&self) {
        let mut rx = self.done.subscribe();
        let _ = rx.wait_for(|d| *d).await;
    }
}

pub(crate) struct Route {
    pub flags: Arc<SubFlags>,
    pub sink: Sink,
}

/// One WebSocket connection: a writer half shared by senders and a reader task
/// that owns the read half.
pub(crate) struct Conn {
    writer: tokio::sync::Mutex<Option<SplitSink<Ws, WsMessage>>>,
    /// `None` once the reader has stopped (connection lost or closed).
    routes: Mutex<Option<HashMap<String, Route>>>,
    cancel: CancellationToken,
    reader: Mutex<Option<JoinHandle<()>>>,
    raw: std::net::TcpStream,
    wire: Wire,
    /// When the reader stopped ([`now_ns`]), 0 while it runs.
    ended_ns: AtomicU64,
}

fn read_error(e: impl std::fmt::Display) -> BenchError {
    BenchError(format!("error reading message from AnyCable server: {e}"))
}

impl Conn {
    /// Dials `url`, waits for the welcome message and starts the reader. Any other
    /// first message is an error.
    pub(crate) async fn dial(url: &str) -> Result<Arc<Conn>, BenchError> {
        Self::dial_with(url, Wire::ActionCable).await
    }

    /// [`Conn::dial`] for `wire`: with [`Wire::Pusher`] the welcome is
    /// `pusher:connection_established`, whose `activity_timeout` sets the keepalive.
    pub(crate) async fn dial_with(url: &str, wire: Wire) -> Result<Arc<Conn>, BenchError> {
        let dialed =
            net::dial(url).await.map_err(|e| BenchError(format!("error connecting to AnyCable server: {e}")))?;
        let (sink, mut stream) = dialed.ws.split();
        let first = loop {
            match stream.next().await {
                Some(Ok(WsMessage::Text(t))) => break t,
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => continue,
                Some(Ok(WsMessage::Binary(_))) => {
                    net::abort(&dialed.raw);
                    return Err(BenchError("received unexpected message type: 2".into()));
                }
                Some(Ok(WsMessage::Close(_))) | None => {
                    net::abort(&dialed.raw);
                    return Err(read_error("connection closed"));
                }
                Some(Err(e)) => {
                    net::abort(&dialed.raw);
                    return Err(read_error(e));
                }
            }
        };
        let undecodable = |e: serde_json::Error| {
            net::abort(&dialed.raw);
            BenchError(format!("could not deserialize message {}: {e}", first.as_str()))
        };
        let mut activity = None;
        match wire {
            Wire::ActionCable => {
                let ev: RawEvent = serde_json::from_str(first.as_str()).map_err(undecodable)?;
                if ev.kind != "welcome" {
                    net::abort(&dialed.raw);
                    return Err(BenchError(format!("unexpected message type: {}", ev.kind.as_ref())));
                }
            }
            Wire::Pusher => {
                let f: pusher::Frame = serde_json::from_str(first.as_str()).map_err(undecodable)?;
                if f.event != pusher::CONNECTION_ESTABLISHED {
                    net::abort(&dialed.raw);
                    return Err(BenchError(format!("unexpected message: {}", first.as_str())));
                }
                let secs = pusher::activity_timeout(f.data).unwrap_or(PUSHER_ACTIVITY_DEFAULT).clamp(1, 120);
                activity = Some(Duration::from_secs(secs));
            }
        }
        let conn = Arc::new(Conn {
            writer: tokio::sync::Mutex::new(Some(sink)),
            routes: Mutex::new(Some(HashMap::new())),
            cancel: CancellationToken::new(),
            reader: Mutex::new(None),
            raw: dialed.raw,
            wire,
            ended_ns: AtomicU64::new(0),
        });
        let reader = match activity {
            Some(a) => rt().spawn(Arc::clone(&conn).read_loop_pusher(stream, a)),
            None => rt().spawn(Arc::clone(&conn).read_loop(stream)),
        };
        *conn.reader.lock().unwrap_or_else(|e| e.into_inner()) = Some(reader);
        Ok(conn)
    }

    async fn read_loop(self: Arc<Self>, mut stream: futures_util::stream::SplitStream<Ws>) {
        loop {
            let msg = tokio::select! {
                () = self.cancel.cancelled() => break,
                m = stream.next() => m,
            };
            let text = match msg {
                Some(Ok(WsMessage::Text(t))) => t,
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => continue,
                // Binary frames, close frames, errors and EOF end the connection.
                _ => break,
            };
            let Ok(ev) = serde_json::from_str::<RawEvent>(text.as_str()) else { break };
            match ev.kind.as_ref() {
                "ping" => {
                    let _ = self.send_text(Command::new("pong", None).encode()).await;
                }
                "disconnect" => {
                    self.cancel.cancel();
                    net::abort(&self.raw);
                    break;
                }
                "reject_subscription" => {
                    let route = ev.identifier.as_deref().and_then(|id| self.unregister(id));
                    if let Some(r) = route {
                        r.flags.rejected.store(true, Ordering::SeqCst);
                        r.flags.signal_done();
                    }
                }
                "confirm_subscription" => {
                    let flags = ev.identifier.as_deref().and_then(|id| self.route_flags(id));
                    if let Some(f) = flags {
                        f.subscribed.store(true, Ordering::SeqCst);
                        f.signal_done();
                    }
                }
                _ => {
                    let sink = ev.identifier.as_deref().and_then(|id| self.route_sink(id));
                    if let Some(sink) = sink {
                        tokio::select! {
                            () = self.cancel.cancelled() => break,
                            () = sink.deliver(ev) => {}
                        }
                    }
                }
            }
        }
        self.close_routes();
    }

    /// Connection lost or closed: close every queue and resolve every wait.
    fn close_routes(&self) {
        self.ended_ns.store(now_ns().max(1), Ordering::SeqCst);
        // Wakes [`Conn::ended`] waiters; the reader has stopped, so nothing else
        // depends on the token any more.
        self.cancel.cancel();
        let routes = self.routes.lock().unwrap_or_else(|e| e.into_inner()).take();
        for route in routes.into_iter().flat_map(HashMap::into_values) {
            route.flags.signal_done();
        }
    }

    /// The Pusher reader: routes events by channel, resolves subscriptions, answers
    /// `pusher:ping` and, like pusher-js, sends `pusher:ping` when nothing arrived for
    /// `activity` (the server's `activity_timeout`).
    async fn read_loop_pusher(self: Arc<Self>, mut stream: futures_util::stream::SplitStream<Ws>, activity: Duration) {
        // Checked every half timeout, so a ping follows at most one timeout of silence.
        let check = activity / 2;
        let mut keepalive = tokio::time::interval_at(tokio::time::Instant::now() + check, check);
        keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut active = false;
        loop {
            let msg = tokio::select! {
                () = self.cancel.cancelled() => break,
                _ = keepalive.tick() => {
                    if !std::mem::take(&mut active) {
                        let _ = self.send_text(pusher::PING_FRAME.to_owned()).await;
                    }
                    continue;
                }
                m = stream.next() => m,
            };
            let text = match msg {
                Some(Ok(WsMessage::Text(t))) => t,
                Some(Ok(WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_))) => continue,
                // Binary frames, close frames, errors and EOF end the connection.
                _ => break,
            };
            active = true;
            let Ok(f) = serde_json::from_str::<pusher::Frame>(text.as_str()) else { break };
            match f.event.as_ref() {
                pusher::PING => {
                    let _ = self.send_text(pusher::PONG_FRAME.to_owned()).await;
                }
                // A fatal error is followed by the server's close.
                pusher::PONG | pusher::ERROR => {}
                pusher::SUBSCRIPTION_SUCCEEDED => {
                    if let Some(flags) = f.channel.as_deref().and_then(|c| self.route_flags(c)) {
                        flags.subscribed.store(true, Ordering::SeqCst);
                        flags.signal_done();
                    }
                }
                e if pusher::is_subscription_error(e) => {
                    if let Some(r) = f.channel.as_deref().and_then(|c| self.unregister(c)) {
                        r.flags.rejected.store(true, Ordering::SeqCst);
                        r.flags.signal_done();
                    }
                }
                _ => {
                    let sink = f.channel.as_deref().and_then(|c| self.route_sink(c));
                    if let Some(sink) = sink {
                        let ev = RawEvent { kind: f.event, message: f.data, identifier: f.channel };
                        tokio::select! {
                            () = self.cancel.cancelled() => break,
                            () = sink.deliver(ev) => {}
                        }
                    }
                }
            }
        }
        self.close_routes();
    }

    /// The route key and the subscribe frame of `stream`: the `$pubsub` identifier and a
    /// `subscribe` command, or the Pusher channel (the stream itself) and
    /// `pusher:subscribe`.
    pub(crate) fn subscription(&self, stream: &str) -> (String, String) {
        match self.wire {
            Wire::ActionCable => {
                let id = crate::client::stream_identifier(stream);
                let cmd = Command { command: "subscribe".into(), identifier: Some(id.clone()), ..Default::default() };
                (id, cmd.encode())
            }
            Wire::Pusher => (stream.to_owned(), pusher::subscribe_frame(stream)),
        }
    }

    fn route_flags(&self, id: &str) -> Option<Arc<SubFlags>> {
        let routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        routes.as_ref()?.get(id).map(|r| Arc::clone(&r.flags))
    }

    fn route_sink(&self, id: &str) -> Option<Sink> {
        let routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        routes.as_ref()?.get(id).map(|r| r.sink.clone())
    }

    /// Registers the route of subscription `id` (replacing an earlier one).
    pub(crate) fn register(&self, id: &str, route: Route) -> Result<(), BenchError> {
        let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
        match routes.as_mut() {
            Some(r) => {
                r.insert(id.to_owned(), route);
                Ok(())
            }
            None => Err(BenchError("connection closed".into())),
        }
    }

    /// Removes the route of `id`, closing its queue.
    pub(crate) fn unregister(&self, id: &str) -> Option<Route> {
        self.routes.lock().unwrap_or_else(|e| e.into_inner()).as_mut()?.remove(id)
    }

    pub(crate) async fn send_text(&self, text: String) -> Result<(), BenchError> {
        let mut writer = self.writer.lock().await;
        let w = writer.as_mut().ok_or_else(|| BenchError("connection closed".into()))?;
        w.send(WsMessage::text(text)).await.map_err(|e| BenchError(format!("error writing to AnyCable server: {e}")))
    }

    pub(crate) async fn send_command(&self, c: &Command) -> Result<(), BenchError> {
        self.send_text(c.encode()).await
    }

    /// When the reader stopped (connection lost or closed), on the harness's
    /// monotonic clock; `None` while it runs.
    pub(crate) fn ended_ns(&self) -> Option<u64> {
        match self.ended_ns.load(Ordering::SeqCst) {
            0 => None,
            t => Some(t),
        }
    }

    /// Completes once the reader has stopped or [`Conn::close`] was called.
    pub(crate) async fn ended(&self) {
        self.cancel.cancelled().await;
    }

    /// Closes the connection with a WebSocket close handshake: sends a normal-closure
    /// frame and waits up to `timeout` for the server to answer and the reader to
    /// stop, then closes the socket ([`Conn::close`]). Returns whether the reader
    /// stopped in time (the server completed the handshake or closed).
    pub(crate) async fn close_clean(&self, timeout: Duration) -> bool {
        {
            let mut writer = self.writer.lock().await;
            if let Some(w) = writer.as_mut() {
                let frame = CloseFrame { code: CloseCode::Normal, reason: "".into() };
                let _ = tokio::time::timeout(timeout, w.send(WsMessage::Close(Some(frame)))).await;
            }
        }
        let reader = self.reader.lock().unwrap_or_else(|e| e.into_inner()).take();
        let mut in_time = true;
        if let Some(mut r) = reader
            && tokio::time::timeout(timeout, &mut r).await.is_err()
        {
            in_time = false;
            self.cancel.cancel();
            net::abort(&self.raw);
            let _ = r.await;
        }
        self.close().await;
        in_time
    }

    /// Closes the socket (no close frame, as upstream's `ws.Close()`) and waits for
    /// the reader to stop.
    pub(crate) async fn close(&self) {
        self.cancel.cancel();
        net::abort(&self.raw);
        self.writer.lock().await.take();
        let reader = self.reader.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(r) = reader {
            let _ = r.await;
        }
    }
}

/// A subscription: its identifier, resolution flags and message queue.
pub struct Subscription {
    pub identifier: String,
    flags: Arc<SubFlags>,
    messages: tokio::sync::Mutex<mpsc::Receiver<Event>>,
}

impl Subscription {
    pub fn subscribed(&self) -> bool {
        self.flags.subscribed()
    }

    pub fn rejected(&self) -> bool {
        self.flags.rejected()
    }

    /// Next message on this subscription; `None` once the queue is closed (reject or
    /// connection loss).
    pub fn recv(&self) -> Option<Event> {
        block_on(async { self.messages.lock().await.recv().await })
    }
}

pub struct Client {
    url: String,
    capacity: AtomicUsize,
    conn: Mutex<Option<Arc<Conn>>>,
}

impl Client {
    /// Builds a client for `url`; nothing is dialed until [`Client::connect`].
    pub fn new(url: &str) -> Self {
        Self { url: url.to_owned(), capacity: AtomicUsize::new(0), conn: Mutex::new(None) }
    }

    pub fn set_subscription_capacity(&self, capacity: usize) {
        self.capacity.store(capacity, Ordering::SeqCst);
    }

    /// Dials and waits for the welcome message; any other first message is an error.
    pub fn connect(&self) -> Result<(), BenchError> {
        let conn = block_on(Conn::dial(&self.url))?;
        *self.conn.lock().unwrap_or_else(|e| e.into_inner()) = Some(conn);
        Ok(())
    }

    fn conn(&self) -> Result<Arc<Conn>, BenchError> {
        self.conn.lock().unwrap_or_else(|e| e.into_inner()).clone().ok_or_else(|| BenchError("not connected".into()))
    }

    pub fn subscribe(&self, identifier: &dyn ChannelIdentifier) -> Result<Subscription, BenchError> {
        let conn = self.conn()?;
        let capacity = match self.capacity.load(Ordering::SeqCst) {
            0 => DEFAULT_SUBSCRIPTION_CAPACITY,
            n => n,
        };
        let id = identifier.identifier();
        let (tx, rx) = mpsc::channel(capacity);
        let flags = SubFlags::new();
        conn.register(&id, Route { flags: Arc::clone(&flags), sink: Sink::Queue(tx) })?;
        if let Err(e) = block_on(conn.send_command(&Command::new("subscribe", Some(&id)))) {
            conn.unregister(&id);
            return Err(e);
        }
        Ok(Subscription { identifier: id, flags, messages: tokio::sync::Mutex::new(rx) })
    }

    /// Subscribes and waits for confirmation, rejection, connection loss or `timeout`;
    /// errors name the identifier.
    pub fn subscribe_and_wait(
        &self,
        identifier: &dyn ChannelIdentifier,
        timeout: Duration,
    ) -> Result<Subscription, BenchError> {
        let sub = self.subscribe(identifier)?;
        let id = sub.identifier.clone();
        match block_on(tokio::time::timeout(timeout, sub.flags.wait_done())) {
            Err(_) => {
                if let Ok(conn) = self.conn() {
                    conn.unregister(&id);
                }
                Err(BenchError(format!("subscription confirmation timed out: {id}")))
            }
            Ok(()) if sub.rejected() => Err(BenchError(format!("subscription rejected: {id}"))),
            Ok(()) if !sub.subscribed() => Err(BenchError(format!("subscription closed before confirmation: {id}"))),
            Ok(()) => Ok(sub),
        }
    }

    pub fn unsubscribe(&self, subscription: &Subscription) -> Result<(), BenchError> {
        let conn = self.conn()?;
        conn.unregister(&subscription.identifier);
        block_on(conn.send_command(&Command::new("unsubscribe", Some(&subscription.identifier))))
    }

    /// Sends a `message` command with `data` set to the JSON encoding of `message`.
    pub fn send(&self, identifier: &dyn ChannelIdentifier, message: &serde_json::Value) -> Result<(), BenchError> {
        let conn = self.conn()?;
        let cmd = Command {
            command: "message".into(),
            identifier: Some(identifier.identifier()),
            data: Some(message.to_string()),
            action: None,
        };
        block_on(conn.send_command(&cmd))
    }

    pub fn close(&self) -> Result<(), BenchError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if let Some(c) = conn {
            block_on(c.close());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_commands_like_go() {
        let c = Command::new("subscribe", Some(r#"{"channel":"$pubsub","stream_name":"<a>"}"#));
        // Backslashes shown as '/'.
        assert_eq!(
            r#"{"command":"subscribe","identifier":"{/"channel/":/"$pubsub/",/"stream_name/":/"/u003ca/u003e/"}"}"#,
            c.encode().replace('\\', "/")
        );
        assert_eq!(r#"{"command":"pong"}"#, Command::new("pong", None).encode());
    }
}
