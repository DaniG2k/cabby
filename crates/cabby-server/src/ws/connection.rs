//! A WebSocket connection as a node transport (upstream `ws.Connection`).
//!
//! # Design
//!
//! One Tokio task per connection (the *io loop*, [`run`]) owns the socket: it
//! writes queued frames, reads messages and answers control frames. Nothing else
//! touches the socket, so there is no lock around it and no second task.
//!
//! The node's [`Connection`] interface is synchronous (`write` with a deadline,
//! blocking `read`), so the connection hands work to the io loop through a small
//! shared state: a mutex-protected queue of at most [`MAX_QUEUED`] outgoing
//! frames and a single inbound slot, a condition variable for the synchronous
//! callers, and a [`Notify`] waking the io loop.
//!
//! * **Push-model reads.** A server does not block a thread on [`Connection::read`]:
//!   it registers an [`InboundHandler`] ([`WsConnection::set_inbound_handler`]),
//!   and the io loop hands each message to it together with a [`ReadPermit`]. The
//!   io loop reads the next message only once the permit is dropped (the message
//!   has been executed), so messages are processed one at a time, in order, and
//!   the client is pushed back through TCP while its commands run. The end of the
//!   connection is reported once, after the last message.
//!
//! * **Back-pressure both ways.** A writer blocks (until its deadline) while the
//!   outgoing queue is full, so slow clients push back on the session's own
//!   pending-size accounting as with Go's synchronous writes. Sessions use the
//!   non-blocking variant ([`Connection::try_write_frame`]): a full queue refuses
//!   the frame, and the connection calls the session's writable handler once the
//!   io loop has taken the queued frames. Ping control frames are queued even
//!   when the queue is full (one per ping interval), so they never wait. The io loop stops
//!   reading from the socket while the inbound slot is full, so a slow reader
//!   pushes back on the client through TCP, as with Go's `ReadMessage`.
//! * **Batching.** The io loop takes every queued frame at once, feeds them to the
//!   WebSocket and flushes once: a fan-out burst becomes one socket write.
//! * **Zero-copy frames.** [`Connection::write_frame`] and `try_write_frame` take
//!   the hub's shared `Arc<SentFrame>` and queue it as is; the payload is referenced (not cloned)
//!   until it is copied into the socket write buffer. The `&[u8]` methods of the
//!   node interface must copy once into a `Bytes`.
//! * **Deadlines.** Each frame carries its deadline: the io loop fails the
//!   connection when writing it takes longer (upstream `SetWriteDeadline`).
//! * **Close.** [`WsConnection::close`] queues the close frame behind the frames
//!   already queued, then waits (at most 1s, upstream `CloseWithReason`) until it
//!   is written and the socket is closed, so a shutdown flushes close frames before
//!   the process exits.
//!
//! Error texts follow gorilla/websocket (e.g. `websocket: close 1000 (normal)`),
//! so [`is_close_error`] can tell expected closures apart, like upstream
//! `ws.IsCloseError`.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use tokio_tungstenite::WebSocketStream;
use tungstenite::error::{CapacityError, Error as WsError, ProtocolError};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::Frame;
use tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
use tungstenite::{Message, Utf8Bytes};

use super::{FrameType, SentFrame};
use crate::node::{Connection, ConnectionError};

/// Upstream `CloseNormalClosure`.
pub const CLOSE_NORMAL_CLOSURE: i32 = 1000;
/// Upstream `CloseGoingAway`.
pub const CLOSE_GOING_AWAY: i32 = 1001;
/// gorilla `CloseProtocolError`.
pub const CLOSE_PROTOCOL_ERROR: i32 = 1002;
/// gorilla `CloseNoStatusReceived`.
pub const CLOSE_NO_STATUS_RECEIVED: i32 = 1005;
/// Upstream `CloseAbnormalClosure`.
pub const CLOSE_ABNORMAL_CLOSURE: i32 = 1006;
/// gorilla `CloseInvalidFramePayloadData`.
pub const CLOSE_INVALID_FRAME_PAYLOAD_DATA: i32 = 1007;
/// gorilla `CloseMessageTooBig`.
pub const CLOSE_MESSAGE_TOO_BIG: i32 = 1009;
/// Upstream `CloseInternalServerErr`.
pub const CLOSE_INTERNAL_SERVER_ERR: i32 = 1011;
/// gorilla `CloseTryAgainLater`.
pub const CLOSE_TRY_AGAIN_LATER: i32 = 1013;

/// Outgoing frames queued before writers block.
pub const MAX_QUEUED: usize = 8;

/// How long writing a close frame may take (upstream `CloseWithReason`).
const CLOSE_DEADLINE: Duration = Duration::from_secs(1);

/// gorilla `ErrCloseSent`.
const ERR_CLOSE_SENT: &str = "websocket: close sent";

/// Whether a read error is a closure clients cause routinely: normal closure,
/// going away (page closed) or no status (upstream `ws.IsCloseError`).
pub fn is_close_error(err: &ConnectionError) -> bool {
    [CLOSE_NORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_NO_STATUS_RECEIVED]
        .iter()
        .any(|code| err.0.strip_prefix("websocket: close ").is_some_and(|rest| rest.starts_with(&format!("{code} "))))
}

/// gorilla `CloseError.Error()`.
fn close_error_text(code: u16, reason: &str) -> String {
    let name = match code {
        1000 => " (normal)",
        1001 => " (going away)",
        1002 => " (protocol error)",
        1003 => " (unsupported data)",
        1005 => " (no status)",
        1006 => " (abnormal closure)",
        1007 => " (invalid payload data)",
        1008 => " (policy violation)",
        1009 => " (message too big)",
        1010 => " (mandatory extension missing)",
        1011 => " (internal server error)",
        1015 => " (TLS handshake error)",
        _ => "",
    };
    if reason.is_empty() {
        format!("websocket: close {code}{name}")
    } else {
        format!("websocket: close {code}{name}: {reason}")
    }
}

/// Receives a connection's messages in push mode (see the module docs).
pub trait InboundHandler: Send + Sync {
    /// A text or binary message. The io loop reads the next one after `permit`
    /// is dropped. Called on the io loop: hand the work over, do not block.
    fn on_message(&self, msg: Bytes, permit: ReadPermit);
    /// The connection ended (the read error, e.g. a close); called once, after
    /// the last message.
    fn on_end(&self, err: ConnectionError);
}

/// Permission for the io loop to read the next message (push mode): dropping
/// it resumes reading.
pub struct ReadPermit(Weak<Shared>);

impl Drop for ReadPermit {
    fn drop(&mut self) {
        if let Some(shared) = self.0.upgrade() {
            shared.lock().inflight = false;
            shared.wake.notify_one();
        }
    }
}

/// A frame payload: copied bytes, or the hub's shared frame.
enum Payload {
    Bytes(Bytes),
    Shared(Arc<SentFrame>),
}

/// Lets `Bytes` reference a shared frame's payload without copying it.
struct SharedPayload(Arc<SentFrame>);

impl AsRef<[u8]> for SharedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.0.payload
    }
}

impl Payload {
    fn into_bytes(self) -> Bytes {
        match self {
            Payload::Bytes(b) => b,
            Payload::Shared(frame) => Bytes::from_owner(SharedPayload(frame)),
        }
    }
}

enum Outbound {
    Data { text: bool, payload: Payload, deadline: Instant },
    Ping { deadline: Instant },
}

impl Outbound {
    fn deadline(&self) -> Instant {
        match self {
            Outbound::Data { deadline, .. } | Outbound::Ping { deadline } => *deadline,
        }
    }

    fn into_message(self) -> Message {
        match self {
            Outbound::Data { text, payload, .. } => {
                // Text payloads are produced by our encoders (valid UTF-8); sending
                // a raw frame skips re-validating them.
                let opcode = OpCode::Data(if text { Data::Text } else { Data::Binary });
                Message::Frame(Frame::message(payload.into_bytes(), opcode, true))
            }
            Outbound::Ping { .. } => Message::Ping(Bytes::new()),
        }
    }
}

#[derive(Default)]
struct State {
    out: VecDeque<Outbound>,
    inbound: Option<Bytes>,
    /// A close frame to send after the queued frames.
    close: Option<(i32, String)>,
    /// Close requested: later writes fail.
    closing: bool,
    /// Every handle is gone: drop the socket.
    abandoned: bool,
    /// The io loop has ended; the error reads return.
    done: Option<String>,
    /// Released when the connection ends: it usually holds the session, which
    /// holds this connection.
    pong_handler: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Push mode: receives messages instead of the inbound slot. Released (after
    /// `on_end`) when the connection ends, like the pong handler.
    inbound_handler: Option<Arc<dyn InboundHandler>>,
    /// Push mode: a message is being processed (its permit is alive).
    inflight: bool,
    /// Called when a refused non-blocking write may succeed (or the connection
    /// ended). Released when the connection ends.
    writable_handler: Option<Arc<dyn Fn() + Send + Sync>>,
    /// A non-blocking write was refused since the last call of the handler.
    want_writable: bool,
}

struct Shared {
    state: Mutex<State>,
    /// Synchronous callers: space in the queue, an inbound message, the end.
    cond: Condvar,
    /// Wakes the io loop: a frame queued, the inbound slot freed, close requested.
    wake: Notify,
    /// The io loop's task, so a close requested from it does not wait for itself.
    io_task: OnceLock<tokio::task::Id>,
    subprotocol: String,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn finish(&self, reason: String) {
        // Released first (and outside the lock: it may drop the session), so
        // whoever sees the connection ended also sees it released.
        let pong_handler = self.lock().pong_handler.take();
        drop(pong_handler);

        let mut state = self.lock();
        let reason = state.done.get_or_insert(reason).clone();
        state.closing = true;
        state.out.clear();
        let late = state.pong_handler.take();
        let inbound = state.inbound_handler.take();
        let writable = state.writable_handler.take();
        let refused = std::mem::take(&mut state.want_writable);
        drop(state);
        self.cond.notify_all();
        drop(late);
        // A writer waiting for space finds out that the connection failed.
        if let Some(handler) = writable.filter(|_| refused) {
            handler();
        }
        if let Some(handler) = inbound {
            handler.on_end(ConnectionError(reason));
        }
    }

    /// Calls the writable handler if a write was refused: the io loop has just
    /// taken the queued frames.
    fn signal_writable(&self) {
        let handler = {
            let mut state = self.lock();
            if !std::mem::take(&mut state.want_writable) {
                return;
            }
            state.writable_handler.clone()
        };
        if let Some(handler) = handler {
            handler();
        }
    }
}

/// Drops the socket once the last handle is gone (a session that never closed
/// its connection must not leave the io loop running).
struct Owner(Arc<Shared>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.lock().abandoned = true;
        self.0.wake.notify_one();
    }
}

/// A WebSocket connection (upstream `ws.Connection`); cheap to clone.
#[derive(Clone)]
pub struct WsConnection {
    owner: Arc<Owner>,
}

impl std::fmt::Debug for WsConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsConnection").field("subprotocol", &self.owner.0.subprotocol).finish_non_exhaustive()
    }
}

impl WsConnection {
    /// A connection and the shared state its io loop ([`run`]) needs.
    fn new(subprotocol: String) -> (WsConnection, Arc<Shared>) {
        let shared = Arc::new(Shared {
            state: Mutex::default(),
            cond: Condvar::new(),
            wake: Notify::new(),
            io_task: OnceLock::new(),
            subprotocol,
        });
        (WsConnection { owner: Arc::new(Owner(Arc::clone(&shared))) }, shared)
    }

    fn shared(&self) -> &Shared {
        &self.owner.0
    }

    /// The negotiated subprotocol (`""`: none; upstream `Conn.Subprotocol`).
    pub fn subprotocol(&self) -> &str {
        &self.shared().subprotocol
    }

    /// Runs `handler` for every pong received (upstream `SetPongHandler`),
    /// replacing any previous one. It runs on the io loop: keep it short and
    /// non-blocking. It is released when the connection ends, so it may hold the
    /// session.
    pub fn set_pong_handler(&self, handler: impl Fn() + Send + Sync + 'static) {
        let mut state = self.shared().lock();
        if state.done.is_none() {
            state.pong_handler = Some(Arc::new(handler));
        }
    }

    /// Switches the connection to push mode: messages (including one already
    /// received) go to `handler` instead of [`Connection::read`]. If the
    /// connection has already ended, `handler` gets the pending message, if any,
    /// and the end right away.
    pub fn set_inbound_handler(&self, handler: Arc<dyn InboundHandler>) {
        let shared = self.shared();
        let mut state = shared.lock();
        if let Some(reason) = state.done.clone() {
            let pending = state.inbound.take();
            drop(state);
            if let Some(msg) = pending {
                handler.on_message(msg, ReadPermit(Weak::new()));
            }
            handler.on_end(ConnectionError(reason));
            return;
        }
        state.inbound_handler = Some(Arc::clone(&handler));
        let pending = state.inbound.take();
        if pending.is_some() {
            state.inflight = true;
        }
        drop(state);
        if let Some(msg) = pending {
            handler.on_message(msg, ReadPermit(Arc::downgrade(&self.owner.0)));
        }
    }

    /// Whether the connection has ended (closed by either side or failed).
    pub fn is_closed(&self) -> bool {
        self.shared().lock().done.is_some()
    }

    /// Queues `item`, waiting until `deadline` for space.
    fn push(&self, item: Outbound) -> Result<(), ConnectionError> {
        let shared = self.shared();
        let deadline = item.deadline();
        let mut state = shared.lock();
        loop {
            if let Some(err) = &state.done {
                return Err(ConnectionError(err.clone()));
            }
            if state.closing {
                return Err(ConnectionError(ERR_CLOSE_SENT.to_owned()));
            }
            if state.out.len() < MAX_QUEUED {
                state.out.push_back(item);
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(ConnectionError("i/o timeout".to_owned()));
            }
            state = shared.cond.wait_timeout(state, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
        drop(state);
        shared.wake.notify_one();
        Ok(())
    }

    /// Requests the close frame and returns without waiting for it to be written.
    fn request_close(&self, code: i32, reason: &str) -> bool {
        let shared = self.shared();
        let mut state = shared.lock();
        if state.done.is_some() || state.closing {
            return false;
        }
        state.closing = true;
        state.close = Some((code, reason.to_owned()));
        drop(state);
        shared.wake.notify_one();
        true
    }
}

impl Connection for WsConnection {
    /// Queues a text frame (upstream `Write`).
    fn write(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        self.push(Outbound::Data { text: true, payload: Payload::Bytes(Bytes::copy_from_slice(msg)), deadline })
    }

    /// Queues a binary frame (upstream `WriteBinary`).
    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        self.push(Outbound::Data { text: false, payload: Payload::Bytes(Bytes::copy_from_slice(msg)), deadline })
    }

    /// Queues a ping control frame (upstream `WritePing`), even when the queue is
    /// full: pings are periodic, and writers must not wait for them.
    fn write_ping(&self, deadline: Instant) -> Result<(), ConnectionError> {
        let shared = self.shared();
        let mut state = shared.lock();
        if let Some(err) = &state.done {
            return Err(ConnectionError(err.clone()));
        }
        if state.closing {
            return Err(ConnectionError(ERR_CLOSE_SENT.to_owned()));
        }
        state.out.push_back(Outbound::Ping { deadline });
        drop(state);
        shared.wake.notify_one();
        Ok(())
    }

    fn nonblocking(&self) -> bool {
        true
    }

    /// Queues a shared text or binary frame (`Ok(false)` when the queue is full;
    /// see the module docs), or requests the close frame without waiting.
    fn try_write_frame(&self, frame: &Arc<SentFrame>, deadline: Instant) -> Result<bool, ConnectionError> {
        if frame.frame_type == FrameType::Close {
            self.request_close(frame.close_code, &frame.close_reason);
            return Ok(true);
        }
        let shared = self.shared();
        let mut state = shared.lock();
        if let Some(err) = &state.done {
            return Err(ConnectionError(err.clone()));
        }
        if state.closing {
            return Err(ConnectionError(ERR_CLOSE_SENT.to_owned()));
        }
        if state.out.len() >= MAX_QUEUED {
            state.want_writable = true;
            return Ok(false);
        }
        state.out.push_back(Outbound::Data {
            text: frame.frame_type == FrameType::Text,
            payload: Payload::Shared(Arc::clone(frame)),
            deadline,
        });
        drop(state);
        shared.wake.notify_one();
        Ok(true)
    }

    fn set_writable_handler(&self, handler: Box<dyn Fn() + Send + Sync>) {
        let mut state = self.shared().lock();
        if state.done.is_none() {
            state.writable_handler = Some(Arc::from(handler));
        }
    }

    /// The next text or binary message (upstream `Read`); blocks until one arrives
    /// or the connection ends.
    fn read(&self) -> Result<Vec<u8>, ConnectionError> {
        let shared = self.shared();
        let mut state = shared.lock();
        loop {
            if let Some(msg) = state.inbound.take() {
                drop(state);
                shared.wake.notify_one();
                return Ok(Vec::from(msg));
            }
            if let Some(err) = &state.done {
                return Err(ConnectionError(err.clone()));
            }
            state = shared.cond.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Writes the hub's shared frame without copying its payload: text, binary,
    /// or close (with its code and reason).
    fn write_frame(&self, frame: &Arc<SentFrame>, deadline: Instant) -> Result<(), ConnectionError> {
        match frame.frame_type {
            FrameType::Close => {
                self.close(frame.close_code, &frame.close_reason);
                Ok(())
            }
            FrameType::Text | FrameType::Binary => self.push(Outbound::Data {
                text: frame.frame_type == FrameType::Text,
                payload: Payload::Shared(Arc::clone(frame)),
                deadline,
            }),
        }
    }

    /// Sends a close frame after the queued frames, then closes the socket
    /// (upstream `Close` / `CloseWithReason`). Waits up to 1s for that.
    fn close(&self, code: i32, reason: &str) {
        if !self.request_close(code, reason) {
            return;
        }
        let shared = self.shared();
        if tokio::task::try_id().is_some_and(|id| shared.io_task.get() == Some(&id)) {
            return;
        }
        let deadline = Instant::now() + CLOSE_DEADLINE;
        let mut state = shared.lock();
        while state.done.is_none() {
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            state = shared.cond.wait_timeout(state, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }
}

/// What the io loop does next, taken from the shared state in one lock.
struct Work {
    close: Option<(i32, String)>,
    abandoned: bool,
    can_read: bool,
}

/// A connection over an established WebSocket stream: the handle, and the io
/// loop to run (on the current task) until the connection ends.
pub(crate) fn connect<S>(
    stream: WebSocketStream<GorillaFrames<S>>,
    subprotocol: String,
) -> (WsConnection, impl std::future::Future<Output = ()> + Send)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (conn, shared) = WsConnection::new(subprotocol);
    (conn, run(shared, stream))
}

/// Closes a connection whose session could not be set up: the close frame is
/// sent by the io loop, which then ends.
pub(crate) fn close_without_waiting(conn: &WsConnection, code: i32, reason: &str) {
    conn.request_close(code, reason);
}

async fn run<S>(shared: Arc<Shared>, mut ws: WebSocketStream<GorillaFrames<S>>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if let Some(id) = tokio::task::try_id() {
        let _ = shared.io_task.set(id);
    }
    let mut batch: VecDeque<Outbound> = VecDeque::new();
    let reason = loop {
        let work = {
            let mut state = shared.lock();
            std::mem::swap(&mut state.out, &mut batch);
            Work {
                close: state.close.take(),
                abandoned: state.abandoned,
                can_read: state.inbound.is_none() && !state.inflight,
            }
        };
        if work.abandoned {
            break "connection abandoned".to_owned();
        }
        let wrote = !batch.is_empty();
        if wrote {
            shared.cond.notify_all();
            shared.signal_writable();
            if let Err(err) = write_batch(&mut ws, &mut batch).await {
                break err;
            }
        }
        if let Some((code, reason)) = work.close {
            let _ = tokio::time::timeout(CLOSE_DEADLINE, ws.close(close_frame(code, &reason))).await;
            break ERR_CLOSE_SENT.to_owned();
        }
        if wrote {
            continue;
        }
        tokio::select! {
            biased;
            () = shared.wake.notified() => {}
            msg = ws.next(), if work.can_read => {
                if let Some(err) = handle_inbound(&shared, &mut ws, msg).await {
                    break err;
                }
            }
        }
    };
    shared.finish(reason);
    // Dropping the stream closes the socket.
}

/// Writes the batch, then flushes; each frame within its deadline.
async fn write_batch<S>(ws: &mut WebSocketStream<S>, batch: &mut VecDeque<Outbound>) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut last = None;
    for item in batch.drain(..) {
        let deadline = tokio::time::Instant::from_std(item.deadline());
        last = Some(deadline);
        match tokio::time::timeout_at(deadline, ws.feed(item.into_message())).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err("i/o timeout".to_owned()),
        }
    }
    if let Some(deadline) = last {
        match tokio::time::timeout_at(deadline, ws.flush()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => return Err("i/o timeout".to_owned()),
        }
    }
    Ok(())
}

/// Handles one read result; `Some(error)` ends the connection.
async fn handle_inbound<S>(
    shared: &Arc<Shared>,
    ws: &mut WebSocketStream<GorillaFrames<S>>,
    msg: Option<Result<Message, WsError>>,
) -> Option<String>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if matches!(msg, Some(Err(_)))
        && let Some(message) = ws.get_ref().protocol_error.clone()
    {
        // gorilla `handleProtocolError`: 1002 with the error text.
        write_raw_close(ws, CLOSE_PROTOCOL_ERROR, &message).await;
        return Some(format!("websocket: {message}"));
    }
    let deliver = |payload: Bytes| {
        let mut state = shared.lock();
        if let Some(handler) = state.inbound_handler.clone() {
            state.inflight = true;
            drop(state);
            handler.on_message(payload, ReadPermit(Arc::downgrade(shared)));
            return;
        }
        state.inbound = Some(payload);
        drop(state);
        shared.cond.notify_all();
    };
    match msg {
        Some(Ok(Message::Text(text))) => deliver(Bytes::from(text)),
        Some(Ok(Message::Binary(data))) => deliver(data),
        Some(Ok(Message::Ping(_))) => {
            // The pong is queued by tungstenite; send it now.
            let _ = tokio::time::timeout(CLOSE_DEADLINE, ws.flush()).await;
        }
        Some(Ok(Message::Pong(_))) => {
            let handler = shared.lock().pong_handler.clone();
            if let Some(handler) = handler {
                handler();
            }
        }
        Some(Ok(Message::Close(frame))) => {
            // gorilla's default close handler echoes the code without the
            // reason (tungstenite's queued reply, which echoes both, is not sent).
            let (code, text) = ws.get_ref().close.clone().unwrap_or_else(|| match frame {
                Some(f) => (u16::from(f.code), f.reason.to_string()),
                None => (1005, String::new()),
            });
            write_raw_close(ws, i32::from(code), "").await;
            return Some(close_error_text(code, &text));
        }
        Some(Err(WsError::Protocol(ProtocolError::InvalidCloseSequence))) => {
            // A 1-byte close payload: gorilla reads it as "no status".
            write_raw_close(ws, CLOSE_NO_STATUS_RECEIVED, "").await;
            return Some(close_error_text(1005, ""));
        }
        Some(Ok(Message::Frame(_))) => {}
        None | Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => {
            return Some(close_error_text(1006, "unexpected EOF"));
        }
        Some(Err(WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => {
            return Some(close_error_text(1006, "unexpected EOF"));
        }
        Some(Err(WsError::Capacity(CapacityError::MessageTooLong { .. }))) => {
            // gorilla answers an oversized message with 1009 and no reason.
            let _ = tokio::time::timeout(CLOSE_DEADLINE, ws.close(close_frame(CLOSE_MESSAGE_TOO_BIG, ""))).await;
            return Some("websocket: read limit exceeded".to_owned());
        }
        Some(Err(WsError::Utf8(e))) => {
            let _ =
                tokio::time::timeout(CLOSE_DEADLINE, ws.close(close_frame(CLOSE_INVALID_FRAME_PAYLOAD_DATA, ""))).await;
            return Some(format!("websocket: invalid UTF-8 in text frame: {e}"));
        }
        Some(Err(WsError::Protocol(e))) => {
            let reason = e.to_string();
            let _ = tokio::time::timeout(CLOSE_DEADLINE, ws.close(close_frame(CLOSE_PROTOCOL_ERROR, &reason))).await;
            return Some(format!("websocket: {reason}"));
        }
        Some(Err(e)) => return Some(e.to_string()),
    }
    None
}

/// Writes a close frame directly to the socket, as gorilla's `WriteControl`
/// (`FormatCloseMessage`: an empty payload for 1005, the payload cut to 125
/// bytes), bypassing tungstenite's reply. Writes are always flushed before the
/// io loop reads, so nothing else is pending in tungstenite.
async fn write_raw_close<S>(ws: &mut WebSocketStream<GorillaFrames<S>>, code: i32, text: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let mut payload = Vec::with_capacity(2 + text.len());
    if code != CLOSE_NO_STATUS_RECEIVED {
        payload.extend_from_slice(&u16::try_from(code).unwrap_or(1011).to_be_bytes());
        payload.extend_from_slice(text.as_bytes());
        payload.truncate(125);
    }
    let mut frame = vec![0x88, payload.len() as u8];
    frame.extend_from_slice(&payload);
    let io = &mut ws.get_mut().io;
    let _ = tokio::time::timeout(CLOSE_DEADLINE, async {
        io.write_all(&frame).await?;
        io.flush().await
    })
    .await;
}

/// The frame checks gorilla/websocket makes while reading (`advanceFrame`),
/// applied to the bytes tungstenite reads, so protocol errors are reported
/// (and answered with a 1002 close frame) with gorilla's texts: `RSV1 set`,
/// `bad opcode 3`, `len > 125 for control`, `FIN not set on control`, `data
/// before FIN`, `continuation after FIN`, `bad MASK` (joined by `, ` when a
/// header has several), `bad close code N`, `invalid utf8 payload in close
/// frame`. It also keeps the code and text of a received close frame as gorilla
/// reads them.
pub(crate) struct GorillaFrames<S> {
    io: S,
    /// The current frame header (at most 2 + 8 + 4 bytes) while it is read.
    header: [u8; 14],
    have: usize,
    /// Header bytes needed; 0 while reading a payload.
    need: usize,
    remaining: u64,
    opcode: u8,
    mask: [u8; 4],
    /// The (unmasked) close payload read so far.
    close_payload: Vec<u8>,
    /// gorilla `readFinal`: the last data frame ended a message.
    read_final: bool,
    /// A protocol error found (reads fail from then on).
    protocol_error: Option<String>,
    /// The last close frame received: code (1005 without one) and text.
    close: Option<(u16, String)>,
}

impl<S> GorillaFrames<S> {
    pub(crate) fn new(io: S) -> Self {
        Self {
            io,
            header: [0; 14],
            have: 0,
            need: 2,
            remaining: 0,
            opcode: 0,
            mask: [0; 4],
            close_payload: Vec::new(),
            read_final: true,
            protocol_error: None,
            close: None,
        }
    }

    /// gorilla's checks of a frame's first two bytes.
    fn check_start(&mut self) -> Result<(), String> {
        let (b0, b1) = (self.header[0], self.header[1]);
        let opcode = b0 & 0x0f;
        let fin = b0 & 0x80 != 0;
        let mut errors = Vec::new();
        for (bit, name) in [(0x40, "RSV1 set"), (0x20, "RSV2 set"), (0x10, "RSV3 set")] {
            if b0 & bit != 0 {
                errors.push(name.to_owned());
            }
        }
        match opcode {
            8..=10 => {
                if b1 & 0x7f > 125 {
                    errors.push("len > 125 for control".to_owned());
                }
                if !fin {
                    errors.push("FIN not set on control".to_owned());
                }
            }
            1 | 2 => {
                if !self.read_final {
                    errors.push("data before FIN".to_owned());
                }
                self.read_final = fin;
            }
            0 => {
                if self.read_final {
                    errors.push("continuation after FIN".to_owned());
                }
                self.read_final = fin;
            }
            _ => errors.push(format!("bad opcode {opcode}")),
        }
        if b1 & 0x80 == 0 {
            errors.push("bad MASK".to_owned());
        }
        if errors.is_empty() { Ok(()) } else { Err(errors.join(", ")) }
    }

    /// gorilla's checks of a complete close frame payload.
    fn check_close(&mut self) -> Result<(), String> {
        let payload = std::mem::take(&mut self.close_payload);
        if payload.len() < 2 {
            self.close = Some((1005, String::new()));
            return Ok(());
        }
        let code = u16::from_be_bytes([payload[0], payload[1]]);
        let valid = matches!(code, 1000..=1003 | 1007..=1013 | 3000..=4999);
        if !valid {
            return Err(format!("bad close code {code}"));
        }
        let Ok(text) = std::str::from_utf8(&payload[2..]) else {
            return Err("invalid utf8 payload in close frame".to_owned());
        };
        self.close = Some((code, text.to_owned()));
        Ok(())
    }

    /// Scans bytes read from the socket; on a protocol error, how many of
    /// them precede the offending frame.
    fn scan(&mut self, data: &[u8]) -> Result<(), (usize, String)> {
        let mut i = 0;
        // Where the current frame starts in `data` (0 if it started earlier).
        let mut frame_start = 0;
        while i < data.len() {
            if self.need > 0 {
                if self.have == 0 {
                    frame_start = i;
                }
                let n = (self.need - self.have).min(data.len() - i);
                self.header[self.have..self.have + n].copy_from_slice(&data[i..i + n]);
                self.have += n;
                i += n;
                if self.have < self.need {
                    continue;
                }
                if self.need == 2 {
                    self.check_start().map_err(|e| (frame_start, e))?;
                    self.opcode = self.header[0] & 0x0f;
                    let ext = match self.header[1] & 0x7f {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    self.need = 2 + ext + 4;
                    if self.have < self.need {
                        continue;
                    }
                }
                let len = match self.header[1] & 0x7f {
                    126 => u64::from(u16::from_be_bytes([self.header[2], self.header[3]])),
                    127 => u64::from_be_bytes(self.header[2..10].try_into().unwrap_or([0; 8])),
                    n => u64::from(n),
                };
                self.mask.copy_from_slice(&self.header[self.need - 4..self.need]);
                self.remaining = len;
                self.need = 0;
                self.have = 0;
                self.close_payload.clear();
            } else {
                let n = usize::try_from(self.remaining).unwrap_or(usize::MAX).min(data.len() - i);
                if self.opcode == 8 {
                    for &b in &data[i..i + n] {
                        let k = self.mask[self.close_payload.len() % 4];
                        self.close_payload.push(b ^ k);
                    }
                }
                self.remaining -= n as u64;
                i += n;
            }
            if self.need == 0 && self.remaining == 0 {
                if self.opcode == 8 {
                    self.check_close().map_err(|e| (frame_start, e))?;
                }
                self.need = 2;
            }
        }
        Ok(())
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for GorillaFrames<S> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        let this = &mut *self;
        if let Some(e) = &this.protocol_error {
            return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.clone())));
        }
        let before = buf.filled().len();
        match std::pin::Pin::new(&mut this.io).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        if let Err((keep, e)) = this.scan(&buf.filled()[before..]) {
            buf.set_filled(before + keep);
            this.protocol_error = Some(e.clone());
            if keep == 0 {
                return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)));
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for GorillaFrames<S> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// The close frame for `code` (none for 1005, which cannot be sent); the reason is
/// cut to fit a control frame.
fn close_frame(code: i32, reason: &str) -> Option<CloseFrame> {
    if code == CLOSE_NO_STATUS_RECEIVED {
        return None;
    }
    let mut end = reason.len().min(123);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    let code = u16::try_from(code).unwrap_or(1011);
    Some(CloseFrame { code: CloseCode::from(code), reason: Utf8Bytes::from(&reason[..end]) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_close_errors_like_gorilla() {
        assert_eq!("websocket: close 1000 (normal)", close_error_text(1000, ""));
        assert_eq!("websocket: close 1001 (going away): bye", close_error_text(1001, "bye"));
        assert_eq!("websocket: close 4000: custom", close_error_text(4000, "custom"));
    }

    #[test]
    fn recognizes_expected_closures() {
        assert!(is_close_error(&ConnectionError(close_error_text(1000, ""))));
        assert!(is_close_error(&ConnectionError(close_error_text(1005, ""))));
        assert!(!is_close_error(&ConnectionError(close_error_text(1006, "unexpected EOF"))));
        assert!(!is_close_error(&ConnectionError("websocket: close 10001 (x)".to_owned())));
    }

    #[test]
    fn cuts_close_reasons_to_fit() {
        let long = "é".repeat(100);
        let frame = close_frame(1000, &long).unwrap();
        assert!(frame.reason.len() <= 123);
        assert!(close_frame(1005, "x").is_none());
    }
}
