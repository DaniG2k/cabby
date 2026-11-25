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
//! * **Reading between batches.** The io loop does not read while it writes a batch.
//!   With nothing to write it waits for a message. When frames are queued again
//!   whenever a batch is written, it looks at its reader once before each next batch:
//!   at most one message (text, binary, ping, pong or close) is handled per batch
//!   written. So a client that is behind with reading is still heard, and one that
//!   sends without pause cannot hold its own frames back. A look that finds nothing
//!   makes no system call (tokio knows that the socket is not readable), and there is
//!   none when the loop waited for a message since the last batch. Nothing is read
//!   once the loop sees that a close frame is requested; a close frame or the end of the
//!   stream read between batches ends the connection, before the next batch, without
//!   writing the frames still queued, as in Go.
//! * **Batching.** The io loop takes every queued frame at once and writes them with
//!   one vectored write and one flush: a fan-out burst becomes one system call (with
//!   TLS, one write of records).
//! * **Pre-framed, zero-copy writes.** Server frames are not masked and
//!   permessage-deflate is never negotiated, so a frame is a header of 2 to 10 bytes
//!   and the payload as it is. A queued frame carries its header, and [`write_batch`]
//!   writes headers and payloads straight to the socket, past tungstenite (see
//!   below). [`Connection::write_frame`] and `try_write_frame` share the hub's
//!   `Bytes` payload (`SentFrame::payload`), which is copied only by the kernel (or
//!   encrypted). The `&[u8]` methods of the node interface must copy once into a
//!   `Bytes`.
//! * **Deadlines.** Each frame carries how long its write may take, and a batch
//!   has one deadline, the earliest of its frames', across its write and flush: the
//!   connection fails when they take longer, whether or not the client takes some
//!   bytes. Frames queued by [`Connection::try_write_frame`] carry a timeout that
//!   starts when the io loop starts writing their batch, as Go sets the deadline when
//!   it writes: producers do not read the clock, and a batch reads it once. A frame
//!   that waits in the queue does so behind the batch being written (or a pong),
//!   whose own deadline bounds that wait. Blocking writers, which wait for space in
//!   the queue, and pings bring a deadline that covers the wait as well. So a ping
//!   never gives the data around it more than its write timeout, and the connection
//!   fails when a ping is not written by its deadline, as in Go (`WriteControl` waits
//!   for the writer until then). A ping that is overdue when its batch starts costs
//!   the connection only if the socket does not take the batch at once.
//! * **Close.** [`WsConnection::close`] queues the close frame behind the frames
//!   already queued, then waits (at most 1s, upstream `CloseWithReason`) until it
//!   is written and the socket is closed, so a shutdown flushes close frames before
//!   the process exits.
//!
//! # Writing past tungstenite
//!
//! tungstenite reads, answers pings and writes most close frames; the io loop writes
//! everything else to the socket itself. Such a write must not land inside a frame
//! tungstenite has partly written, so whenever the io loop writes, **tungstenite
//! has nothing to write**: its output buffer is empty, no reply is queued in it, and
//! what it wrote is flushed. This holds because (tungstenite 0.30
//! `protocol/mod.rs`, `protocol/frame/mod.rs`):
//!
//! * It buffers output only in `write`, `flush` and `close`, and in `read` when a
//!   reply is queued or was not flushed. The io loop never uses `write` for frames,
//!   and `close` is the last thing it does with a connection.
//! * Only `read` queues a reply (`additional_send`), on receiving a ping or a close
//!   frame, and the same call returns that message. So a poll of the reader that is
//!   pending (`select!` drops it, or it was the look between two batches) has queued
//!   nothing and (as nothing was queued before) has written nothing.
//! * The io loop flushes a pong before it does anything else, so before the next
//!   batch, outside of `select!`'s reach. A flush that completes has buffered the
//!   pong (it always fits: the output buffer's size is not limited), written the
//!   buffer and flushed the socket. One that fails or times out, perhaps in the
//!   middle of the pong, ends the connection ([`handle_inbound`]).
//! * Everything else tungstenite could answer ends the connection: the io loop
//!   answers a close frame itself ([`write_raw_close`], the queued reply is dropped
//!   with the stream) and closes after read errors.
//!
//! In the other direction, [`write_batch`] returns successfully only after the last
//! byte is written and flushed, and its failure ends the connection, so tungstenite
//! never writes inside one of the io loop's frames.
//!
//! Error texts follow gorilla/websocket (e.g. `websocket: close 1000 (normal)`),
//! so [`is_close_error`] can tell expected closures apart, like upstream
//! `ws.IsCloseError`.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::task::{Context, Poll, ready};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use tokio_tungstenite::WebSocketStream;
use tungstenite::error::{CapacityError, Error as WsError, ProtocolError};
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::{Message, Utf8Bytes};

use super::{FrameType, SentFrame};
use crate::node::{Connection, ConnectionError, SessionReader, deadline_after};

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

/// How long writing a pong may take (gorilla `writeWait`).
const PONG_DEADLINE: Duration = Duration::from_secs(1);

/// A write missed its deadline (Go network errors).
const ERR_TIMEOUT: &str = "i/o timeout";

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

/// A session's push-model reader takes the connection's messages directly: the
/// Action Cable and Pusher endpoints serve sessions without a reader thread.
impl InboundHandler for SessionReader {
    fn on_message(&self, msg: Bytes, permit: ReadPermit) {
        self.message(msg, Box::new(permit));
    }

    fn on_end(&self, err: ConnectionError) {
        self.end(err);
    }
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

/// The header of a server frame (RFC 6455 section 5.2): FIN set, no extension
/// bits (permessage-deflate is never negotiated) and no mask, so 2, 4 or 10 bytes.
struct FrameHeader([u8; 10]);

impl FrameHeader {
    const TEXT: u8 = 0x81;
    const BINARY: u8 = 0x82;
    /// A ping without a payload.
    const PING: FrameHeader = FrameHeader([0x89, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

    /// The header of a frame with `len` payload bytes; `b0` is FIN and the opcode.
    fn new(b0: u8, len: usize) -> Self {
        let mut header = [0; 10];
        header[0] = b0;
        match len {
            0..=125 => header[1] = len as u8,
            126..=0xffff => {
                header[1] = 126;
                header[2..4].copy_from_slice(&(len as u16).to_be_bytes());
            }
            _ => {
                header[1] = 127;
                header[2..].copy_from_slice(&(len as u64).to_be_bytes());
            }
        }
        Self(header)
    }

    fn bytes(&self) -> &[u8] {
        let len = match self.0[1] {
            126 => 4,
            127 => 10,
            _ => 2,
        };
        &self.0[..len]
    }
}

/// How long writing a frame may take.
#[derive(Clone, Copy)]
enum WriteLimit {
    /// Until then (the deadline of a blocking writer, which started with its wait for
    /// space, or of a ping).
    Until(Instant),
    /// This many nanoseconds (up to about 584 years) from the start of its batch's
    /// write. A bare `u64` fits beside `Instant`'s niche, unlike a `Duration`: a
    /// queued frame stays at 64 bytes.
    Within(u64),
}

impl WriteLimit {
    fn within(timeout: Duration) -> Self {
        WriteLimit::Within(u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX))
    }
}

/// The deadline of a batch whose write starts now: the earliest of its frames'
/// limits. Reads the clock at most once.
fn batch_deadline(batch: &VecDeque<Outbound>) -> Option<Instant> {
    let (mut until, mut within) = (None, None);
    for frame in batch {
        match frame.limit {
            WriteLimit::Until(at) => until = Some(until.map_or(at, |u: Instant| u.min(at))),
            WriteLimit::Within(nanos) => within = Some(within.map_or(nanos, |w: u64| w.min(nanos))),
        }
    }
    [until, within.map(|nanos| deadline_after(Duration::from_nanos(nanos)))].into_iter().flatten().min()
}

/// A queued frame: its header and the shared payload, ready to be written.
struct Outbound {
    header: FrameHeader,
    payload: Bytes,
    limit: WriteLimit,
}

impl Outbound {
    fn data(text: bool, payload: Bytes, limit: WriteLimit) -> Self {
        let b0 = if text { FrameHeader::TEXT } else { FrameHeader::BINARY };
        Self { header: FrameHeader::new(b0, payload.len()), payload, limit }
    }

    fn ping(limit: WriteLimit) -> Self {
        Self { header: FrameHeader::PING, payload: Bytes::new(), limit }
    }

    /// The bytes on the wire.
    fn parts(&self) -> impl Iterator<Item = &[u8]> {
        [self.header.bytes(), &self.payload].into_iter().filter(|part| !part.is_empty())
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
    /// Synchronous callers parked on `Shared::cond` (avoids waking on every
    /// write batch when no blocking caller is waiting).
    cond_waiters: usize,
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

    /// Queues a data frame that must be written by `deadline`, waiting until then
    /// for space.
    fn push(&self, text: bool, payload: Bytes, deadline: Instant) -> Result<(), ConnectionError> {
        let shared = self.shared();
        let mut state = shared.lock();
        let was_empty;
        loop {
            if let Some(err) = &state.done {
                return Err(ConnectionError(err.clone()));
            }
            if state.closing {
                return Err(ConnectionError(ERR_CLOSE_SENT.to_owned()));
            }
            if state.out.len() < MAX_QUEUED {
                was_empty = state.out.is_empty();
                state.out.push_back(Outbound::data(text, payload, WriteLimit::Until(deadline)));
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(ConnectionError(ERR_TIMEOUT.to_owned()));
            }
            state.cond_waiters += 1;
            state = shared.cond.wait_timeout(state, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
            state.cond_waiters -= 1;
        }
        drop(state);
        if was_empty {
            shared.wake.notify_one();
        }
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
        self.push(true, Bytes::copy_from_slice(msg), deadline)
    }

    /// Queues a binary frame (upstream `WriteBinary`).
    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
        self.push(false, Bytes::copy_from_slice(msg), deadline)
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
        let was_empty = state.out.is_empty();
        state.out.push_back(Outbound::ping(WriteLimit::Until(deadline)));
        drop(state);
        if was_empty {
            shared.wake.notify_one();
        }
        Ok(())
    }

    fn nonblocking(&self) -> bool {
        true
    }

    /// Queues a shared text or binary frame (`Ok(false)` when the queue is full;
    /// see the module docs), or requests the close frame without waiting.
    /// `timeout` starts when the io loop starts writing the frame.
    fn try_write_frame(&self, frame: &Arc<SentFrame>, timeout: Duration) -> Result<bool, ConnectionError> {
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
        let was_empty = state.out.is_empty();
        let text = frame.frame_type == FrameType::Text;
        state.out.push_back(Outbound::data(text, frame.payload.clone(), WriteLimit::within(timeout)));
        drop(state);
        if was_empty {
            shared.wake.notify_one();
        }
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
            state.cond_waiters += 1;
            state = shared.cond.wait(state).unwrap_or_else(|e| e.into_inner());
            state.cond_waiters -= 1;
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
            FrameType::Text | FrameType::Binary => {
                self.push(frame.frame_type == FrameType::Text, frame.payload.clone(), deadline)
            }
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
            state.cond_waiters += 1;
            state = shared.cond.wait_timeout(state, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
            state.cond_waiters -= 1;
        }
    }
}

/// What the io loop does next, taken from the shared state in one lock.
struct Work {
    close: Option<(i32, String)>,
    abandoned: bool,
    can_read: bool,
    notify_cond: bool,
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
    // A batch was written since the reader was last polled.
    let mut behind = false;
    let reason = loop {
        let work = {
            let mut state = shared.lock();
            std::mem::swap(&mut state.out, &mut batch);
            Work {
                close: state.close.take(),
                abandoned: state.abandoned,
                can_read: state.inbound.is_none() && !state.inflight,
                notify_cond: state.cond_waiters > 0,
            }
        };
        if work.abandoned {
            break "connection abandoned".to_owned();
        }
        let wrote = !batch.is_empty();
        if wrote {
            if work.notify_cond {
                shared.cond.notify_all();
            }
            shared.signal_writable();
            // Frames were queued again before the loop got to wait for a message: one
            // look at the reader between the batches (see the module docs).
            if behind
                && work.can_read
                && work.close.is_none()
                && let Poll::Ready(msg) = std::future::poll_fn(|cx| Poll::Ready(ws.poll_next_unpin(cx))).await
                && let Some(err) = handle_inbound(&shared, &mut ws, msg).await
            {
                break err;
            }
            if let Err(err) = write_batch(&mut ws.get_mut().io, &mut batch).await {
                break err;
            }
            behind = true;
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
            msg = std::future::poll_fn(|cx| {
                behind = false;
                ws.poll_next_unpin(cx)
            }), if work.can_read => {
                if let Some(err) = handle_inbound(&shared, &mut ws, msg).await {
                    break err;
                }
            }
        }
    };
    shared.finish(reason);
    // Dropping the stream closes the socket.
}

/// Writes the batch to the socket, then flushes it (TLS needs that). One deadline
/// covers both, the earliest of the frames' limits, so a fan-out burst reads the
/// clock once and registers one Tokio timer instead of one per frame. Empties the
/// batch.
async fn write_batch<W>(io: &mut W, batch: &mut VecDeque<Outbound>) -> Result<(), String>
where
    W: AsyncWrite + Unpin,
{
    let Some(deadline) = batch_deadline(batch) else {
        return Ok(());
    };
    match tokio::time::timeout_at(deadline.into(), async {
        write_frames(io, batch).await?;
        io.flush().await
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(WsError::Io(e).to_string()),
        Err(_) => Err(ERR_TIMEOUT.to_owned()),
    }
}

/// Frames written by one vectored write: a full queue and a few pings.
const MAX_WRITE_FRAMES: usize = MAX_QUEUED + 2;

/// Writes the frames with vectored writes, without copying their payloads.
async fn write_frames<W>(io: &mut W, batch: &mut VecDeque<Outbound>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if !io.is_write_vectored() {
        // Such a writer takes one slice at a time: one buffer instead of a write
        // (or a TLS record) per slice.
        let bytes = batch.iter().flat_map(Outbound::parts).collect::<Vec<_>>().concat();
        let mut written = 0;
        std::future::poll_fn(|cx| poll_write_slices(Pin::new(&mut *io), cx, &mut [IoSlice::new(&bytes)], &mut written))
            .await?;
        batch.clear();
    }
    while !batch.is_empty() {
        let count = batch.len().min(MAX_WRITE_FRAMES);
        let mut written = 0;
        // The slices are gathered on every poll: kept across polls they would be
        // part of every idle connection's task.
        std::future::poll_fn(|cx| {
            let mut slices = [IoSlice::new(&[]); 2 * MAX_WRITE_FRAMES];
            let mut len = 0;
            for part in batch.iter().take(count).flat_map(Outbound::parts) {
                slices[len] = IoSlice::new(part);
                len += 1;
            }
            poll_write_slices(Pin::new(&mut *io), cx, &mut slices[..len], &mut written)
        })
        .await?;
        batch.drain(..count);
    }
    Ok(())
}

/// Writes what follows the first `written` bytes of `slices`, counting in `written`.
fn poll_write_slices<W>(
    mut io: Pin<&mut W>,
    cx: &mut Context<'_>,
    mut slices: &mut [IoSlice<'_>],
    written: &mut usize,
) -> Poll<io::Result<()>>
where
    W: AsyncWrite,
{
    IoSlice::advance_slices(&mut slices, *written);
    while !slices.is_empty() {
        let n = ready!(io.as_mut().poll_write_vectored(cx, slices))?;
        if n == 0 {
            // As tungstenite reports it.
            let e = io::Error::new(io::ErrorKind::ConnectionReset, "Connection reset while sending");
            return Poll::Ready(Err(e));
        }
        *written += n;
        IoSlice::advance_slices(&mut slices, n);
    }
    Poll::Ready(Ok(()))
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
            // The pong is queued by tungstenite: send it now. One that is not
            // written within gorilla's `writeWait` ends the connection: nothing may
            // be written after a part of it (see the module docs). gorilla's ping
            // handler also fails the read then. (It only skips a pong that cannot
            // have the writer within `writeWait`; the io loop never has a ping in
            // hand while it writes a batch.)
            return match tokio::time::timeout(PONG_DEADLINE, ws.flush()).await {
                Ok(res) => res.err().map(|e| e.to_string()),
                Err(_) => Some(ERR_TIMEOUT.to_owned()),
            };
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
/// bytes), bypassing tungstenite's reply. tungstenite has written nothing else (see
/// the module docs).
async fn write_raw_close<S>(ws: &mut WebSocketStream<GorillaFrames<S>>, code: i32, text: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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
    use std::task::Waker;

    use tokio::io::ReadBuf;
    use tungstenite::protocol::Role;

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

    /// A socket under the test's control: what the client sends and how many bytes
    /// the server may write.
    #[derive(Clone, Default)]
    struct Gate(Arc<Mutex<Wire>>);

    #[derive(Default)]
    struct Wire {
        input: VecDeque<u8>,
        out: Vec<u8>,
        allowed: usize,
        broken: bool,
        waker: Option<Waker>,
        /// Writes attempted.
        writes: usize,
        /// Reads attempted.
        reads: usize,
    }

    impl Gate {
        fn wire(&self) -> MutexGuard<'_, Wire> {
            self.0.lock().unwrap()
        }

        fn update(&self, f: impl FnOnce(&mut Wire)) {
            let mut wire = self.wire();
            f(&mut wire);
            if let Some(waker) = wire.waker.take() {
                waker.wake();
            }
        }

        /// A client frame (masked with zeros).
        fn client_sends(&self, opcode: u8, payload: &[u8]) {
            self.update(|w| {
                w.input.extend([0x80 | opcode, 0x80 | u8::try_from(payload.len()).unwrap(), 0, 0, 0, 0]);
                w.input.extend(payload);
            });
        }

        /// Lets the server write `n` more bytes.
        fn allow(&self, n: usize) {
            self.update(|w| w.allowed = w.allowed.saturating_add(n));
        }

        fn out(&self) -> Vec<u8> {
            self.wire().out.clone()
        }

        /// A connection over this socket and its io loop, not running yet.
        async fn connect(&self) -> (WsConnection, impl Future<Output = ()> + Send + use<>) {
            let stream = GorillaFrames::new(self.clone());
            let ws = WebSocketStream::from_raw_socket(stream, Role::Server, None).await;
            connect(ws, String::new())
        }

        async fn connection(&self) -> WsConnection {
            let (conn, io_loop) = self.connect().await;
            tokio::spawn(io_loop);
            conn
        }
    }

    impl AsyncRead for Gate {
        fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            let mut wire = self.wire();
            wire.reads += 1;
            if wire.input.is_empty() {
                wire.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let n = buf.remaining().min(wire.input.len());
            let bytes: Vec<u8> = wire.input.drain(..n).collect();
            buf.put_slice(&bytes);
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for Gate {
        fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            let mut wire = self.wire();
            wire.writes += 1;
            if wire.broken {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let n = wire.allowed.min(buf.len());
            if n == 0 {
                wire.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            wire.allowed -= n;
            wire.out.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    async fn until(what: &str, cond: impl Fn() -> bool) {
        let started = Instant::now();
        while !cond() {
            assert!(started.elapsed() < Duration::from_secs(10), "not reached: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The error the connection ended with.
    async fn ended(conn: &WsConnection) -> String {
        until("the end of the connection", || conn.is_closed()).await;
        conn.read().unwrap_err().0
    }

    fn text(payload: &'static str) -> Arc<SentFrame> {
        Arc::new(SentFrame::text(payload.as_bytes().to_vec()))
    }

    fn queue(conn: &WsConnection, payload: &'static str) {
        assert_eq!(Ok(true), conn.try_write_frame(&text(payload), Duration::from_secs(10)));
    }

    /// Long enough.
    const LIMIT: WriteLimit = WriteLimit::Within(Duration::from_secs(10).as_nanos() as u64);

    /// The frames tungstenite writes for `messages`.
    fn tungstenite_bytes(messages: impl IntoIterator<Item = Message>) -> Vec<u8> {
        let mut ws = tungstenite::WebSocket::from_raw_socket(io::Cursor::new(Vec::new()), Role::Server, None);
        for message in messages {
            ws.send(message).unwrap();
        }
        ws.into_inner().into_inner()
    }

    /// `len` payload bytes that differ from their neighbours.
    fn payload(len: usize) -> Bytes {
        (0..len).map(|i| b'a' + (i % 23) as u8).collect::<Vec<_>>().into()
    }

    /// The lengths around which the header's size changes.
    const EDGES: [usize; 6] = [0, 1, 125, 126, 65535, 65536];

    #[tokio::test]
    async fn frames_are_tungstenites_byte_for_byte() {
        for len in EDGES {
            for text in [true, false] {
                let message = if text {
                    Message::Text(Utf8Bytes::try_from(payload(len)).unwrap())
                } else {
                    Message::Binary(payload(len))
                };
                let mut out = Vec::new();
                let mut batch = VecDeque::from([Outbound::data(text, payload(len), LIMIT)]);
                write_batch(&mut out, &mut batch).await.unwrap();
                assert!(batch.is_empty());
                assert!(tungstenite_bytes([message]) == out, "text={text} len={len}");
            }
        }
        let mut out = Vec::new();
        write_batch(&mut out, &mut VecDeque::from([Outbound::ping(LIMIT)])).await.unwrap();
        assert_eq!(tungstenite_bytes([Message::Ping(Bytes::new())]), out);
        assert_eq!(vec![0x89, 0x00], out);
    }

    /// A socket that takes at most `first` bytes in the first write and `chunk` in
    /// the others, and is not ready every other time.
    struct Choppy {
        out: Vec<u8>,
        first: usize,
        chunk: usize,
        vectored: bool,
        ready: bool,
        writes: usize,
        /// Bytes written before the last flush.
        flushed: usize,
        /// Flushing never completes.
        stuck: bool,
    }

    impl Choppy {
        fn new(chunk: usize, vectored: bool) -> Self {
            Self { out: Vec::new(), first: chunk, chunk, vectored, ready: false, writes: 0, flushed: 0, stuck: false }
        }

        fn take(&mut self, cx: &mut Context<'_>, slices: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
            self.ready = !self.ready;
            if !self.ready {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let limit = if self.writes == 0 { self.first } else { self.chunk };
            self.writes += 1;
            let before = self.out.len();
            for slice in slices {
                let n = slice.len().min(limit - (self.out.len() - before));
                self.out.extend_from_slice(&slice[..n]);
            }
            Poll::Ready(Ok(self.out.len() - before))
        }
    }

    impl AsyncWrite for Choppy {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            self.take(cx, &[IoSlice::new(buf)])
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            // As tokio's default implementation, unless vectored.
            let first = bufs.iter().position(|b| !b.is_empty()).unwrap_or(0);
            let bufs = if self.vectored { bufs } else { &bufs[first..=first] };
            self.take(cx, bufs)
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }

        fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushed = self.out.len();
            if self.stuck { Poll::Pending } else { Poll::Ready(Ok(())) }
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Payload lengths of full batches with every header size in several positions.
    const BATCHES: [[usize; MAX_QUEUED]; 3] =
        [[0, 1, 125, 126, 300, 0, 2, 127], [70_000, 3, 126, 0, 65_536, 65_535, 1, 5], [5; MAX_QUEUED]];

    fn batch_of(lens: &[usize]) -> (VecDeque<Outbound>, Vec<u8>) {
        let batch = lens.iter().enumerate().map(|(i, len)| Outbound::data(i % 2 == 1, payload(*len), LIMIT)).collect();
        let messages = lens.iter().enumerate().map(|(i, len)| match i % 2 {
            1 => Message::Text(Utf8Bytes::try_from(payload(*len)).unwrap()),
            _ => Message::Binary(payload(*len)),
        });
        (batch, tungstenite_bytes(messages))
    }

    async fn check_partial(lens: &[usize], mut io: Choppy) {
        let (first, chunk, vectored) = (io.first, io.chunk, io.vectored);
        let (mut batch, expected) = batch_of(lens);
        write_batch(&mut io, &mut batch).await.unwrap();
        assert!(expected == io.out, "{lens:?} vectored={vectored} first={first} chunk={chunk}");
        assert_eq!(expected.len(), io.flushed, "flushed after the last write");
        assert!(batch.is_empty());
    }

    #[tokio::test]
    async fn partial_writes_lose_and_repeat_nothing() {
        for vectored in [true, false] {
            for lens in BATCHES {
                let (_, expected) = batch_of(&lens);
                let chunks = if expected.len() < 1000 { 1..expected.len() + 1 } else { 4095..4097 };
                for chunk in chunks {
                    check_partial(&lens, Choppy::new(chunk, vectored)).await;
                }
                // One split, at every byte of every header and around it.
                let mut start = 0;
                for len in lens {
                    let header = FrameHeader::new(FrameHeader::TEXT, len).bytes().len();
                    for first in start..=start + header + 1 {
                        let io = Choppy { first, ..Choppy::new(usize::MAX, vectored) };
                        if (1..=expected.len()).contains(&first) {
                            check_partial(&lens, io).await;
                        }
                    }
                    start += header + len;
                }
                assert_eq!(expected.len(), start);
            }
        }
    }

    /// A batch is one write, also where only single buffers can be written, and
    /// with pings beyond the queue's limit still few.
    #[tokio::test]
    async fn a_batch_is_one_write() {
        for vectored in [true, false] {
            for lens in BATCHES {
                let (mut batch, expected) = batch_of(&lens);
                let mut io = Choppy::new(usize::MAX, vectored);
                write_batch(&mut io, &mut batch).await.unwrap();
                assert!(expected == io.out);
                assert_eq!(1, io.writes, "{lens:?} vectored={vectored}");
            }
        }

        let (mut batch, mut expected) = batch_of(&BATCHES[0]);
        for _ in 0..2 * MAX_WRITE_FRAMES {
            batch.push_back(Outbound::ping(LIMIT));
            expected.extend([0x89, 0]);
        }
        let mut io = Choppy::new(usize::MAX, true);
        write_batch(&mut io, &mut batch).await.unwrap();
        assert_eq!((expected, 3), (io.out, io.writes));
    }

    /// tokio-rustls takes the slices of a batch together, and keeps the records a
    /// full socket did not take until it is flushed: the client must get them all.
    #[tokio::test]
    async fn batches_are_flushed_through_tls() {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
        use tokio::io::AsyncReadExt;

        let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");
        let cert = CertificateDer::from_pem_file(format!("{fixtures}/server.crt")).unwrap();
        let key = PrivateKeyDer::from_pem_file(format!("{fixtures}/server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let server = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key)
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();

        // Smaller than a TLS record, so that writes are often partial.
        let (near, far) = tokio::io::duplex(1024);
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client));
        let client = connector.connect(ServerName::try_from("localhost").unwrap(), far);
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
        let (client, server) = tokio::join!(client, acceptor.accept(near));
        let (mut client, mut server) = (client.unwrap(), server.unwrap());
        assert!(server.is_write_vectored());

        for lens in BATCHES {
            let (mut batch, expected) = batch_of(&lens);
            let mut got = vec![0; expected.len()];
            let (wrote, read) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(write_batch(&mut server, &mut batch), client.read_exact(&mut got))
            })
            .await
            .expect("the whole batch arrives");
            wrote.unwrap();
            read.unwrap();
            assert!(expected == got, "{lens:?}");
        }
    }

    #[tokio::test]
    async fn a_write_of_nothing_is_an_error() {
        for vectored in [true, false] {
            let (mut batch, _) = batch_of(&[1, 2]);
            let err = write_batch(&mut Choppy::new(0, vectored), &mut batch).await;
            assert_eq!(Err("IO error: Connection reset while sending".to_owned()), err);
        }
    }

    /// The timeout of a queued frame starts with its write, not while it waits.
    #[tokio::test]
    async fn write_timeouts_start_with_the_write() {
        let timeout = Duration::from_millis(200);
        let gate = Gate::default();
        let (conn, io_loop) = gate.connect().await;
        assert_eq!(Ok(true), conn.try_write_frame(&text("data"), timeout));
        tokio::time::sleep(2 * timeout).await;

        tokio::spawn(io_loop);
        until("a write", || gate.wire().writes > 0).await;
        gate.allow(usize::MAX);
        until("the data", || gate.out() == b"\x81\x04data").await;
        assert!(!conn.is_closed());
    }

    /// Each batch has the whole timeout: a client that keeps taking them is not
    /// dropped, however long a frame waited behind another batch.
    #[tokio::test]
    async fn a_slow_client_that_takes_every_batch_in_time_stays() {
        let timeout = Duration::from_secs(1);
        let stall = Duration::from_millis(600);
        let gate = Gate::default();
        let conn = gate.connection().await;
        assert_eq!(Ok(true), conn.try_write_frame(&text("first"), timeout));
        until("a write", || gate.wire().writes > 0).await;
        for _ in 0..MAX_QUEUED {
            assert_eq!(Ok(true), conn.try_write_frame(&text("more"), timeout));
        }
        assert_eq!(Ok(false), conn.try_write_frame(&text("full"), timeout));

        let mut expected = b"\x81\x05first".to_vec();
        tokio::time::sleep(stall).await;
        gate.allow(expected.len());
        until("the first frame", || gate.out() == expected).await;
        tokio::time::sleep(stall).await;
        gate.allow(usize::MAX);
        expected.extend(b"\x81\x04more".repeat(MAX_QUEUED));
        until("the other frames", || gate.out() == expected).await;
        assert!(!conn.is_closed());
    }

    /// A client that does not take a batch within the timeout is dropped, with the
    /// frames behind it, even if it takes some bytes all along.
    #[tokio::test]
    async fn a_client_that_does_not_take_a_batch_in_time_is_dropped() {
        let timeout = Duration::from_millis(400);
        for trickle in [false, true] {
            let gate = Gate::default();
            let conn = gate.connection().await;
            let started = Instant::now();
            assert_eq!(Ok(true), conn.try_write_frame(&text("a frame with a few bytes"), timeout));
            until("a write", || gate.wire().writes > 0).await;
            assert_eq!(Ok(true), conn.try_write_frame(&text("behind"), timeout));
            while !conn.is_closed() && started.elapsed() < Duration::from_secs(5) {
                tokio::time::sleep(Duration::from_millis(40)).await;
                gate.allow(usize::from(trickle));
            }
            assert_eq!("i/o timeout", ended(&conn).await, "trickle={trickle}");
            let after = started.elapsed();
            assert!(after >= timeout && after < timeout + Duration::from_secs(2), "dropped after {after:?}");
            assert_eq!(trickle, !gate.out().is_empty());
            assert_eq!(
                Err(ConnectionError("i/o timeout".into())),
                conn.try_write_frame(&text("late"), timeout),
                "trickle={trickle}"
            );
        }
    }

    /// How long a batch may take on a socket that takes nothing.
    async fn stalled_batch(name: &str, batch: impl IntoIterator<Item = Outbound>) -> Duration {
        let started = Instant::now();
        let mut batch = batch.into_iter().collect();
        let res = tokio::time::timeout(Duration::from_secs(10), write_batch(&mut Gate::default(), &mut batch)).await;
        assert_eq!(Ok(Err("i/o timeout".to_owned())), res, "{name}: the batch fails by itself");
        started.elapsed()
    }

    /// The flush (where TLS writes what the socket did not take) counts.
    #[tokio::test]
    async fn the_limit_covers_the_flush() {
        let limit = Duration::from_millis(200);
        let started = Instant::now();
        let mut io = Choppy { stuck: true, ..Choppy::new(usize::MAX, true) };
        let mut batch = [Outbound::data(true, payload(4), WriteLimit::within(limit))].into();
        let res = tokio::time::timeout(Duration::from_secs(10), write_batch(&mut io, &mut batch)).await;
        assert_eq!(Ok(Err("i/o timeout".to_owned())), res, "the flush ends with the limit");
        assert!(started.elapsed() >= limit);
        assert_eq!(b"\x81\x04abcd".to_vec(), io.out, "only the flush was pending");
    }

    /// The earliest limit of a batch bounds it, wherever its frame is.
    #[tokio::test]
    async fn the_earliest_limit_bounds_a_batch() {
        const SHORT: Duration = Duration::from_millis(200);
        const LONG: Duration = Duration::from_secs(3600);
        // Built when its batch starts, as a ping's deadline is absolute.
        let data: fn(Duration) -> Outbound = |t| Outbound::data(true, payload(5), WriteLimit::within(t));
        let ping: fn(Duration) -> Outbound = |t| Outbound::ping(WriteLimit::Until(Instant::now() + t));
        for (name, batch) in [
            ("ping last and shorter", [(data, LONG), (data, LONG), (ping, SHORT)]),
            ("ping first and shorter", [(ping, SHORT), (data, LONG), (data, LONG)]),
            ("ping last and longer", [(data, SHORT), (data, SHORT), (ping, LONG)]),
            ("ping first and longer", [(ping, LONG), (data, SHORT), (data, SHORT)]),
            ("shorter ping first", [(ping, SHORT), (data, LONG), (ping, LONG)]),
            ("shorter ping last", [(ping, LONG), (data, LONG), (ping, SHORT)]),
            ("shorter data first", [(data, SHORT), (data, LONG), (data, LONG)]),
            ("shorter data in the middle", [(data, LONG), (data, SHORT), (data, LONG)]),
            ("shorter data last", [(data, LONG), (data, LONG), (data, SHORT)]),
        ] {
            let after = stalled_batch(name, batch.map(|(frame, limit)| frame(limit))).await;
            assert!(after >= SHORT, "{name}: {after:?}");
        }
        assert!(stalled_batch("ping alone", [ping(SHORT)]).await >= SHORT);
    }

    /// A ping that waited past its deadline fails the connection, with the data in
    /// its batch, unless the socket takes the batch at once.
    #[tokio::test]
    async fn an_overdue_ping_fails_a_batch_that_cannot_be_written_at_once() {
        let batch = |overdue: Instant| {
            let data = || Outbound::data(true, payload(4), LIMIT);
            [data(), Outbound::ping(WriteLimit::Until(overdue)), data()]
        };
        let overdue = Instant::now();
        tokio::time::sleep(Duration::from_millis(5)).await;
        let after = stalled_batch("overdue ping", batch(overdue)).await;
        assert!(after < Duration::from_secs(5), "at once, not after the data's limit: {after:?}");

        let mut gate = Gate::default();
        gate.allow(usize::MAX);
        write_batch(&mut gate, &mut batch(overdue).into()).await.unwrap();
        assert_eq!(b"\x81\x04abcd\x89\x00\x81\x04abcd".to_vec(), gate.out());
    }

    /// A connection that had frames queued keeps room for several.
    #[test]
    fn queued_frames_stay_small() {
        assert!(size_of::<Outbound>() <= 64, "{}", size_of::<Outbound>());
    }

    #[tokio::test]
    async fn endless_write_timeouts_do_not_overflow() {
        let mut batch = [Outbound::data(true, Bytes::new(), WriteLimit::within(Duration::MAX))].into();
        assert!(batch_deadline(&batch).unwrap() > Instant::now() + Duration::from_secs(3600));
        // Not ready at first, so that the timer is armed.
        write_batch(&mut Choppy::new(usize::MAX, true), &mut batch).await.unwrap();
        assert_eq!(None, batch_deadline(&batch));
    }

    /// A pong that is not written within a second (gorilla's `writeWait`) ends the
    /// connection, as in Go: nothing may follow the part that was written.
    #[tokio::test]
    async fn a_pong_that_cannot_be_written_ends_the_connection() {
        let mut fastest = Duration::MAX;
        // Again only if the machine was too busy to tell one second from two.
        for _ in 0..3 {
            let gate = Gate::default();
            gate.allow(1);
            let conn = gate.connection().await;
            let started = Instant::now();
            gate.client_sends(9, b"hi");
            until("the start of the pong", || gate.out() == [0x8a]).await;
            queue(&conn, "data");

            assert_eq!("i/o timeout", ended(&conn).await);
            let after = started.elapsed();
            assert!(after >= Duration::from_secs(1), "ended after {after:?}");
            gate.allow(usize::MAX);
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(vec![0x8a], gate.out(), "no frame inside the pong");

            fastest = fastest.min(after);
            if fastest < Duration::from_millis(1250) {
                return;
            }
        }
        panic!("ended after {fastest:?} at best, not a second");
    }

    #[tokio::test]
    async fn a_pong_that_fails_ends_the_connection() {
        let gate = Gate::default();
        gate.update(|w| w.broken = true);
        let conn = gate.connection().await;
        gate.client_sends(9, b"hi");
        assert_eq!("IO error: broken pipe", ended(&conn).await);
        assert!(gate.out().is_empty());
    }

    /// Frames queued while a pong is written follow it.
    #[tokio::test]
    async fn frames_follow_a_slow_pong() {
        let gate = Gate::default();
        gate.allow(1);
        let conn = gate.connection().await;
        gate.client_sends(9, b"hi");
        until("the start of the pong", || gate.out() == [0x8a]).await;
        queue(&conn, "data");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(vec![0x8a], gate.out());

        gate.allow(usize::MAX);
        until("the data", || gate.out() == b"\x8a\x02hi\x81\x04data").await;
        assert!(!conn.is_closed());
    }

    /// Records the messages delivered, and what the client had been sent by then.
    struct Inbox {
        gate: Gate,
        messages: Mutex<Vec<(Bytes, Vec<u8>)>>,
        /// Keeps the permits (the messages are being executed).
        busy: std::sync::atomic::AtomicBool,
        permits: Mutex<Vec<ReadPermit>>,
    }

    impl Inbox {
        fn of(gate: &Gate, conn: &WsConnection) -> Arc<Inbox> {
            let inbox = Arc::new(Inbox {
                gate: gate.clone(),
                messages: Mutex::default(),
                busy: false.into(),
                permits: Mutex::default(),
            });
            conn.set_inbound_handler(inbox.clone());
            inbox
        }

        fn messages(&self) -> Vec<(Bytes, Vec<u8>)> {
            self.messages.lock().unwrap().clone()
        }
    }

    impl InboundHandler for Inbox {
        fn on_message(&self, msg: Bytes, permit: ReadPermit) {
            let sent = self.gate.out();
            self.messages.lock().unwrap().push((msg, sent));
            if self.busy.load(std::sync::atomic::Ordering::SeqCst) {
                self.permits.lock().unwrap().push(permit);
            }
        }

        fn on_end(&self, _err: ConnectionError) {}
    }

    /// The bytes of text frames.
    fn texts<'a>(payloads: impl IntoIterator<Item = &'a str>) -> Vec<u8> {
        payloads.into_iter().flat_map(|p| [&[0x81, p.len() as u8], p.as_bytes()].concat()).collect()
    }

    /// A connection that is writing "one" (the socket takes nothing) with "two" queued.
    async fn behind(gate: &Gate) -> (WsConnection, Arc<Inbox>) {
        let conn = gate.connection().await;
        let inbox = Inbox::of(gate, &conn);
        queue(&conn, "one");
        until("a write", || gate.wire().writes > 0).await;
        queue(&conn, "two");
        (conn, inbox)
    }

    /// Queues `frames` frames, refilling the queue whenever the io loop has taken it, as
    /// a session with a backlog does. Returns how many are left.
    fn refill_queue(conn: &WsConnection, frames: usize) -> Arc<Mutex<usize>> {
        let left = Arc::new(Mutex::new(frames));
        let refill = {
            let (owner, left) = (Arc::downgrade(&conn.owner), Arc::clone(&left));
            move || {
                let Some(owner) = owner.upgrade() else { return };
                let conn = WsConnection { owner };
                let mut left = left.lock().unwrap();
                while *left > 0 && conn.try_write_frame(&text("frame"), Duration::from_secs(10)) == Ok(true) {
                    *left -= 1;
                }
            }
        };
        conn.set_writable_handler(Box::new(refill.clone()));
        refill();
        left
    }

    /// Deviation D-072: while a batch of frames for a client is being written, its
    /// messages stay unread. They are delivered before the next batch.
    #[tokio::test]
    async fn deviation_d072_messages_wait_for_the_batch_being_written() {
        let gate = Gate::default();
        let (_conn, inbox) = behind(&gate).await;
        gate.client_sends(1, b"hello");
        let unread = gate.wire().input.len();
        for part in [0, 1] {
            gate.allow(part);
            tokio::time::sleep(Duration::from_millis(30)).await;
            assert_eq!(part, gate.out().len());
            assert_eq!((vec![], unread), (inbox.messages(), gate.wire().input.len()));
        }

        gate.allow(texts(["one"]).len() - 1);
        until("the message", || !inbox.messages().is_empty()).await;
        assert_eq!(vec![(Bytes::from_static(b"hello"), texts(["one"]))], inbox.messages());
        assert_eq!(texts(["one"]), gate.out());
        gate.allow(usize::MAX);
        until("the other frame", || gate.out() == texts(["one", "two"])).await;
    }

    /// A client whose queue is never empty has one message handled per batch written,
    /// in the order it sent them.
    #[tokio::test]
    async fn a_client_with_a_backlog_is_read_one_message_per_batch() {
        let gate = Gate::default();
        let (conn, inbox) = behind(&gate).await;
        for message in [b"m0", b"m1", b"m2", b"m3"] {
            gate.client_sends(1, message);
        }
        let mut written = vec!["one"];
        for (i, message) in [b"m0", b"m1", b"m2", b"m3"].iter().enumerate() {
            let frame = texts(["one"]).len();
            assert_eq!(i, inbox.messages().len(), "one message per batch");
            gate.allow(frame);
            until("the message", || inbox.messages().len() > i).await;
            // The next batch is being written; queue one behind it.
            assert_eq!((Bytes::from_static(*message), texts(written.iter().copied())), inbox.messages()[i]);
            queue(&conn, "two");
            written.push("two");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(4, inbox.messages().len());
    }

    /// A message that is still being executed holds the next one back, also between
    /// batches; the frames go on.
    #[tokio::test]
    async fn a_message_in_progress_holds_the_next_one_back_between_batches() {
        let gate = Gate::default();
        let (conn, inbox) = behind(&gate).await;
        inbox.busy.store(true, std::sync::atomic::Ordering::SeqCst);
        gate.client_sends(1, b"m0");
        gate.client_sends(1, b"m1");
        let frame = texts(["one"]).len();
        for batch in 1..=5 {
            gate.allow(frame);
            until("the batch", || gate.out().len() == batch * frame).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(1, inbox.messages().len());
            queue(&conn, "two");
        }
        inbox.permits.lock().unwrap().clear();
        gate.allow(frame);
        until("the other message", || inbox.messages().len() == 2).await;
    }

    /// A client that sends without pause does not hold its frames back: between two of
    /// its messages a whole batch is written.
    #[tokio::test]
    async fn a_flood_of_messages_does_not_stop_frames() {
        const FRAMES: usize = 400;
        let gate = Gate::default();
        gate.allow(usize::MAX);
        for _ in 0..2 * FRAMES {
            gate.client_sends(1, b"flood");
        }
        let (conn, io_loop) = gate.connect().await;
        let inbox = Inbox::of(&gate, &conn);
        refill_queue(&conn, FRAMES);
        tokio::spawn(io_loop);
        let all = texts(std::iter::repeat_n("frame", FRAMES));
        until("every frame", || gate.out() == all).await;

        // A batch is a full queue; all but the first follow a message.
        let sent: Vec<usize> = inbox.messages().iter().map(|(_, sent)| sent.len()).filter(|n| *n < all.len()).collect();
        assert!(sent.is_sorted_by(|a, b| a < b), "no two messages without frames in between: {sent:?}");
        let batches = FRAMES / MAX_QUEUED;
        assert_eq!(batches - 1, sent.len(), "the client is read all along");
    }

    /// A ping between two batches is answered between them, and pongs are handled.
    #[tokio::test]
    async fn pings_and_pongs_are_handled_between_batches() {
        let gate = Gate::default();
        let (conn, inbox) = behind(&gate).await;
        let pongs = Arc::new(Mutex::new(Vec::new()));
        let (seen, socket) = (Arc::clone(&pongs), gate.clone());
        conn.set_pong_handler(move || seen.lock().unwrap().push(socket.out().len()));
        gate.client_sends(9, b"hi");
        gate.client_sends(10, b"");
        gate.allow(texts(["one"]).len() + b"\x8a\x02hi".len());
        let one = [&texts(["one"])[..], b"\x8a\x02hi"].concat();
        until("the pong", || gate.out() == one).await;
        // "two" is being written.
        tokio::time::sleep(Duration::from_millis(10)).await;
        queue(&conn, "three");
        gate.allow(texts(["two"]).len());
        let two = [&one[..], &texts(["two"])].concat();
        until("the pong handler", || !pongs.lock().unwrap().is_empty()).await;
        assert_eq!((vec![two.len()], two), (pongs.lock().unwrap().clone(), gate.out()));
        assert!(inbox.messages().is_empty() && !conn.is_closed());
    }

    /// A pong that cannot be written ends the connection between two batches as well:
    /// the next batch does not land inside it.
    #[tokio::test]
    async fn a_pong_stuck_between_batches_ends_the_connection() {
        let gate = Gate::default();
        let (conn, _inbox) = behind(&gate).await;
        gate.client_sends(9, b"hi");
        gate.allow(texts(["one"]).len() + 1);
        assert_eq!("i/o timeout", ended(&conn).await);
        gate.allow(usize::MAX);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!([&texts(["one"])[..], &[0x8a]].concat(), gate.out());
    }

    /// A close frame between two batches is answered before the next batch, without the frames that
    /// are still queued.
    #[tokio::test]
    async fn a_close_frame_between_batches_ends_the_connection() {
        let gate = Gate::default();
        let (conn, _inbox) = behind(&gate).await;
        gate.client_sends(8, b"\x03\xe8");
        gate.allow(usize::MAX);
        assert_eq!("websocket: close 1000 (normal)", ended(&conn).await);
        assert_eq!([&texts(["one"])[..], b"\x88\x02\x03\xe8"].concat(), gate.out());
    }

    /// Once a close frame is requested, the queued frames and it are written and
    /// nothing more is read.
    #[tokio::test]
    async fn nothing_is_read_once_a_close_is_requested() {
        let gate = Gate::default();
        let (conn, inbox) = behind(&gate).await;
        gate.client_sends(1, b"late");
        assert!(conn.request_close(CLOSE_NORMAL_CLOSURE, ""));
        gate.allow(usize::MAX);
        assert_eq!("websocket: close sent", ended(&conn).await);
        assert_eq!([&texts(["one", "two"])[..], b"\x88\x02\x03\xe8"].concat(), gate.out());
        assert!(inbox.messages().is_empty());
    }

    /// Looking at the reader between batches costs a connection that keeps up
    /// nothing: the reader is polled once per batch, when the loop waits for a message,
    /// and once per batch at most otherwise.
    #[tokio::test]
    async fn the_reader_is_polled_once_per_batch() {
        let gate = Gate::default();
        gate.allow(usize::MAX);
        let conn = gate.connection().await;
        until("the loop to wait", || gate.wire().reads > 0).await;
        for i in 1..=20 {
            let reads = gate.wire().reads;
            let out = gate.out().len();
            queue(&conn, "frame");
            until("the frame", || gate.out().len() > out).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert_eq!(reads + 1, gate.wire().reads, "frame {i}");
        }

        let gate = Gate::default();
        let (conn, _inbox) = behind(&gate).await;
        assert_eq!(0, gate.wire().reads, "the first batch comes first");
        for batch in 1..=20 {
            gate.allow(texts(["one"]).len());
            until("the next batch", || gate.out().len() == batch * texts(["one"]).len()).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert_eq!(batch, gate.wire().reads);
            queue(&conn, "two");
        }
    }

    /// tokio's readiness flag makes a look at a socket without data free of system
    /// calls: this stream reads as `TcpStream` does, counting the calls.
    #[tokio::test]
    async fn looking_at_a_socket_without_data_makes_no_system_call() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::AsyncReadExt;
        use tokio::net::{TcpListener, TcpStream};

        struct Counted {
            stream: TcpStream,
            /// Calls of `poll_read`, and `read` system calls.
            counts: Arc<[AtomicUsize; 2]>,
        }

        impl AsyncRead for Counted {
            fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
                self.counts[0].fetch_add(1, Ordering::SeqCst);
                loop {
                    ready!(self.stream.poll_read_ready(cx))?;
                    self.counts[1].fetch_add(1, Ordering::SeqCst);
                    match self.stream.try_read(buf.initialize_unfilled()) {
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        res => {
                            buf.advance(res?);
                            return Poll::Ready(Ok(()));
                        }
                    }
                }
            }
        }

        impl AsyncWrite for Counted {
            fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
                Pin::new(&mut self.stream).poll_write(cx, buf)
            }

            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_flush(cx)
            }

            fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Pin::new(&mut self.stream).poll_shutdown(cx)
            }
        }

        const FRAMES: usize = 400;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap());
        let (client, server) = tokio::join!(client, listener.accept());
        let mut client = client.unwrap();
        let counts = Arc::new([const { AtomicUsize::new(0) }; 2]);
        let io = Counted { stream: server.unwrap().0, counts: Arc::clone(&counts) };
        let ws = WebSocketStream::from_raw_socket(GorillaFrames::new(io), Role::Server, None).await;
        let (conn, io_loop) = connect(ws, String::new());
        let left = refill_queue(&conn, FRAMES);
        tokio::spawn(io_loop);
        let expected = texts(std::iter::repeat_n("frame", FRAMES));
        let mut got = vec![0; expected.len()];
        tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut got)).await.unwrap().unwrap();
        assert_eq!(expected, got);
        assert_eq!(0, *left.lock().unwrap());

        let [polls, reads] = [0, 1].map(|i| counts[i].load(Ordering::SeqCst));
        // One look between every two batches, and one before waiting.
        assert!(polls >= FRAMES / MAX_QUEUED - 1, "{polls} looks");
        // Tokio takes an accepted socket to be readable until its first read says otherwise.
        assert!(reads <= 1, "{reads} system calls for {polls} looks");
    }

    /// Deviation D-072: messages received while a write is blocked are not delivered
    /// if that write fails.
    #[tokio::test]
    async fn deviation_d072_messages_behind_a_failed_write_are_not_delivered() {
        let gate = Gate::default();
        let conn = gate.connection().await;
        let inbox = Inbox::of(&gate, &conn);
        assert_eq!(Ok(true), conn.try_write_frame(&text("data"), Duration::from_millis(100)));
        until("a write", || gate.wire().writes > 0).await;
        gate.client_sends(1, b"hello");
        let unread = gate.wire().input.len();

        assert_eq!("i/o timeout", ended(&conn).await);
        gate.allow(usize::MAX);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!((vec![], unread), (inbox.messages(), gate.wire().input.len()));
    }
}
