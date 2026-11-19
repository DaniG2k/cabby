//! Go-style response writing for long-running handlers (SSE, long polling):
//! [`ResponseWriter`] (Go's `http.ResponseWriter` plus `http.Flusher`) and the
//! adapter serving such a handler on the hyper server ([`channel_response`]).
//!
//! # Design
//!
//! The SSE and Durable Streams handlers are written like their Go counterparts:
//! headers are edited, a status line is sent, and body bytes are written and
//! flushed by whoever holds the writer, which includes the session's writer on
//! the node's task pool. The writer is shared ([`SharedResponseWriter`]) between
//! the handler and the transport connection, so writes from both sides stay
//! ordered under one lock.
//!
//! On the server, [`ChannelResponseWriter`] turns these calls into a hyper
//! response: the head goes out with the first status line, write or flush; body
//! bytes are queued for a streaming [`Body`]. The queue is bounded
//! ([`MAX_BUFFERED`] bytes): a writer that finds it full waits for the client to
//! catch up, at most [`WRITE_TIMEOUT`], then fails the write (the session then
//! disconnects, as for a failed socket write). Writers always run on blocking
//! threads (handler setup, session writers), never on the async executor.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Frame;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{Body, BoxError, Response};

/// Go's `http.ResponseWriter` plus `http.Flusher`.
pub trait ResponseWriter: Send {
    /// The headers sent with the status line (edits after it have no effect).
    fn headers_mut(&mut self) -> &mut http::HeaderMap;
    /// Sends the status line (Go `WriteHeader`; only the first call has effect).
    fn write_header(&mut self, status: u16);
    /// Writes body bytes (implicitly `write_header(200)` first).
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize>;
    /// Sends buffered data to the client (implicitly `write_header(200)` first).
    fn flush(&mut self);
    /// The handler returned: complete the response (Go's server does this after
    /// `ServeHTTP`). Later calls have no effect on the response.
    fn finish(&mut self) {}
}

impl<W: ResponseWriter + ?Sized> ResponseWriter for Box<W> {
    fn headers_mut(&mut self) -> &mut http::HeaderMap {
        (**self).headers_mut()
    }

    fn write_header(&mut self, status: u16) {
        (**self).write_header(status);
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        (**self).write(data)
    }

    fn flush(&mut self) {
        (**self).flush();
    }

    fn finish(&mut self) {
        (**self).finish();
    }
}

/// A response writer shared by a handler and its transport connection.
pub type SharedResponseWriter = Arc<Mutex<dyn ResponseWriter>>;

/// Wraps a writer for sharing.
pub fn shared_writer(w: Box<dyn ResponseWriter>) -> SharedResponseWriter {
    Arc::new(Mutex::new(w))
}

/// Locks a shared writer (a panic while it was held does not poison it for others).
pub fn lock_writer(w: &SharedResponseWriter) -> MutexGuard<'_, dyn ResponseWriter + 'static> {
    w.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Body bytes queued for a slow client before writers wait.
pub const MAX_BUFFERED: usize = 1024 * 1024;

/// How long a writer waits for a full queue to drain before the write fails.
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
struct ChannelState {
    chunks: VecDeque<Bytes>,
    buffered: usize,
    /// The handler finished: the body ends once the queue is drained.
    finished: bool,
    /// The client is gone (the body or the pending response was dropped).
    gone: bool,
    waker: Option<Waker>,
}

struct ChannelShared {
    state: Mutex<ChannelState>,
    space: Condvar,
    /// [`MAX_BUFFERED`] and [`WRITE_TIMEOUT`] (smaller in tests).
    max_buffered: usize,
    write_timeout: Duration,
    /// Cancelled when the client goes away (Go's request context).
    client_gone: CancellationToken,
}

impl ChannelShared {
    fn state(&self) -> MutexGuard<'_, ChannelState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn mark_gone(&self) {
        let waker = {
            let mut s = self.state();
            s.gone = true;
            s.chunks.clear();
            s.buffered = 0;
            s.waker.take()
        };
        self.space.notify_all();
        self.client_gone.cancel();
        drop(waker);
    }
}

/// A [`ResponseWriter`] producing a hyper response (see the module documentation).
pub struct ChannelResponseWriter {
    headers: http::HeaderMap,
    head: Option<oneshot::Sender<http::response::Parts>>,
    shared: Arc<ChannelShared>,
    finished: bool,
}

impl ChannelResponseWriter {
    fn send_head(&mut self, status: u16) {
        let Some(tx) = self.head.take() else { return };
        let mut resp = http::Response::new(());
        *resp.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
        *resp.headers_mut() = std::mem::take(&mut self.headers);
        let (parts, ()) = resp.into_parts();
        // The receiver is gone when the client went away before the head.
        let _ = tx.send(parts);
    }
}

impl ResponseWriter for ChannelResponseWriter {
    fn headers_mut(&mut self) -> &mut http::HeaderMap {
        &mut self.headers
    }

    fn write_header(&mut self, status: u16) {
        self.send_head(status);
    }

    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.send_head(200);
        if self.finished {
            return Err(std::io::Error::other("http: response already finished"));
        }
        if data.is_empty() {
            return Ok(0);
        }
        let deadline = Instant::now() + self.shared.write_timeout;
        let mut s = self.shared.state();
        loop {
            if s.gone {
                return Err(std::io::ErrorKind::BrokenPipe.into());
            }
            if s.buffered < self.shared.max_buffered {
                break;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(std::io::ErrorKind::TimedOut.into());
            }
            s = self.shared.space.wait_timeout(s, left).unwrap_or_else(PoisonError::into_inner).0;
        }
        s.buffered += data.len();
        s.chunks.push_back(Bytes::copy_from_slice(data));
        let waker = s.waker.take();
        drop(s);
        if let Some(w) = waker {
            w.wake();
        }
        Ok(data.len())
    }

    fn flush(&mut self) {
        // Queued bytes are sent as soon as the body is polled.
        self.send_head(200);
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        self.send_head(200);
        self.finished = true;
        let waker = {
            let mut s = self.shared.state();
            s.finished = true;
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

impl Drop for ChannelResponseWriter {
    fn drop(&mut self) {
        self.finish();
    }
}

/// The streaming body of a [`ChannelResponseWriter`]'s response.
struct ChannelBody {
    shared: Arc<ChannelShared>,
}

impl hyper::body::Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        let mut s = self.shared.state();
        if let Some(chunk) = s.chunks.pop_front() {
            s.buffered -= chunk.len();
            drop(s);
            self.shared.space.notify_all();
            return Poll::Ready(Some(Ok(Frame::data(chunk))));
        }
        if s.finished || s.gone {
            return Poll::Ready(None);
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        let s = self.shared.state();
        s.chunks.is_empty() && (s.finished || s.gone)
    }
}

impl Drop for ChannelBody {
    fn drop(&mut self) {
        self.shared.mark_gone();
    }
}

/// Cancels the client token if the pending response is dropped (the client went
/// away before the head was sent), unless disarmed.
struct PendingGuard(Option<Arc<ChannelShared>>);

impl PendingGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if let Some(shared) = self.0.take() {
            shared.mark_gone();
        }
    }
}

/// A writer, the token cancelled when the client goes away, and the future of the
/// response the writer produces (it resolves once the head is sent; the body
/// streams what is written afterwards and ends when the writer finishes or is
/// dropped).
pub fn channel_response()
-> (ChannelResponseWriter, CancellationToken, impl std::future::Future<Output = Response> + Send + 'static) {
    channel_response_with_limits(MAX_BUFFERED, WRITE_TIMEOUT)
}

fn channel_response_with_limits(
    max_buffered: usize,
    write_timeout: Duration,
) -> (ChannelResponseWriter, CancellationToken, impl std::future::Future<Output = Response> + Send + 'static) {
    let (tx, rx) = oneshot::channel();
    let shared = Arc::new(ChannelShared {
        state: Mutex::new(ChannelState::default()),
        space: Condvar::new(),
        max_buffered,
        write_timeout,
        client_gone: CancellationToken::new(),
    });
    let writer = ChannelResponseWriter {
        headers: http::HeaderMap::new(),
        head: Some(tx),
        shared: shared.clone(),
        finished: false,
    };
    let client_gone = shared.client_gone.clone();
    let mut guard = PendingGuard(Some(shared.clone()));
    let response = async move {
        let parts = match rx.await {
            Ok(parts) => parts,
            // The writer was dropped without a status line (cannot happen: dropping
            // finishes it, which sends one).
            Err(_) => return super::status_response(500),
        };
        guard.disarm();
        let body: Body = ChannelBody { shared }.boxed();
        http::Response::from_parts(parts, body)
    };
    (writer, client_gone, response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
    }

    #[test]
    fn head_is_sent_with_the_first_write_and_body_streams() {
        runtime().block_on(async {
            let (mut w, gone, resp) = channel_response();
            w.headers_mut().insert("x-test", http::HeaderValue::from_static("1"));
            let writer = std::thread::spawn(move || {
                w.write(b"hello ").unwrap();
                w.write(b"world").unwrap();
                w.finish();
            });
            let resp = resp.await;
            assert_eq!(200, resp.status());
            assert_eq!("1", resp.headers()["x-test"]);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(&b"hello world"[..], &body[..]);
            writer.join().unwrap();
            assert!(gone.is_cancelled(), "the body was dropped after being read");
        });
    }

    #[test]
    fn status_line_without_body() {
        runtime().block_on(async {
            let (mut w, _gone, resp) = channel_response();
            w.write_header(401);
            drop(w);
            let resp = resp.await;
            assert_eq!(401, resp.status());
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert!(body.is_empty());
        });
    }

    #[test]
    fn dropping_the_pending_response_cancels_the_client_token() {
        let (_w, gone, resp) = channel_response();
        drop(resp);
        assert!(gone.is_cancelled());
    }

    /// Deviation D-028: a write to a client that stopped reading fails once the
    /// queue is full and stays full for the write timeout (Go blocks indefinitely).
    #[test]
    fn deviation_d028_writes_to_a_stalled_client_time_out() {
        runtime().block_on(async {
            let (mut w, _gone, resp) = channel_response_with_limits(4, Duration::from_millis(50));
            w.write_header(200);
            // The client got the head but reads nothing.
            let _resp = resp.await;
            let writer = std::thread::spawn(move || {
                w.write(b"1234").expect("fits the queue");
                let started = Instant::now();
                let err = w.write(b"5").expect_err("the queue is full");
                (err.kind(), started.elapsed())
            });
            let (kind, waited) = writer.join().unwrap();
            assert_eq!(std::io::ErrorKind::TimedOut, kind);
            assert!(waited >= Duration::from_millis(50), "{waited:?}");
        });
    }

    #[test]
    fn a_reading_client_unblocks_writers() {
        runtime().block_on(async {
            let (mut w, _gone, resp) = channel_response_with_limits(4, Duration::from_secs(5));
            w.write_header(200);
            let resp = resp.await;
            let writer = std::thread::spawn(move || {
                for _ in 0..10 {
                    w.write(b"1234").expect("the client reads");
                }
            });
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            writer.join().unwrap();
            assert_eq!(40, body.len());
        });
    }

    #[test]
    fn writes_fail_once_the_client_is_gone() {
        runtime().block_on(async {
            let (mut w, gone, resp) = channel_response();
            w.write_header(200);
            drop(resp.await);
            assert!(gone.is_cancelled());
            assert!(w.write(b"late").is_err());
        });
    }
}
