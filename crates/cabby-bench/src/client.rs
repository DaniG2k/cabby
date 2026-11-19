//! Benchi subscriber client (upstream `lib/client.go`): a [`crate::cable`]
//! connection with one merged message queue and subscribe-with-confirm semantics
//! over public streams (`{"channel":"$pubsub","stream_name":"<stream>"}`).

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::BenchError;
use crate::cable::{Command, Conn, Route, Sink, SubFlags};
use crate::gojson;
use crate::pool::Recorder;
use crate::runtime::block_on;

/// Merged queue capacity (upstream `defaultMergedCapacity`).
pub const DEFAULT_MERGED_CAPACITY: usize = 256;
/// Per-subscription queue capacity (upstream `defaultSubscriptionCapacity`).
///
/// Messages are routed straight into the merged queue here, so there is no
/// per-subscription queue; the constant is kept for reference.
pub const DEFAULT_SUBSCRIPTION_CAPACITY: usize = 16;
/// Confirmation wait per stream (upstream `defaultSubscribeTimeout`).
pub const DEFAULT_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(5);

/// A broadcast on a subscribed stream. String payloads are decoded; other JSON is
/// kept as raw text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Message {
    pub stream: String,
    pub data: String,
}

/// Outcome of a bounded receive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Received {
    Message(Message),
    /// The client was closed and every in-flight message has been drained
    /// (upstream: `Receive` returns `(Message{}, false)`).
    Closed,
    TimedOut,
}

/// The `$pubsub` identifier for `stream`, as JSON text.
pub fn stream_identifier(stream: &str) -> String {
    let mut out = String::from(r#"{"channel":"$pubsub","stream_name":"#);
    gojson::push_str(&mut out, stream);
    out.push('}');
    out
}

/// A public-streams subscriber. `Sync`: `close` may race with `receive` from other
/// threads (upstream asserts `Close` is safe from multiple goroutines).
pub struct Client {
    url: String,
    conn: Mutex<Option<Arc<Conn>>>,
    merged_tx: Mutex<Option<mpsc::Sender<Message>>>,
    merged_rx: tokio::sync::Mutex<mpsc::Receiver<Message>>,
    subscribed: Mutex<HashSet<String>>,
    /// When set (pool clients), messages are recorded in the reader instead of
    /// going through the merged queue.
    recorder: Mutex<Option<Arc<Recorder>>>,
    closed: AtomicBool,
    /// Cancelled by `close`: interrupts an in-flight connect.
    cancel: CancellationToken,
}

impl Client {
    /// Rejects an empty URL; does not dial.
    pub fn build(server_url: &str) -> Result<Client, BenchError> {
        if server_url.is_empty() {
            return Err(BenchError("server URL is required".into()));
        }
        let (tx, rx) = mpsc::channel(DEFAULT_MERGED_CAPACITY);
        Ok(Client {
            url: server_url.to_owned(),
            conn: Mutex::new(None),
            merged_tx: Mutex::new(Some(tx)),
            merged_rx: tokio::sync::Mutex::new(rx),
            subscribed: Mutex::new(HashSet::new()),
            recorder: Mutex::new(None),
            closed: AtomicBool::new(false),
            cancel: CancellationToken::new(),
        })
    }

    /// Records every message of later subscriptions with `recorder` instead of
    /// queueing it for [`Client::receive`].
    pub(crate) fn record_into(&self, recorder: Arc<Recorder>) {
        *self.recorder.lock().unwrap_or_else(|e| e.into_inner()) = Some(recorder);
    }

    /// Dials and consumes the welcome message, giving up after `timeout` if set
    /// (upstream: context cancellation).
    pub fn connect(&self, timeout: Option<Duration>) -> Result<(), BenchError> {
        block_on(self.connect_async(timeout))
    }

    pub(crate) async fn connect_async(&self, timeout: Option<Duration>) -> Result<(), BenchError> {
        let dial = Conn::dial(&self.url);
        let conn = tokio::select! {
            r = dial => r?,
            () = sleep_or_forever(timeout) => return Err(BenchError("context deadline exceeded".into())),
            () = self.cancel.cancelled() => return Err(BenchError("context canceled".into())),
        };
        if self.closed.load(Ordering::SeqCst) {
            conn.close().await;
            return Err(BenchError("client closed".into()));
        }
        *self.conn.lock().unwrap_or_else(|e| e.into_inner()) = Some(conn);
        Ok(())
    }

    /// Subscribes to each stream not yet subscribed and waits for its confirmation.
    /// Already-subscribed streams send nothing. The error names every stream that
    /// timed out or was rejected.
    pub fn subscribe(&self, streams: &[String]) -> Result<(), BenchError> {
        block_on(self.subscribe_async(streams))
    }

    /// Sends every subscribe command first, then waits for the confirmations (each
    /// with the default timeout from its send), so a client with `s` streams needs
    /// one round trip instead of `s`.
    pub(crate) async fn subscribe_async(&self, streams: &[String]) -> Result<(), BenchError> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let tx = self.merged_tx.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let recorder = self.recorder.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut failed = Vec::new();
        let mut waiting = Vec::new();
        let mut seen = HashSet::new();
        for stream in streams {
            if !seen.insert(stream.as_str())
                || self.subscribed.lock().unwrap_or_else(|e| e.into_inner()).contains(stream)
            {
                continue;
            }
            let (Some(conn), Some(tx)) = (conn.as_ref(), tx.as_ref()) else {
                failed.push(stream.clone());
                continue;
            };
            let id = stream_identifier(stream);
            let flags = SubFlags::new();
            let sink = match &recorder {
                Some(r) => Sink::Record { recorder: Arc::clone(r), stream: stream.as_str().into() },
                None => Sink::Merged { tx: tx.clone(), stream: stream.as_str().into() },
            };
            let route = Route { flags: Arc::clone(&flags), sink };
            let sent = match conn.register(&id, route) {
                Ok(()) => {
                    let cmd =
                        Command { command: "subscribe".into(), identifier: Some(id.clone()), ..Default::default() };
                    conn.send_command(&cmd).await
                }
                Err(e) => Err(e),
            };
            match sent {
                Ok(()) => waiting.push((stream, id, flags, tokio::time::Instant::now() + DEFAULT_SUBSCRIBE_TIMEOUT)),
                Err(_) => {
                    conn.unregister(&id);
                    failed.push(stream.clone());
                }
            }
        }
        for (stream, id, flags, deadline) in waiting {
            let resolved = tokio::time::timeout_at(deadline, flags.wait_done()).await.is_ok();
            if resolved && flags.subscribed() && !flags.rejected() {
                self.subscribed.lock().unwrap_or_else(|e| e.into_inner()).insert(stream.clone());
            } else {
                if let Some(c) = conn.as_ref() {
                    c.unregister(&id);
                }
                failed.push(stream.clone());
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(BenchError(format!("subscribe failed for streams: [{}]", failed.join(" "))))
        }
    }

    /// Blocks until the next message, or until the client is closed and drained.
    pub fn receive(&self) -> Option<Message> {
        block_on(async { self.merged_rx.lock().await.recv().await })
    }

    pub fn receive_timeout(&self, timeout: Duration) -> Received {
        let r =
            block_on(async { tokio::time::timeout(timeout, async { self.merged_rx.lock().await.recv().await }).await });
        match r {
            Ok(Some(m)) => Received::Message(m),
            Ok(None) => Received::Closed,
            Err(_) => Received::TimedOut,
        }
    }

    /// Closes the connection, drains the forwarders and closes the merged queue.
    /// Idempotent.
    pub fn close(&self) {
        if self.closed.load(Ordering::SeqCst) {
            return;
        }
        block_on(self.close_async());
    }

    pub(crate) async fn close_async(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.cancel.cancel();
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(c) = conn {
            // Waits for the reader, which drops its routes (and their senders).
            c.close().await;
        }
        self.merged_tx.lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

async fn sleep_or_forever(timeout: Option<Duration>) {
    match timeout {
        Some(t) => tokio::time::sleep(t).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_is_go_json() {
        assert_eq!(r#"{"channel":"$pubsub","stream_name":"foo"}"#, stream_identifier("foo"));
    }
}
