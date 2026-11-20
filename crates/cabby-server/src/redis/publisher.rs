//! Publishing to Redis pub/sub channels from synchronous callers: one task
//! sends `PUBLISH`es in call order (pipelined in batches) over a commands
//! connection that is reopened when it fails.
//!
//! Upstream publishes synchronously (rueidis `Do`), so a stalled Redis holds
//! the publisher back. Here publications wait in a bounded queue
//! ([`QUEUE_CAPACITY`]); a caller that finds it full blocks until there is
//! room, which gives the same back-pressure with bounded memory. Replies may
//! take up to rueidis' `ConnWriteTimeout` ([`ConnectSpec::write_timeout`]);
//! a batch that times out is dropped and the connection reopened, as rueidis
//! fails the commands of a connection it gives up on. An idle connection is
//! checked with `PING` every keep-alive period, as rueidis does.
//!
//! In a Redis Cluster, `PUBLISH` is routed by the channel's slot and a
//! pipeline must stay within one slot, so a batch is split into one pipeline
//! per slot (sent concurrently; the order of each channel is kept).

use std::collections::HashMap;
use std::time::Duration;

use cabby_core::logger::Level;
use cabby_core::utils::backpressure;
use redis::cluster_routing::Slot;
use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;

use super::connect::{self, Commands, ConnectSpec};

/// Publications that may wait for Redis before callers are held back (the
/// size of rueidis' per-connection command ring).
pub(crate) const QUEUE_CAPACITY: usize = 1024;
/// Most messages sent in one pipeline.
const MAX_BATCH: usize = 512;

type Publication = (String, Vec<u8>);

pub(crate) struct Publisher {
    tx: Sender<Publication>,
    stop: CancellationToken,
}

impl Publisher {
    pub(crate) fn start(spec: ConnectSpec, context: &'static str) -> Self {
        let (tx, rx) = channel(QUEUE_CAPACITY);
        let stop = CancellationToken::new();
        connect::runtime().spawn(run(spec, context, rx, stop.clone()));
        Self { tx, stop }
    }

    /// Queues a `PUBLISH`, waiting while the queue is full (Redis is slower
    /// than the publishers, or unreachable).
    pub(crate) fn publish(&self, channel: &str, payload: Vec<u8>) {
        let _ = backpressure::send_blocking(&self.tx, (channel.to_owned(), payload), connect::runtime(), None);
    }

    pub(crate) fn shutdown(&self) {
        self.stop.cancel();
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn log_error(context: &'static str, msg: &str, err: &redis::RedisError) {
    super::log(Level::Error, context, msg, &[("error", err.to_string())]);
}

/// A batch split into pipelines that can each be sent as one request.
struct Batch {
    pipes: Vec<redis::Pipeline>,
}

impl Batch {
    fn new(messages: Vec<Publication>, cluster: bool) -> Self {
        let pipe = |msgs: &mut dyn Iterator<Item = Publication>| {
            let mut p = redis::pipe();
            for (channel, payload) in msgs {
                p.cmd("PUBLISH").arg(channel).arg(payload).ignore();
            }
            p
        };
        if !cluster {
            return Self { pipes: vec![pipe(&mut messages.into_iter())] };
        }
        let mut order: Vec<Slot> = Vec::new();
        let mut by_slot: HashMap<Slot, Vec<Publication>> = HashMap::new();
        for m in messages {
            let slot = Slot::for_key(m.0.as_bytes());
            by_slot
                .entry(slot)
                .or_insert_with(|| {
                    order.push(slot);
                    Vec::new()
                })
                .push(m);
        }
        let pipes = order.into_iter().filter_map(|s| by_slot.remove(&s)).map(|m| pipe(&mut m.into_iter())).collect();
        Self { pipes }
    }
}

async fn run(spec: ConnectSpec, context: &'static str, mut rx: Receiver<Publication>, stop: CancellationToken) {
    let mut conn: Option<Commands> = None;
    loop {
        let idle_check = async {
            match spec.keepalive {
                Some(period) => tokio::time::sleep(period).await,
                None => std::future::pending().await,
            }
        };
        let first = tokio::select! {
            () = stop.cancelled() => return,
            m = rx.recv() => match m { Some(m) => m, None => return },
            () = idle_check, if conn.is_some() => {
                if let Some(c) = conn.as_mut() {
                    let cmd = redis::cmd("PING");
                    let ping = tokio::time::timeout(spec.write_timeout, cmd.query_async(c));
                    let pong: redis::RedisResult<redis::Value> = tokio::select! {
                        () = stop.cancelled() => return,
                        r = ping => r.unwrap_or_else(|_| Err(timeout_error())),
                    };
                    if pong.is_err() {
                        conn = None;
                    }
                }
                continue;
            }
        };
        let mut messages = Vec::with_capacity(rx.len().min(MAX_BATCH) + 1);
        messages.push(first);
        while messages.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(m) => messages.push(m),
                Err(_) => break,
            }
        }

        // One retry on a fresh connection, then the batch is dropped (upstream
        // ignores publish failures).
        let mut pending: Option<Batch> = None;
        for attempt in 0..2 {
            if conn.is_none() {
                let opened = tokio::select! {
                    () = stop.cancelled() => return,
                    c = spec.commands(Some(spec.write_timeout)) => c,
                };
                match opened {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        log_error(context, "failed to connect to Redis", &e);
                        break;
                    }
                }
            }
            let Some(c) = conn.as_ref() else { break };
            let batch = match pending.take() {
                Some(b) => b,
                None => Batch::new(std::mem::take(&mut messages), matches!(c, Commands::Cluster(_))),
            };
            let failed = tokio::select! {
                () = stop.cancelled() => return,
                f = send(c, batch, spec.write_timeout, context, attempt == 1) => f,
            };
            if failed.pipes.is_empty() {
                break;
            }
            conn = None;
            pending = Some(failed);
        }
    }
}

/// Sends every pipeline of `batch`; returns those that failed because of the
/// connection (to retry), logging them when `last` or when Redis rejected them.
async fn send(conn: &Commands, batch: Batch, timeout: Duration, context: &'static str, last: bool) -> Batch {
    let requests = batch.pipes.into_iter().map(|pipe| {
        let mut c = conn.clone();
        async move {
            let sent: redis::RedisResult<()> =
                tokio::time::timeout(timeout, pipe.query_async(&mut c)).await.unwrap_or_else(|_| Err(timeout_error()));
            (pipe, sent)
        }
    });
    let mut failed = Vec::new();
    for (pipe, sent) in futures_util::future::join_all(requests).await {
        match sent {
            Ok(()) => {}
            Err(e) if e.code().is_some() && !e.is_io_error() => log_error(context, "failed to publish", &e),
            Err(e) => {
                if last {
                    log_error(context, "failed to publish", &e);
                }
                failed.push(pipe);
            }
        }
    }
    Batch { pipes: failed }
}

fn timeout_error() -> redis::RedisError {
    redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_batches_are_split_by_slot_keeping_channel_order() {
        let msgs: Vec<Publication> =
            ["a", "b", "a", "{a}x", "c"].iter().map(|c| (c.to_string(), b"1".to_vec())).collect();
        assert_eq!(1, Batch::new(msgs.clone(), false).pipes.len());
        let batch = Batch::new(msgs, true);
        // "a" and "{a}x" share a slot (hash tag); "b" and "c" have their own.
        assert_eq!(3, batch.pipes.len());
        assert_eq!(3, batch.pipes[0].len());
    }
}
