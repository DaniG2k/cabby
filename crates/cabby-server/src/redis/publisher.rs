//! Publishing to Redis pub/sub channels from synchronous callers: one task
//! sends `PUBLISH`es in call order (pipelined in batches) over a commands
//! connection that is reopened when it fails.

use std::time::Duration;

use cabby_core::logger::Level;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

use super::connect::{self, Commands, ConnectSpec};

/// How long a `PUBLISH` batch may take.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
/// Most messages sent in one pipeline.
const MAX_BATCH: usize = 512;

pub(crate) struct Publisher {
    tx: UnboundedSender<(String, Vec<u8>)>,
    stop: CancellationToken,
}

impl Publisher {
    pub(crate) fn start(spec: ConnectSpec, context: &'static str) -> Self {
        let (tx, rx) = unbounded_channel();
        let stop = CancellationToken::new();
        connect::runtime().spawn(run(spec, context, rx, stop.clone()));
        Self { tx, stop }
    }

    pub(crate) fn publish(&self, channel: &str, payload: Vec<u8>) {
        let _ = self.tx.send((channel.to_owned(), payload));
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

async fn run(
    spec: ConnectSpec,
    context: &'static str,
    mut rx: UnboundedReceiver<(String, Vec<u8>)>,
    stop: CancellationToken,
) {
    let mut conn: Option<Commands> = None;
    loop {
        let first = tokio::select! {
            () = stop.cancelled() => return,
            m = rx.recv() => match m { Some(m) => m, None => return },
        };
        let mut pipe = redis::pipe();
        let mut n = 0;
        let mut next = Some(first);
        while let Some((channel, payload)) = next {
            pipe.cmd("PUBLISH").arg(channel).arg(payload).ignore();
            n += 1;
            next = if n < MAX_BATCH { rx.try_recv().ok() } else { None };
        }

        // One retry on a fresh connection, then the batch is dropped (upstream
        // ignores publish failures).
        for attempt in 0..2 {
            if conn.is_none() {
                match spec.commands(Some(RESPONSE_TIMEOUT)).await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        super::log(Level::Error, context, &format!("failed to connect to Redis: {e}"), &[]);
                        break;
                    }
                }
            }
            let Some(c) = conn.as_mut() else { break };
            let sent: redis::RedisResult<()> = pipe.query_async(c).await;
            match sent {
                Ok(_) => break,
                Err(e) if e.code().is_some() && !e.is_io_error() => {
                    super::log(Level::Error, context, &format!("failed to publish: {e}"), &[]);
                    break;
                }
                Err(e) => {
                    conn = None;
                    if attempt == 1 {
                        super::log(Level::Error, context, &format!("failed to publish: {e}"), &[]);
                    }
                }
            }
        }
    }
}
