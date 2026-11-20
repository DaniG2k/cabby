//! Bounded hand-off from synchronous callers to an async worker.
//!
//! Upstream publishes to Redis and NATS synchronously: a stalled server blocks
//! the publisher. Cabby hands publications to a worker task over a bounded
//! channel; [`send_blocking`] waits while that channel is full, so the caller
//! is held back the same way and the memory a stall can take stays bounded.

use std::time::Duration;

use tokio::runtime::{Handle, Runtime, RuntimeFlavor};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;

/// Why [`send_blocking`] did not deliver its item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// The worker is gone.
    Closed,
    /// The channel stayed full for the whole timeout.
    TimedOut,
}

/// Sends `item` on `tx`, waiting while the channel is full (up to `timeout`,
/// or for as long as it takes with `None`).
///
/// The wait runs on `runtime` and the calling thread blocks for it, so this
/// works from plain threads and from any Tokio runtime (a multi-threaded
/// runtime's worker is told first, with `block_in_place`). Callers waiting
/// for room are served in order.
pub fn send_blocking<T: Send + 'static>(
    tx: &Sender<T>,
    item: T,
    runtime: &Runtime,
    timeout: Option<Duration>,
) -> Result<(), SendError> {
    let item = match tx.try_send(item) {
        Ok(()) => return Ok(()),
        Err(TrySendError::Closed(_)) => return Err(SendError::Closed),
        Err(TrySendError::Full(item)) => item,
    };
    let tx = tx.clone();
    let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
    runtime.spawn(async move {
        let sent = match timeout {
            Some(t) => match tokio::time::timeout(t, tx.send(item)).await {
                Ok(r) => r.map_err(|_| SendError::Closed),
                Err(_) => Err(SendError::TimedOut),
            },
            None => tx.send(item).await.map_err(|_| SendError::Closed),
        };
        let _ = done_tx.send(sent);
    });
    let wait = move || done_rx.recv().unwrap_or(Err(SendError::Closed));
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => tokio::task::block_in_place(wait),
        _ => wait(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn runtime() -> Runtime {
        tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap()
    }

    #[test]
    fn waits_while_full_and_resumes_in_order() {
        let rt = runtime();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(2);
        send_blocking(&tx, 1, &rt, None).unwrap();
        send_blocking(&tx, 2, &rt, None).unwrap();

        let sent = Arc::new(AtomicUsize::new(0));
        let (t, s) = (tx.clone(), sent.clone());
        let rt2 = runtime();
        let blocked = std::thread::spawn(move || {
            send_blocking(&t, 3, &rt2, None).unwrap();
            s.fetch_add(1, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(0, sent.load(Ordering::SeqCst), "the sender must wait while the channel is full");

        assert_eq!(Some(1), rx.blocking_recv());
        blocked.join().unwrap();
        assert_eq!(1, sent.load(Ordering::SeqCst));
        assert_eq!(Some(2), rx.blocking_recv());
        assert_eq!(Some(3), rx.blocking_recv());
    }

    #[test]
    fn gives_up_after_the_timeout_or_when_closed() {
        let rt = runtime();
        let (tx, rx) = tokio::sync::mpsc::channel::<u32>(1);
        send_blocking(&tx, 1, &rt, None).unwrap();
        assert_eq!(Err(SendError::TimedOut), send_blocking(&tx, 2, &rt, Some(Duration::from_millis(50))));
        drop(rx);
        assert_eq!(Err(SendError::Closed), send_blocking(&tx, 3, &rt, None));
    }

    #[test]
    fn works_inside_runtimes_when_full() {
        let rt = runtime();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
        send_blocking(&tx, 0, &rt, None).unwrap();
        let drain = std::thread::spawn(move || {
            let mut got = Vec::new();
            for _ in 0..3 {
                std::thread::sleep(Duration::from_millis(30));
                got.push(rx.blocking_recv());
            }
            got
        });
        let current = tokio::runtime::Builder::new_current_thread().build().unwrap();
        current.block_on(async { send_blocking(&tx, 1, &rt, None).unwrap() });
        let mt = runtime();
        let rt = Arc::new(rt);
        let worker_rt = rt.clone();
        mt.block_on(async {
            let tx = tx.clone();
            tokio::spawn(async move { send_blocking(&tx, 2, &worker_rt, None).unwrap() }).await.unwrap();
        });
        assert_eq!(vec![Some(0), Some(1), Some(2)], drain.join().unwrap());
    }
}
