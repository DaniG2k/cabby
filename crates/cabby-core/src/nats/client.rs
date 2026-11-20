//! The NATS client runtime shared by the NATS broker, pub/sub subscriber and
//! legacy broadcaster.
//!
//! # Bridging the synchronous interfaces to the async client
//!
//! Cabby's node, hub and broker interfaces are synchronous: the node runs
//! sessions on threads, and `broker::Broker`, `pubsub::Subscriber` and
//! `broadcast::Broadcaster` are plain traits called from those threads (and,
//! once transports run on Tokio, from Tokio tasks too). The NATS client
//! (`async-nats`) is asynchronous, so all NATS I/O runs on one dedicated,
//! process-wide Tokio runtime ([`runtime`], two worker threads named
//! `cabby-nats`), created on first use:
//!
//! * Calls that need a result (JetStream history, the session cache, the epoch)
//!   spawn the async operation on that runtime and wait for it with
//!   [`block_on`]. The wait never runs the future on the caller's thread, so
//!   it works from plain threads and from any Tokio runtime; on a
//!   multi-threaded Tokio worker it goes through `block_in_place` so other
//!   tasks of the caller's runtime keep running.
//! * Pub/sub publications are sent to a per-client actor task over a bounded
//!   channel: the call returns once the publication is queued and waits while
//!   the queue is full, as Go's `Publish` blocks on a stalled connection.
//!   Subscribe and unsubscribe go to the same actor over a separate channel
//!   that it serves first, so they never wait behind queued publications.
//! * Incoming messages that reach node code (pub/sub handlers, broadcast
//!   handlers) are delivered from a dedicated dispatcher thread per client,
//!   never from a runtime worker, so a slow handler cannot stall NATS I/O and
//!   handlers may call back into synchronous NATS APIs.
//!
//! No lock is held across an `.await` or a blocking wait.

use std::future::Future;
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::Duration;

use async_nats::{Client, ConnectOptions, ServerAddr};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

use super::{NatsConfig, NatsError};
use crate::utils::backpressure;

/// The process-wide runtime that runs NATS I/O.
pub fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("cabby-nats")
            .enable_all()
            .build()
            .expect("build the NATS runtime")
    })
}

/// Runs `fut` on the NATS runtime and blocks the calling thread until it
/// completes (see the module documentation).
pub fn block_on<F>(fut: F) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = mpsc::sync_channel(1);
    runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    blocking(move || rx.recv()).expect("NATS runtime task panicked")
}

/// Runs a blocking closure, telling a multi-threaded Tokio runtime first when
/// the caller is one of its workers.
pub fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// Go nats.go `ReconnectWait`: the delay before retrying a server that was
/// just tried (the first attempt after a disconnect is immediate).
const RECONNECT_WAIT: Duration = Duration::from_secs(2);

/// Parses the comma-separated server list (`nats.Connect(servers)`).
pub fn server_addrs(servers: &str) -> Result<Vec<ServerAddr>, NatsError> {
    servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse::<ServerAddr>().map_err(|e| NatsError(format!("nats: invalid server URL {s:?}: {e}"))))
        .collect()
}

/// Connects as upstream does (`RetryOnFailedConnect(true)`,
/// `MaxReconnects(max_reconnect_attempts)`, optional `DontRandomize()`): the
/// client is returned at once and connects (and reconnects) in the background.
///
/// See [`max_reconnects`] for the reconnect limit.
pub async fn connect(config: &NatsConfig) -> Result<Client, NatsError> {
    let addrs = server_addrs(&config.servers)?;
    let mut options = ConnectOptions::new()
        .retry_on_initial_connect()
        .max_reconnects(max_reconnects(config.max_reconnect_attempts))
        .reconnect_delay_callback(|attempts| if attempts <= 1 { Duration::ZERO } else { RECONNECT_WAIT });
    if config.dont_randomize_servers {
        options = options.retain_servers_order();
    }
    options.connect(addrs).await.map_err(|e| NatsError(e.to_string()))
}

/// The async-nats reconnect limit for Go's `MaxReconnects(n)`: negative is
/// unlimited (`None`); 0, "never reconnect" in Go, has no async-nats
/// equivalent and allows one attempt (deviation D-024).
pub fn max_reconnects(n: i64) -> Option<usize> {
    match n {
        n if n < 0 => None,
        0 => Some(1),
        n => Some(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

/// Upstream `utils.NextRetry(step)`: 250ms for the first retry, then a random
/// delay in [2^step, 2^(step+1)) seconds, truncated to whole seconds.
pub(crate) fn next_retry(step: u32) -> Duration {
    if step == 0 {
        return Duration::from_millis(250);
    }
    let left = 2f64.powi(step as i32);
    let mut buf = [0u8; 8];
    let r = match getrandom::fill(&mut buf) {
        Ok(()) => (u64::from_le_bytes(buf) >> 11) as f64 / (1u64 << 53) as f64,
        Err(_) => 0.5,
    };
    Duration::from_secs((left + left * r) as u64)
}

/// A thread delivering incoming messages to synchronous handlers in arrival
/// order. Dropping the sender (or the dispatcher) stops the thread once the
/// queue is drained.
pub struct Dispatcher<T: Send + 'static> {
    tx: mpsc::Sender<T>,
}

impl<T: Send + 'static> Clone for Dispatcher<T> {
    fn clone(&self) -> Self {
        Self { tx: self.tx.clone() }
    }
}

impl<T: Send + 'static> Dispatcher<T> {
    pub fn spawn(name: &str, mut handle: impl FnMut(T) + Send + 'static) -> Self {
        let (tx, rx) = mpsc::channel::<T>();
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                while let Ok(item) = rx.recv() {
                    // A panicking handler must not stop delivery.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handle(item)));
                }
            })
            .expect("spawn a NATS dispatcher thread");
        Self { tx }
    }

    pub fn send(&self, item: T) {
        let _ = self.tx.send(item);
    }
}

/// A message received on a subscribed subject.
#[derive(Debug, Clone)]
pub struct Incoming {
    pub subject: String,
    pub payload: Vec<u8>,
}

/// Subscription changes and closing, applied before any queued publication.
enum Control {
    Subscribe(String, Option<mpsc::SyncSender<Result<(), NatsError>>>),
    Unsubscribe(String),
    Close(mpsc::SyncSender<()>),
}

/// How long closing waits for the connection to drain.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Publications that may wait for the connection before publishers are held
/// back (Go buffers about as much in the connection's writer and socket).
pub const PUBLISH_QUEUE_CAPACITY: usize = 1024;

/// The longest a publisher waits for room in the queue before the
/// publication fails (Go nats.go `DefaultFlusherTimeout`, the write deadline
/// of a stalled connection).
pub const PUBLISH_TIMEOUT: Duration = Duration::from_secs(60);

/// A core NATS connection for pub/sub with a synchronous API (Go `*nats.Conn`
/// as used by the pub/sub subscriber and the legacy broadcaster).
///
/// Publications go through a bounded queue in call order: a publisher that
/// finds it full waits (up to [`PUBLISH_TIMEOUT`]), as Go's `Publish` blocks
/// on a stalled connection. Subscribe and unsubscribe return at once and are
/// applied in call order by the same actor task, ahead of queued
/// publications, so a publication backlog never delays them. Incoming
/// messages of all subscriptions go to one handler, called from a dedicated
/// thread in arrival order.
pub struct Connection {
    publications: tokio::sync::mpsc::Sender<(String, Vec<u8>)>,
    control: tokio::sync::mpsc::UnboundedSender<Control>,
}

impl Connection {
    /// Connects (retrying in the background, see [`connect`]) and starts the
    /// actor and the dispatcher thread named `name`.
    pub fn open(
        config: &NatsConfig,
        name: &str,
        on_message: impl FnMut(Incoming) + Send + 'static,
    ) -> Result<Self, NatsError> {
        let config = config.clone();
        let client = block_on(async move { connect(&config).await })?;
        let dispatcher = Dispatcher::spawn(name, on_message);
        let (publications, publications_rx) = tokio::sync::mpsc::channel(PUBLISH_QUEUE_CAPACITY);
        let (control, control_rx) = tokio::sync::mpsc::unbounded_channel();
        runtime().spawn(run_connection(client, control_rx, publications_rx, dispatcher));
        Ok(Self { publications, control })
    }

    /// Queues a publication, waiting while the queue is full (up to
    /// [`PUBLISH_TIMEOUT`], after which it fails like Go's write deadline).
    pub fn publish(&self, subject: &str, payload: Vec<u8>) -> Result<(), NatsError> {
        backpressure::send_blocking(&self.publications, (subject.to_owned(), payload), runtime(), Some(PUBLISH_TIMEOUT))
            .map_err(|e| match e {
                backpressure::SendError::Closed => NatsError("nats: connection closed".into()),
                backpressure::SendError::TimedOut => NatsError("nats: timeout".into()),
            })
    }

    /// Subscribes to `subject` (no-op when already subscribed).
    pub fn subscribe(&self, subject: &str) {
        let _ = self.control.send(Control::Subscribe(subject.to_owned(), None));
    }

    /// Subscribes and waits for the subscription to be registered with the
    /// client, reporting failures.
    pub fn subscribe_sync(&self, subject: &str) -> Result<(), NatsError> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.control
            .send(Control::Subscribe(subject.to_owned(), Some(tx)))
            .map_err(|_| NatsError("nats: connection closed".into()))?;
        blocking(move || rx.recv()).unwrap_or_else(|_| Err(NatsError("nats: connection closed".into())))
    }

    pub fn unsubscribe(&self, subject: &str) {
        let _ = self.control.send(Control::Unsubscribe(subject.to_owned()));
    }

    /// Drops all subscriptions and closes the connection, waiting (bounded) for
    /// queued publications to be flushed.
    pub fn close(&self) {
        let (tx, rx) = mpsc::sync_channel(1);
        if self.control.send(Control::Close(tx)).is_ok() {
            let _ = blocking(move || rx.recv_timeout(DRAIN_TIMEOUT * 2 + Duration::from_secs(1)));
        }
    }
}

/// One subscription's reader task: forwards messages to the dispatcher until
/// told to stop, then unsubscribes and acknowledges.
struct SubscriptionTask {
    stop: tokio::sync::oneshot::Sender<tokio::sync::oneshot::Sender<()>>,
}

async fn run_connection(
    client: async_nats::Client,
    mut control: tokio::sync::mpsc::UnboundedReceiver<Control>,
    mut publications: tokio::sync::mpsc::Receiver<(String, Vec<u8>)>,
    dispatcher: Dispatcher<Incoming>,
) {
    let mut subscriptions: std::collections::HashMap<String, SubscriptionTask> = std::collections::HashMap::new();
    loop {
        tokio::select! {
            biased;
            cmd = control.recv() => match cmd {
                None => return,
                Some(Control::Subscribe(subject, reply)) => {
                    let result = subscribe(&client, &mut subscriptions, &subject, &dispatcher).await;
                    if let Some(r) = reply {
                        let _ = r.send(result);
                    }
                }
                Some(Control::Unsubscribe(subject)) => {
                    if let Some(task) = subscriptions.remove(&subject) {
                        stop_subscription(task).await;
                    }
                }
                Some(Control::Close(done)) => {
                    for (_, task) in subscriptions.drain() {
                        stop_subscription(task).await;
                    }
                    // Publications queued before closing still go out (Go
                    // flushes the connection's buffer when closing).
                    let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
                        while let Ok((subject, payload)) = publications.try_recv() {
                            let _ = client.publish(subject, payload.into()).await;
                        }
                    })
                    .await;
                    let _ = tokio::time::timeout(DRAIN_TIMEOUT, client.drain()).await;
                    let _ = done.send(());
                    return;
                }
            },
            p = publications.recv() => match p {
                None => return,
                Some((subject, payload)) => {
                    let _ = client.publish(subject, payload.into()).await;
                }
            },
        }
    }
}

async fn subscribe(
    client: &async_nats::Client,
    subscriptions: &mut std::collections::HashMap<String, SubscriptionTask>,
    subject: &str,
    dispatcher: &Dispatcher<Incoming>,
) -> Result<(), NatsError> {
    use tokio_stream::StreamExt;

    if subscriptions.contains_key(subject) {
        return Ok(());
    }
    let mut sub = client.subscribe(subject.to_owned()).await.map_err(|e| NatsError(e.to_string()))?;
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<tokio::sync::oneshot::Sender<()>>();
    let d = dispatcher.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = sub.next() => match msg {
                    Some(m) => d.send(Incoming { subject: m.subject.to_string(), payload: m.payload.to_vec() }),
                    None => return,
                },
                ack = &mut stop_rx => {
                    let _ = sub.unsubscribe().await;
                    if let Ok(ack) = ack {
                        let _ = ack.send(());
                    }
                    return;
                }
            }
        }
    });
    subscriptions.insert(subject.to_owned(), SubscriptionTask { stop: stop_tx });
    Ok(())
}

/// Stops a reader task and waits until its UNSUB is queued, so that later
/// publications are not delivered to it (Go unsubscribes synchronously).
async fn stop_subscription(task: SubscriptionTask) {
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    if task.stop.send(ack_tx).is_ok() {
        let _ = ack_rx.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_addrs_split_on_commas() {
        let addrs = server_addrs("nats://a:4222, nats://b:4223").unwrap();
        assert_eq!(2, addrs.len());
        assert!(server_addrs("nats://a:4222,::bad::").is_err());
    }

    #[test]
    fn deviation_d024_zero_max_reconnects_allows_one_attempt() {
        assert_eq!(None, max_reconnects(-1));
        assert_eq!(Some(1), max_reconnects(0));
        assert_eq!(Some(5), max_reconnects(5));
    }

    #[test]
    fn next_retry_ranges() {
        assert_eq!(Duration::from_millis(250), next_retry(0));
        for step in 1..5 {
            let d = next_retry(step).as_secs();
            assert!((1 << step..1 << (step + 1)).contains(&d), "{step}: {d}");
        }
    }

    #[test]
    fn block_on_works_inside_other_runtimes() {
        assert_eq!(2, block_on(async { 1 + 1 }));
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        assert_eq!(3, rt.block_on(async { block_on(async { 3 }) }));
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(1).build().unwrap();
        assert_eq!(4, rt.block_on(async { tokio::spawn(async { block_on(async { 4 }) }).await.unwrap() }));
    }
}
