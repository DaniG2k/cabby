//! StatsD interval writer (upstream `metrics/statsd_writer.go`, built on
//! `smira/go-statsd`).
//!
//! As in go-statsd, the metrics task only renders packets and queues them; a
//! sender thread of the writer owns the socket: it resolves the host,
//! connects, sends the queued packets and, after a failed connect or send,
//! waits [`RETRY_DELAY`] before it connects again. No lookup, connect or send
//! happens on a runtime worker, and stopping the writer never waits for a
//! lookup or connect. As go-statsd's `Close`, stopping a connected writer
//! waits for the packets already queued to be sent, for at most
//! [`STOP_TIMEOUT`]; a writer that is not connected drops them, as go-statsd
//! does while it dials or waits to retry.

use std::convert::Infallible;
use std::net::{ToSocketAddrs, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cabby_core::common::StringMap;
use cabby_core::logger::{Level, Logger};

use super::{IntervalWriter, Metrics, MetricsError, StatsdConfig};

/// How long the sender waits after a failed connect or send before it connects
/// again (go-statsd `DefaultRetryTimeout`).
const RETRY_DELAY: Duration = Duration::from_secs(5);
/// Packets waiting for the sender (go-statsd `DefaultSendQueueCapacity`). When
/// the queue is full, the packet being queued is dropped, as in go-statsd.
const QUEUE_CAPACITY: usize = 10;
/// How often the sender reports dropped packets (go-statsd
/// `DefaultReportInterval`).
const REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// How long stopping a connected writer waits for the queued packets to be
/// sent (go-statsd waits without a limit). The metrics stop after the other
/// shutdown steps, often with the shutdown timeout used up, so this is the
/// least time the runner gives a final write in that case (the SQLite broker's
/// `FINAL_FLUSH_FLOOR`); sending the at most [`QUEUE_CAPACITY`] queued UDP
/// packets takes far less.
const STOP_TIMEOUT: Duration = Duration::from_secs(1);

/// go-statsd's logger (upstream `StatsdLogger`: the runner's logger with
/// `service=statsd`; messages containing `Error` at error level).
fn statsd_log(level: Level, msg: &str) {
    Logger::default().with("service", "statsd").log(level, msg, &[]);
}

/// Where tags go and how they are separated (go-statsd `TagFormat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TagFormat {
    /// Tags follow the metric name (InfluxDB, Graphite) rather than the type (Datadog).
    in_name: bool,
    first: &'static str,
    other: char,
    key_value: char,
}

fn resolve_tag_format(name: &str) -> Result<TagFormat, MetricsError> {
    match name {
        "datadog" => Ok(TagFormat { in_name: false, first: "|#", other: ',', key_value: ':' }),
        "influxdb" => Ok(TagFormat { in_name: true, first: ",", other: ',', key_value: '=' }),
        "graphite" => Ok(TagFormat { in_name: true, first: ";", other: ';', key_value: '=' }),
        other => Err(MetricsError(format!("unknown StatsD tags format: {other}"))),
    }
}

/// Resolves `host` and returns a UDP socket connected to its first address
/// (go-statsd dials `udp`). Blocks for as long as the lookup takes.
fn connect_udp(host: &str) -> std::io::Result<UdpSocket> {
    let addr = host
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "no address found"))?;
    let local = if addr.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
    let socket = UdpSocket::bind(local)?;
    socket.connect(addr)?;
    Ok(socket)
}

type Connect = Arc<dyn Fn(&str) -> std::io::Result<UdpSocket> + Send + Sync>;
type SendPacket = Arc<dyn Fn(&UdpSocket, &[u8]) -> std::io::Result<usize> + Send + Sync>;

/// How the sender thread connects and sends, and how long it waits (tests
/// replace them).
struct Transport {
    connect: Connect,
    send: SendPacket,
    retry_delay: Duration,
    report_interval: Duration,
    stop_timeout: Duration,
}

impl Default for Transport {
    fn default() -> Self {
        Self {
            connect: Arc::new(connect_udp),
            send: Arc::new(|socket: &UdpSocket, packet: &[u8]| socket.send(packet)),
            retry_delay: RETRY_DELAY,
            report_interval: REPORT_INTERVAL,
            stop_timeout: STOP_TIMEOUT,
        }
    }
}

/// The metrics task's side of a running writer.
#[derive(Debug)]
struct Client {
    /// Pre-rendered tags (with the leading separator), placed per `in_name`.
    tags: String,
    in_name: bool,
    queue: mpsc::SyncSender<Vec<u8>>,
    /// Never sends; dropping it (with the queue) stops the sender thread.
    stop: mpsc::Sender<Infallible>,
    /// Packets dropped because the queue was full, since the last report.
    lost: Arc<AtomicU64>,
    /// Whether the sender has a socket (it then sends what is queued at stop).
    connected: Arc<AtomicBool>,
    /// Never receives; disconnected once the sender thread has ended.
    finished: mpsc::Receiver<Infallible>,
}

impl Client {
    /// Stops the sender thread: a connected sender sends the packets already
    /// queued first, and this waits for it for at most `timeout`.
    fn stop(self, timeout: Duration) {
        let Client { queue, stop, connected, finished, .. } = self;
        drop((queue, stop));
        if connected.load(Ordering::Acquire) {
            let _ = finished.recv_timeout(timeout);
        }
    }
}

/// The sender thread (go-statsd `sendLoop` and `reportLoop`).
struct Sender {
    host: String,
    transport: Arc<Transport>,
    queue: mpsc::Receiver<Vec<u8>>,
    stop: mpsc::Receiver<Infallible>,
    lost: Arc<AtomicU64>,
    next_report: Instant,
    connected: Arc<AtomicBool>,
    /// Dropped when the thread ends.
    _finished: mpsc::Sender<Infallible>,
}

impl Sender {
    fn run(mut self) {
        loop {
            match (self.transport.connect)(&self.host) {
                Ok(socket) => {
                    self.connected.store(true, Ordering::Release);
                    let again = self.deliver(&socket);
                    self.connected.store(false, Ordering::Release);
                    if !again {
                        return;
                    }
                }
                Err(e) => statsd_log(Level::Error, &format!("Error connecting to server: {e}")),
            }
            if !self.wait_to_retry() {
                return;
            }
        }
    }

    /// Sends queued packets until a send fails (`true`: connect again) or the
    /// writer stops (`false`, once the queued packets are sent).
    fn deliver(&mut self, socket: &UdpSocket) -> bool {
        loop {
            match self.queue.recv_timeout(self.until_report()) {
                Ok(packet) => {
                    if let Err(e) = (self.transport.send)(socket, &packet) {
                        // go-statsd logs the failure, closes the socket and reconnects later.
                        statsd_log(Level::Error, &format!("Error writing to socket: {e}"));
                        return true;
                    }
                }
                Err(RecvTimeoutError::Timeout) => self.report_if_due(),
                Err(RecvTimeoutError::Disconnected) => return false,
            }
        }
    }

    /// Waits the retry delay, leaving queued packets in the queue; `false` when
    /// the writer stops meanwhile.
    fn wait_to_retry(&mut self) -> bool {
        let deadline = Instant::now() + self.transport.retry_delay;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return true;
            }
            match self.stop.recv_timeout(left.min(self.until_report())) {
                Ok(never) => match never {},
                Err(RecvTimeoutError::Timeout) => self.report_if_due(),
                Err(RecvTimeoutError::Disconnected) => return false,
            }
        }
    }

    fn until_report(&self) -> Duration {
        self.next_report.saturating_duration_since(Instant::now())
    }

    /// go-statsd `reportLoop`: the packets dropped since the last report.
    fn report_if_due(&mut self) {
        let now = Instant::now();
        if now < self.next_report {
            return;
        }
        self.next_report = now + self.transport.report_interval;
        let lost = self.lost.swap(0, Ordering::Relaxed);
        if lost > 0 {
            statsd_log(Level::Warn, &format!("{lost} packets lost (overflow)"));
        }
    }
}

/// The writer's side of a started sender thread.
struct SenderHandles {
    queue: mpsc::SyncSender<Vec<u8>>,
    stop: mpsc::Sender<Infallible>,
    connected: Arc<AtomicBool>,
    finished: mpsc::Receiver<Infallible>,
}

/// Sends counter interval values (`|c`) and gauge values (`|g`) over UDP with the
/// configured prefix and default tags in the configured tag format.
pub struct StatsdWriter {
    config: StatsdConfig,
    tags: Option<StringMap>,
    transport: Arc<Transport>,
    /// Set by `run` (boxed: the writer stays small, as upstream's `*statsd.Client`).
    client: Mutex<Option<Box<Client>>>,
}

impl std::fmt::Debug for StatsdWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsdWriter")
            .field("config", &self.config)
            .field("tags", &self.tags)
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl StatsdWriter {
    pub fn new(config: StatsdConfig, tags: Option<StringMap>) -> Self {
        Self::with_transport(config, tags, Transport::default())
    }

    fn with_transport(config: StatsdConfig, tags: Option<StringMap>, transport: Transport) -> Self {
        Self { config, tags, transport: Arc::new(transport), client: Mutex::new(None) }
    }

    /// Starts the sender thread, which connects right away (as go-statsd's
    /// `NewClient`), and returns the queue to it.
    fn start_sender(&self, lost: &Arc<AtomicU64>) -> Result<SenderHandles, MetricsError> {
        let (queue, queue_rx) = mpsc::sync_channel(QUEUE_CAPACITY);
        let (stop, stop_rx) = mpsc::channel();
        let (finished_tx, finished) = mpsc::channel();
        let connected = Arc::new(AtomicBool::new(false));
        let sender = Sender {
            host: self.config.host.clone(),
            transport: Arc::clone(&self.transport),
            queue: queue_rx,
            stop: stop_rx,
            lost: Arc::clone(lost),
            next_report: Instant::now() + self.transport.report_interval,
            connected: Arc::clone(&connected),
            _finished: finished_tx,
        };
        std::thread::Builder::new()
            .name("statsd-sender".into())
            .spawn(move || sender.run())
            .map_err(|e| MetricsError(format!("starting the StatsD sender: {e}")))?;
        Ok(SenderHandles { queue, stop, connected, finished })
    }
}

impl IntervalWriter for StatsdWriter {
    fn run(&self, interval: u64) -> Result<(), MetricsError> {
        let mut rendered = String::new();
        let mut in_name = false;
        if let Some(tags) = &self.tags {
            let format = resolve_tag_format(&self.config.tag_format)?;
            in_name = format.in_name;
            // Upstream passes a Go map (random order); sorted here.
            let mut sorted: Vec<_> = tags.iter().collect();
            sorted.sort();
            for (i, (k, v)) in sorted.into_iter().enumerate() {
                if i == 0 {
                    rendered.push_str(format.first);
                } else {
                    rendered.push(format.other);
                }
                rendered.push_str(k);
                rendered.push(format.key_value);
                rendered.push_str(v);
            }
        }
        let lost = Arc::new(AtomicU64::new(0));
        let SenderHandles { queue, stop, connected, finished } = self.start_sender(&lost)?;
        *self.client.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(Box::new(Client { tags: rendered, in_name, queue, stop, lost, connected, finished }));

        let tags_info = match &self.tags {
            // Go's `%v` of a map: sorted `map[k:v ...]`.
            Some(tags) => {
                let mut sorted: Vec<_> = tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
                sorted.sort();
                format!(", tags=map[{}], style={}", sorted.join(" "), self.config.tag_format)
            }
            None => String::new(),
        };
        // Upstream logs through the runner's logger (no context).
        Logger::default().log(
            Level::Info,
            &format!(
                "Send statsd metrics to {} with every {interval}s (prefix={}{tags_info})",
                self.config.host, self.config.prefix
            ),
            &[],
        );
        Ok(())
    }

    /// Stops the sender (go-statsd `Close`): the packets already queued are
    /// sent if it is connected, for at most [`STOP_TIMEOUT`].
    fn stop(&self) {
        let client = self.client.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(client) = client {
            client.stop(self.transport.stop_timeout);
        }
    }

    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        let guard = self.client.lock().unwrap_or_else(|e| e.into_inner());
        let Some(client) = guard.as_ref() else {
            return Ok(());
        };

        let prefix = &self.config.prefix;
        let mut lines: Vec<String> = Vec::new();
        let mut line = |name: &str, value: i64, kind: &str| {
            let mut l = String::with_capacity(prefix.len() + name.len() + client.tags.len() + 24);
            l.push_str(prefix);
            l.push_str(name);
            if client.in_name {
                l.push_str(&client.tags);
            }
            l.push(':');
            l.push_str(&value.to_string());
            l.push('|');
            l.push_str(kind);
            if !client.in_name {
                l.push_str(&client.tags);
            }
            l.push('\n');
            lines.push(l);
        };

        metrics.each_counter(&mut |c| {
            // go-statsd skips zero increments.
            let v = c.interval_value() as i64;
            if v != 0 {
                line(c.name(), v, "c");
            }
        });
        metrics.each_gauge(&mut |g| {
            let v = g.value() as i64;
            // go-statsd resets a gauge to 0 before sending a negative value.
            if v < 0 {
                line(g.name(), 0, "g");
            }
            line(g.name(), v, "g");
        });

        let max = usize::try_from(self.config.max_packet_size).unwrap_or(0).max(1);
        let mut packets = Vec::new();
        let mut packet = String::with_capacity(max);
        for l in lines {
            if !packet.is_empty() && packet.len() + l.len() > max {
                packets.push(std::mem::take(&mut packet));
            }
            packet.push_str(&l);
        }
        if !packet.is_empty() {
            packets.push(packet);
        }
        for packet in packets {
            match client.queue.try_send(packet.into_bytes()) {
                Ok(()) => {}
                // go-statsd drops the packet being queued and reports the count.
                Err(TrySendError::Full(_)) => {
                    client.lost.fetch_add(1, Ordering::Relaxed);
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::metrics::Instrumenter;
    use crate::metrics::worker_tests::{assert_beats, heartbeat, one_worker};

    /// How long a test waits for something that must happen.
    const DEADLINE: Duration = Duration::from_secs(2);
    /// How long a test waits to see that something does not happen.
    const QUIET: Duration = Duration::from_millis(150);

    /// A loopback StatsD server.
    fn sink() -> (UdpSocket, SocketAddr) {
        let socket = UdpSocket::bind("127.0.0.1:0").expect("UDP sink");
        socket.set_read_timeout(Some(DEADLINE)).expect("read timeout");
        let addr = socket.local_addr().expect("sink address");
        (socket, addr)
    }

    fn recv(socket: &UdpSocket) -> Option<String> {
        let mut buf = [0u8; 2048];
        let n = socket.recv(&mut buf).ok()?;
        Some(String::from_utf8_lossy(&buf[..n]).into_owned())
    }

    /// A connect that reports each call on the returned receiver and then
    /// waits for what the returned sender gives it: a sink's address to
    /// connect to, or an error. It fails once the sender is dropped, so a
    /// failing test does not leave the sender thread blocked.
    fn gated_connect() -> (Connect, mpsc::Receiver<Instant>, mpsc::Sender<std::io::Result<SocketAddr>>) {
        let (called, calls) = mpsc::channel();
        let (release, gate) = mpsc::channel::<std::io::Result<SocketAddr>>();
        let gate = Mutex::new(gate);
        let connect: Connect = Arc::new(move |_host: &str| {
            let _ = called.send(Instant::now());
            let target = gate.lock().unwrap_or_else(|e| e.into_inner()).recv();
            match target {
                Ok(Ok(addr)) => connect_udp(&addr.to_string()),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(std::io::Error::other("the test ended")),
            }
        });
        (connect, calls, release)
    }

    /// A writer for a host that is never resolved (`connect` replaces the lookup).
    fn writer(connect: Connect, retry_delay: Duration) -> Arc<StatsdWriter> {
        let mut config = StatsdConfig::new();
        config.host = "statsd.invalid:8125".into();
        let transport = Transport { connect, retry_delay, ..Transport::default() };
        Arc::new(StatsdWriter::with_transport(config, None, transport))
    }

    /// Metrics with one gauge set to `value`: one packet per write.
    fn metrics(value: i64) -> Arc<Metrics> {
        let m = Arc::new(Metrics::new(Vec::new(), 0));
        m.register_gauge("test_gauge", "");
        m.gauge("test_gauge").expect("gauge").set(value);
        m
    }

    fn packet(value: i64) -> String {
        format!("anycable_go.test_gauge:{value}|g\n")
    }

    /// While the host lookup and connect are blocked, `run` and `write` return
    /// at once on a one-worker runtime and the worker keeps running other
    /// tasks; the metrics written meanwhile are sent once the connect succeeds.
    /// Before, `run` and every `write` without a socket resolved the host on the
    /// worker (5 s per interval for a `.local` name that does not resolve).
    #[test]
    fn a_blocked_connect_blocks_no_runtime_worker() {
        let rt = one_worker();
        let (connect, calls, release) = gated_connect();
        let w = writer(connect, Duration::from_secs(60));
        let m = metrics(123);
        let beats = heartbeat(&rt);

        let (done, written) = mpsc::channel();
        let (w2, m2) = (Arc::clone(&w), Arc::clone(&m));
        rt.spawn(async move {
            w2.run(1).expect("run");
            for _ in 0..5 {
                w2.write(&m2).expect("write");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let _ = done.send(());
        });
        calls.recv_timeout(DEADLINE).expect("the sender connects");
        assert_beats(&beats, 10);
        written.recv_timeout(DEADLINE).expect("run and the writes return while connecting");

        let (socket, addr) = sink();
        release.send(Ok(addr)).expect("release the connect");
        for i in 0..5 {
            assert_eq!(Some(packet(123)), recv(&socket), "packet {i}");
        }
        w.stop();
    }

    /// Metrics::shutdown returns at once while the connect is still blocked,
    /// and so does Metrics::run.
    #[test]
    fn shutdown_does_not_wait_for_a_blocked_connect() {
        let rt = one_worker();
        let (connect, calls, _release) = gated_connect();
        let w = writer(connect, Duration::from_secs(60));
        let m = Arc::new(Metrics::new(vec![w as Arc<dyn IntervalWriter>], 1));
        let beats = heartbeat(&rt);

        let (done, ran) = mpsc::channel();
        let m2 = Arc::clone(&m);
        rt.spawn(async move {
            let _ = done.send(m2.run().await);
        });
        calls.recv_timeout(DEADLINE).expect("the sender connects");
        assert_beats(&beats, 10);

        let started = Instant::now();
        rt.block_on(m.shutdown()).expect("shutdown");
        assert!(started.elapsed() < Duration::from_millis(200), "shutdown took {:?}", started.elapsed());
        assert_eq!(Ok(Ok(())), ran.recv_timeout(DEADLINE), "run returns once shut down");
    }

    /// After a failed connect, the sender waits the retry delay before it
    /// connects again (go-statsd: 5 s), whatever the writes in between; the
    /// metrics queued meanwhile are sent after the reconnect.
    #[test]
    fn a_failed_connect_is_retried_after_the_delay() {
        let (connect, calls, release) = gated_connect();
        let w = writer(connect, Duration::from_secs(60));
        let m = metrics(1);
        release.send(Err(std::io::Error::other("lookup failed"))).expect("fail the first connect");
        w.run(1).expect("run");
        calls.recv_timeout(DEADLINE).expect("the first connect");
        let (done, written) = mpsc::channel();
        let (w2, m2) = (Arc::clone(&w), Arc::clone(&m));
        std::thread::spawn(move || {
            for _ in 0..5 {
                w2.write(&m2).expect("write");
            }
            let _ = done.send(());
        });
        written.recv_timeout(DEADLINE).expect("the writes return without connecting");
        assert!(calls.recv_timeout(QUIET).is_err(), "no connect before the retry delay");
        w.stop();

        let retry_delay = Duration::from_millis(100);
        let (connect, calls, release) = gated_connect();
        let w = writer(connect, retry_delay);
        let (socket, addr) = sink();
        release.send(Err(std::io::Error::other("lookup failed"))).expect("fail the first connect");
        release.send(Ok(addr)).expect("let the second connect succeed");
        w.run(1).expect("run");
        let first = calls.recv_timeout(DEADLINE).expect("the first connect");
        w.write(&m).expect("write");
        let second = calls.recv_timeout(DEADLINE).expect("the connect after the delay");
        assert!(second.duration_since(first) >= retry_delay, "retried after {:?}", second.duration_since(first));
        assert_eq!(Some(packet(1)), recv(&socket));
        w.stop();
    }

    /// A writer connected to `addr` whose sends go through `send` and whose
    /// stop waits at most `stop_timeout`.
    fn sending_writer(addr: SocketAddr, send: SendPacket, stop_timeout: Duration) -> Arc<StatsdWriter> {
        let mut config = StatsdConfig::new();
        config.host = addr.to_string();
        let transport = Transport { send, stop_timeout, ..Transport::default() };
        Arc::new(StatsdWriter::with_transport(config, None, transport))
    }

    /// Waits until the writer's sender has a socket.
    fn wait_connected(w: &StatsdWriter) {
        let deadline = Instant::now() + DEADLINE;
        while !w.client.lock().unwrap().as_ref().is_some_and(|c| c.connected.load(Ordering::Acquire)) {
            assert!(Instant::now() < deadline, "the sender connects");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// Packets queued right before the writer stops are sent before `stop`
    /// returns, as go-statsd's `Close` flushes them. Before, `stop` returned at
    /// once and the packets were sent only if the process was still running.
    #[test]
    fn packets_queued_before_stop_are_sent_when_it_returns() {
        let (socket, addr) = sink();
        let sent = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&sent);
        let send: SendPacket = Arc::new(move |socket: &UdpSocket, packet: &[u8]| {
            // A send that takes a while, so the packets are still queued at stop.
            std::thread::sleep(Duration::from_millis(20));
            let n = socket.send(packet)?;
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(n)
        });
        let w = sending_writer(addr, send, STOP_TIMEOUT);
        w.run(1).expect("run");
        wait_connected(&w);
        for value in 0..QUEUE_CAPACITY as i64 {
            w.write(&metrics(value)).expect("write");
        }
        w.stop();
        assert_eq!(QUEUE_CAPACITY as u64, sent.load(Ordering::SeqCst), "every queued packet is sent when stop returns");
        for value in 0..QUEUE_CAPACITY as i64 {
            assert_eq!(Some(packet(value)), recv(&socket), "packet {value}");
        }
    }

    /// A send that does not return holds `stop` for the stop timeout only.
    #[test]
    fn stop_waits_for_a_stalled_send_for_the_stop_timeout_only() {
        let (_socket, addr) = sink();
        let (release, gate) = mpsc::channel::<()>();
        let gate = Mutex::new(gate);
        let send: SendPacket = Arc::new(move |socket: &UdpSocket, packet: &[u8]| {
            let _ = gate.lock().unwrap_or_else(|e| e.into_inner()).recv();
            socket.send(packet)
        });
        let stop_timeout = Duration::from_millis(200);
        let w = sending_writer(addr, send, stop_timeout);
        w.run(1).expect("run");
        wait_connected(&w);
        w.write(&metrics(1)).expect("write");
        let started = Instant::now();
        w.stop();
        let took = started.elapsed();
        assert!(took >= stop_timeout && took < stop_timeout + QUIET * 2, "stop took {took:?}");
        drop(release);
    }

    /// With an address nothing listens on, or one that is not reachable,
    /// stopping returns within the stop timeout.
    #[test]
    fn stop_with_an_unreachable_address_returns_within_the_stop_timeout() {
        let closed = UdpSocket::bind("127.0.0.1:0").expect("a port").local_addr().expect("its address");
        let unroutable: SocketAddr = "192.0.2.1:8125".parse().expect("a TEST-NET-1 address");
        for addr in [closed, unroutable] {
            let w = sending_writer(addr, Transport::default().send, STOP_TIMEOUT);
            w.run(1).expect("run");
            for value in 0..(QUEUE_CAPACITY * 2) as i64 {
                w.write(&metrics(value)).expect("write");
            }
            let started = Instant::now();
            w.stop();
            assert!(started.elapsed() < STOP_TIMEOUT + QUIET, "{addr}: stop took {:?}", started.elapsed());
        }
    }

    /// With the sender's queue full, a write drops the packets it cannot
    /// queue (the newest, as go-statsd) and counts them, instead of waiting.
    #[test]
    fn a_full_queue_drops_the_newest_packets() {
        let (connect, calls, release) = gated_connect();
        let w = writer(connect, Duration::from_secs(60));
        w.run(1).expect("run");
        calls.recv_timeout(DEADLINE).expect("the sender connects");
        let extra = 3;
        for value in 0..(QUEUE_CAPACITY + extra) as i64 {
            w.write(&metrics(value)).expect("write");
        }
        let lost = |w: &StatsdWriter| w.client.lock().unwrap().as_ref().map(|c| c.lost.load(Ordering::Relaxed));
        assert_eq!(Some(extra as u64), lost(&w));

        let (socket, addr) = sink();
        release.send(Ok(addr)).expect("release the connect");
        for value in 0..QUEUE_CAPACITY as i64 {
            assert_eq!(Some(packet(value)), recv(&socket));
        }
        socket.set_read_timeout(Some(QUIET)).expect("read timeout");
        assert_eq!(None, recv(&socket), "the dropped packets are not sent");
        w.stop();
    }
}
