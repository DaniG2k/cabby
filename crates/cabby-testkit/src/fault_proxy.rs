//! A TCP proxy that injects faults between a server under test and Redis.
//!
//! Used to pin fixes for upstream issues where a node silently stops receiving
//! pub/sub broadcasts (anycable/anycable#275): the proxy can drop a subscription
//! behind the server's back (by injecting `UNSUBSCRIBE` on its subscriber
//! connection) or sever the Redis side of that connection while leaving the
//! server's socket half-open (no FIN, no RST; it just goes silent).
//!
//! Connections are classified as *subscriber* connections once the client sends
//! a `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE` command through them.
//!
//! [`FaultProxy::freeze`] stalls every connection (a black hole: nothing is
//! forwarded either way, sockets stay open) until [`FaultProxy::thaw`]; it works
//! for any TCP protocol (Redis, NATS).

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Conn {
    /// Writes towards Redis are serialized so injected commands never interleave
    /// with the client's own bytes.
    upstream: Mutex<Option<TcpStream>>,
    subscriber: AtomicBool,
    severed: AtomicBool,
}

pub struct FaultProxy {
    addr: SocketAddr,
    conns: Arc<Mutex<Vec<Arc<Conn>>>>,
    stop: Arc<AtomicBool>,
    frozen: Arc<AtomicBool>,
}

/// Waits while the proxy is frozen.
fn hold(frozen: &AtomicBool) {
    while frozen.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Whether a client chunk carries a (P/S)SUBSCRIBE command (not UNSUBSCRIBE).
fn is_subscribe(chunk: &[u8]) -> bool {
    let upper: Vec<u8> = chunk.iter().map(u8::to_ascii_uppercase).collect();
    upper.windows(9).enumerate().any(|(i, w)| w == b"SUBSCRIBE" && !(i >= 2 && &upper[i - 2..i] == b"UN"))
}

impl FaultProxy {
    /// Starts forwarding `127.0.0.1:<ephemeral>` to `upstream`.
    pub fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding fault proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let conns: Arc<Mutex<Vec<Arc<Conn>>>> = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let frozen = Arc::new(AtomicBool::new(false));
        let (c, st, fz) = (Arc::clone(&conns), Arc::clone(&stop), Arc::clone(&frozen));
        std::thread::spawn(move || {
            for client in listener.incoming() {
                if st.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(client) = client else { continue };
                hold(&fz);
                let Ok(server) = TcpStream::connect_timeout(&upstream, Duration::from_secs(5)) else {
                    let _ = client.shutdown(Shutdown::Both);
                    continue;
                };
                let conn = Arc::new(Conn {
                    upstream: Mutex::new(Some(server.try_clone().expect("clone upstream"))),
                    subscriber: AtomicBool::new(false),
                    severed: AtomicBool::new(false),
                });
                c.lock().unwrap_or_else(|e| e.into_inner()).push(Arc::clone(&conn));
                // client -> Redis
                let (mut from_client, cn) = (client.try_clone().expect("clone client"), Arc::clone(&conn));
                let f = Arc::clone(&fz);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 16 * 1024];
                    while let Ok(n @ 1..) = from_client.read(&mut buf) {
                        hold(&f);
                        if cn.severed.load(Ordering::SeqCst) {
                            continue; // half-open: swallow everything
                        }
                        if is_subscribe(&buf[..n]) {
                            cn.subscriber.store(true, Ordering::SeqCst);
                        }
                        let mut up = cn.upstream.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(s) = up.as_mut()
                            && s.write_all(&buf[..n]).is_err()
                        {
                            break;
                        }
                    }
                    if !cn.severed.load(Ordering::SeqCst)
                        && let Some(s) = cn.upstream.lock().unwrap_or_else(|e| e.into_inner()).as_ref()
                    {
                        let _ = s.shutdown(Shutdown::Write);
                    }
                });
                // Redis -> client
                let (mut from_server, mut to_client, cn) = (server, client, conn);
                let f = Arc::clone(&fz);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 16 * 1024];
                    while let Ok(n @ 1..) = from_server.read(&mut buf) {
                        hold(&f);
                        if cn.severed.load(Ordering::SeqCst) || to_client.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    // A severed connection stays open towards the client, silently.
                    if !cn.severed.load(Ordering::SeqCst) {
                        let _ = to_client.shutdown(Shutdown::Write);
                    }
                });
            }
        });
        Self { addr, conns, stop, frozen }
    }

    /// Stops forwarding on every connection, both ways, and holds new
    /// connections, until [`FaultProxy::thaw`]: the server sees a stalled peer.
    pub fn freeze(&self) {
        self.frozen.store(true, Ordering::SeqCst);
    }

    /// Resumes forwarding (data held meanwhile is delivered).
    pub fn thaw(&self) {
        self.frozen.store(false, Ordering::SeqCst);
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    fn subscribers(&self) -> Vec<Arc<Conn>> {
        let conns = self.conns.lock().unwrap_or_else(|e| e.into_inner());
        conns
            .iter()
            .filter(|c| c.subscriber.load(Ordering::SeqCst) && !c.severed.load(Ordering::SeqCst))
            .cloned()
            .collect()
    }

    /// Waits until at least one live subscriber connection exists.
    pub fn await_subscriber(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if !self.subscribers().is_empty() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    /// Sends a Redis command (RESP array) to Redis on every live subscriber
    /// connection, as if the server had sent it. Returns how many connections
    /// received it.
    pub fn inject_command(&self, args: &[&str]) -> usize {
        let mut cmd = format!("*{}\r\n", args.len());
        for a in args {
            cmd.push_str(&format!("${}\r\n{a}\r\n", a.len()));
        }
        let mut n = 0;
        for c in self.subscribers() {
            let mut up = c.upstream.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(s) = up.as_mut()
                && s.write_all(cmd.as_bytes()).is_ok()
            {
                n += 1;
            }
        }
        n
    }

    /// Closes the Redis side of every live subscriber connection while keeping
    /// the server's side open and silent (a half-open connection). Returns how
    /// many connections were severed.
    pub fn sever_subscribers(&self) -> usize {
        let subs = self.subscribers();
        for c in &subs {
            c.severed.store(true, Ordering::SeqCst);
            if let Some(s) = c.upstream.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = s.shutdown(Shutdown::Both);
            }
        }
        subs.len()
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.frozen.store(false, Ordering::SeqCst);
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(200));
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn classifies_subscribe_commands() {
        assert!(super::is_subscribe(b"*2\r\n$9\r\nSUBSCRIBE\r\n$1\r\na\r\n"));
        assert!(super::is_subscribe(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$1\r\na\r\n"));
        assert!(!super::is_subscribe(b"*2\r\n$11\r\nUNSUBSCRIBE\r\n$1\r\na\r\n"));
        assert!(!super::is_subscribe(b"*3\r\n$7\r\nPUBLISH\r\n$1\r\na\r\n$1\r\nb\r\n"));
    }
}
