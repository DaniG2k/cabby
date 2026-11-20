//! A fake single-node Redis Cluster for pub/sub: it owns every slot, answers
//! the connection handshakes of the `redis` crate and rueidis (RESP2 and
//! RESP3), `CLUSTER SLOTS`, `PING`, `SUBSCRIBE`/`UNSUBSCRIBE` and `PUBLISH`
//! (delivered to its subscribers), and records every `PUBLISH`.
//!
//! A real cluster node routes keyed commands by slot; clients check that a
//! pipeline stays within one slot. That client-side check is what the fake
//! exercises: a server publishing a batch of channels in one pipeline fails
//! against any cluster (review finding H6).

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

type Writer = Arc<Mutex<TcpStream>>;

struct Client {
    writer: Writer,
    resp3: bool,
}

#[derive(Default)]
struct State {
    /// Channel -> subscribed connections (by id).
    subscribers: HashMap<Vec<u8>, Vec<u64>>,
    clients: HashMap<u64, Client>,
    published: Vec<String>,
    /// Command names (with the subcommand for `CLUSTER` and `CLIENT`).
    commands: Vec<String>,
}

pub struct FakeRedisCluster {
    addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

fn bulk(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Reads one command: a RESP array of bulk strings, or an inline command.
fn read_command(r: &mut impl BufRead) -> Option<Vec<Vec<u8>>> {
    let mut line = Vec::new();
    if r.read_until(b'\n', &mut line).ok()? == 0 {
        return None;
    }
    let line = line.strip_suffix(b"\r\n").unwrap_or(&line).to_vec();
    let Some(n) = line.strip_prefix(b"*") else {
        return Some(line.split(|b| *b == b' ').filter(|p| !p.is_empty()).map(<[u8]>::to_vec).collect());
    };
    let n: usize = std::str::from_utf8(n).ok()?.parse().ok()?;
    let mut args = Vec::with_capacity(n);
    for _ in 0..n {
        let mut head = Vec::new();
        r.read_until(b'\n', &mut head).ok()?;
        let len: usize = std::str::from_utf8(head.strip_prefix(b"$")?.strip_suffix(b"\r\n")?).ok()?.parse().ok()?;
        let mut arg = vec![0; len + 2];
        r.read_exact(&mut arg).ok()?;
        arg.truncate(len);
        args.push(arg);
    }
    Some(args)
}

impl FakeRedisCluster {
    /// Starts the node; each `PUBLISH` takes `publish_delay` (so that clients
    /// queue up publications and batch them).
    pub fn start(publish_delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding the fake cluster");
        let addr = listener.local_addr().expect("fake cluster address");
        let state: Arc<Mutex<State>> = Arc::default();
        let st = Arc::clone(&state);
        std::thread::spawn(move || {
            static NEXT_ID: AtomicU64 = AtomicU64::new(1);
            for conn in listener.incoming() {
                let Ok(conn) = conn else { continue };
                let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
                let st = Arc::clone(&st);
                std::thread::spawn(move || serve(id, conn, addr, &st, publish_delay));
            }
        });
        Self { addr, state }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The channels of every `PUBLISH` received, in arrival order.
    pub fn published(&self) -> Vec<String> {
        lock(&self.state).published.clone()
    }

    /// Every command received (`CLUSTER SLOTS`, `PUBLISH`, ...), in arrival order.
    pub fn commands(&self) -> Vec<String> {
        lock(&self.state).commands.clone()
    }
}

fn serve(id: u64, conn: TcpStream, addr: SocketAddr, state: &Mutex<State>, publish_delay: Duration) {
    let writer: Writer = Arc::new(Mutex::new(conn.try_clone().expect("clone fake cluster connection")));
    lock(state).clients.insert(id, Client { writer: Arc::clone(&writer), resp3: false });
    let mut reader = BufReader::new(conn);
    while let Some(args) = read_command(&mut reader) {
        let Some(name) = args.first().map(|a| a.to_ascii_uppercase()) else { continue };
        let resp3 = {
            let mut st = lock(state);
            let mut cmd = String::from_utf8_lossy(&name).into_owned();
            if matches!(name.as_slice(), b"CLUSTER" | b"CLIENT")
                && let Some(sub) = args.get(1)
            {
                cmd.push(' ');
                cmd.push_str(&String::from_utf8_lossy(sub).to_ascii_uppercase());
            }
            st.commands.push(cmd);
            st.clients.get(&id).is_some_and(|c| c.resp3)
        };
        let mut out = Vec::new();
        match name.as_slice() {
            b"HELLO" => {
                let v3 = args.get(1).is_some_and(|v| v == b"3");
                if let Some(c) = lock(state).clients.get_mut(&id) {
                    c.resp3 = v3;
                }
                // 6.2: rueidis probes with CLUSTER SLOTS (CLUSTER SHARDS from 7.0).
                out.extend_from_slice(if v3 { b"%3\r\n" } else { b"*6\r\n" });
                for (k, v) in [("server", "redis"), ("version", "6.2.0")] {
                    bulk(&mut out, k.as_bytes());
                    bulk(&mut out, v.as_bytes());
                }
                bulk(&mut out, b"proto");
                out.extend_from_slice(if v3 { b":3\r\n" } else { b":2\r\n" });
            }
            b"AUTH" | b"SELECT" | b"READONLY" | b"READWRITE" | b"CLIENT" => out.extend_from_slice(b"+OK\r\n"),
            b"PING" => out.extend_from_slice(b"+PONG\r\n"),
            b"CLUSTER" if args.get(1).is_some_and(|a| a.eq_ignore_ascii_case(b"SLOTS")) => {
                out.extend_from_slice(b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n");
                bulk(&mut out, addr.ip().to_string().as_bytes());
                out.extend_from_slice(format!(":{}\r\n", addr.port()).as_bytes());
                bulk(&mut out, &[b'a'; 40]);
            }
            b"SUBSCRIBE" | b"UNSUBSCRIBE" => {
                let sub = name.as_slice() == b"SUBSCRIBE";
                let mut st = lock(state);
                let channels: Vec<Vec<u8>> = if args.len() > 1 {
                    args[1..].to_vec()
                } else {
                    st.subscribers.iter().filter(|(_, ids)| ids.contains(&id)).map(|(c, _)| c.clone()).collect()
                };
                for ch in channels {
                    let ids = st.subscribers.entry(ch.clone()).or_default();
                    ids.retain(|i| *i != id);
                    if sub {
                        ids.push(id);
                    }
                    let count = st.subscribers.values().filter(|ids| ids.contains(&id)).count();
                    out.extend_from_slice(if resp3 { b">3\r\n" } else { b"*3\r\n" });
                    bulk(&mut out, if sub { b"subscribe" } else { b"unsubscribe" });
                    bulk(&mut out, &ch);
                    out.extend_from_slice(format!(":{count}\r\n").as_bytes());
                }
            }
            b"PUBLISH" if args.len() == 3 => {
                std::thread::sleep(publish_delay);
                let mut st = lock(state);
                st.published.push(String::from_utf8_lossy(&args[1]).into_owned());
                let ids = st.subscribers.get(&args[1]).cloned().unwrap_or_default();
                for sid in &ids {
                    let Some(c) = st.clients.get(sid) else { continue };
                    let mut msg = Vec::new();
                    msg.extend_from_slice(if c.resp3 { b">3\r\n" } else { b"*3\r\n" });
                    bulk(&mut msg, b"message");
                    bulk(&mut msg, &args[1]);
                    bulk(&mut msg, &args[2]);
                    let _ = lock(&c.writer).write_all(&msg);
                }
                out.extend_from_slice(format!(":{}\r\n", ids.len()).as_bytes());
            }
            _ => {
                out.extend_from_slice(b"-ERR unknown command '");
                out.extend_from_slice(&args[0]);
                out.extend_from_slice(b"'\r\n");
            }
        }
        if lock(&writer).write_all(&out).is_err() {
            break;
        }
    }
    let mut st = lock(state);
    st.clients.remove(&id);
    for ids in st.subscribers.values_mut() {
        ids.retain(|i| *i != id);
    }
}
