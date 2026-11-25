//! A fake Redis Sentinel that names one real Redis as the master of
//! [`MASTER_SET`]. It answers what the `redis` crate asks a Sentinel (the
//! connection handshake, `ROLE`, `SENTINEL MASTERS`); the master itself is the
//! test's Redis service, so the connections opened afterwards are real.
//!
//! rueidis discovers masters with other commands, so the scenarios call
//! [`rust_only`] first.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

use crate::services::RedisScope;
use crate::{Backend, backend};

/// The master set the fake monitors.
pub const MASTER_SET: &str = "mymaster";

/// Reports the case as not observable on the Go backend.
pub fn rust_only() {
    if backend() == Backend::Go {
        unimplemented!(
            "not_observable: the fake Sentinel answers the redis crate's discovery only; AnyCable's own behavior is in D-082"
        );
    }
}

pub struct FakeSentinel {
    addr: SocketAddr,
}

fn bulk(s: &str) -> String {
    format!("${}\r\n{s}\r\n", s.len())
}

/// Reads one command (a RESP array of bulk strings).
fn read_command(r: &mut impl BufRead) -> Option<Vec<String>> {
    let mut line = String::new();
    r.read_line(&mut line).ok()?;
    let n: usize = line.trim().strip_prefix('*')?.parse().ok()?;
    (0..n)
        .map(|_| {
            let mut head = String::new();
            r.read_line(&mut head).ok()?;
            let len: usize = head.trim().strip_prefix('$')?.parse().ok()?;
            let mut arg = vec![0; len + 2];
            r.read_exact(&mut arg).ok()?;
            arg.truncate(len);
            String::from_utf8(arg).ok()
        })
        .collect()
}

fn serve(conn: TcpStream, master: SocketAddr) -> Option<()> {
    let mut writer = conn.try_clone().ok()?;
    let mut reader = BufReader::new(conn);
    loop {
        let args = read_command(&mut reader)?;
        let reply = match args.first()?.to_uppercase().as_str() {
            "ROLE" => format!("*2\r\n{}*1\r\n{}", bulk("sentinel"), bulk(MASTER_SET)),
            "SENTINEL" => {
                let (ip, port) = (master.ip().to_string(), master.port().to_string());
                let fields = ["name", MASTER_SET, "ip", &ip, "port", &port, "flags", "master"];
                format!("*1\r\n*{}\r\n{}", fields.len(), fields.map(bulk).concat())
            }
            _ => "+OK\r\n".to_owned(),
        };
        writer.write_all(reply.as_bytes()).ok()?;
    }
}

impl FakeSentinel {
    pub fn start(master: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binding the fake Sentinel");
        let addr = listener.local_addr().expect("fake Sentinel address");
        std::thread::spawn(move || {
            for conn in listener.incoming().flatten() {
                std::thread::spawn(move || serve(conn, master));
            }
        });
        Self { addr }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `redis://<user>:<password>@<this Sentinel>?master_set=mymaster`: the
    /// implicit form, with the credentials of the scope's user on the master.
    pub fn implicit_url(&self, scope: &RedisScope) -> String {
        format!("{}?master_set={MASTER_SET}", scope.url_via(self.addr).trim_end_matches('/'))
    }
}
