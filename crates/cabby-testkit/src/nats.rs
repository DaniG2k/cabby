//! A real NATS server (with JetStream) for broker integration tests.
//!
//! Upstream tests start the embedded server (`enats`); here the server is an
//! external process: `CABBY_NATS_SERVER`, or `nats-server` built from the
//! version pinned by the reference `go.mod` into `target/test-services/`.
//! Every instance gets its own storage directory and binds a port of its own
//! choosing (`-p -1`), which it reports in its start log.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::process::Supervised;
use crate::workspace_root;

const START_DEADLINE: Duration = Duration::from_secs(20);
/// Upstream `service.WaitJetStreamReady(5)`.
const JETSTREAM_DEADLINE: Duration = Duration::from_secs(5);
const PROBE_INTERVAL: Duration = Duration::from_millis(50);

/// Waits until JetStream answers `$JS.API.INFO` with account info (upstream
/// `enats.Service.WaitJetStreamReady`): accepting TCP connections does not mean
/// JetStream has finished starting. Speaks the NATS text protocol directly.
fn wait_jetstream_ready(addr: SocketAddr, deadline: Duration) -> Result<()> {
    let until = Instant::now() + deadline;
    let mut last = String::from("no attempt");
    while Instant::now() < until {
        match jetstream_info(addr) {
            Ok(true) => return Ok(()),
            Ok(false) => last = "JetStream not ready".into(),
            Err(e) => last = format!("{e:#}"),
        }
        std::thread::sleep(PROBE_INTERVAL);
    }
    bail!("JetStream not ready within {deadline:?}: {last}")
}

/// One `$JS.API.INFO` request: `Ok(true)` for an account info response.
fn jetstream_info(addr: SocketAddr) -> Result<bool> {
    const INBOX: &str = "_CABBY_JS_PROBE";
    let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    let mut writer = stream.try_clone()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?; // INFO {...}
    // `no_responders` makes an absent JetStream answer with a 503 status (HMSG).
    write!(
        writer,
        "CONNECT {{\"verbose\":false,\"pedantic\":false,\"headers\":true,\"no_responders\":true}}\r\n\
         SUB {INBOX} 1\r\nPUB $JS.API.INFO {INBOX} 0\r\n\r\nPING\r\n"
    )?;
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            bail!("connection closed");
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        match fields.first().copied() {
            Some("PING") => write!(writer, "PONG\r\n")?,
            Some("PONG") | Some("+OK") | Some("INFO") => {}
            Some("-ERR") => bail!("server error: {}", line.trim()),
            Some("HMSG") => return Ok(false), // no responders yet
            Some("MSG") => {
                let len: usize = fields.last().and_then(|n| n.parse().ok()).context("MSG size")?;
                let mut payload = vec![0; len + 2];
                reader.read_exact(&mut payload)?;
                let text = String::from_utf8_lossy(&payload[..len]);
                return Ok(
                    text.contains("io.nats.jetstream.api.v1.account_info_response") && !text.contains("\"error\"")
                );
            }
            _ => bail!("unexpected protocol line {:?}", line.trim()),
        }
    }
}

fn server_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("CABBY_NATS_SERVER") {
        return Ok(PathBuf::from(p));
    }
    let root = workspace_root();
    let how = crate::go_build::Build {
        what: "nats-server",
        hint: "run `cargo xtask reference-checkout` first",
        reuse: crate::go_build::Reuse::Existing,
    };
    crate::go_build::build(&root.join("target/test-services/nats-server"), &how, |tmp| {
        let mut cmd = Command::new("go");
        cmd.args(["build", "-o"])
            .arg(tmp)
            .arg("github.com/nats-io/nats-server/v2")
            .current_dir(root.join("compat/reference"));
        cmd
    })
}

/// The client port of `nats-server`'s start log line `Listening for client
/// connections on 127.0.0.1:<port>`.
fn listening_port(output: &str) -> Option<u16> {
    output
        .lines()
        .find_map(|l| l.split("Listening for client connections on ").nth(1)?.trim().rsplit(':').next()?.parse().ok())
}

pub struct NatsServer {
    process: Supervised,
    pub port: u16,
    store_dir: PathBuf,
}

impl NatsServer {
    /// Starts a JetStream-enabled server and waits until it accepts connections.
    pub fn start() -> Self {
        Self::try_start().unwrap_or_else(|e| panic!("starting nats-server: {e:#}"))
    }

    fn try_start() -> Result<Self> {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let bin = server_binary()?;
        let store_dir = workspace_root().join(format!(
            "target/tmp/nats-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&store_dir)?;
        let mut cmd = Command::new(bin);
        // `-p -1`: the server binds a port of its own choosing and logs it.
        cmd.args(["-js", "-a", "127.0.0.1", "-p", "-1", "-sd"]).arg(&store_dir);
        let mut process = Supervised::spawn("nats-server", cmd)?;
        let port = process.wait_output(START_DEADLINE, listening_port)?;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        process.wait_tcp(addr, START_DEADLINE)?;
        wait_jetstream_ready(addr, JETSTREAM_DEADLINE)
            .with_context(|| format!("nats-server output:\n{}", process.output()))?;
        Ok(Self { process, port, store_dir })
    }

    /// `nats://127.0.0.1:<port>`.
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.process.stop(Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(&self.store_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_client_port_comes_from_the_start_log() {
        let log = "[1] 2026/11/27 16:02:39.736168 [INF] Starting http monitor on 127.0.0.1:56817\n\
                   [1] 2026/11/27 16:02:39.737205 [INF] Listening for client connections on 127.0.0.1:56818\n";
        assert_eq!(Some(56818), listening_port(log));
        assert_eq!(None, listening_port("[1] [INF] Starting nats-server\n"));
    }
}
