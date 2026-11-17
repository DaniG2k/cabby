//! A real NATS server (with JetStream) for broker integration tests.
//!
//! Upstream tests start the embedded server (`enats`); here the server is an
//! external process: `CABBY_NATS_SERVER`, or `nats-server` built from the
//! version pinned by the reference `go.mod` into `target/test-services/`.
//! Every instance gets its own port and storage directory.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::ports::free_port;
use crate::process::Supervised;
use crate::workspace_root;

const START_DEADLINE: Duration = Duration::from_secs(20);

fn server_binary() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("CABBY_NATS_SERVER") {
        return Ok(PathBuf::from(p));
    }
    let root = workspace_root();
    let out = root.join("target/test-services/nats-server");
    if out.exists() {
        return Ok(out);
    }
    let status = Command::new("go")
        .args(["build", "-o"])
        .arg(&out)
        .arg("github.com/nats-io/nats-server/v2")
        .current_dir(root.join("compat/reference"))
        .status()
        .context("running `go build` for nats-server (is the Go toolchain on PATH?)")?;
    if !status.success() {
        bail!("building nats-server failed; run `cargo xtask reference-checkout` first");
    }
    Ok(out)
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
        let bin = server_binary()?;
        let port = free_port();
        let store_dir = workspace_root().join(format!("target/tmp/nats-{}-{port}", std::process::id()));
        std::fs::create_dir_all(&store_dir)?;
        let mut cmd = Command::new(bin);
        cmd.args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"]).arg(&store_dir);
        let mut process = Supervised::spawn("nats-server", cmd)?;
        process.wait_tcp(SocketAddr::from(([127, 0, 0, 1], port)), START_DEADLINE)?;
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
