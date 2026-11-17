//! Local port allocation for isolated test processes.

use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};

/// A loopback port that is free now and was never handed out before by this
/// process. Tests run in parallel threads, and the OS can return a just-released
/// port to a second `bind(0)`, so allocations are tracked to rule out in-process
/// collisions. Another process may still take the port before the caller binds:
/// supervisors retry startup on bind failures (see `sut::Sut::try_start`).
pub fn free_port() -> u16 {
    static TAKEN: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
    let taken = TAKEN.get_or_init(Default::default);
    loop {
        let port =
            TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).expect("allocating a local port").port();
        if taken.lock().unwrap_or_else(|e| e.into_inner()).insert(port) {
            return port;
        }
    }
}
