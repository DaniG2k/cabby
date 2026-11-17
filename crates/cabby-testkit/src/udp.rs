//! A loopback UDP receiver (e.g. a fake StatsD server).

use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

pub struct UdpSink {
    socket: UdpSocket,
}

impl Default for UdpSink {
    fn default() -> Self {
        Self::bind()
    }
}

impl UdpSink {
    pub fn bind() -> Self {
        Self { socket: UdpSocket::bind("127.0.0.1:0").expect("binding a loopback UDP port") }
    }

    pub fn addr(&self) -> SocketAddr {
        self.socket.local_addr().expect("local addr")
    }

    /// The next datagram, or `None` if none arrives within `deadline`.
    pub fn recv(&self, deadline: Duration) -> Option<Vec<u8>> {
        self.socket.set_read_timeout(Some(deadline)).expect("read timeout");
        let mut buf = vec![0u8; 65_536];
        match self.socket.recv(&mut buf) {
            Ok(n) => {
                buf.truncate(n);
                Some(buf)
            }
            Err(_) => None,
        }
    }
}
