//! upstream file: pubsub/nats_test.go
//!
//! Upstream starts an embedded NATS server (`enats.NewService`) on the default
//! port. Embedded NATS is a separate milestone (SPEC.md §3), so these tests use
//! an external server: the shared one at `CABBY_NATS_URL` (with unique subjects
//! per test) and, for the restart test, a private `nats-server` process.
//! Subscription readiness is read from the server's monitoring endpoint
//! (`/subsz`) instead of subscriber internals plus a 100 ms sleep.

use std::time::Duration;

use cabby_core::common::StreamMessage;
use cabby_server::pubsub::NatsConfig;
use cabby_testkit::reference::nats::new_nats_config as new_client_config;
use cabby_testkit::reference::pubsub::{
    Subscriber, TestHandler, nats_config_decode_toml, nats_config_to_toml, new_nats_config,
};
use cabby_testkit::services::{self, NatsMonitor, NatsServer};

use super::subscriber_test::{self as shared, Harness, ShutdownGuard};

/// Upstream waits up to 6 x 500 ms for the connection plus 100 ms per
/// subscription; NATS clients retry reconnects every 2 s. One bounded deadline.
const WAIT: Duration = Duration::from_secs(20);

struct Nats {
    prefix: String,
    monitor: NatsMonitor,
    config: NatsConfig,
}

impl Nats {
    fn new() -> Self {
        let svc = services::nats();
        let prefix = svc.prefix("pubsub");
        let mut nconfig = new_client_config();
        nconfig.servers = svc.url.clone();
        let mut config = new_nats_config();
        config.channel = format!("{prefix}.__anycable_internal__");
        config.nats = Some(nconfig);
        Nats { prefix, monitor: svc.monitor.clone(), config }
    }
}

impl Harness for Nats {
    fn subscriber(&self, handler: &TestHandler) -> Subscriber {
        Subscriber::nats(handler, &self.config).expect("NewNATSSubscriber")
    }

    fn stream(&self, name: &str) -> String {
        format!("{}.{name}", self.prefix)
    }

    fn internal(&self) -> String {
        self.config.channel.clone()
    }

    fn wait_subscriptions(&self, subject: &str, count: usize) -> Result<(), String> {
        self.monitor.wait_subscriptions(subject, count, WAIT).map_err(|e| e.to_string())
    }
}

/// upstream: pubsub/nats_test.go#TestNATSConfig__ToToml
#[test]
fn test_nats_config_to_toml() {
    let mut conf = new_nats_config();
    conf.channel = "_test_".into();

    let toml_str = nats_config_to_toml(&conf);

    assert!(toml_str.contains("channel = \"_test_\""), "{toml_str}");

    // Round-trip test
    let conf2 = nats_config_decode_toml(new_nats_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}

mod test_nats_common {
    use super::*;

    /// upstream: pubsub/nats_test.go#TestNATSCommon/Broadcast
    /// upstream: pubsub/nats_test.go#TestNATSCommon
    #[test]
    fn broadcast() {
        shared::broadcast(&Nats::new());
    }

    /// upstream: pubsub/nats_test.go#TestNATSCommon/Broadcast_commands
    /// upstream: pubsub/nats_test.go#TestNATSCommon
    #[test]
    fn broadcast_commands() {
        shared::broadcast_commands(&Nats::new());
    }

    /// upstream: pubsub/nats_test.go#TestNATSCommon/Subscribe_-_Broadcast
    /// upstream: pubsub/nats_test.go#TestNATSCommon
    #[test]
    fn subscribe_broadcast() {
        shared::subscribe_broadcast(&Nats::new());
    }

    /// upstream: pubsub/nats_test.go#TestNATSCommon/Re-transmit_commands
    /// upstream: pubsub/nats_test.go#TestNATSCommon
    #[test]
    fn retransmit_commands() {
        shared::retransmit_commands(&Nats::new());
    }

    /// upstream: pubsub/nats_test.go#TestNATSCommon/Subscribe_-_Broadcast_-_Unsubscribe_-_Broadcast
    /// upstream: pubsub/nats_test.go#TestNATSCommon
    #[test]
    fn subscribe_broadcast_unsubscribe_broadcast() {
        shared::subscribe_broadcast_unsubscribe_broadcast(&Nats::new());
    }
}

/// upstream: pubsub/nats_test.go#TestNATSReconnect
///
/// The server is private to this test (it is restarted), so upstream's subject
/// names are kept. Upstream waits for `conn.Status() == CONNECTED` after the
/// restart; here for the re-established subscription on the new server.
#[test]
fn test_nats_reconnect() {
    let mut server = NatsServer::start();
    let monitor = server.monitor();

    let handler = TestHandler::new();
    let mut nconfig = new_client_config();
    nconfig.servers = server.url();
    let mut config = new_nats_config();
    config.nats = Some(nconfig);

    let subscriber = ShutdownGuard(Subscriber::nats(&handler, &config).expect("NewNATSSubscriber"));
    subscriber.0.start().expect("Start");

    monitor.wait_subscriptions(&config.channel, 1, WAIT).expect("wait internal");

    subscriber.0.subscribe("reconnectos");
    monitor.wait_subscriptions("reconnectos", 1, WAIT).expect("wait reconnectos");

    let msg = StreamMessage { stream: "reconnectos".into(), data: "2023".into(), ..Default::default() };
    subscriber.0.broadcast(&msg);

    let received = handler.receive().expect("message");
    assert_eq!("2023", received.data);

    // Reload NATS server
    server.shutdown();
    server.start_again();

    monitor.wait_subscriptions("reconnectos", 1, WAIT).expect("connection restored");

    subscriber.0.broadcast(&msg);

    let received = handler.receive().expect("message");
    assert_eq!("2023", received.data);
}
