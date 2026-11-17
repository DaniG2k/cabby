//! upstream file: metrics/statsd_writer_test.go
//!
//! Upstream shares one UDP socket across subtests; each test here has its own.

use std::time::Duration;

use cabby_testkit::reference::metrics::*;
use cabby_testkit::udp::UdpSink;

/// Upstream waits up to 1s for the datagram.
const PAYLOAD_DEADLINE: Duration = Duration::from_secs(1);

/// The parent test's setup.
fn metrics() -> Metrics {
    let m = Metrics::new(0);

    m.register_counter("test_count", "");
    m.register_gauge("test_gauge", "");

    for _ in 0..10 {
        m.counter("test_count").expect("counter").inc();
    }

    m.gauge("test_gauge").expect("gauge").set(123);
    m
}

/// Runs a writer with `configure`d settings and `tags`, writes `m`, and returns
/// the first datagram received.
fn write_and_receive(configure: impl FnOnce(&mut StatsdConfig), tags: Option<&[(&str, &str)]>) -> String {
    let m = metrics();
    let socket = UdpSink::bind();

    let mut c = new_statsd_config();
    c.host = socket.addr().to_string();
    configure(&mut c);
    let w = StatsdWriter::new(&c, tags);
    let _ = w.run(0);

    let err = w.write(&m);
    assert!(err.is_ok(), "{err:?}");

    let buf = socket.recv(PAYLOAD_DEADLINE);
    w.stop();
    String::from_utf8(buf.expect("timeout waiting for UDP payload")).expect("UTF-8 payload")
}

mod test_statsd_writer {
    use super::*;

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Write_send_UDP_with_metrics
    #[test]
    fn write_send_udp_with_metrics() {
        let payload = write_and_receive(|_| {}, None);

        assert!(payload.contains("anycable_go.test_count:10|c"), "{payload}");
        assert!(payload.contains("anycable_go.test_gauge:123|g"), "{payload}");
    }

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Write_uses_custom_prefix
    #[test]
    fn write_uses_custom_prefix() {
        let payload = write_and_receive(|c| c.prefix = "ws.".into(), None);

        assert!(payload.contains("ws.test_count:10|c"), "{payload}");
        assert!(payload.contains("ws.test_gauge:123|g"), "{payload}");
    }

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Adds_tags_in_DataDog_format
    #[test]
    fn adds_tags_in_datadog_format() {
        let payload = write_and_receive(|c| c.tag_format = "datadog".into(), Some(&[("env", "dev")]));

        assert!(payload.contains("anycable_go.test_count:10|c|#env:dev"), "{payload}");
        assert!(payload.contains("anycable_go.test_gauge:123|g|#env:dev"), "{payload}");
    }

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Adds_multiple_tags_in_DataDog_format
    #[test]
    fn adds_multiple_tags_in_datadog_format() {
        let payload = write_and_receive(|c| c.tag_format = "datadog".into(), Some(&[("env", "dev"), ("rev", "1.1")]));

        assert!(payload.contains("anycable_go.test_count:10|c|#"), "{payload}");
        assert!(payload.contains("anycable_go.test_gauge:123|g|#"), "{payload}");
        assert!(payload.contains("env:dev"), "{payload}");
        assert!(payload.contains("rev:1.1"), "{payload}");
    }

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Adds_tags_in_InfluxDB_format
    #[test]
    fn adds_tags_in_influxdb_format() {
        let payload = write_and_receive(|c| c.tag_format = "influxdb".into(), Some(&[("env", "dev")]));

        assert!(payload.contains("anycable_go.test_count,env=dev:10|c"), "{payload}");
        assert!(payload.contains("anycable_go.test_gauge,env=dev:123|g"), "{payload}");
    }

    /// upstream: metrics/statsd_writer_test.go#TestStatsdWriter/Adds_tags_in_Graphite_format
    #[test]
    fn adds_tags_in_graphite_format() {
        let payload = write_and_receive(|c| c.tag_format = "graphite".into(), Some(&[("env", "dev")]));

        assert!(payload.contains("anycable_go.test_count;env=dev:10|c"), "{payload}");
        assert!(payload.contains("anycable_go.test_gauge;env=dev:123|g"), "{payload}");
    }
}
