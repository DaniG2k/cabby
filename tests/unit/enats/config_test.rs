//! upstream file: enats/config_test.go
//!
//! Configuration only; the embedded NATS server is a separate milestone
//! (SPEC.md §3).

use cabby_server::enats::Config;
use cabby_testkit::reference::enats::{decode_toml, to_toml};

fn contains(haystack: &str, needle: &str) {
    assert!(haystack.contains(needle), "expected {needle:?} in:\n{haystack}");
}

/// upstream: enats/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let conf = Config {
        enabled: true,
        debug: false,
        trace: true,
        name: "test-service".into(),
        service_addr: "localhost:4222".into(),
        cluster_addr: "localhost:6222".into(),
        cluster_name: "test-cluster".into(),
        gateway_addr: "localhost:7222".into(),
        gateway_advertise: "public.example.com:7222".into(),
        gateways: vec!["nats://gateway1:7222".into(), "nats://gateway2:7222".into()],
        routes: vec!["nats://route1:6222".into(), "nats://route2:6222".into()],
        jetstream: true,
        store_dir: "/tmp/nats-store".into(),
        jetstream_ready_timeout: 30,
        max_payload: 4194304,
    };

    let toml_str = to_toml(&conf);

    contains(&toml_str, "enabled = true");
    contains(&toml_str, "# debug = true");
    contains(&toml_str, "trace = true");
    contains(&toml_str, "name = \"test-service\"");
    contains(&toml_str, "service_addr = \"localhost:4222\"");
    contains(&toml_str, "cluster_addr = \"localhost:6222\"");
    contains(&toml_str, "cluster_name = \"test-cluster\"");
    contains(&toml_str, "gateway_addr = \"localhost:7222\"");
    contains(&toml_str, "gateway_advertise = \"public.example.com:7222\"");
    contains(&toml_str, "gateways = [\"nats://gateway1:7222\", \"nats://gateway2:7222\"]");
    contains(&toml_str, "routes = [\"nats://route1:6222\", \"nats://route2:6222\"]");
    contains(&toml_str, "jetstream = true");
    contains(&toml_str, "jetstream_store_dir = \"/tmp/nats-store\"");
    contains(&toml_str, "jetstream_ready_timeout = 30");
    contains(&toml_str, "max_payload = 4194304");

    // Round-trip test (upstream decodes into the zero value `var conf2 Config`)
    let conf2 = decode_toml(Config::default(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}

/// upstream: enats/config_test.go#TestConfig_ToToml_DefaultMaxPayload
#[test]
fn test_config_to_toml_default_max_payload() {
    let conf = Config { service_addr: "localhost:4222".into(), ..Default::default() };

    let toml_str = to_toml(&conf);

    // When MaxPayload is 0 (default), it should be commented out
    contains(&toml_str, "# max_payload = 1048576");
}
