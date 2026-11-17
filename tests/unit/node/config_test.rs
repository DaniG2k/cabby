//! upstream file: node/config_test.go

use cabby_testkit::reference::node::*;

/// upstream: node/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.disconnect_mode = "always".into();
    conf.hub_gopool_size = 100;
    conf.ping_timestamp_precision = "ns".into();
    conf.shutdown_disconnect_pool_size = 1024;

    let toml_str = config_to_toml(&conf);

    for expected in [
        "disconnect_mode = \"always\"",
        "broadcast_gopool_size = 100",
        "ping_timestamp_precision = \"ns\"",
        "# pong_timeout = 6",
        "shutdown_disconnect_gopool_size = 1024",
    ] {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }

    // Round-trip test
    let mut conf2 = new_config();

    config_merge_toml(&mut conf2, &toml_str).expect("decode");

    assert_eq!(conf, conf2);
}
