//! upstream file: ds/config_test.go

use cabby_testkit::reference::ds::{config_to_toml, decode_config_toml, new_config};

/// upstream: ds/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.path = "/streams/v1".into();

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("path = \"/streams/v1\""), "{toml_str}");
    assert!(toml_str.contains("# enabled = true"), "{toml_str}");

    // Round-trip test
    let conf2 = decode_config_toml(&toml_str).expect("decoding the generated TOML");

    assert_eq!(conf, conf2);
}

/// upstream: ds/config_test.go#TestConfig_ToToml_SSEPingInterval
#[test]
fn test_config_to_toml_sse_ping_interval() {
    let mut conf = new_config();

    let toml_str = config_to_toml(&conf);
    assert!(toml_str.contains("# sse_ping_interval = 15"), "{toml_str}");

    conf.sse_ping_interval = 30;
    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("\nsse_ping_interval = 30"), "{toml_str}");

    let conf2 = decode_config_toml(&toml_str).expect("decoding the generated TOML");

    assert_eq!(conf, conf2);
}
