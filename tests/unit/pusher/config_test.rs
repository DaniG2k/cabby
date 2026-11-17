//! upstream file: pusher/config_test.go

use cabby_testkit::reference::pusher::{config_to_toml, decode_config_toml, new_config};

/// upstream: pusher/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.app_key = "test-app".into();

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("app_key = \"test-app\""), "{toml_str}");

    // Round-trip test
    let conf2 = decode_config_toml(&toml_str).expect("decoding the generated TOML");

    assert_eq!(conf, conf2);
}
