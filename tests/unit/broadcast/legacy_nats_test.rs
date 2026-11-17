//! upstream file: broadcast/legacy_nats_test.go

use cabby_testkit::reference::broadcast::{
    legacy_nats_config_decode_toml, legacy_nats_config_to_toml, new_legacy_nats_config,
};

/// upstream: broadcast/legacy_nats_test.go#TestLegacyNATSConfig__ToToml
#[test]
fn test_legacy_nats_config_to_toml() {
    let mut conf = new_legacy_nats_config();
    conf.channel = "_test_".into();

    let toml_str = legacy_nats_config_to_toml(&conf);

    assert!(toml_str.contains("channel = \"_test_\""), "{toml_str}");

    // Round-trip test
    let conf2 = legacy_nats_config_decode_toml(new_legacy_nats_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}
