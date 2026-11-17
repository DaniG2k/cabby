//! upstream file: nats/config_test.go

use cabby_testkit::reference::nats::{decode_toml, filtered_servers, new_nats_config, to_toml};

/// upstream: nats/config_test.go#TestNATSConfig_ToToml
#[test]
fn test_nats_config_to_toml() {
    let mut conf = new_nats_config();
    conf.servers = "nats://localhost:4222".into();
    conf.dont_randomize_servers = true;
    conf.max_reconnect_attempts = 10;

    let toml_str = to_toml(&conf);

    assert!(toml_str.contains("servers = \"nats://localhost:4222\""), "{toml_str}");
    assert!(toml_str.contains("dont_randomize_servers = true"), "{toml_str}");
    assert!(toml_str.contains("max_reconnect_attempts = 10"), "{toml_str}");

    // Round-trip test
    let conf2 = decode_toml(new_nats_config(), &toml_str).expect("decoding TOML");

    assert_eq!(conf, conf2);
}

/// upstream: nats/config_test.go#TestNATSConfig_FilteredServers
#[test]
fn test_nats_config_filtered_servers() {
    let mut conf = new_nats_config();
    conf.servers = "nats://anycable:s3cr3t-passw0rd@nats-1:4222, nats://nats-2:4222".into();

    assert_eq!("nats://a***e:s3***rd@nats-1:4222, nats://nats-2:4222", filtered_servers(&conf));
}
