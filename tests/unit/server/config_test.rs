//! upstream file: server/config_test.go

use cabby_server::server::Config;
use cabby_testkit::reference::server::{config_decode_toml, config_to_toml, new_config};

/// upstream: server/config_test.go#TestConfig__DecodeToml
#[test]
fn test_config_decode_toml() {
    let toml_string = r#"
  host = "0.0.0.0"
  port = 8081
  max_conn = 100
  health_path = "/healthz"
 "#;

    let mut conf = new_config();
    config_decode_toml(&mut conf, toml_string).expect("require.NoError");

    assert_eq!("0.0.0.0", conf.host);
    assert_eq!(8081, conf.port);
    assert_eq!(100, conf.max_conn);
    assert_eq!("/healthz", conf.health_path);
}

/// upstream: server/config_test.go#TestConfig__ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.host = "local.test".into();
    conf.port = 8082;
    conf.health_path = "/healthz".into();
    conf.ssl.cert_path = "/path/to/cert".into();
    conf.ssl.key_path = "/path/to/key".into();
    conf.allowed_origins = "http://example.com".into();

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("host = \"local.test\""), "{toml_str}");
    assert!(toml_str.contains("port = 8082"), "{toml_str}");
    assert!(toml_str.contains("# max_conn = 1000"), "{toml_str}");
    assert!(toml_str.contains("health_path = \"/healthz\""), "{toml_str}");
    assert!(toml_str.contains("allowed_origins = \"http://example.com\""), "{toml_str}");
    assert!(toml_str.contains("ssl.cert_path = \"/path/to/cert\""), "{toml_str}");
    assert!(toml_str.contains("ssl.key_path = \"/path/to/key\""), "{toml_str}");

    // Round-trip test
    let mut conf2 = Config::default();

    config_decode_toml(&mut conf2, &toml_str).expect("require.NoError");

    assert_eq!(conf, conf2);
}
