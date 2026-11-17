//! upstream file: broker/config_test.go

use cabby_core::broker::Config;
use cabby_testkit::reference::broker::*;

/// upstream: broker/config_test.go#TestConfig__DecodeToml
#[test]
fn test_config_decode_toml() {
    let toml_string = r#"
  adapter = "nats"
  history_ttl = 100
  history_limit = 1000
  sessions_ttl = 600
 "#;

    let mut conf = new_config();
    config_merge_toml(&mut conf, toml_string).expect("decode");

    assert_eq!("nats", conf.adapter);
    assert_eq!(100, conf.history_ttl);
    assert_eq!(1000, conf.history_limit);
    assert_eq!(600, conf.sessions_ttl);
}

/// upstream: broker/config_test.go#TestConfig__ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.adapter = "nats".into();
    conf.history_ttl = 100;
    conf.history_limit = 1000;
    conf.sessions_ttl = 600;

    let toml_str = config_to_toml(&conf);

    for expected in ["adapter = \"nats\"", "history_ttl = 100", "history_limit = 1000", "sessions_ttl = 600"] {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }

    // Round-trip test (upstream decodes into the zero value `Config{}`)
    let mut conf2 = Config::default();

    config_merge_toml(&mut conf2, &toml_str).expect("decode");

    assert_eq!(conf, conf2);
}
