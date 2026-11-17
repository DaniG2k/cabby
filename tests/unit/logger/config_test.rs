//! upstream file: logger/config_test.go

use cabby_core::logger::Config;
use cabby_testkit::reference::logger::{config_decode_toml, config_to_toml, new_config};

/// upstream: logger/config_test.go#TestConfig__DecodeToml
#[test]
fn test_config_decode_toml() {
    let toml_string = r#"
  level = "warn"
  format = "json"
  debug = true
 "#;

    let mut conf = new_config();
    config_decode_toml(&mut conf, toml_string).expect("require.NoError");

    assert_eq!("warn", conf.log_level);
    assert_eq!("json", conf.log_format);
    assert!(conf.debug);
}

/// upstream: logger/config_test.go#TestConfig__ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.log_level = "warn".into();
    conf.log_format = "json".into();
    conf.debug = false;

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("level = \"warn\""), "{toml_str}");
    assert!(toml_str.contains("format = \"json\""), "{toml_str}");
    assert!(toml_str.contains("# debug = true"), "{toml_str}");

    // Round-trip test
    let mut conf2 = Config::default();

    config_decode_toml(&mut conf2, &toml_str).expect("require.NoError");

    assert_eq!(conf, conf2);
}
