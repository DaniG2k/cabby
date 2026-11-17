//! upstream file: metrics/config_test.go

use cabby_testkit::reference::metrics::*;

/// upstream: metrics/config_test.go#TestLogEnabled
#[test]
fn test_log_enabled() {
    let mut config = new_config();
    assert!(!log_enabled(&config));

    config.log = true;
    assert!(log_enabled(&config));

    config.log = false;
    assert!(!log_enabled(&config));

    config = new_config();
    config.log_formatter = "test".into();
    assert!(log_enabled(&config));
}

/// upstream: metrics/config_test.go#TestHTTPEnabled
#[test]
fn test_http_enabled() {
    let mut config = new_config();
    assert!(!http_enabled(&config));

    config.http = "/metrics".into();
    assert!(http_enabled(&config));
}

/// upstream: metrics/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.log = true;
    conf.rotate_interval = 30;
    conf.log_filter = Some(vec!["metric1".into(), "metric2".into()]);
    conf.host = "example.com".into();
    conf.port = 9090;
    conf.http = "/metrics".into();
    conf.tags =
        Some([("env", "prod"), ("region", "us-west")].iter().map(|(k, v)| ((*k).into(), (*v).into())).collect());

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("host = \"example.com\""));
    assert!(toml_str.contains("port = 9090"));
    assert!(toml_str.contains("http_path = \"/metrics\""));
    assert!(toml_str.contains("log = true"));
    assert!(toml_str.contains("rotate_interval = 30"));
    assert!(toml_str.contains("log_filter = [ \"metric1\", \"metric2\" ]"));
    assert!(toml_str.contains("tags.env = \"prod\""));
    assert!(toml_str.contains("tags.region = \"us-west\""));

    // Round-trip test
    let conf2 = config_decode_toml(&new_config(), &toml_str).expect("decoding the TOML");

    assert_eq!(conf, conf2);
}
