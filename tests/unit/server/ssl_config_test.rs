//! upstream file: server/ssl_config_test.go

use cabby_testkit::reference::server::{new_ssl_config, ssl_available};

/// upstream: server/ssl_config_test.go#TestSSLAvailable
#[test]
fn test_ssl_available() {
    let mut config = new_ssl_config();
    assert!(!ssl_available(&config));

    config.cert_path = "secret.cert".into();
    assert!(!ssl_available(&config));

    config.key_path = "secret.key".into();
    assert!(ssl_available(&config));
}
