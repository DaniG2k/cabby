//! upstream file: api/config_test.go

use cabby_testkit::reference::api::*;

/// upstream: api/config_test.go#TestConfig_NewConfig
#[test]
fn test_config_new_config() {
    let config = new_config();

    assert_eq!(DEFAULT_API_PATH, config.path);
    assert_eq!(0, config.port);
    assert_eq!("", config.host);
    assert_eq!("", config.secret);
    assert_eq!("", config.secret_base);
    assert!(!config.add_cors_headers);
    assert_eq!("", config.cors_hosts);
}

mod test_config_is_secured {
    use super::*;

    /// upstream: api/config_test.go#TestConfig_IsSecured/returns_false_when_no_secret_configured
    #[test]
    fn returns_false_when_no_secret_configured() {
        let config = new_config();
        assert!(!config_is_secured(&config));
    }

    /// upstream: api/config_test.go#TestConfig_IsSecured/returns_true_when_Secret_is_set
    #[test]
    fn returns_true_when_secret_is_set() {
        let mut config = new_config();
        config.secret = "test-secret".into();
        assert!(config_is_secured(&config));
    }

    /// upstream: api/config_test.go#TestConfig_IsSecured/returns_true_when_SecretBase_is_set
    #[test]
    fn returns_true_when_secret_base_is_set() {
        let mut config = new_config();
        config.secret_base = "test-secret-base".into();
        assert!(config_is_secured(&config));
    }
}

mod test_config_derive_secret {
    use super::*;

    /// upstream: api/config_test.go#TestConfig_DeriveSecret/does_nothing_when_Secret_is_already_set
    #[test]
    fn does_nothing_when_secret_is_already_set() {
        let mut config = new_config();
        config.secret = "existing-secret".into();
        config.secret_base = "some-base".into();

        config_derive_secret(&mut config).expect("derive");

        assert_eq!("existing-secret", config.secret);
    }

    /// upstream: api/config_test.go#TestConfig_DeriveSecret/does_nothing_when_SecretBase_is_empty
    #[test]
    fn does_nothing_when_secret_base_is_empty() {
        let mut config = new_config();

        config_derive_secret(&mut config).expect("derive");

        assert_eq!("", config.secret);
    }

    /// upstream: api/config_test.go#TestConfig_DeriveSecret/derives_secret_from_SecretBase
    #[test]
    fn derives_secret_from_secret_base() {
        let mut config = new_config();
        config.secret_base = "qwerty".into();

        config_derive_secret(&mut config).expect("derive");

        assert!(!config.secret.is_empty());
        // The derived secret should be deterministic
        let expected_secret = config.secret.clone();

        let mut config2 = new_config();
        config2.secret_base = "qwerty".into();
        config_derive_secret(&mut config2).expect("derive");

        assert_eq!(expected_secret, config2.secret);
    }

    /// upstream: api/config_test.go#TestConfig_DeriveSecret/different_SecretBase_produces_different_secrets
    #[test]
    fn different_secret_base_produces_different_secrets() {
        let mut config1 = new_config();
        config1.secret_base = "secret1".into();
        config_derive_secret(&mut config1).expect("derive");

        let mut config2 = new_config();
        config2.secret_base = "secret2".into();
        config_derive_secret(&mut config2).expect("derive");

        assert_ne!(config1.secret, config2.secret);
    }
}

/// upstream: api/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut config = new_config();
    config.host = "0.0.0.0".into();
    config.port = 8081;
    config.path = "/api".into();
    config.secret = "my-secret".into();
    config.add_cors_headers = true;

    let toml_str = config_to_toml(&config);

    for expected in
        ["host = \"0.0.0.0\"", "port = 8081", "path = \"/api\"", "secret = \"my-secret\"", "cors_headers = true"]
    {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }

    // Round-trip test
    let mut config2 = new_config();
    config_merge_toml(&mut config2, &toml_str).expect("decode");

    assert_eq!(config.host, config2.host);
    assert_eq!(config.port, config2.port);
    assert_eq!(config.path, config2.path);
    assert_eq!(config.secret, config2.secret);
    assert_eq!(config.add_cors_headers, config2.add_cors_headers);
}

/// upstream: api/config_test.go#TestConfig_ToToml_Defaults
#[test]
fn test_config_to_toml_defaults() {
    let config = new_config();

    let toml_str = config_to_toml(&config);

    // Should have commented out defaults
    for expected in ["# host = \"localhost\"", "port = 0", "# secret = \"\"", "# cors_headers = false"] {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }
}
