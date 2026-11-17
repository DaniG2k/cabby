//! upstream file: redis/config_test.go
//!
//! Upstream generates a self-signed client certificate/key pair and a CA
//! certificate per test (`generateTestCertFiles`, `generateTestCACertFile`). The
//! code under test only loads them, so this translation uses equivalent fixed
//! fixtures in `tests/unit/redis/fixtures/` (RSA 2048, PKCS#8 key, client-auth
//! EKU; CA with keyCertSign/cRLSign), generated with:
//!
//! ```text
//! openssl req -x509 -newkey rsa:2048 -nodes -keyout client.key -out client.crt -days 36500 -subj /CN=test \
//!   -addext keyUsage=keyEncipherment,digitalSignature -addext extendedKeyUsage=clientAuth \
//!   -addext basicConstraints=CA:FALSE
//! openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 36500 -subj "/CN=Test CA" \
//!   -addext keyUsage=keyCertSign,cRLSign -addext basicConstraints=critical,CA:TRUE
//! ```

use std::path::PathBuf;
use std::time::Duration;

use cabby_testkit::reference::redis::{Config, decode_toml};
use cabby_testkit::services::unique;

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("unit/redis/fixtures").join(name).to_string_lossy().into_owned()
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_owned()).collect()
}

/// upstream: redis/config_test.go#TestBasic
#[test]
fn test_basic() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(!config.is_cluster());
    assert!(!config.is_sentinel());

    assert_eq!("localhost:6379", config.hostname());
    assert_eq!(strings(&["localhost:6379"]), config.hostnames());

    assert_eq!(strings(&["localhost:6379"]), options.init_address);
    assert_eq!(0, options.select_db);
    assert_eq!(Duration::from_secs(30), options.keep_alive);
    assert!(!options.shuffle_init);
    assert!(options.tls.is_none());
}

/// upstream: redis/config_test.go#TestTrailingSlashHostname
#[test]
fn test_trailing_slash_hostname() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/".into();
    let options = config.to_rueidis_options().expect("options");

    assert_eq!(strings(&["localhost:6379"]), options.init_address);
    assert_eq!(0, options.select_db);
}

/// upstream: redis/config_test.go#TestCustomDatabase
#[test]
fn test_custom_database() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/1".into();
    let options = config.to_rueidis_options().expect("options");

    assert_eq!(1, options.select_db);
}

/// upstream: redis/config_test.go#TestCustomOptions
#[test]
fn test_custom_options() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/1?dial_timeout=30s".into();
    let options = config.to_rueidis_options().expect("options");

    assert_eq!(Duration::from_secs(30), options.dial_timeout);
}

/// upstream: redis/config_test.go#TestTLS
#[test]
fn test_tls() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(options.tls.expect("TLS config").insecure_skip_verify);

    config.tls_verify = true;
    let options = config.to_rueidis_options().expect("options");

    assert!(!options.tls.expect("TLS config").insecure_skip_verify);
}

/// upstream: redis/config_test.go#TestTLSClientCertAvailable
#[test]
fn test_tls_client_cert_available() {
    let mut config = Config::new();
    assert!(!config.tls_client_cert_available());

    config.tls_client_cert_path = "/path/to/cert.pem".into();
    assert!(!config.tls_client_cert_available());

    config.tls_client_key_path = "/path/to/key.pem".into();
    assert!(config.tls_client_cert_available());
}

/// upstream: redis/config_test.go#TestTLSMutualTLS
#[test]
fn test_tls_mutual_tls() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    config.tls_client_cert_path = fixture("client.crt");
    config.tls_client_key_path = fixture("client.key");

    let options = config.to_rueidis_options().expect("options");

    let tls = options.tls.expect("TLS config");
    assert_eq!(1, tls.certificates);
}

/// upstream: redis/config_test.go#TestTLSCACert
#[test]
fn test_tls_ca_cert() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    config.tls_ca_cert_path = fixture("ca.crt");

    let options = config.to_rueidis_options().expect("options");

    let tls = options.tls.expect("TLS config");
    assert!(tls.root_cas);
}

/// upstream: redis/config_test.go#TestTLSFullMutualTLS
#[test]
fn test_tls_full_mutual_tls() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    config.tls_client_cert_path = fixture("client.crt");
    config.tls_client_key_path = fixture("client.key");
    config.tls_ca_cert_path = fixture("ca.crt");
    config.tls_verify = true;

    let options = config.to_rueidis_options().expect("options");

    let tls = options.tls.expect("TLS config");
    assert_eq!(1, tls.certificates);
    assert!(tls.root_cas);
    assert!(!tls.insecure_skip_verify);
}

/// upstream: redis/config_test.go#TestTLSMutualTLSInvalidCert
#[test]
fn test_tls_mutual_tls_invalid_cert() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    config.tls_client_cert_path = "/nonexistent/cert.pem".into();
    config.tls_client_key_path = "/nonexistent/key.pem".into();

    let err = config.to_rueidis_options().expect_err("must fail");
    assert!(err.contains("failed to load Redis client certificate"), "{err}");
}

/// upstream: redis/config_test.go#TestTLSCACertInvalid
#[test]
fn test_tls_ca_cert_invalid() {
    let mut config = Config::new();
    config.url = "rediss://localhost:6379/1".into();
    config.tls_ca_cert_path = "/nonexistent/ca.pem".into();

    let err = config.to_rueidis_options().expect_err("must fail");
    assert!(err.contains("failed to read Redis CA certificate"), "{err}");
}

/// upstream: redis/config_test.go#TestTLSCACertInvalidPEM
#[test]
fn test_tls_ca_cert_invalid_pem() {
    // (Config created first so a pending stub cannot leave the temp dir behind.)
    let mut config = Config::new();

    let tmp_dir = std::env::temp_dir().join(unique("cabby_redis_pem"));
    std::fs::create_dir_all(&tmp_dir).expect("temp dir");
    let ca_path = tmp_dir.join("ca.crt");

    std::fs::write(&ca_path, "invalid pem content").expect("writing ca.crt");

    config.url = "rediss://localhost:6379/1".into();
    config.tls_ca_cert_path = ca_path.to_string_lossy().into_owned();

    let res = config.to_rueidis_options();
    let _ = std::fs::remove_dir_all(&tmp_dir);
    let err = res.expect_err("must fail");
    assert!(err.contains("failed to parse Redis CA certificate"), "{err}");
}

/// upstream: redis/config_test.go#TestTLSMutualTLSNoTLSURL
#[test]
fn test_tls_mutual_tls_no_tls_url() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/1".into();
    config.tls_client_cert_path = "/path/to/cert.pem".into();
    config.tls_client_key_path = "/path/to/key.pem".into();

    let options = config.to_rueidis_options().expect("options");

    assert!(options.tls.is_none());
}

/// upstream: redis/config_test.go#TestAuth
#[test]
fn test_auth() {
    let mut config = Config::new();
    config.url = "redis://user:pass@localhost:6379/1".into();
    let options = config.to_rueidis_options().expect("options");

    assert_eq!("user", options.username);
    assert_eq!("pass", options.password);
}

/// upstream: redis/config_test.go#TestCluster
#[test]
fn test_cluster() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/1,redis://localhost:6389/1".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(config.is_cluster());
    assert!(!config.is_sentinel());

    assert_eq!("localhost:6379", config.hostname());
    assert_eq!(strings(&["localhost:6379", "localhost:6389"]), config.hostnames());

    assert_eq!(strings(&["localhost:6379", "localhost:6389"]), options.init_address);
    assert!(options.shuffle_init);
}

/// upstream: redis/config_test.go#TestClusterShortSyntax
#[test]
fn test_cluster_short_syntax() {
    let mut config = Config::new();
    config.url = "redis://localhost:6379/1,localhost:6389/1".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(config.is_cluster());
    assert!(!config.is_sentinel());

    assert_eq!("localhost:6379", config.hostname());
    assert_eq!(strings(&["localhost:6379", "localhost:6389"]), config.hostnames());

    assert_eq!(strings(&["localhost:6379", "localhost:6389"]), options.init_address);
    assert!(options.shuffle_init);
}

/// upstream: redis/config_test.go#TestSentinel
#[test]
fn test_sentinel() {
    let mut config = Config::new();
    config.url = "redis://master-name".into();
    config.sentinels = "user:pass@localhost:1234,localhost:1235".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(!config.is_cluster());
    assert!(config.is_sentinel());

    assert_eq!("master-name", config.hostname());
    assert_eq!(strings(&["localhost:1234", "localhost:1235"]), config.hostnames());

    assert_eq!(strings(&["localhost:1234", "localhost:1235"]), options.init_address);
    assert_eq!("user", options.sentinel.username);
    assert_eq!("pass", options.sentinel.password);
    assert_eq!("", options.username);
    assert_eq!("", options.password);
}

/// upstream: redis/config_test.go#TestSentinelWithMasterAuth
#[test]
fn test_sentinel_with_master_auth() {
    let mut config = Config::new();
    config.url = "redis://:data_pass@mymaster".into();
    config.sentinels = ":sentinel_pass@localhost:26379".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(config.is_sentinel());
    assert_eq!("mymaster", options.sentinel.master_set);
    assert_eq!(strings(&["localhost:26379"]), options.init_address);
    assert_eq!("", options.sentinel.username);
    assert_eq!("sentinel_pass", options.sentinel.password);
    assert_eq!("", options.username);
    assert_eq!("data_pass", options.password);
}

/// upstream: redis/config_test.go#TestSentinelNoAuth
#[test]
fn test_sentinel_no_auth() {
    let mut config = Config::new();
    config.url = "redis://mymaster".into();
    config.sentinels = "localhost:26379".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(config.is_sentinel());
    assert_eq!("mymaster", options.sentinel.master_set);
    assert_eq!(strings(&["localhost:26379"]), options.init_address);
    assert_eq!("", options.sentinel.username);
    assert_eq!("", options.sentinel.password);
    assert_eq!("", options.username);
    assert_eq!("", options.password);
    assert_eq!(0, options.select_db);
}

/// upstream: redis/config_test.go#TestSentinelConfigPreservesRedisDB
#[test]
fn test_sentinel_config_preserves_redis_db() {
    let mut config = Config::new();
    config.url = "redis://mymaster/5".into();
    config.sentinels = "localhost:26379".into();

    let options = config.to_rueidis_options().expect("options");

    assert_eq!(5, options.select_db);
}

/// upstream: redis/config_test.go#TestSentinelAuthOnly
#[test]
fn test_sentinel_auth_only() {
    let mut config = Config::new();
    config.url = "redis://mymaster".into();
    config.sentinels = ":sentinel_pass@localhost:26379".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(config.is_sentinel());
    assert_eq!("mymaster", options.sentinel.master_set);
    assert_eq!(strings(&["localhost:26379"]), options.init_address);
    assert_eq!("", options.sentinel.username);
    assert_eq!("sentinel_pass", options.sentinel.password);
    assert_eq!("", options.username);
    assert_eq!("", options.password);
}

/// upstream: redis/config_test.go#TestSentinelImplicitFormat
#[test]
fn test_sentinel_implicit_format() {
    let mut config = Config::new();
    config.url = "redis://user:pass@localhost:1234?master_set=master-name".into();
    let options = config.to_rueidis_options().expect("options");

    assert!(!config.is_cluster());
    assert!(config.is_sentinel());

    assert_eq!("master-name", config.hostname());
    assert_eq!(strings(&["localhost:1234"]), config.hostnames());

    assert_eq!(strings(&["localhost:1234"]), options.init_address);
    assert_eq!("user", options.username);
    assert_eq!("pass", options.password);
}

/// upstream: redis/config_test.go#TestDefaultScheme
#[test]
fn test_default_scheme() {
    let mut config = Config::new();
    config.url = "localhost".into();
    let options = config.to_rueidis_options().expect("options");

    assert_eq!("localhost:6379", config.hostname());
    assert_eq!(strings(&["localhost:6379"]), config.hostnames());

    assert_eq!(strings(&["localhost:6379"]), options.init_address);
}

/// upstream: redis/config_test.go#TestInvalidURL
#[test]
fn test_invalid_url() {
    let mut config = Config::new();
    config.url = "invalid://".into();
    assert!(config.to_rueidis_options().is_err());
}

/// upstream: redis/config_test.go#TestRedisConfig__ToToml
#[test]
fn test_redis_config_to_toml() {
    let mut config = Config::new();
    config.url = "redis://example.com:6379".into();
    config.sentinels = "sentinel1:26379,sentinel2:26379".into();
    config.sentinel_discovery_interval = 60;
    config.keepalive_ping_interval = 45;
    config.tls_verify = true;
    config.tls_ca_cert_path = "/path/to/ca.pem".into();
    config.tls_client_cert_path = "/path/to/cert.pem".into();
    config.tls_client_key_path = "/path/to/key.pem".into();
    config.max_reconnect_attempts = 10;
    config.disable_cache = true;

    let toml_str = config.to_toml();

    for needle in [
        "url = \"redis://example.com:6379\"",
        "sentinels = \"sentinel1:26379,sentinel2:26379\"",
        "sentinel_discovery_interval = 60",
        "keepalive_ping_interval = 45",
        "tls_verify = true",
        "tls_ca_cert_path = \"/path/to/ca.pem\"",
        "tls_client_cert_path = \"/path/to/cert.pem\"",
        "tls_client_key_path = \"/path/to/key.pem\"",
        "max_reconnect_attempts = 10",
        "disable_cache = true",
    ] {
        assert!(toml_str.contains(needle), "expected {needle:?} in:\n{toml_str}");
    }

    // Round-trip test
    let config2 = decode_toml(&Config::new().config(), &toml_str).expect("decoding TOML");

    assert_eq!(config.config(), config2);
}
