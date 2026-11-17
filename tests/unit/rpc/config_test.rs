//! upstream file: rpc/config_test.go

use cabby_testkit::reference::rpc::*;

use super::helpers::strings;

/// upstream: rpc/config_test.go#TestConfig_Impl
#[test]
fn test_config_impl() {
    let mut c = new_config();

    c.implementation = "http".into();
    assert_eq!("http", config_impl(&c));

    c.implementation = "trpc".into();
    assert_eq!("trpc", config_impl(&c));

    c.implementation = String::new();
    assert_eq!("grpc", config_impl(&c));

    c.host = "http://localhost:8080/anycable".into();
    assert_eq!("http", config_impl(&c));

    c.host = "https://localhost:8080/anycable".into();
    assert_eq!("http", config_impl(&c));

    c.host = "grpc://localhost:50051/anycable".into();
    assert_eq!("grpc", config_impl(&c));

    c.host = "dns:///rpc:50051".into();
    assert_eq!("grpc", config_impl(&c));

    c.host = "localhost:50051/anycable".into();
    assert_eq!("grpc", config_impl(&c));

    c.host = "127.0.0.1:50051/anycable".into();
    assert_eq!("grpc", config_impl(&c));
}

mod test_config_to_toml {
    use super::*;

    /// upstream: rpc/config_test.go#TestConfig__ToToml
    #[test]
    fn base() {
        let mut conf = new_config();
        conf.host = "rpc.test".into();
        conf.concurrency = 10;
        conf.implementation = "http".into();
        conf.proxy_headers = strings(&["Cookie", "X-Api-Key"]);
        conf.proxy_cookies = strings(&["_session_id", "_csrf_token"]);

        let toml_str = config_to_toml(&conf);

        assert!(toml_str.contains("implementation = \"http\""));
        assert!(toml_str.contains("host = \"rpc.test\""));
        assert!(toml_str.contains("concurrency = 10"));
        assert!(toml_str.contains("proxy_headers = [\"Cookie\", \"X-Api-Key\"]"));
        assert!(toml_str.contains("proxy_cookies = [\"_session_id\", \"_csrf_token\"]"));

        // Round-trip test (config files are decoded on top of the defaults)
        let conf2 = config_decode_toml(&new_config(), &toml_str).expect("decoding the TOML");

        assert_eq!(conf, conf2);
    }

    /// upstream: rpc/config_test.go#TestConfig__ToToml/wait_timeout
    #[test]
    fn wait_timeout() {
        for val in [-1, 0, 500] {
            let mut conf = new_config();
            conf.wait_timeout = val;

            let decoded = config_decode_toml(&new_config(), &config_to_toml(&conf)).expect("decoding the TOML");

            assert_eq!(val, decoded.wait_timeout);
        }
    }
}
