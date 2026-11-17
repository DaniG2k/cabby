//! upstream file: ws/config_test.go

use cabby_server::ws::Config;
use cabby_testkit::reference::ws::{config_decode_toml, config_to_toml, new_config};

/// upstream: ws/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.paths = vec!["/ws".into(), "/socket".into()];
    conf.read_buffer_size = 2048;
    conf.write_buffer_size = 2048;
    conf.max_message_size = 131072;
    conf.enable_compression = true;

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("paths = [\"/ws\", \"/socket\"]"), "{toml_str}");
    assert!(toml_str.contains("read_buffer_size = 2048"), "{toml_str}");
    assert!(toml_str.contains("write_buffer_size = 2048"), "{toml_str}");
    assert!(toml_str.contains("max_message_size = 131072"), "{toml_str}");
    assert!(toml_str.contains("enable_compression = true"), "{toml_str}");

    // Round-trip test
    let mut conf2 = Config::default();

    config_decode_toml(&mut conf2, &toml_str).expect("require.NoError");

    assert_eq!(conf, conf2);
}
