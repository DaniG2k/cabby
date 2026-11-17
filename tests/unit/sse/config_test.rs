//! upstream file: sse/config_test.go

use cabby_testkit::reference::sse::{config_to_toml, decode_config_toml, new_config};

/// upstream: sse/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.path = "/events".into();

    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("path = \"/events\""), "{toml_str}");
    assert!(toml_str.contains("# enabled = true"), "{toml_str}");

    // Round-trip test
    let conf2 = decode_config_toml(&toml_str).expect("decoding the generated TOML");

    assert_eq!(conf, conf2);
}

/// upstream: sse/config_test.go#TestConfig_ToToml_CommentPings
#[test]
fn test_config_to_toml_comment_pings() {
    let mut conf = new_config();

    let toml_str = config_to_toml(&conf);
    assert!(toml_str.contains("# comment_pings = true"), "{toml_str}");

    conf.comment_pings = true;
    let toml_str = config_to_toml(&conf);

    assert!(toml_str.contains("\ncomment_pings = true"), "{toml_str}");

    let conf2 = decode_config_toml(&toml_str).expect("decoding the generated TOML");

    assert_eq!(conf, conf2);
}
