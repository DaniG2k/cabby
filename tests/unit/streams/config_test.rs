//! upstream file: streams/config_test.go

use cabby_core::streams::Config;
use cabby_testkit::reference::streams::*;

/// upstream: streams/config_test.go#TestConfig_ToToml
#[test]
fn test_config_to_toml() {
    let mut conf = new_config();
    conf.secret = "test-secret".into();
    conf.public = true;
    conf.whisper = false;
    conf.pubsub_channel = "test-channel".into();
    conf.turbo = true;
    conf.turbo_secret = "turbo-secret".into();
    conf.cable_ready = false;
    conf.cable_ready_secret = "cable-ready-secret".into();

    let toml_str = config_to_toml(&conf);

    for expected in [
        "secret = \"test-secret\"",
        "public = true",
        "# whisper = true",
        "pubsub_channel = \"test-channel\"",
        "turbo = true",
        "turbo_secret = \"turbo-secret\"",
        "# cable_ready = true",
        "cable_ready_secret = \"cable-ready-secret\"",
    ] {
        assert!(toml_str.contains(expected), "missing {expected:?} in:\n{toml_str}");
    }

    // Round-trip test (upstream decodes into the zero value `Config{}`)
    let mut conf2 = Config::default();

    config_merge_toml(&mut conf2, &toml_str).expect("decode");

    assert_eq!(conf, conf2);
}
