//! upstream file: config/config_test.go

use cabby_server::config::Config;
use cabby_testkit::reference::config::{load_from_file, new_config, to_toml};

use crate::support::TempDir;

/// Upstream helper `loadConfigFromString`: writes the contents to a temporary
/// `anycable.toml` and loads it over the defaults.
fn load_config_from_string(contents: &str) -> Config {
    let dir = TempDir::new();
    let path = dir.write("anycable.toml", contents);

    let mut config = new_config();
    config.config_file_path = Some(path);

    load_from_file(&mut config).expect("require.NoError(config.LoadFromFile())");

    config
}

mod test_load_from_file_unknown_keys {
    use super::*;

    /// upstream: config/config_test.go#TestLoadFromFileUnknownKeys/valid_configuration
    #[test]
    fn valid_configuration() {
        let config = load_config_from_string(
            "secret = \"s3cr3t\"\n\n[redis]\nurl = \"redis://redis:6379\"\n\n[metrics.tags]\nenv = \"test\"\n",
        );

        assert_eq!("s3cr3t", config.secret);
        assert_eq!("redis://redis:6379", config.redis.url);
        assert_eq!(std::collections::HashMap::from([("env".to_owned(), "test".to_owned())]), config.metrics.tags);
        assert!(config.unknown_config_keys.is_empty());
    }

    /// upstream: config/config_test.go#TestLoadFromFileUnknownKeys/unknown_top-level_key
    #[test]
    fn unknown_top_level_key() {
        let config = load_config_from_string("secret = \"s3cr3t\"\nsecert = \"typo\"\n");

        assert_eq!("s3cr3t", config.secret);
        assert_eq!(vec!["secert"], config.unknown_config_keys);
    }

    /// upstream: config/config_test.go#TestLoadFromFileUnknownKeys/unknown_nested_key
    #[test]
    fn unknown_nested_key() {
        let config = load_config_from_string(
            "[redis]\nurll = \"redis://redis:6379\"\n\n[server]\nport = 8081\nhots = \"0.0.0.0\"\n",
        );

        assert_eq!(8081, config.server.port);
        assert_eq!(vec!["redis.urll", "server.hots"], config.unknown_config_keys);
    }

    /// upstream: config/config_test.go#TestLoadFromFileUnknownKeys/unknown_table
    #[test]
    fn unknown_table() {
        let config = load_config_from_string(
            "[redis_broadcast]\nstream = \"test\"\ngroup = \"test\"\n\n[sse]\nenabled = true\n",
        );

        assert!(config.sse.enabled);
        assert_eq!(vec!["redis_broadcast"], config.unknown_config_keys);
    }
}

/// The key of a `key = value` line matching `^([a-z0-9_.]+) = ` after `prefix`.
fn setting_key<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = line.strip_prefix(prefix)?;
    let end = rest.find(|c: char| !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '.'))?;
    (end > 0 && rest[end..].starts_with(" = ")).then(|| &rest[..end])
}

/// Upstream helper `uncommentSettings`: activates commented out settings (e.g.,
/// "# port = 8080"), so we can check that all the keys mentioned in the generated
/// configuration are known. Settings already defined in the same table are kept
/// commented out.
fn uncomment_settings(contents: &str) -> String {
    let mut lines: Vec<String> = contents.split('\n').map(str::to_owned).collect();
    let mut defined = std::collections::HashSet::new();
    let mut table = String::new();

    for line in &lines {
        if line.starts_with('[') {
            table = line.clone();
        } else if let Some(key) = setting_key(line, "") {
            defined.insert(format!("{table}{key}"));
        }
    }

    table.clear();

    for line in lines.iter_mut() {
        if line.starts_with('[') {
            table = line.clone();
        } else if let Some(key) = setting_key(line, "# ").map(str::to_owned)
            && !defined.contains(&format!("{table}{key}"))
        {
            defined.insert(format!("{table}{key}"));
            *line = line.strip_prefix("# ").expect("commented setting").to_owned();
        }
    }

    lines.join("\n")
}

mod test_to_toml_round_trip {
    use super::*;

    /// upstream: config/config_test.go#TestToTomlRoundTrip/generated_configuration
    #[test]
    fn generated_configuration() {
        let defaults = new_config();

        let config = load_config_from_string(&to_toml(&defaults));

        assert!(config.unknown_config_keys.is_empty(), "{:?}", config.unknown_config_keys);
    }

    /// upstream: config/config_test.go#TestToTomlRoundTrip/with_commented_out_settings_enabled
    #[test]
    fn with_commented_out_settings_enabled() {
        let defaults = new_config();

        let config = load_config_from_string(&uncomment_settings(&to_toml(&defaults)));

        assert!(config.unknown_config_keys.is_empty(), "{:?}", config.unknown_config_keys);
    }
}
