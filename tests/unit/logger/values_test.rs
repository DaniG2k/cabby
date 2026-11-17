//! upstream file: logger/values_test.go
//!
//! Upstream keeps the filtering switch in a package global (initialized from
//! `ANYCABLE_FILTER_LOGS`); tests pass the policy explicitly as a `LogFilter`.

use std::collections::BTreeMap;

use cabby_core::logger::{LogFilter, MAX_VALUE_LENGTH};
use cabby_testkit::reference::logger::*;

const ENABLED: LogFilter = LogFilter { enabled: true };

/// upstream: logger/values_test.go#TestCompactValue_string
#[test]
fn test_compact_value_string() {
    let shortvalue = "log-no-long";
    assert_eq!(shortvalue, compact_value(shortvalue));

    let longvalue = "log-long".repeat(50);

    let truncated = compact_value(&longvalue);
    assert_eq!("log-longlog-l", &truncated[..13]);
    assert_eq!(truncated.len(), MAX_VALUE_LENGTH + 8);
}

/// upstream: logger/values_test.go#TestCompactValue_bytes
#[test]
fn test_compact_value_bytes() {
    let shortvalue = b"log-no-long";
    assert_eq!("log-no-long", compact_value_bytes(shortvalue));

    let longvalue = "log-long".repeat(50).into_bytes();

    let truncated = compact_value_bytes(&longvalue);
    assert_eq!("log-longlog-l", &truncated[..13]);
    assert_eq!(truncated.len(), MAX_VALUE_LENGTH + 8);
}

/// upstream: logger/values_test.go#TestCompactValues_string
#[test]
fn test_compact_values_string() {
    let long = "log-long".repeat(50);
    let values = ["log-no-long", long.as_str()];

    let compacts = compact_values(&values);

    assert_eq!("log-no-long", compacts[0]);

    let truncated = &compacts[1];
    assert_eq!("log-longlog-l", &truncated[..13]);
    assert_eq!(truncated.len(), MAX_VALUE_LENGTH + 8);
}

/// upstream: logger/values_test.go#TestCompactValues_bytes
#[test]
fn test_compact_values_bytes() {
    let long = "log-long".repeat(50).into_bytes();
    let values: [&[u8]; 2] = [b"log-no-long", &long];

    // slog.Any("t", CompactValues(values)).String()
    let any = compact_values_bytes_attr("t", &values);
    assert!(any.contains("log-no-long"), "{any}");

    let compacts = compact_values_bytes(&values);

    assert_eq!("log-no-long", compacts[0]);

    let truncated = &compacts[1];
    assert_eq!("log-longlog-l", &truncated[..13]);
    assert_eq!(truncated.len(), MAX_VALUE_LENGTH + 8);
}

/// upstream: logger/values_test.go#TestCompactAny
#[test]
fn test_compact_any() {
    // CompactAny(struct{ val string }{strings.Repeat("log-long", 50)})
    let str = compact_any_struct_val(&"log-long".repeat(50));

    assert_eq!("{val:log-long", &str[..13]);
    assert_eq!(str.len(), MAX_VALUE_LENGTH + 8);
}

/// upstream: logger/values_test.go#TestWithJSONHandler
#[test]
fn test_with_json_handler() {
    let str = "short";
    let longstr = "long".repeat(500);

    // logger.Debug("test", "b", CompactValue([]byte(str)), "l", CompactValues([]string{str, longstr}))
    let logged = json_handler_compact_values("test", str, &longstr);

    assert!(logged.contains(str), "{logged}");
    assert!(logged.len() < 300, "{logged}");
}

mod test_filtered_url {
    use super::*;

    const TOKEN: &str = "eyJhbGciOiJIUzI1NiJ9.eyJleHQiOiIxMjMifQ.sig-we";

    fn check(input: &str, expected: &str) {
        assert_eq!(expected, filtered_url(ENABLED, input));
    }

    /// upstream: logger/values_test.go#TestFilteredURL/no_query
    #[test]
    fn no_query() {
        check("wss://example.com/cable", "wss://example.com/cable");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/long_value
    #[test]
    fn long_value() {
        check(&format!("wss://example.com/cable?jwt={TOKEN}"), "wss://example.com/cable?jwt=ey***we");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/short_value
    #[test]
    fn short_value() {
        check("wss://example.com/cable?room=12345", "wss://example.com/cable?room=***");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/medium_value
    #[test]
    fn medium_value() {
        check("wss://example.com/cable?room=123456", "wss://example.com/cable?room=1***6");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/medium_value_(upper_bound)
    #[test]
    fn medium_value_upper_bound() {
        check("wss://example.com/cable?room=12345678", "wss://example.com/cable?room=1***8");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/long_value_(lower_bound)
    #[test]
    fn long_value_lower_bound() {
        check("wss://example.com/cable?room=123456789", "wss://example.com/cable?room=12***89");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/empty_value
    #[test]
    fn empty_value() {
        check("wss://example.com/cable?room=", "wss://example.com/cable?room=");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/no_value
    #[test]
    fn no_value() {
        check("wss://example.com/cable?debug", "wss://example.com/cable?debug");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/keeps_order
    #[test]
    fn keeps_order() {
        check("/cable?b=1&a=2", "/cable?b=***&a=***");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/user_and_password
    #[test]
    fn user_and_password() {
        check(&format!("nats://anycable:{TOKEN}@nats:4222"), "nats://a***e:ey***we@nats:4222");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/token_as_user
    #[test]
    fn token_as_user() {
        check(&format!("nats://{TOKEN}@nats:4222"), "nats://ey***we@nats:4222");
    }

    /// upstream: logger/values_test.go#TestFilteredURL/invalid
    #[test]
    fn invalid() {
        check("http://[::1", "[invalid url]");
    }
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// upstream: logger/values_test.go#TestFilteredMap
#[test]
fn test_filtered_map() {
    assert_eq!("[a b c]", filtered_map(ENABLED, Some(&map(&[("c", "3"), ("a", "1"), ("b", "2")]))));
    assert_eq!("[]", filtered_map(ENABLED, None));
}

/// upstream: logger/values_test.go#TestFilteringEnabledFromEnv
#[test]
fn test_filtering_enabled_from_env() {
    assert!(filtering_enabled_from_env(""));
    assert!(filtering_enabled_from_env("true"));
    assert!(filtering_enabled_from_env("whatever"));
    assert!(!filtering_enabled_from_env("false"));
    assert!(!filtering_enabled_from_env("0"));
}

/// upstream: logger/values_test.go#TestFilteringDisabled
#[test]
fn test_filtering_disabled() {
    // Upstream flips the package global `filteringEnabled = false` for the test.
    let disabled = LogFilter { enabled: false };

    let raw_url = "nats://anycable:secret-password@nats:4222/cable?jwt=eyJhbGciOiJIUzI1NiJ9";

    assert_eq!(raw_url, filtered_url(disabled, raw_url));
    assert_eq!("http://[::1", filtered_url(disabled, "http://[::1"));
    assert_eq!("secret-password", mask_value(disabled, "secret-password"));
    assert_eq!("map[a:1 b:2]", filtered_map(disabled, Some(&map(&[("b", "2"), ("a", "1")]))));
}
