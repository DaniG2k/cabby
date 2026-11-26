//! upstream file: pusher/verifier_test.go
//!
//! Based on the official docs:
//! https://pusher.com/docs/channels/library_auth_reference/auth-signatures/#worked-examples

use cabby_testkit::reference::pusher::Verifier;

const PRESENCE_DATA: &str = "{\"user_id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}";

/// upstream: pusher/verifier_test.go#TestVerifyChannel
#[test]
fn test_verify_channel() {
    let v = Verifier::new("278d425bdf160c739803", "7ad3773142a6692b25b8");

    assert!(v.verify_channel(
        "1234.1234",
        "private-foobar",
        "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4"
    ));
    // channel mismatch
    assert!(!v.verify_channel(
        "1234.1234",
        "foobar",
        "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4"
    ));
    // signature mismatch
    assert!(!v.verify_channel(
        "1234.1234",
        "private-foobar",
        "278d425bdf160c739803:df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a458"
    ));
    // socket mismatch
    assert!(!v.verify_channel(
        "1234.4321",
        "private-foobar",
        "278d425bdf160c739803:58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4"
    ));
}

/// upstream: pusher/verifier_test.go#TestVerifyPresenceChannel
#[test]
fn test_verify_presence_channel() {
    let v = Verifier::new("278d425bdf160c739803", "7ad3773142a6692b25b8");

    assert!(v.verify_presence_channel(
        "1234.1234",
        "presence-foobar",
        PRESENCE_DATA,
        "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800"
    ));
    // signature mismatch
    assert!(!v.verify_presence_channel(
        "1234.1234",
        "presence-foobar",
        PRESENCE_DATA,
        "278d425bdf160c739803:aed3695da2ffd16931f457e338e6c9f2921fa133ce7dac49f529792be6304cdd"
    ));
    // channel mismatch
    assert!(!v.verify_presence_channel(
        "1234.1234",
        "presence-foobaz",
        PRESENCE_DATA,
        "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800"
    ));
    // socket mismatch
    assert!(!v.verify_presence_channel(
        "1234.4321",
        "presence-foobar",
        PRESENCE_DATA,
        "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800"
    ));
    // data mismatch
    assert!(!v.verify_presence_channel(
        "1234.1234",
        "presence-foobar",
        "{\"id\":\"10\",\"user_info\":{\"name\":\"Mr. Channels\"}}",
        "278d425bdf160c739803:4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800"
    ));
}

/// upstream: pusher/verifier_test.go#TestVerifyUser
#[test]
fn test_verify_user() {
    let v = Verifier::new("278d425bdf160c739803", "7ad3773142a6692b25b8");

    assert!(v.verify_user(
        "1234.1234",
        "{\"id\":\"12345\"}",
        "278d425bdf160c739803:4708d583dada6a56435fb8bc611c77c359a31eebde13337c16ab43aa6de336ba"
    ));
    assert!(!v.verify_user(
        "1234.1234",
        "{\"id\":\"12345\"}",
        "278d425bdf160c739803:df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a458"
    ));
    assert!(!v.verify_user(
        "1234.4321",
        "{\"id\":\"12345\"}",
        "278d425bdf160c739803:4708d583dada6a56435fb8bc611c77c359a31eebde13337c16ab43aa6de336ba"
    ));
    assert!(!v.verify_user(
        "1234.1234",
        "{\"user_id\":\"12345\"}",
        "278d425bdf160c739803:4708d583dada6a56435fb8bc611c77c359a31eebde13337c16ab43aa6de336ba"
    ));
}

/// Cabby-only checks next to the upstream cases: a signature is
/// `<app id>:<hex HMAC>` and only that exact text is accepted. An app id that
/// only shares a prefix with the verifier's (either way round), a missing or
/// empty app id, and a truncated, extended or upper-cased HMAC are rejected.
mod signature_format {
    use super::*;

    const APP_ID: &str = "278d425bdf160c739803";
    const KEY: &str = "7ad3773142a6692b25b8";
    const CHANNEL_HMAC: &str = "58df8b0c36d6982b82c3ecf6b4662e34fe8c25bba48f5369f135bf843651c3a4";
    const PRESENCE_HMAC: &str = "4c6d8fc42a207ba96a0779844171b0bb819d96ffceef9609f5cce596ab17a800";
    const USER_HMAC: &str = "4708d583dada6a56435fb8bc611c77c359a31eebde13337c16ab43aa6de336ba";

    /// Signatures that differ from `<APP_ID>:<hmac>` only in their format.
    fn altered(hmac: &str) -> Vec<String> {
        vec![
            format!("{}:{hmac}", &APP_ID[..APP_ID.len() - 1]),
            format!("{APP_ID}0:{hmac}"),
            format!(":{hmac}"),
            hmac.to_owned(),
            format!("{APP_ID}:{}", &hmac[..hmac.len() - 1]),
            format!("{APP_ID}:{hmac}0"),
            format!("{APP_ID}:{}", hmac.to_uppercase()),
            format!("{APP_ID}:{hmac}:{hmac}"),
            format!("{APP_ID}:"),
            String::new(),
        ]
    }

    /// regression-for: pusher/verifier_test.go#TestVerifyChannel
    #[test]
    fn channel_signatures_must_match_exactly() {
        let v = Verifier::new(APP_ID, KEY);
        assert!(v.verify_channel("1234.1234", "private-foobar", &format!("{APP_ID}:{CHANNEL_HMAC}")));
        for signature in altered(CHANNEL_HMAC) {
            assert!(!v.verify_channel("1234.1234", "private-foobar", &signature), "{signature:?}");
        }
    }

    /// regression-for: pusher/verifier_test.go#TestVerifyPresenceChannel
    #[test]
    fn presence_signatures_must_match_exactly() {
        let v = Verifier::new(APP_ID, KEY);
        assert!(v.verify_presence_channel(
            "1234.1234",
            "presence-foobar",
            PRESENCE_DATA,
            &format!("{APP_ID}:{PRESENCE_HMAC}")
        ));
        for signature in altered(PRESENCE_HMAC) {
            assert!(
                !v.verify_presence_channel("1234.1234", "presence-foobar", PRESENCE_DATA, &signature),
                "{signature:?}"
            );
        }
    }

    /// regression-for: pusher/verifier_test.go#TestVerifyUser
    #[test]
    fn user_signatures_must_match_exactly() {
        let v = Verifier::new(APP_ID, KEY);
        assert!(v.verify_user("1234.1234", "{\"id\":\"12345\"}", &format!("{APP_ID}:{USER_HMAC}")));
        for signature in altered(USER_HMAC) {
            assert!(!v.verify_user("1234.1234", "{\"id\":\"12345\"}", &signature), "{signature:?}");
        }
    }

    /// regression-for: pusher/verifier_test.go#TestVerifyChannel
    #[test]
    fn a_verifier_for_an_app_id_sharing_a_prefix_rejects_the_signature() {
        let signature = format!("{APP_ID}:{CHANNEL_HMAC}");
        for app_id in [&APP_ID[..APP_ID.len() - 1], format!("{APP_ID}0").as_str(), ""] {
            let v = Verifier::new(app_id, KEY);
            assert!(!v.verify_channel("1234.1234", "private-foobar", &signature), "app id {app_id:?}");
        }
    }
}
