//! upstream file: utils/message_verifier_test.go

use cabby_testkit::reference::utils::{verifier_generate, verifier_validate, verifier_verified};
use serde_json::json;

/// upstream: utils/message_verifier_test.go#TestMessageVerifier
#[test]
fn test_message_verifier() {
    let key = "s3Krit";

    // Turbo.signed_stream_verifier_key = 's3Krit'
    // Turbo::StreamsChannel.signed_stream_name([:chat, "2021"])
    let example = "ImNoYXQ6MjAyMSI=--f9ee45dbccb1da04d8ceb99cc820207804370ba0d06b46fc3b8b373af1315628";

    let generated = verifier_generate(key, &json!("chat:2021"));
    let generated = generated.expect("require.NoError");

    assert_eq!(example, generated);

    let res = verifier_verified(key, example);

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(json!("chat:2021"), res.unwrap());

    let a_generated = verifier_generate("secret", &json!("chat:2021")).unwrap_or_default();

    let res = verifier_verified(key, &a_generated);
    assert!(res.is_err(), "{res:?}");
}

/// upstream: utils/message_verifier_test.go#TestMessageVerifierNotString
#[test]
fn test_message_verifier_not_string() {
    let key = "s3Krit";
    let example = "WyJjaGF0LzIwMjMiLDE2ODUwMjQwMTdd--5b6661024d4c463c4936cd1542bc9a7672dd8039ac407d0b6c901697190e8aeb";

    let res = verifier_verified(key, example);

    // Upstream type-asserts the result to []interface{} before checking the error.
    let arr = res.as_ref().ok().and_then(|v| v.as_array().cloned()).expect("an array payload");

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(json!("chat/2023"), arr[0]);
}

/// upstream: utils/message_verifier_test.go#TestMessageVerifierEmptyKey
#[test]
fn test_message_verifier_empty_key() {
    let key = "";
    let example = "ImNoYXQvMjAyMyI=--2b2e054b6413bf3bc7e8bbad2f68962aa2ba19821800ccbd8de6fd7314e8320c";

    let res = verifier_verified(key, example);

    assert!(res.is_err(), "{res:?}");

    // Signed with an empty key
    let forged = "Im5vdGlmaWNhdGlvbnNfMiI=--e3f666cd48e2a59bb441b94c424ab6a52294db6e4d620c4f8459e659dcab29f8";

    let res = verifier_verified(key, forged);

    assert!(res.is_err(), "{res:?}");
    assert!(verifier_validate(key, forged).is_err());
}
