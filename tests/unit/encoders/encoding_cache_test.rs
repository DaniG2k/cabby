//! upstream file: encoders/encoding_cache_test.go
//!
//! The upstream `MockMessage` and encoding callback are part of the facade
//! (`encoding_cache_fetch_mock`), so the same fetch sequence runs against both
//! implementations.

use cabby_testkit::reference::encoders::encoding_cache_fetch_mock;

/// upstream: encoders/encoding_cache_test.go#TestEncodingCache
#[test]
fn test_encoding_cache() {
    // msg := &MockMessage{Value: "mock"}; two c.Fetch(msg, "mock", callback) calls.
    let out = encoding_cache_fetch_mock("mock", &["mock", "mock"]);

    let v = out.frames[0].as_ref().expect("first fetch");
    assert_eq!(b"mock".to_vec(), v.payload);

    let v = out.frames[1].as_ref().expect("second fetch");
    assert_eq!(b"mock".to_vec(), v.payload);
    assert_eq!(1, out.encoded);
}
