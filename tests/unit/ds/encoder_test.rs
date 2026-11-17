//! upstream file: ds/encoder_test.go

use cabby_testkit::reference::ds::{decode_offset, encode_offset};

mod test_encode_offset {
    use super::*;

    fn check(offset: u64, epoch: &str, want: &str) {
        assert_eq!(encode_offset(offset, epoch), want);
    }

    /// upstream: ds/encoder_test.go#TestEncodeOffset/without_epoch
    #[test]
    fn without_epoch() {
        check(123, "", "0");
    }

    /// upstream: ds/encoder_test.go#TestEncodeOffset/with_epoch
    #[test]
    fn with_epoch() {
        check(123, "epoch1", "123::epoch1");
    }

    /// upstream: ds/encoder_test.go#TestEncodeOffset/zero_offset
    #[test]
    fn zero_offset() {
        check(0, "epoch1", "0::epoch1");
    }
}

mod test_decode_offset {
    use super::*;

    fn check(offset_str: &str, want_offset: u64, want_epoch: &str, want_err: bool) {
        let got = decode_offset(offset_str);

        if want_err {
            assert!(got.error.is_some(), "expected an error, got {got:?}");
        } else {
            assert_eq!(got.error, None);
            assert_eq!(got.offset, want_offset);
            assert_eq!(got.epoch, want_epoch);
        }
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/simple_offset
    #[test]
    fn simple_offset() {
        check("123", 123, "", false);
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/with_epoch
    #[test]
    fn with_epoch() {
        check("123::epoch1", 123, "epoch1", false);
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/start
    #[test]
    fn start() {
        check("-1", 0, "", false);
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/now
    #[test]
    fn now() {
        check("now", 0, "", false);
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/empty
    #[test]
    fn empty() {
        check("", 0, "", false);
    }

    /// upstream: ds/encoder_test.go#TestDecodeOffset/invalid
    #[test]
    fn invalid() {
        check("abc", 0, "", true);
    }
}
