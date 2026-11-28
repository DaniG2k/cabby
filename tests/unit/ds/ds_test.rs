//! upstream file: ds/ds_test.go

use cabby_testkit::reference::ds::next_cursor;

mod test_stream_generate_cursor {
    use super::*;

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/generates_non-empty_cursor
    #[test]
    fn generates_non_empty_cursor() {
        let cursor = next_cursor("");
        assert!(!cursor.is_empty());
    }

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/returns_greater_cursor_when_client_cursor_is_old
    /// note: upstream `assert.Greater` on two strings compares them lexicographically; kept as is (the numeric comparison is the linked regression case).
    #[test]
    fn returns_greater_cursor_when_client_cursor_is_old() {
        let old_cursor = "1";

        let new_cursor = next_cursor(old_cursor);

        assert!(!new_cursor.is_empty());
        assert!(new_cursor.as_str() > old_cursor, "{new_cursor:?} > {old_cursor:?}");
    }

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/ensures_monotonic_progression_for_current_cursor
    /// note: upstream `assert.Greater` on two strings compares them lexicographically; kept as is (the numeric comparison is the linked regression case).
    #[test]
    fn ensures_monotonic_progression_for_current_cursor() {
        let current_cursor = next_cursor("");

        let next = next_cursor(&current_cursor);

        assert!(next > current_cursor, "{next:?} > {current_cursor:?}");
    }

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/handles_invalid_cursor_gracefully
    #[test]
    fn handles_invalid_cursor_gracefully() {
        let cursor = next_cursor("not-a-number");
        assert!(!cursor.is_empty());
    }

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/ignores_cursors_that_would_overflow
    #[test]
    fn ignores_cursors_that_would_overflow() {
        for val in [i64::MAX, i64::MAX - 60] {
            let cursor = next_cursor(&val.to_string()).parse::<i64>();

            assert!(cursor.is_ok(), "{cursor:?}");
            assert!(cursor.expect("assert.NoError") > 0);
        }
    }

    /// upstream: ds/ds_test.go#TestStream_GenerateCursor/advances_large_valid_cursors
    #[test]
    fn advances_large_valid_cursors() {
        let val = i64::MAX - 61;

        let cursor = next_cursor(&val.to_string()).parse::<i64>();

        assert!(cursor.is_ok(), "{cursor:?}");
        assert!(cursor.expect("assert.NoError") > val);
    }

    /// regression-for: ds/ds_test.go#TestStream_GenerateCursor/ignores_cursors_that_would_overflow
    /// note: a cursor too large to advance by 61 intervals is ignored: the cursor is the current interval, as for an old client cursor.
    #[test]
    fn cursors_too_large_to_advance_give_the_current_interval() {
        let current: i64 = next_cursor("").parse().expect("a decimal cursor");
        for val in [i64::MAX, i64::MAX - 60] {
            let cursor: i64 = next_cursor(&val.to_string()).parse().expect("a decimal cursor");
            assert!((current..current + 2).contains(&cursor), "{cursor} is the current interval ({current})");
        }
    }

    /// regression-for: ds/ds_test.go#TestStream_GenerateCursor/returns_greater_cursor_when_client_cursor_is_old
    /// regression-for: ds/ds_test.go#TestStream_GenerateCursor/ensures_monotonic_progression_for_current_cursor
    /// note: cursors are decimal interval numbers (DS spec §8.1). Compared as numbers, a fresh cursor beats an old client cursor and an echoed cursor yields a strictly greater one, including across a digit-count change, which the upstream string comparison gets wrong.
    #[test]
    fn cursors_progress_numerically() {
        let parse = |c: &str| c.parse::<i64>().unwrap_or_else(|_| panic!("cursor {c:?} is not a decimal integer"));

        let fresh = parse(&next_cursor("9"));
        assert!(fresh > 9);

        let current = next_cursor("");
        let next = next_cursor(&current);
        assert!(parse(&next) > parse(&current), "{next} > {current}");

        // A client cursor far ahead (more digits than now) still progresses.
        let ahead = (parse(&current) * 10).to_string();
        let after_ahead = next_cursor(&ahead);
        assert!(parse(&after_ahead) > parse(&ahead), "{after_ahead} > {ahead}");
    }
}
