//! upstream file: cli/session_options_test.go
//!
//! `sessionOptionsFromParams` is an unexported method of the runner; against Go
//! these cases are not observable (the `?pi=` handling is observable through a
//! server: `conformance::ping_intervals`).

use cabby_testkit::reference::cli::session_options_from_params;

/// Upstream's table row body: a request with `?pi=<val>` gives one option when
/// the value is valid, none otherwise.
fn check(val: &str, valid: bool) {
    let opts = session_options_from_params(&format!("/cable?pi={val}"));

    if valid {
        assert_eq!(1, opts, "{val}");
    } else {
        assert_eq!(0, opts, "{val}");
    }
}

mod test_session_options_from_params_ping_interval {
    use super::check;

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/-1
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn negative_one() {
        check("-1", false);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/0
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn zero() {
        check("0", false);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/abc
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn abc() {
        check("abc", false);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/1
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn one() {
        check("1", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/86400
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn a_day() {
        check("86400", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/86401
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn a_day_and_a_second() {
        check("86401", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/3074457346
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v3074457346() {
        check("3074457346", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/9223372036
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v9223372036() {
        check("9223372036", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/9223372037
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v9223372037() {
        check("9223372037", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/6148914691236517206
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v6148914691236517206() {
        check("6148914691236517206", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/20211507185753197
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v20211507185753197() {
        check("20211507185753197", true);
    }

    /// upstream: cli/session_options_test.go#TestSessionOptionsFromParamsPingInterval/99999999999999999999
    /// note: not_observable on Go: Runner.sessionOptionsFromParams is unexported.
    #[test]
    fn v99999999999999999999() {
        check("99999999999999999999", false);
    }
}
