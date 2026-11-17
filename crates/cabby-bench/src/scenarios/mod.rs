//! Scenario entry points (upstream `scenarios/<name>/main.go`).
//!
//! Each scenario is a testable `run(args, stdout, stderr, ..) -> exit code`, like
//! upstream. Flags keep upstream names, Go duration syntax (`500ms`, `1s`, `2m`) and
//! the key=value stdout contract. Two flags are new because the server is external:
//!
//! * `--sut <path>`: server binary to launch (Cabby or `anycable-go`);
//! * `--server-arg <flag>` (repeatable): extra server CLI flags.
//!
//! Go-runtime flags have no meaning for an external Rust harness and are not
//! accepted: stress_publications' `--cpu-profile`, `--mem-profile`,
//! `--block-profile`, `--mutex-profile` and reconnect_floods' `--pprof` (profile the
//! server process with external tools instead).

use std::time::Duration;

use cabby_core::pending;

pub mod reconnect_floods;
pub mod stress_publications;

/// Parses a Go `time.ParseDuration` string (`300ms`, `1.5s`, `2m`, `-1ns`).
pub fn parse_go_duration(_s: &str) -> Result<Duration, String> {
    pending!("benchi::parse_go_duration")
}

/// Exit code for an interrupted run (SIGINT/SIGTERM).
pub const EXIT_INTERRUPTED: i32 = 130;
/// Exit code for flag errors.
pub const EXIT_USAGE: i32 = 2;
