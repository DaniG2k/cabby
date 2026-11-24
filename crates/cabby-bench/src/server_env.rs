//! The environment a server under test inherits (D-064), shared by the
//! benchmark harness ([`crate::server::Server`]) and the test kit's `Sut` and
//! feature runner: no `CABBY_*` or `ANYCABLE_*` variable and no unprefixed
//! `PORT` or `REDIS_URL`, apart from [`KEPT_VARS`] and `CABBY_TEST_*`. Otherwise
//! a developer's shell would configure the server the harness starts. Settings a
//! harness passes explicitly are applied after the scrub, so they still count.

use std::process::Command;

/// Variables a server under test keeps: the `nats-server` binary for embedded
/// NATS. `CABBY_TEST_*` (the test services) is kept too.
pub const KEPT_VARS: &[&str] = &["CABBY_NATS_SERVER", "CABBY_NATS_SERVER_BIN"];

/// Unprefixed variables the server reads for a flag (`--port`, `--redis_url`).
pub const UNPREFIXED_VARS: &[&str] = &["PORT", "REDIS_URL"];

/// The names in `vars` a server under test must not inherit.
pub fn scrubbed<'a>(vars: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    vars.into_iter()
        .filter(|k| k.starts_with("CABBY_") || k.starts_with("ANYCABLE_") || UNPREFIXED_VARS.contains(k))
        .filter(|k| !k.starts_with("CABBY_TEST_") && !KEPT_VARS.contains(k))
        .collect()
}

/// Removes the variables [`scrubbed`] picks from `inherited` from what `cmd` inherits.
pub fn scrub_env_of<'a>(cmd: &mut Command, inherited: impl IntoIterator<Item = &'a str>) {
    for k in scrubbed(inherited) {
        cmd.env_remove(k);
    }
}

/// [`scrub_env_of`] with this process's environment.
pub fn scrub_env(cmd: &mut Command) {
    let names: Vec<String> = std::env::vars_os().map(|(k, _)| k.to_string_lossy().into_owned()).collect();
    scrub_env_of(cmd, names.iter().map(String::as_str));
}
