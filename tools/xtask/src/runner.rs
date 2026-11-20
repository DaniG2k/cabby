//! Compatibility runner (SPEC.md §5 "Pending tests and CI").
//!
//! Runs every translated test binary against one backend, classifies each Rust
//! case as pass / pending (panicked with the pending marker) / fail / ignored,
//! and reconciles with `compat/test-map.json`:
//!
//! * fail, crash, timeout, ignore, or an unmapped test => error;
//! * a test binary that exits abnormally (a signal, an abort, or a failure
//!   exit without libtest's summary), or reports fewer results than the tests
//!   it started => error;
//! * a mapped case whose tests did not all run => `MISSING` error (without a
//!   filter), and the number of tests run must match the manifest;
//! * a recorded `pass` that no longer passes => regression error;
//! * a `pending` case that now passes => error until promoted with `--record`;
//! * against Go, every case must keep its recorded reference status (`pass`,
//!   `not_observable`, `upstream_failure`); a change is an error until
//!   recorded with `--record`.
//!
//! `--record` only updates cases whose tests all ran in that invocation, so a
//! filtered run never promotes a case from a partial result.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use cabby_core::pending::pending_id;

use crate::inventory::derive_aggregates;
use crate::manifest;
use crate::reference::sh;
use crate::{Backend, root};

const BINARY_DEADLINE: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Pass,
    Pending,
    Fail,
    Ignored,
    /// The Go facade reported `unimplemented!("not_observable: <reason>")`: the
    /// behavior cannot be reached through the reference (SPEC.md §5).
    NotObservable,
    /// An `/// issue:` test failed on Go: the upstream defect it pins.
    UpstreamFailure,
}

/// Marker used by Go facade arms for behavior the reference cannot expose.
pub const NOT_OBSERVABLE: &str = "not_observable:";

/// (test target name, executable path)
fn test_binaries() -> Result<Vec<(String, String)>> {
    let out = Command::new("cargo")
        .args(["test", "--no-run", "--message-format=json", "-p", "cabby-compat"])
        .stderr(Stdio::inherit())
        .output()
        .context("building test binaries")?;
    if !out.status.success() {
        bail!("compiling the translated tests failed");
    }
    let mut bins = Vec::new();
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { continue };
        if v["reason"] == "compiler-artifact"
            && v["profile"]["test"] == true
            && let (Some(name), Some(exe)) = (v["target"]["name"].as_str(), v["executable"].as_str())
        {
            bins.push((name.to_owned(), exe.to_owned()));
        }
    }
    bins.sort();
    Ok(bins)
}

fn run_binary(
    exe: &str,
    backend: Backend,
    sut_bin: &str,
    filter: Option<&str>,
) -> Result<(String, std::process::ExitStatus)> {
    let mut cmd = Command::new(exe);
    cmd.env("CABBY_BACKEND", backend.as_str()).env("SUT_BIN", sut_bin);
    // Cabby's embedded NATS (`--embed_nats`) runs an external `nats-server`
    // (docs/enats.md); tests use the one built from the pinned module.
    if std::env::var_os("CABBY_NATS_SERVER").is_none() {
        cmd.env("CABBY_NATS_SERVER", root().join("target/test-services/nats-server"));
    }
    if std::env::var_os("CABBY_GO_BRIDGE").is_none() {
        cmd.env("CABBY_GO_BRIDGE", root().join("target/reference-adapters/bridge"));
    }
    if std::env::var_os("CABBY_GO_MRUBY_BRIDGE").is_none() {
        cmd.env("CABBY_GO_MRUBY_BRIDGE", root().join("target/reference-adapters/mruby-bridge"));
    }
    if let Some(f) = filter {
        cmd.arg(f);
    }
    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn()?;
    let mut so = child.stdout.take().unwrap();
    let mut se = child.stderr.take().unwrap();
    let t1 = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = so.read_to_string(&mut s);
        s
    });
    let t2 = std::thread::spawn(move || {
        let mut s = String::new();
        let _ = se.read_to_string(&mut s);
        s
    });
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait()? {
            break s;
        }
        if start.elapsed() > BINARY_DEADLINE {
            let _ = child.kill();
            bail!("{exe} exceeded {BINARY_DEADLINE:?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut out = t1.join().unwrap_or_default();
    out.push_str(&t2.join().unwrap_or_default());
    Ok((out, status))
}

/// libtest's exit status for a run in which some tests failed.
const LIBTEST_FAILED: i32 = 101;

/// Why a test binary's run cannot be trusted, if it can't: it must print
/// libtest's summary and exit successfully, or with libtest's failure status
/// (failing and pending tests). A signal, an abort, a `process::exit` from a
/// test or any other exit means the binary died mid-run, whatever results it
/// printed before.
fn abnormal_exit(status: std::process::ExitStatus, output: &str) -> Option<String> {
    let summary = |verdict: &str| output.lines().any(|l| l.starts_with(&format!("test result: {verdict}.")));
    if status.success() && summary("ok") {
        return None;
    }
    if status.code() == Some(LIBTEST_FAILED) && summary("FAILED") {
        return None;
    }
    let printed = summary("ok") || summary("FAILED");
    Some(format!("exited with {status}{}", if printed { "" } else { " before libtest's summary" }))
}

/// The number of tests libtest said it would run (`running N tests`; one line
/// per test harness in the binary).
fn started(output: &str) -> usize {
    output.lines().filter_map(|l| l.strip_prefix("running ")?.split_whitespace().next()?.parse::<usize>().ok()).sum()
}

/// Parses libtest's human output into per-test outcomes.
fn parse(output: &str) -> BTreeMap<String, Outcome> {
    let mut results = BTreeMap::new();
    for line in output.lines() {
        let Some(rest) = line.strip_prefix("test ") else { continue };
        let Some((name, status)) = rest.rsplit_once(" ... ") else { continue };
        let o = match status.trim() {
            "ok" => Outcome::Pass,
            "FAILED" => Outcome::Fail,
            s if s.starts_with("ignored") => Outcome::Ignored,
            _ => continue,
        };
        results.insert(name.trim().to_owned(), o);
    }
    // Failure sections: "---- <name> stdout ----" until the next section header.
    let mut current: Option<String> = None;
    let mut sections: HashMap<String, String> = HashMap::new();
    for line in output.lines() {
        if let Some(n) = line.strip_prefix("---- ").and_then(|l| l.strip_suffix(" stdout ----")) {
            current = Some(n.to_owned());
            continue;
        }
        if line == "failures:" || line.starts_with("test result:") {
            current = None;
        }
        if let Some(c) = &current {
            let s = sections.entry(c.clone()).or_default();
            s.push_str(line);
            s.push('\n');
        }
    }
    for (name, text) in sections {
        if results.get(&name) == Some(&Outcome::Fail) {
            if pending_id(&text).is_some() {
                results.insert(name, Outcome::Pending);
            } else if text.contains(&format!("not implemented: {NOT_OBSERVABLE}")) {
                results.insert(name, Outcome::NotObservable);
            }
        }
    }
    results
}

fn status_str(o: Outcome) -> &'static str {
    match o {
        Outcome::Pass => "pass",
        Outcome::Pending => "pending",
        Outcome::Fail | Outcome::Ignored => "fail",
        Outcome::NotObservable => "not_observable",
        Outcome::UpstreamFailure => "upstream_failure",
    }
}

fn combine(outcomes: &[Outcome]) -> Outcome {
    if outcomes.iter().any(|o| matches!(o, Outcome::Fail | Outcome::Ignored)) {
        Outcome::Fail
    } else if outcomes.contains(&Outcome::NotObservable) {
        Outcome::NotObservable
    } else if outcomes.contains(&Outcome::UpstreamFailure) {
        Outcome::UpstreamFailure
    } else if outcomes.iter().all(|o| *o == Outcome::Pass) {
        Outcome::Pass
    } else {
        Outcome::Pending
    }
}

/// Builds `target/test-services/nats-server` from the reference's pinned module
/// if missing (the same binary `cabby_testkit::nats` uses).
fn ensure_nats_server() -> Result<()> {
    let out = root().join("target/test-services/nats-server");
    if out.exists() || std::env::var_os("CABBY_NATS_SERVER").is_some() {
        return Ok(());
    }
    sh(Command::new("go")
        .args(["build", "-o"])
        .arg(&out)
        .arg("github.com/nats-io/nats-server/v2")
        .current_dir(root().join("compat/reference")))
}

pub fn run(backend: Backend, record: bool, filter: Option<&str>) -> Result<()> {
    let root = root();
    let sut_bin = match backend {
        Backend::Rust => {
            sh(Command::new("cargo").args(["build", "--quiet", "-p", "cabby-server", "--bin", "cabby"]))?;
            root.join("target/debug/cabby")
        }
        Backend::Go => {
            let bin = root.join("target/reference-bin/anycable-go");
            let mruby_missing = root.join("compat/reference/vendorlib/go-mruby/libmruby.a").exists()
                && !root.join("target/reference-adapters/mruby-bridge").exists();
            if !bin.exists() || !root.join("target/reference-adapters/bridge").exists() || mruby_missing {
                crate::reference::build()?;
            }
            bin
        }
    };
    let sut_bin = std::env::var("SUT_BIN").unwrap_or_else(|_| sut_bin.to_string_lossy().into_owned());

    ensure_nats_server()?;
    let mut results: BTreeMap<String, Outcome> = BTreeMap::new();
    let mut errors: Vec<String> = Vec::new();
    // Tests pinning fixes for upstream issues may fail on Go: that failure is the
    // documented upstream defect (recorded as `upstream_failure`), not an error.
    let issue_tests: std::collections::HashSet<String> =
        manifest::load()?.rust_only.iter().filter(|r| r.relation == "issue").map(|r| r.rust_case.clone()).collect();
    for (name, exe) in test_binaries()? {
        let (out, status) = run_binary(&exe, backend, &sut_bin, filter)?;
        let parsed = parse(&out);
        if let Some(why) = abnormal_exit(status, &out) {
            let lines: Vec<&str> = out.lines().collect();
            let tail = lines[lines.len().saturating_sub(40)..].join("\n");
            errors.push(format!("CRASH {name}: the test binary {why}; last output:\n{tail}"));
        } else if parsed.is_empty() && !out.contains("running 0 tests") {
            errors.push(format!("{name}: could not parse test output (crash?):\n{out}"));
        }
        let expected = started(&out);
        if parsed.len() < expected {
            errors.push(format!("INCOMPLETE {name}: {expected} tests started, {} reported a result", parsed.len()));
        }
        for (t, o) in parsed {
            if o == Outcome::Fail && backend == Backend::Go && issue_tests.contains(&format!("{name}::{t}")) {
                results.insert(format!("{name}::{t}"), Outcome::UpstreamFailure);
                continue;
            }
            if o == Outcome::Fail {
                let section = out
                    .split(&format!("---- {t} stdout ----"))
                    .nth(1)
                    .map(|s| s.split("\n---- ").next().unwrap_or(s).split("\nfailures:").next().unwrap_or(s))
                    .unwrap_or("")
                    .trim();
                errors.push(format!("FAIL {name}::{t}\n{section}"));
            }
            if o == Outcome::Ignored {
                errors.push(format!("IGNORED {name}::{t}: ignore attributes are not allowed"));
            }
            if o == Outcome::NotObservable && backend == Backend::Rust {
                errors.push(format!("NOT-OBSERVABLE-ON-RUST {name}::{t}: only Go facade arms may use this marker"));
            }
            results.insert(format!("{name}::{t}"), o);
        }
    }

    let mut m = manifest::load()?;
    let mapped: std::collections::HashSet<String> = m
        .cases
        .iter()
        .flat_map(|c| c.rust_cases.iter().cloned())
        .chain(m.rust_only.iter().map(|r| r.rust_case.clone()))
        .collect();
    for t in results.keys() {
        if !mapped.contains(t) && filter.is_none() {
            errors.push(format!("UNMAPPED {t}: run `cargo xtask inventory` (missing annotation?)"));
        }
    }
    if filter.is_none() && results.len() != mapped.len() {
        errors.push(format!(
            "COUNT: {} Rust tests ran, the manifest maps {} (see MISSING/UNMAPPED)",
            results.len(),
            mapped.len()
        ));
    }

    let mut promoted = 0;
    let mut tally: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut check = |label: &str,
                     rust_cases: &[String],
                     ref_status: &mut String,
                     rust_status: &mut String,
                     errors: &mut Vec<String>| {
        let outs: Vec<Outcome> = rust_cases.iter().filter_map(|r| results.get(r).copied()).collect();
        let missing: Vec<&String> = rust_cases.iter().filter(|r| !results.contains_key(*r)).collect();
        if filter.is_none() && !missing.is_empty() {
            errors.push(format!("MISSING {label}: mapped Rust cases did not run: {missing:?}"));
        }
        if outs.is_empty() {
            return;
        }
        // Only a case whose tests all ran may change status in the manifest.
        let complete = missing.is_empty();
        let o = combine(&outs);
        *tally.entry(status_str(o)).or_default() += 1;
        match backend {
            Backend::Go => {
                if o == Outcome::Pending {
                    errors.push(format!("PENDING-ON-GO {label}: the Go reference cannot be pending"));
                    return;
                }
                let now = status_str(o);
                if o == Outcome::Pass && ref_status != "pass" {
                    promoted += 1;
                }
                match ref_status.as_str() {
                    // A failure is already reported as FAIL above.
                    _ if o == Outcome::Fail => {}
                    recorded if recorded == now => {}
                    "pass" => errors.push(format!("REGRESSION-ON-GO {label}: recorded pass, now {now}")),
                    recorded @ ("not_observable" | "upstream_failure") if !record => errors.push(format!(
                        "CHANGED-ON-GO {label}: recorded {recorded}, now {now}; review and record with --record"
                    )),
                    _ => {}
                }
                if record && complete && o != Outcome::Fail {
                    *ref_status = now.into();
                }
            }
            Backend::Rust => match (rust_status.as_str(), o) {
                ("pass", Outcome::Pass) | ("pending", Outcome::Pending) => {}
                ("pass", _) => errors.push(format!("REGRESSION {label}: recorded pass, now {}", status_str(o))),
                (_, Outcome::Pass) => {
                    promoted += 1;
                    if record && complete {
                        *rust_status = "pass".into();
                    } else if !record {
                        errors.push(format!("UNREVIEWED-PASS {label}: passes now; review and promote with --record"));
                    }
                }
                (_, Outcome::Pending) if complete => *rust_status = "pending".into(),
                _ => {}
            },
        }
    };
    for c in &mut m.cases {
        check(&c.id, &c.rust_cases, &mut c.reference_status, &mut c.rust_status, &mut errors);
    }
    for r in &mut m.rust_only {
        check(
            &r.rust_case,
            std::slice::from_ref(&r.rust_case),
            &mut r.reference_status,
            &mut r.rust_status,
            &mut errors,
        );
    }
    derive_aggregates(&mut m.cases);

    println!("backend {}: {} Rust tests", backend.as_str(), results.len());
    let counts = results.values().fold(BTreeMap::<&str, usize>::new(), |mut acc, o| {
        *acc.entry(match o {
            Outcome::Pass => "pass",
            Outcome::Pending => "pending",
            Outcome::Fail => "fail",
            Outcome::Ignored => "ignored",
            Outcome::NotObservable => "not_observable",
            Outcome::UpstreamFailure => "upstream_failure",
        })
        .or_default() += 1;
        acc
    });
    println!("  tests:  {counts:?}");
    println!("  mapped: {tally:?}");
    if promoted > 0 {
        println!("  {promoted} mapped cases newly passing{}", if record { " (recorded)" } else { "" });
    }
    if record {
        manifest::save(&mut m)?;
        println!("  manifest updated");
    }
    if !errors.is_empty() {
        for e in &errors {
            eprintln!("\n{e}");
        }
        bail!("{} compatibility error(s)", errors.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const OUTPUT: &str = "\
running 4 tests
test a::passes ... ok
test a::pending ... FAILED
test a::asserts ... FAILED
test a::skipped ... ignored

failures:

---- a::pending stdout ----

thread 'a::pending' panicked at crates/cabby-core/src/protocol.rs:1:5:
CABBY_PENDING:protocol::new_connect_message
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace

---- a::asserts stdout ----

thread 'a::asserts' panicked at tests/unit/x.rs:3:5:
assertion `left == right` failed
  left: 1
 right: 2

failures:
    a::asserts
    a::pending

test result: FAILED. 1 passed; 2 failed; 1 ignored; 0 measured; 0 filtered out
";

    #[test]
    fn classifies_libtest_output() {
        let r = parse(OUTPUT);
        assert_eq!(r["a::passes"], Outcome::Pass);
        assert_eq!(r["a::pending"], Outcome::Pending);
        // An ordinary assertion failure must never be classified as pending.
        assert_eq!(r["a::asserts"], Outcome::Fail);
        assert_eq!(r["a::skipped"], Outcome::Ignored);
    }

    #[test]
    fn marker_in_a_later_section_does_not_leak() {
        let out = OUTPUT.replace("---- a::pending stdout ----", "---- a::other stdout ----");
        let r = parse(&out);
        assert_eq!(r["a::pending"], Outcome::Fail);
    }

    #[test]
    fn not_observable_go_arms_are_recognized() {
        let out = "test b::hidden ... FAILED\n\nfailures:\n\n---- b::hidden stdout ----\n\nthread 'b::hidden' panicked at x.rs:1:1:\nnot implemented: not_observable: unexported state\n\nfailures:\n    b::hidden\n";
        assert_eq!(parse(out)["b::hidden"], Outcome::NotObservable);
    }

    #[test]
    fn a_binary_must_exit_cleanly_or_with_libtests_failure_summary() {
        use std::os::unix::process::ExitStatusExt;
        let ok = std::process::ExitStatus::from_raw(0);
        let failed = std::process::ExitStatus::from_raw(LIBTEST_FAILED << 8);
        let segv = std::process::ExitStatus::from_raw(11);
        let abort = std::process::ExitStatus::from_raw(134 << 8);
        assert_eq!(abnormal_exit(ok, "test result: ok."), None);
        // Failing or pending tests: libtest exits 101 after its summary.
        assert_eq!(abnormal_exit(failed, OUTPUT), None);
        // A crash after some results, a panic outside a test, an abort.
        assert!(abnormal_exit(segv, "running 2 tests\ntest a ... ok\n").is_some());
        assert!(abnormal_exit(failed, "running 2 tests\ntest a ... ok\n").is_some());
        assert!(abnormal_exit(abort, OUTPUT).is_some());
        // A test that calls `process::exit(0)`: success, but no summary.
        assert!(abnormal_exit(ok, "running 2 tests\ntest a ... ok\n").is_some());
    }

    #[test]
    fn counts_started_tests() {
        assert_eq!(started(OUTPUT), 4);
        assert_eq!(started("running 1 test\ntest a ... ok\n\nrunning 0 tests\n"), 1);
    }

    #[test]
    fn combining_mapped_cases() {
        use Outcome::*;
        assert_eq!(combine(&[Pass, Pass]), Pass);
        assert_eq!(combine(&[Pass, Pending]), Pending);
        assert_eq!(combine(&[Pending, Fail]), Fail);
        assert_eq!(combine(&[Ignored]), Fail);
    }
}
