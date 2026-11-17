//! Compatibility runner (SPEC.md §5 "Pending tests and CI").
//!
//! Runs every translated test binary against one backend, classifies each Rust
//! case as pass / pending (panicked with the pending marker) / fail / ignored,
//! and reconciles with `compat/test-map.json`:
//!
//! * fail, crash, timeout, ignore, or an unmapped test => error;
//! * a recorded `pass` that no longer passes => regression error;
//! * a `pending` case that now passes => error until promoted with `--record`;
//! * against Go, every translated case must pass (or carry a documented
//!   reference status such as `upstream_failure`).

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

fn run_binary(exe: &str, backend: Backend, sut_bin: &str, filter: Option<&str>) -> Result<(String, bool)> {
    let mut cmd = Command::new(exe);
    cmd.env("CABBY_BACKEND", backend.as_str()).env("SUT_BIN", sut_bin);
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
    Ok((out, status.success()))
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
    }
}

fn combine(outcomes: &[Outcome]) -> Outcome {
    if outcomes.iter().any(|o| matches!(o, Outcome::Fail | Outcome::Ignored)) {
        Outcome::Fail
    } else if outcomes.contains(&Outcome::NotObservable) {
        Outcome::NotObservable
    } else if outcomes.iter().all(|o| *o == Outcome::Pass) {
        Outcome::Pass
    } else {
        Outcome::Pending
    }
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

    let mut results: BTreeMap<String, Outcome> = BTreeMap::new();
    let mut errors: Vec<String> = Vec::new();
    for (name, exe) in test_binaries()? {
        let (out, _ok) = run_binary(&exe, backend, &sut_bin, filter)?;
        let parsed = parse(&out);
        if parsed.is_empty() && !out.contains("running 0 tests") {
            errors.push(format!("{name}: could not parse test output (crash?):\n{out}"));
        }
        for (t, o) in parsed {
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

    let mut promoted = 0;
    let mut tally: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut check = |label: &str,
                     rust_cases: &[String],
                     ref_status: &mut String,
                     rust_status: &mut String,
                     errors: &mut Vec<String>| {
        let outs: Vec<Outcome> = rust_cases.iter().filter_map(|r| results.get(r).copied()).collect();
        if outs.is_empty() {
            return;
        }
        if filter.is_none() && outs.len() != rust_cases.len() {
            errors.push(format!("MISSING {label}: mapped Rust cases did not run: {rust_cases:?}"));
        }
        let o = combine(&outs);
        *tally.entry(status_str(o)).or_default() += 1;
        match backend {
            Backend::Go => {
                if o == Outcome::Pass {
                    if ref_status != "pass" {
                        promoted += 1;
                    }
                    if record {
                        *ref_status = "pass".into();
                    }
                } else if o == Outcome::Fail && !matches!(ref_status.as_str(), "upstream_failure" | "not_observable") {
                    // already reported as FAIL above
                } else if o == Outcome::Pending {
                    errors.push(format!("PENDING-ON-GO {label}: the Go reference cannot be pending"));
                } else if o == Outcome::NotObservable && record {
                    *ref_status = "not_observable".into();
                }
            }
            Backend::Rust => match (rust_status.as_str(), o) {
                ("pass", Outcome::Pass) | ("pending", Outcome::Pending) => {}
                ("pass", _) => errors.push(format!("REGRESSION {label}: recorded pass, now {}", status_str(o))),
                (_, Outcome::Pass) => {
                    promoted += 1;
                    if record {
                        *rust_status = "pass".into();
                    } else {
                        errors.push(format!("UNREVIEWED-PASS {label}: passes now; review and promote with --record"));
                    }
                }
                (_, Outcome::Pending) => *rust_status = "pending".into(),
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
    fn combining_mapped_cases() {
        use Outcome::*;
        assert_eq!(combine(&[Pass, Pass]), Pass);
        assert_eq!(combine(&[Pass, Pending]), Pending);
        assert_eq!(combine(&[Pending, Fail]), Fail);
        assert_eq!(combine(&[Ignored]), Fail);
    }
}
