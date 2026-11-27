//! `compat/test-map.json`: the case-level traceability manifest.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::root;

pub const PATH: &str = "compat/test-map.json";
pub const COMMIT: &str = "26a1a703a54b870e42f5660c3d0785db2c602764";
pub const SHORT: &str = "26a1a703";

#[derive(Serialize, Deserialize, Default)]
pub struct Manifest {
    pub schema: u32,
    pub upstream: Upstream,
    pub generated_by: String,
    pub summary: BTreeMap<String, BTreeMap<String, usize>>,
    pub cases: Vec<Case>,
    /// Rust cases without a 1:1 upstream case: regressions and external checks.
    pub rust_only: Vec<RustOnly>,
    /// Helpers and mocks in upstream test code that translated cases depend on.
    pub dependencies: Vec<Dependency>,
}

#[derive(Serialize, Deserialize, Default)]
pub struct Upstream {
    pub repository: String,
    pub commit: String,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Case {
    /// `<commit8>:<file>#<path>`; Go paths use runtime subtest names.
    pub id: String,
    pub language: String,
    /// go_unit | go_benchi | go_vendored | feature | benchmark | anyt | anyt_extension
    pub suite: String,
    pub file: String,
    /// test | benchmark | example | fuzz | scenario
    pub kind: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_lines: Option<[u32; 2]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_constraint: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub services: Vec<String>,
    /// How the case was discovered: static (AST/source), runtime (test run), or both.
    pub discovered: String,
    /// The case only aggregates subtests (no direct assertions of its own).
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub aggregate: bool,
    /// Table-driven row: source lines point at the shared `t.Run` body.
    #[serde(skip_serializing_if = "std::ops::Not::not", default)]
    pub table_row: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub direct_assertions: Option<u32>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub upstream_skips: Vec<String>,
    /// Result of the original upstream test in the recorded baseline.
    pub upstream_result: String,
    // ---- curated (preserved across `cargo xtask inventory`) ----
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub notes: Vec<String>,
    // ---- translation and verification ----
    #[serde(default)]
    pub rust_cases: Vec<String>,
    /// unreviewed | translated | reviewed
    pub translation_status: String,
    /// Translated Rust cases against Go: unverified | pass | upstream_failure | upstream_skip | not_observable
    pub reference_status: String,
    /// Translated Rust cases against Cabby: pending | fail | pass | retired
    /// (`retired`: removed by an implemented deviation, D-063)
    pub rust_status: String,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct RustOnly {
    pub rust_case: String,
    /// regression | external
    pub relation: String,
    pub links: Vec<String>,
    pub reference_status: String,
    pub rust_status: String,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Dependency {
    pub file: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_lines: Option<[u32; 2]>,
}

pub fn load() -> Result<Manifest> {
    let text = std::fs::read_to_string(root().join(PATH)).with_context(|| format!("reading {PATH}"))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {PATH}"))
}

pub fn save(m: &mut Manifest) -> Result<()> {
    m.summary = summarize(m);
    let mut text = serde_json::to_string_pretty(m)?;
    text.push('\n');
    std::fs::write(root().join(PATH), text)?;
    Ok(())
}

fn summarize(m: &Manifest) -> BTreeMap<String, BTreeMap<String, usize>> {
    let mut s: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    for c in &m.cases {
        *s.entry("cases_by_suite".into()).or_default().entry(c.suite.clone()).or_default() += 1;
        *s.entry("translation".into()).or_default().entry(c.translation_status.clone()).or_default() += 1;
        *s.entry("reference".into()).or_default().entry(c.reference_status.clone()).or_default() += 1;
        *s.entry("rust".into()).or_default().entry(c.rust_status.clone()).or_default() += 1;
        *s.entry("upstream_result".into()).or_default().entry(c.upstream_result.clone()).or_default() += 1;
    }
    *s.entry("rust_only".into()).or_default().entry("cases".into()).or_default() += m.rust_only.len();
    s
}

pub fn report() -> Result<()> {
    report_with(false)
}

/// Upstream package of a case: the Go file's directory, or the suite for Ruby.
fn package_of(c: &Case) -> String {
    if c.language == "go" {
        c.file.rsplit_once('/').map_or(".".to_owned(), |(d, _)| d.to_owned())
    } else {
        c.suite.clone()
    }
}

pub fn report_with(by_package: bool) -> Result<()> {
    report_summary()?;
    if by_package {
        let m = load()?;
        let mut rows: BTreeMap<String, [usize; 7]> = BTreeMap::new();
        for c in &m.cases {
            let r = rows.entry(package_of(c)).or_default();
            r[0] += 1;
            r[1] += usize::from(c.translation_status != "unreviewed");
            r[2] += usize::from(c.translation_status == "reviewed");
            r[3] += usize::from(c.reference_status == "pass");
            r[4] += usize::from(c.reference_status == "not_observable");
            r[5] += usize::from(c.rust_status == "pass");
            r[6] += usize::from(c.rust_status == "retired");
        }
        println!(
            "\n  {:44} {:>6} {:>10} {:>8} {:>6} {:>7} {:>9} {:>7}",
            "package", "cases", "translated", "reviewed", "go", "not_obs", "rust_pass", "retired"
        );
        for (k, r) in rows {
            println!("  {k:44} {:>6} {:>10} {:>8} {:>6} {:>7} {:>9} {:>7}", r[0], r[1], r[2], r[3], r[4], r[5], r[6]);
        }
    }
    Ok(())
}

fn matches_pattern(id: &str, pattern: &str) -> bool {
    if pattern.ends_with('/') || pattern.ends_with('#') { id.starts_with(pattern) } else { id == pattern }
}

pub fn mark_reviewed(patterns: &[String], except: &[String]) -> Result<()> {
    let mut m = load()?;
    let mut marked = 0;
    for p in patterns.iter().chain(except) {
        if !m.cases.iter().any(|c| matches_pattern(&c.id, p)) {
            anyhow::bail!("pattern {p:?} matches no case");
        }
    }
    for c in &mut m.cases {
        let hit = patterns.iter().any(|p| matches_pattern(&c.id, p));
        let excluded = except.iter().any(|p| matches_pattern(&c.id, p));
        if hit && !excluded {
            if c.translation_status == "unreviewed" {
                anyhow::bail!("{} is not translated; cannot mark it reviewed", c.id);
            }
            if c.translation_status != "reviewed" {
                c.translation_status = "reviewed".into();
                marked += 1;
            }
        }
    }
    crate::inventory::derive_aggregates(&mut m.cases);
    save(&mut m)?;
    println!("marked {marked} cases reviewed");
    Ok(())
}

fn report_summary() -> Result<()> {
    let m = load()?;
    let total = m.cases.len();
    println!("upstream {} — {} cases ({} Rust-only)", m.upstream.commit, total, m.rust_only.len());
    for (k, v) in summarize(&m) {
        let parts: Vec<String> = v.iter().map(|(a, n)| format!("{a}={n}")).collect();
        println!("  {k:16} {}", parts.join("  "));
    }
    Ok(())
}
