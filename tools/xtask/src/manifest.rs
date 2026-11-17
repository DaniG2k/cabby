//! `compat/test-map.json`: the case-level traceability manifest (SPEC.md §4).

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
    /// Translated Rust cases against Cabby: pending | fail | pass
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
    let m = load()?;
    let total = m.cases.len();
    println!("upstream {} — {} cases ({} Rust-only)", m.upstream.commit, total, m.rust_only.len());
    for (k, v) in summarize(&m) {
        let parts: Vec<String> = v.iter().map(|(a, n)| format!("{a}={n}")).collect();
        println!("  {k:16} {}", parts.join("  "));
    }
    Ok(())
}
