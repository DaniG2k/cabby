//! Builds `compat/test-map.json` from:
//! * the Go AST inventory (`compat/reference-adapters/inventory`),
//! * recorded `go test -json` baseline runs (runtime case names, results),
//! * feature/benchmark scenario files and AnyT conformance logs,
//! * `/// upstream:` annotations on translated Rust tests.
//!
//! Curated fields (description, notes, `reviewed`) survive regeneration.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::manifest::{self, COMMIT, Case, Dependency, Manifest, RustOnly, SHORT, Upstream};
use crate::reference::sha256_file;
use crate::root;

// ---------------------------------------------------------------- Go AST

#[derive(Deserialize)]
struct AstFile {
    path: String,
    sha256: String,
    #[serde(default)]
    build_constraint: Option<String>,
    #[serde(default)]
    helpers: Option<Vec<AstFunc>>,
    #[serde(default)]
    cases: Option<Vec<AstCase>>,
    #[serde(default)]
    parse_error: Option<String>,
}

#[derive(Deserialize)]
struct AstFunc {
    name: String,
    start_line: u32,
    end_line: u32,
}

#[derive(Deserialize, Clone)]
struct AstCase {
    kind: String,
    path: Vec<Seg>,
    start_line: u32,
    end_line: u32,
    assertions: u32,
    #[serde(default)]
    skips: Option<Vec<AstSkip>>,
    #[serde(default)]
    children: Option<Vec<AstCase>>,
}

#[derive(Deserialize, Clone)]
struct Seg {
    name: String,
    #[serde(default)]
    dynamic: bool,
}

#[derive(Deserialize, Clone)]
struct AstSkip {
    line: u32,
    #[serde(default)]
    reason: Option<String>,
}

fn go_ast() -> Result<Vec<AstFile>> {
    let root = root();
    // Canonicalize: agent worktrees symlink compat/reference, and Go's WalkDir does
    // not descend into a symlinked root.
    let reference = root.join("compat/reference").canonicalize().context("resolving compat/reference")?;
    let out = Command::new("go")
        .args(["run", ".", "-root"])
        .arg(&reference)
        .arg(".")
        .current_dir(root.join("compat/reference-adapters/inventory"))
        .output()
        .context("running the Go AST inventory (is Go on PATH?)")?;
    if !out.status.success() {
        bail!("inventory tool failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    let files: Vec<AstFile> = serde_json::from_slice(&out.stdout)?;
    for f in &files {
        if let Some(e) = &f.parse_error {
            bail!("parse error in {}: {e}", f.path);
        }
    }
    Ok(files)
}

/// Go's `testing` rewrite of subtest names: whitespace becomes `_`.
fn go_rewrite(name: &str) -> String {
    name.chars().map(|c| if c.is_whitespace() { '_' } else { c }).collect()
}

// ---------------------------------------------------------------- Go runtime

/// (package dir, runtime test path) -> action (pass/fail/skip)
type Runtime = BTreeMap<(String, String), String>;

fn package_dir(pkg: &str) -> String {
    if pkg == "github.com/mitchellh/go-mruby" {
        return "vendorlib/go-mruby".into();
    }
    pkg.strip_prefix("github.com/anycable/anycable-go").unwrap_or(pkg).trim_start_matches('/').to_owned()
}

fn runtime(files: &[&str]) -> Result<Runtime> {
    let mut rt = Runtime::new();
    for f in files {
        let path = root().join("compat/baseline").join(f);
        let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        for line in text.lines() {
            let Ok(ev) = serde_json::from_str::<serde_json::Value>(line) else { continue };
            let (Some(action), Some(test), Some(pkg)) =
                (ev["Action"].as_str(), ev["Test"].as_str(), ev["Package"].as_str())
            else {
                continue;
            };
            if matches!(action, "pass" | "fail" | "skip") {
                rt.insert((package_dir(pkg), test.to_owned()), action.to_owned());
            }
        }
    }
    Ok(rt)
}

// ---------------------------------------------------------------- Go cases

struct GoCtx<'a> {
    file: &'a AstFile,
    suite: &'static str,
    evidence: &'static str,
}

fn suite_of(path: &str) -> (&'static str, &'static str) {
    if path.starts_with("etc/benchi/") {
        ("go_benchi", "compat/baseline/go-benchi.jsonl")
    } else if path.starts_with("vendorlib/") {
        ("go_vendored", "compat/baseline/go-mruby-vendored.jsonl")
    } else {
        ("go_unit", "compat/baseline/go-unit-mrb.jsonl")
    }
}

fn dir_of(path: &str) -> String {
    Path::new(path).parent().map(|p| p.to_string_lossy().into_owned()).unwrap_or_default()
}

fn new_go_case(ctx: &GoCtx, node: &AstCase, path: String, discovered: &str, table_row: bool) -> Case {
    let skips: Vec<String> = node
        .skips
        .iter()
        .flatten()
        .map(|s| format!("line {}: {}", s.line, s.reason.clone().unwrap_or_default()))
        .collect();
    let services = if skips.iter().any(|s| s.contains("Redis")) { vec!["redis".to_owned()] } else { vec![] };
    let has_children = node.children.as_ref().is_some_and(|c| !c.is_empty());
    Case {
        id: format!("{SHORT}:{}#{path}", ctx.file.path),
        language: "go".into(),
        suite: ctx.suite.into(),
        file: ctx.file.path.clone(),
        kind: node.kind.clone(),
        path,
        source_lines: Some([node.start_line, node.end_line]),
        file_sha256: Some(ctx.file.sha256.clone()),
        build_constraint: ctx.file.build_constraint.clone(),
        services,
        discovered: discovered.into(),
        aggregate: has_children && node.assertions == 0 && !table_row,
        table_row,
        direct_assertions: Some(node.assertions),
        upstream_skips: skips,
        upstream_result: "not_run".into(),
        evidence: vec![ctx.evidence.into()],
        ..Default::default()
    }
}

/// Finds the AST node for runtime subtest `rest` below `node`. Returns the node
/// and whether a dynamic (table-driven) segment was crossed.
fn match_runtime<'a>(node: &'a AstCase, rest: &str) -> (&'a AstCase, bool) {
    if rest.is_empty() {
        return (node, false);
    }
    for child in node.children.iter().flatten() {
        let seg = child.path.last().expect("child segment");
        if seg.dynamic {
            continue;
        }
        let name = go_rewrite(&seg.name);
        if let Some(after) = rest.strip_prefix(&name)
            && (after.is_empty() || after.starts_with('/') || is_dedupe_suffix(after))
        {
            let after = strip_dedupe(after).trim_start_matches('/');
            return match_runtime(child, after);
        }
    }
    // Table-driven: one dynamic child consumes one segment.
    if let Some(child) = node.children.iter().flatten().find(|c| c.path.last().is_some_and(|s| s.dynamic)) {
        let after = rest.split_once('/').map_or("", |(_, a)| a);
        let (n, _) = match_runtime(child, after);
        return (n, true);
    }
    (node, true)
}

fn is_dedupe_suffix(s: &str) -> bool {
    s.len() >= 3 && s.starts_with('#') && s[1..].chars().take_while(char::is_ascii_digit).count() >= 2
}

fn strip_dedupe(s: &str) -> &str {
    if is_dedupe_suffix(s) {
        let digits = s[1..].chars().take_while(char::is_ascii_digit).count();
        &s[1 + digits..]
    } else {
        s
    }
}

fn static_path(node: &AstCase) -> Option<String> {
    if node.path.iter().any(|s| s.dynamic) {
        return None;
    }
    Some(node.path.iter().map(|s| go_rewrite(&s.name)).collect::<Vec<_>>().join("/"))
}

fn go_cases(files: &[AstFile], rt: &Runtime) -> (Vec<Case>, Vec<Dependency>) {
    let mut cases = Vec::new();
    let mut deps = Vec::new();
    // (dir, top-level name) -> (file index, node)
    let mut tops: HashMap<(String, String), (usize, &AstCase)> = HashMap::new();
    for (i, f) in files.iter().enumerate() {
        for c in f.cases.iter().flatten() {
            tops.insert((dir_of(&f.path), c.path[0].name.clone()), (i, c));
        }
        for h in f.helpers.iter().flatten() {
            deps.push(Dependency {
                file: f.path.clone(),
                name: h.name.clone(),
                source_lines: Some([h.start_line, h.end_line]),
            });
        }
    }
    let mut seen: HashSet<String> = HashSet::new();
    // Runtime-discovered cases.
    for ((dir, test), action) in rt {
        let top = test.split('/').next().unwrap_or(test);
        let Some(&(fi, node)) = tops.get(&(dir.clone(), top.to_owned())) else {
            eprintln!("warning: runtime case {dir} {test} has no AST match");
            continue;
        };
        let f = &files[fi];
        let (suite, evidence) = suite_of(&f.path);
        let ctx = GoCtx { file: f, suite, evidence };
        let rest = test.strip_prefix(top).unwrap_or("").trim_start_matches('/');
        let (matched, dynamic) = match_runtime(node, rest);
        let is_static = !dynamic && static_path(matched).as_deref() == Some(test.as_str());
        let mut c =
            new_go_case(&ctx, matched, test.clone(), if is_static { "static+runtime" } else { "runtime" }, dynamic);
        if dynamic {
            // Table rows execute the matched node's body; children are reported separately.
            c.aggregate = false;
        } else if !rt.keys().any(|(d, t)| d == dir && t.starts_with(&format!("{test}/"))) {
            c.aggregate = false;
        }
        c.upstream_result = action.clone();
        seen.insert(c.id.clone());
        cases.push(c);
    }
    // Static cases that never ran (benchmarks, conditionally executed subtests).
    fn walk(ctx: &GoCtx, node: &AstCase, cases: &mut Vec<Case>, seen: &HashSet<String>) {
        let path = static_path(node).unwrap_or_else(|| {
            node.path
                .iter()
                .map(|s| if s.dynamic { format!("<{}>", s.name) } else { go_rewrite(&s.name) })
                .collect::<Vec<_>>()
                .join("/")
        });
        let id = format!("{SHORT}:{}#{path}", ctx.file.path);
        let dynamic_unrun = node.path.iter().any(|s| s.dynamic);
        if !seen.contains(&id)
            && !(dynamic_unrun
                && seen.iter().any(|s| s.starts_with(&format!("{SHORT}:{}#{}", ctx.file.path, node.path[0].name))))
        {
            let mut c = new_go_case(ctx, node, path, "static", dynamic_unrun);
            if node.kind == "benchmark" {
                // `go test -bench` output, recorded per package in compat/baseline/benchmarks/.
                let pkg = dir_of(&ctx.file.path).replace('/', "-");
                let evidence = format!("compat/baseline/benchmarks/go-bench-{pkg}.txt");
                let text = std::fs::read_to_string(root().join(&evidence)).unwrap_or_default();
                if text.contains(&format!("{}/", node.path[0].name)) && text.lines().any(|l| l == "PASS") {
                    c.upstream_result = "pass".into();
                    c.evidence = vec![evidence];
                } else {
                    c.notes.push("Benchmark: not executed by `go test`; run with -bench for reference evidence".into());
                }
            } else {
                c.notes.push("Static case with no runtime event in the baseline".into());
            }
            cases.push(c);
        }
        for child in node.children.iter().flatten() {
            walk(ctx, child, cases, seen);
        }
    }
    for f in files {
        let (suite, evidence) = suite_of(&f.path);
        let ctx = GoCtx { file: f, suite, evidence };
        for c in f.cases.iter().flatten() {
            walk(&ctx, c, &mut cases, &seen);
        }
    }
    (cases, deps)
}

// ---------------------------------------------------------------- Ruby suites

fn feature_cases() -> Result<Vec<Case>> {
    let root = root();
    let summary = std::fs::read_to_string(root.join("compat/baseline/features/summary.txt")).unwrap_or_default()
        + &std::fs::read_to_string(root.join("compat/baseline/benchmarks/summary.txt")).unwrap_or_default();
    let mut results = HashMap::new();
    for line in summary.lines() {
        if let Some((name, rest)) = line.split_once(" exit=") {
            let code = rest.split_whitespace().next().unwrap_or("");
            results.insert(name.to_owned(), if code == "0" { "pass" } else { "fail" });
        }
    }
    let mut out = Vec::new();
    let dir = root.join("compat/reference/features");
    let mut entries: Vec<_> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok()).map(|e| e.path()).collect();
    entries.sort();
    for p in entries {
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        let (suite, stem) = if let Some(s) = name.strip_suffix(".testfile") {
            ("feature", s)
        } else if let Some(s) = name.strip_suffix(".benchfile") {
            ("benchmark", s)
        } else {
            continue;
        };
        let lines = std::fs::read_to_string(&p)?.lines().count() as u32;
        let file = format!("features/{name}");
        out.push(Case {
            id: format!("{SHORT}:{file}#scenario"),
            language: "ruby".into(),
            suite: suite.into(),
            file,
            kind: "scenario".into(),
            path: "scenario".into(),
            source_lines: Some([1, lines]),
            file_sha256: Some(sha256_file(&p)?),
            discovered: "static".into(),
            upstream_result: results.get(stem).copied().unwrap_or("not_run").into(),
            evidence: vec![format!(
                "compat/baseline/{}/{stem}.log",
                if suite == "feature" { "features" } else { "benchmarks" }
            )],
            ..Default::default()
        });
    }
    Ok(out)
}

/// Pinned AnyT core gem (Gemfile.lock); its scenario sources are unpacked to
/// target/upstream-gems by `ensure_anyt_core`.
pub const ANYT_CORE: &str = "anyt-core-1.4.0";
const ANYT_CORE_SHA256: &str = "f32116c37f3d8384ed81f441306e1c831554cabbd1739b320fa0627d08134985";

fn ensure_anyt_core() -> Result<std::path::PathBuf> {
    let dir = root().join("target/upstream-gems");
    let unpacked = dir.join(ANYT_CORE);
    if unpacked.join("MIT-LICENSE").exists() {
        return Ok(unpacked);
    }
    std::fs::create_dir_all(&unpacked)?;
    let gem = dir.join(format!("{ANYT_CORE}.gem"));
    crate::reference::sh(
        Command::new("curl")
            .args(["-sfL", "-A", "cabby-xtask", "-o"])
            .arg(&gem)
            .arg(format!("https://rubygems.org/downloads/{ANYT_CORE}.gem")),
    )?;
    let sum = sha256_file(&gem)?;
    if sum != ANYT_CORE_SHA256 {
        bail!("{ANYT_CORE}.gem sha256 {sum} != pinned {ANYT_CORE_SHA256}");
    }
    crate::reference::sh(Command::new("sh").arg("-c").arg(format!(
        "tar -xOf '{}' data.tar.gz | tar -xz -C '{}'",
        gem.display(),
        unpacked.display()
    )))?;
    Ok(unpacked)
}

/// scenario title -> (feature, file relative to `base`, line) for AnyT-style sources.
fn scenario_titles(base: &Path, dir: &Path) -> Result<HashMap<String, (String, String, u32)>> {
    let mut out = HashMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir)? {
            let p = e?.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().is_none_or(|x| x != "rb") {
                continue;
            }
            let rel = p.strip_prefix(base)?.to_string_lossy().into_owned();
            let text = std::fs::read_to_string(&p)?;
            let mut feature = String::new();
            let lines: Vec<&str> = text.lines().collect();
            let mut i = 0;
            while i < lines.len() {
                let t = lines[i].trim_start();
                if let Some(r) = t.strip_prefix("feature \"") {
                    feature = r.split('"').next().unwrap_or("").to_owned();
                } else if let Some(r) = t.strip_prefix("scenario ") {
                    let start = i as u32 + 1;
                    let title = if let Some(r) = r.strip_prefix("%(") {
                        let mut buf = r.to_owned();
                        while !buf.contains(')') && i + 1 < lines.len() {
                            i += 1;
                            buf.push(' ');
                            buf.push_str(lines[i]);
                        }
                        buf.split(')').next().unwrap_or("").to_owned()
                    } else {
                        r.trim_start_matches('"').split('"').next().unwrap_or("").to_owned()
                    };
                    let title = title.split_whitespace().collect::<Vec<_>>().join(" ");
                    if out.insert(title.clone(), (feature.clone(), rel.clone(), start)).is_some() {
                        bail!("duplicate AnyT scenario title {title:?} ({rel})");
                    }
                }
                i += 1;
            }
        }
    }
    Ok(out)
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for d in chars.by_ref() {
                if d.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn anyt_cases() -> Result<Vec<Case>> {
    let reference = root().join("compat/reference");
    let ext = scenario_titles(&reference, &reference.join("etc/anyt"))?;
    let core_dir = ensure_anyt_core()?;
    let core = scenario_titles(&core_dir, &core_dir)?;
    // scenario title -> target -> result. Result lines are the only reliable
    // structure in the logs (server output interleaves with reporter output).
    let mut seen: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let dir = root().join("compat/baseline/conformance");
    let mut logs: Vec<_> = std::fs::read_dir(&dir)?.filter_map(|e| e.ok()).map(|e| e.path()).collect();
    logs.sort();
    for log in logs.iter().filter(|p| p.extension().is_some_and(|x| x == "log")) {
        let target = log.file_stem().unwrap().to_string_lossy().into_owned();
        let text = strip_ansi(&String::from_utf8_lossy(&std::fs::read(log)?));
        // Multi-line titles print as "  first line" then "    rest STATUS (t)".
        // Server logs can interleave mid-line: "  Title     <log>" ... " PASS (2.02s)".
        let mut fragment: Option<String> = None;
        let mut interrupted: Option<String> = None;
        for line in text.lines() {
            let lone = line.trim();
            if let Some(status) = ["PASS", "FAIL", "ERROR", "SKIP"]
                .into_iter()
                .find(|s| lone.strip_prefix(s).is_some_and(|r| r.starts_with(" (") && r.ends_with("s)")))
            {
                if let Some(title) = interrupted.take() {
                    seen.entry(title).or_default().insert(target.clone(), status.to_lowercase());
                }
                continue;
            }
            if line.starts_with("  ") && !line.starts_with("    ") && line.contains("    ") {
                let head = line.trim_start().split("    ").next().unwrap_or("").trim();
                if !head.is_empty() && !lone.ends_with("s)") {
                    interrupted = Some(head.split_whitespace().collect::<Vec<_>>().join(" "));
                }
            }
            if !line.starts_with("  ") {
                if !line.as_bytes().first().is_some_and(u8::is_ascii_digit) {
                    fragment = None;
                }
                continue;
            }
            let trimmed = line.trim_end();
            let result = ["PASS", "FAIL", "ERROR", "SKIP"].into_iter().find_map(|status| {
                trimmed.ends_with("s)").then(|| trimmed.rfind(&format!(" {status} (")).map(|i| (status, i))).flatten()
            });
            match result {
                Some((status, idx)) => {
                    let mut title = trimmed[..idx].trim().to_owned();
                    if line.starts_with("    ")
                        && let Some(f) = fragment.take()
                    {
                        title = format!("{f} {title}");
                    }
                    let title = title.split_whitespace().collect::<Vec<_>>().join(" ");
                    seen.entry(title).or_default().insert(target.clone(), status.to_lowercase());
                    fragment = None;
                }
                None if !line.starts_with("    ") => fragment = Some(trimmed.trim().to_owned()),
                None => {}
            }
        }
    }
    let mut out = Vec::new();
    let mut add = |title: &str, src: &(String, String, u32), suite: &str, file: String, sha: Option<String>| {
        let (feature, _, line) = src;
        let targets = seen.get(title);
        let result = match targets {
            None => "not_run",
            Some(t) if t.values().all(|r| r == "pass") => "pass",
            Some(_) => "fail",
        };
        let id = if suite == "anyt" {
            format!("{ANYT_CORE}:{file}#{feature}/{title}")
        } else {
            format!("{SHORT}:{file}#{feature}/{title}")
        };
        out.push(Case {
            id,
            language: "ruby".into(),
            suite: suite.into(),
            file,
            kind: "scenario".into(),
            path: format!("{feature}/{title}"),
            source_lines: Some([*line, *line]),
            file_sha256: sha,
            discovered: if targets.is_some() { "static+runtime".into() } else { "static".into() },
            upstream_result: result.into(),
            evidence: targets
                .into_iter()
                .flatten()
                .map(|(t, r)| format!("compat/baseline/conformance/{t}.log ({r})"))
                .collect(),
            ..Default::default()
        });
    };
    for (title, src) in &core {
        let sha = sha256_file(&core_dir.join(&src.1)).ok();
        add(title, src, "anyt", src.1.clone(), sha);
    }
    for (title, src) in &ext {
        let sha = sha256_file(&reference.join(&src.1)).ok();
        add(title, src, "anyt_extension", src.1.clone(), sha);
    }
    for title in seen.keys() {
        if !core.contains_key(title) && !ext.contains_key(title) {
            bail!("AnyT log scenario {title:?} not found in {ANYT_CORE} or etc/anyt");
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------- Rust annotations

pub struct Annotation {
    pub rust_case: String,
    pub relation: &'static str, // upstream | regression | external
    pub target: String,
    /// `/// note: …` lines on the same test (adaptations, not_observable reasons).
    pub notes: Vec<String>,
}

/// Prefix of manifest notes generated from `/// note:` annotations; they are
/// recomputed on every inventory run, unlike curated notes.
pub const SOURCE_NOTE: &str = "[src] ";

#[derive(Deserialize)]
struct TestsToml {
    #[serde(default)]
    test: Vec<TestTarget>,
}

#[derive(Deserialize)]
struct TestTarget {
    name: String,
    path: String,
}

/// Counts `{`/`}` outside string and char literals and comments.
fn brace_delta(line: &str) -> i32 {
    let mut d = 0;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '/' if chars.peek() == Some(&'/') => break,
            '"' => {
                while let Some(s) = chars.next() {
                    match s {
                        '\\' => {
                            chars.next();
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            '\'' => {
                // char literal or lifetime: skip a short literal like '{' or '\''
                let rest: String = chars.clone().take(3).collect();
                if rest.starts_with('\\') {
                    chars.nth(2);
                } else if rest.chars().nth(1) == Some('\'') {
                    chars.nth(1);
                }
            }
            '{' => d += 1,
            '}' => d -= 1,
            _ => {}
        }
    }
    d
}

pub fn rust_annotations() -> Result<Vec<Annotation>> {
    let tests_dir = root().join("tests");
    let toml: TestsToml = toml::from_str(&std::fs::read_to_string(tests_dir.join("Cargo.toml"))?)?;
    let mut out = Vec::new();
    for t in toml.test {
        let main = tests_dir.join(&t.path);
        let base = main.parent().unwrap().to_path_buf();
        let mut files = vec![(main.clone(), Vec::<String>::new())];
        // `read_dir` order depends on the filesystem; sort so that notes merged from
        // several files come out in the same order on every machine (CI checks it).
        let mut others: Vec<_> = std::fs::read_dir(&base)?.map(|e| e.map(|e| e.path())).collect::<Result<_, _>>()?;
        others.retain(|p| p.extension().is_some_and(|x| x == "rs") && *p != main);
        others.sort();
        for p in others {
            let stem = p.file_stem().unwrap().to_string_lossy().into_owned();
            files.push((p, vec![stem]));
        }
        for (file, prefix) in files {
            let text = std::fs::read_to_string(&file)?;
            let mut mods: Vec<(String, i32)> = Vec::new();
            let mut depth = 0;
            let mut pending: Vec<(&'static str, String)> = Vec::new();
            let mut notes: Vec<String> = Vec::new();
            let mut is_test = false;
            for line in text.lines() {
                let tl = line.trim_start();
                for (prefix_s, rel) in [
                    ("/// upstream: ", "upstream"),
                    ("/// regression-for: ", "regression"),
                    ("/// external-for: ", "external"),
                    ("/// issue: ", "issue"),
                ] {
                    if let Some(r) = tl.strip_prefix(prefix_s) {
                        pending.push((rel, r.trim().to_owned()));
                    }
                }
                if let Some(r) = tl.strip_prefix("/// note: ") {
                    notes.push(r.trim().to_owned());
                }
                if tl.starts_with("#[test]") {
                    is_test = true;
                }
                if let Some(r) = tl.strip_prefix("mod ").or_else(|| tl.strip_prefix("pub mod "))
                    && let Some(name) = r.strip_suffix(" {")
                {
                    mods.push((name.to_owned(), depth + 1));
                }
                if let Some(r) = tl.strip_prefix("fn ").or_else(|| tl.strip_prefix("async fn ")) {
                    let name = r.split('(').next().unwrap_or("").to_owned();
                    if is_test {
                        let mut path = vec![t.name.clone()];
                        path.extend(prefix.iter().cloned());
                        path.extend(mods.iter().map(|(m, _)| m.clone()));
                        path.push(name);
                        let rust_case = path.join("::");
                        if pending.is_empty() {
                            bail!(
                                "test {rust_case} has no `/// upstream:`, `/// regression-for:`, `/// external-for:` or `/// issue:` annotation"
                            );
                        }
                        for (relation, target) in pending.drain(..) {
                            out.push(Annotation {
                                rust_case: rust_case.clone(),
                                relation,
                                target,
                                notes: notes.clone(),
                            });
                        }
                    }
                    pending.clear();
                    notes.clear();
                    is_test = false;
                }
                depth += brace_delta(line);
                while mods.last().is_some_and(|(_, d)| depth < *d) {
                    mods.pop();
                }
            }
        }
    }
    Ok(out)
}

pub fn upstream_id(target: &str) -> String {
    if target.starts_with(ANYT_CORE) { target.to_owned() } else { format!("{SHORT}:{target}") }
}

// ---------------------------------------------------------------- assemble

pub fn run() -> Result<()> {
    if !root().join("compat/reference/go.mod").exists() {
        bail!("compat/reference missing: run `cargo xtask reference-checkout`");
    }
    let ast = go_ast()?;
    let rt = runtime(&["go-unit-mrb.jsonl", "go-benchi.jsonl", "go-mruby-vendored.jsonl"])?;
    let (mut cases, mut deps) = go_cases(&ast, &rt);
    cases.extend(feature_cases()?);
    cases.extend(anyt_cases()?);
    cases.sort_by(|a, b| a.id.cmp(&b.id));
    for dir in ["mocks", "node_mocks"] {
        let mut names: Vec<_> = std::fs::read_dir(root().join("compat/reference").join(dir))?
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        for n in names {
            deps.push(Dependency { file: format!("{dir}/{n}"), name: "(mock file)".into(), source_lines: None });
        }
    }

    let previous = manifest::load().map(migrate_ids);
    let old: HashMap<String, Case> =
        previous.as_ref().map(|m| m.cases.iter().map(|c| (c.id.clone(), c.clone())).collect()).unwrap_or_default();
    let old_only: HashMap<String, RustOnly> = previous
        .as_ref()
        .map(|m| m.rust_only.iter().map(|r| (r.rust_case.clone(), r.clone())).collect())
        .unwrap_or_default();

    let index: HashMap<String, usize> = cases.iter().enumerate().map(|(i, c)| (c.id.clone(), i)).collect();
    let mut rust_only: Vec<RustOnly> = Vec::new();
    for a in rust_annotations()? {
        // Upstream issues (`anycable/anycable/issues/275`) are links, not inventory cases.
        let id = if a.relation == "issue" {
            if !(a.target.starts_with("anycable/") && a.target.contains("/issues/")) {
                bail!("{}: issue annotation must look like `anycable/anycable/issues/123`", a.rust_case);
            }
            a.target.clone()
        } else {
            upstream_id(&a.target)
        };
        let i = match index.get(&id) {
            Some(&i) => i,
            None if a.relation == "issue" => usize::MAX, // never used: issues are rust_only
            None => bail!("{} references unknown upstream case {id}", a.rust_case),
        };
        if a.relation == "upstream" {
            for n in &a.notes {
                let n = format!("{SOURCE_NOTE}{n}");
                if !cases[i].notes.contains(&n) {
                    cases[i].notes.push(n);
                }
            }
            cases[i].rust_cases.push(a.rust_case);
        } else {
            match rust_only.iter_mut().find(|r| r.rust_case == a.rust_case) {
                Some(r) => r.links.push(id),
                None => rust_only.push(RustOnly {
                    rust_case: a.rust_case.clone(),
                    relation: a.relation.into(),
                    links: vec![id],
                    reference_status: old_only
                        .get(&a.rust_case)
                        .map_or("unverified".into(), |o| o.reference_status.clone()),
                    rust_status: old_only.get(&a.rust_case).map_or("pending".into(), |o| o.rust_status.clone()),
                }),
            }
        }
    }

    rust_only.sort_by(|a, b| a.rust_case.cmp(&b.rust_case));
    for r in &mut rust_only {
        r.links.sort();
    }
    for c in &mut cases {
        c.rust_cases.sort();
        let prev = old.get(&c.id);
        if let Some(p) = prev {
            c.description = p.description.clone();
            // Curated notes survive; source notes come only from the current annotations.
            for n in p.notes.iter().filter(|n| !n.starts_with(SOURCE_NOTE)) {
                if !c.notes.contains(n) {
                    c.notes.push(n.clone());
                }
            }
        }
        let same_mapping = prev.is_some_and(|p| p.rust_cases == c.rust_cases);
        c.translation_status = match prev {
            Some(p) if p.translation_status == "reviewed" && same_mapping => "reviewed".into(),
            _ if !c.rust_cases.is_empty() => "translated".into(),
            _ => "unreviewed".into(),
        };
        c.reference_status = match prev {
            Some(p) if same_mapping => p.reference_status.clone(),
            _ => "unverified".into(),
        };
        c.rust_status = match prev {
            Some(p) if same_mapping => p.rust_status.clone(),
            _ => "pending".into(),
        };
    }
    derive_aggregates(&mut cases);

    let mut m = Manifest {
        schema: 1,
        upstream: Upstream { repository: "https://github.com/anycable/anycable".into(), commit: COMMIT.into() },
        generated_by: "cargo xtask inventory".into(),
        cases,
        rust_only,
        dependencies: deps,
        ..Default::default()
    };
    manifest::save(&mut m)?;
    manifest::report()
}

/// A manifest recorded at an earlier pin, with its case IDs and links moved to the
/// current one (`<old commit8>:file#path` becomes `<SHORT>:file#path`), so curated
/// fields and recorded statuses carry over when the reference moves
/// (`docs/translating-tests.md`, "Moving the reference"). AnyT IDs carry no commit.
fn migrate_ids(mut m: Manifest) -> Manifest {
    let Some(old) = m.upstream.commit.get(..8).map(|s| format!("{s}:")) else { return m };
    if m.upstream.commit == COMMIT {
        return m;
    }
    let moved = |id: &mut String| {
        if let Some(rest) = id.strip_prefix(&old) {
            *id = format!("{SHORT}:{rest}");
        }
    };
    for c in &mut m.cases {
        moved(&mut c.id);
    }
    for r in &mut m.rust_only {
        r.links.iter_mut().for_each(moved);
    }
    m.upstream.commit = COMMIT.into();
    m
}

/// Aggregate Go parents (no direct assertions) take their status from children.
/// Nested aggregates (a subtest that only runs table rows) are derived first, so a
/// parent sees its aggregate children's derived statuses.
pub fn derive_aggregates(cases: &mut [Case]) {
    let mut order: Vec<usize> =
        (0..cases.len()).filter(|&i| cases[i].aggregate && cases[i].rust_cases.is_empty()).collect();
    order.sort_by_key(|&i| std::cmp::Reverse(cases[i].id.matches('/').count()));
    for i in order {
        let prefix = format!("{}/", cases[i].id);
        let kids: Vec<&Case> = cases.iter().filter(|k| k.id.starts_with(&prefix)).collect();
        if kids.is_empty() {
            continue;
        }
        let all = |f: &dyn Fn(&Case) -> bool| kids.iter().all(|k| f(k));
        let translation = if all(&|k| k.translation_status == "reviewed") {
            "reviewed"
        } else if all(&|k| k.translation_status != "unreviewed") {
            "translated"
        } else {
            "unreviewed"
        };
        let reference = if all(&|k| k.reference_status == "pass") {
            "pass"
        } else if all(&|k| k.reference_status == "not_observable") {
            "not_observable"
        } else {
            "unverified"
        };
        let rust = if all(&|k| k.rust_status == "pass") {
            "pass"
        } else if kids.iter().any(|k| k.rust_status == "fail") {
            "fail"
        } else {
            "pending"
        };
        let c = &mut cases[i];
        c.translation_status = translation.into();
        c.reference_status = reference.into();
        c.rust_status = rust.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_aggregates_are_derived_before_their_parents() {
        let case = |id: &str, aggregate: bool, status: &str| Case {
            id: id.into(),
            aggregate,
            translation_status: status.into(),
            reference_status: status.into(),
            rust_status: status.into(),
            ..Default::default()
        };
        let mut cases = vec![
            case("x:f#T", true, "unreviewed"),
            case("x:f#T/Rows", true, "unreviewed"),
            case("x:f#T/Rows/a", false, "pass"),
            case("x:f#T/Rows/b", false, "pass"),
            case("x:f#T/Other", false, "pass"),
        ];
        cases[2].translation_status = "translated".into();
        cases[3].translation_status = "translated".into();
        cases[4].translation_status = "translated".into();
        derive_aggregates(&mut cases);
        assert_eq!(
            ("translated", "pass", "pass"),
            (&*cases[1].translation_status, &*cases[1].reference_status, &*cases[1].rust_status)
        );
        assert_eq!(
            ("translated", "pass", "pass"),
            (&*cases[0].translation_status, &*cases[0].reference_status, &*cases[0].rust_status)
        );
    }

    #[test]
    fn an_aggregate_of_cases_not_observable_on_go_is_not_observable() {
        let case = |id: &str, aggregate: bool, reference: &str| Case {
            id: id.into(),
            aggregate,
            translation_status: "translated".into(),
            reference_status: reference.into(),
            rust_status: "pass".into(),
            ..Default::default()
        };
        let mut cases = vec![
            case("x:f#T", true, "unverified"),
            case("x:f#T/a", false, "not_observable"),
            case("x:f#T/b", false, "not_observable"),
        ];
        derive_aggregates(&mut cases);
        assert_eq!("not_observable", cases[0].reference_status);
        cases[2].reference_status = "pass".into();
        derive_aggregates(&mut cases);
        assert_eq!("unverified", cases[0].reference_status, "mixed children");
    }

    #[test]
    fn ids_recorded_at_an_earlier_pin_move_to_the_current_one() {
        let case = |id: &str| Case { id: id.into(), ..Default::default() };
        let m = Manifest {
            upstream: Upstream { repository: String::new(), commit: "0123456789abcdef0123456789abcdef01234567".into() },
            cases: vec![case("01234567:hub/hub_test.go#TestHub"), case("anyt-core-1.4.0:a.rb#F/S")],
            rust_only: vec![RustOnly { links: vec!["01234567:node/node_test.go#TestX".into()], ..Default::default() }],
            ..Default::default()
        };
        let m = migrate_ids(m);
        assert_eq!(COMMIT, m.upstream.commit);
        assert_eq!(format!("{SHORT}:hub/hub_test.go#TestHub"), m.cases[0].id);
        assert_eq!("anyt-core-1.4.0:a.rb#F/S", m.cases[1].id);
        assert_eq!(vec![format!("{SHORT}:node/node_test.go#TestX")], m.rust_only[0].links);
    }
}
