//! The pinned Go reference: checkout, builds, input verification.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::root;

#[derive(serde::Deserialize)]
struct Lock {
    reference: LockRef,
}

#[derive(serde::Deserialize)]
struct LockRef {
    repository: String,
    commit: String,
    rpc_proto_sha256: String,
}

fn lock() -> Result<LockRef> {
    let text = std::fs::read_to_string(root().join("compat/upstream.lock"))?;
    Ok(toml::from_str::<Lock>(&text).context("parsing compat/upstream.lock")?.reference)
}

pub fn sh(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("running {cmd:?}"))?;
    if !status.success() {
        bail!("{cmd:?} failed with {status}");
    }
    Ok(())
}

pub fn checkout() -> Result<()> {
    let l = lock()?;
    let dir = root().join("compat/reference");
    if !dir.join(".git").exists() {
        sh(Command::new("git").args(["clone", "--filter=blob:none", &l.repository]).arg(&dir))?;
    }
    sh(Command::new("git").arg("-C").arg(&dir).args(["checkout", "--quiet", &l.commit]))?;
    let head = String::from_utf8(Command::new("git").arg("-C").arg(&dir).args(["rev-parse", "HEAD"]).output()?.stdout)?;
    if head.trim() != l.commit {
        bail!("reference HEAD {head} != pinned {}", l.commit);
    }
    println!("reference at {}", l.commit);
    Ok(())
}

pub fn build() -> Result<()> {
    let root = root();
    let reference = root.join("compat/reference");
    if !reference.join("go.mod").exists() {
        bail!("compat/reference missing: run `cargo xtask reference-checkout`");
    }
    // The `mrb` tag needs libmruby.a (built with Ruby 2.7, see compat/baseline/README.md).
    let mruby = reference.join("vendorlib/go-mruby/libmruby.a").exists();
    let tags = if mruby { "mrb gops" } else { "gops" };
    println!("building anycable-go (tags: {tags})");
    sh(Command::new("go")
        .args(["build", "-tags", tags, "-o"])
        .arg(root.join("target/reference-bin/anycable-go"))
        .arg("./cmd/anycable-go")
        .current_dir(&reference))?;
    println!("building reference bridge");
    sh(Command::new("go")
        .args(["build", "-o"])
        .arg(root.join("target/reference-adapters/bridge"))
        .arg(".")
        .current_dir(root.join("compat/reference-adapters/bridge")))?;
    // The go-mruby bridge links libmruby.a through cgo (tests `go_mruby`, `mrb`).
    if mruby {
        println!("building mruby reference bridge");
        sh(Command::new("go")
            .args(["build", "-tags", "mrb", "-o"])
            .arg(root.join("target/reference-adapters/mruby-bridge"))
            .arg(".")
            .current_dir(root.join("compat/reference-adapters/mruby-bridge")))?;
    } else {
        println!("skipping mruby reference bridge: vendorlib/go-mruby/libmruby.a is missing");
    }
    Ok(())
}

pub fn sha256_file(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect())
}

pub fn check_inputs() -> Result<()> {
    let l = lock()?;
    let proto = sha256_file(&root().join("crates/cabby-core/proto/rpc.proto"))?;
    if proto != l.rpc_proto_sha256 {
        bail!("crates/cabby-core/proto/rpc.proto sha256 {proto} != upstream {}", l.rpc_proto_sha256);
    }
    println!("rpc.proto matches upstream");

    // The vendored mruby (compat/scripts/vendor-mruby.sh) must match its SHA256SUMS.
    let sys = root().join("crates/cabby-mruby-sys");
    let sums = std::fs::read_to_string(sys.join("SHA256SUMS")).context("crates/cabby-mruby-sys/SHA256SUMS")?;
    let mut count = 0;
    for line in sums.lines().filter(|l| !l.trim().is_empty()) {
        let (want, rel) = line.split_once("  ").with_context(|| format!("bad SHA256SUMS line: {line}"))?;
        let have = sha256_file(&sys.join(rel))?;
        if have != want {
            bail!("crates/cabby-mruby-sys/{rel} sha256 {have} != SHA256SUMS {want}");
        }
        count += 1;
    }
    println!("vendored mruby matches SHA256SUMS ({count} files)");

    let hits = forbidden_strings(&root().join("crates"))?;
    if !hits.is_empty() {
        bail!("telemetry strings under crates/ (D-058):\n{}", hits.join("\n"));
    }
    println!("no telemetry strings under crates/ (D-058)");
    Ok(())
}

/// Strings that must not appear in Cabby's sources: D-058 removed telemetry, and
/// nothing may post to AnyCable's telemetry service again.
const FORBIDDEN_IN_CRATES: &[&str] = &["telemetry.anycable.io", "ANYCABLEPLUS_APP_NAME"];

/// `path:line: string` for every forbidden string in the `.rs` and `.toml` files under `dir`.
fn forbidden_strings(dir: &Path) -> Result<Vec<String>> {
    let mut hits = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("reading {}", d.display()))? {
            let path = entry?.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if !path.extension().is_some_and(|x| x == "rs" || x == "toml") {
                continue;
            }
            let text = std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            for (i, line) in text.lines().enumerate() {
                for s in FORBIDDEN_IN_CRATES.iter().filter(|s| line.contains(*s)) {
                    hits.push(format!("{}:{}: {s}", path.display(), i + 1));
                }
            }
        }
    }
    hits.sort();
    Ok(hits)
}

/// Pinned in compat/upstream.lock ([images] ruby_2_7).
const RUBY_2_7: &str = "ruby@sha256:2347de892e419c7160fc21dec721d5952736909f8c3fbb7f84cb4a07aaf9ce7d";

pub fn build_mruby() -> Result<()> {
    let reference = root().join("compat/reference").canonicalize().context("compat/reference missing")?;
    let lib = reference.join("vendorlib/go-mruby/libmruby.a");
    if lib.exists() {
        println!("libmruby.a already built");
        return Ok(());
    }
    let uid = String::from_utf8(Command::new("id").arg("-u").output()?.stdout)?.trim().to_owned();
    let gid = String::from_utf8(Command::new("id").arg("-g").output()?.stdout)?.trim().to_owned();
    let script = format!(
        "apt-get update -qq >/dev/null && apt-get install -y -qq bison >/dev/null && \
         MRUBY_COMMIT=1.2.0 MRUBY_CONFIG=/src/etc/build_config.rb make libmruby.a >/tmp/build.log 2>&1; \
         rc=$?; tail -5 /tmp/build.log; chown -R {uid}:{gid} /src/vendorlib/go-mruby; exit $rc"
    );
    sh(Command::new("docker").args(["run", "--rm", "-v"]).arg(format!("{}:/src", reference.display())).args([
        "-w",
        "/src/vendorlib/go-mruby",
        RUBY_2_7,
        "bash",
        "-c",
        &script,
    ]))?;
    let sum = sha256_file(&lib)?;
    println!("built libmruby.a (sha256 {sum})");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_strings_are_found() {
        let dir = std::env::temp_dir().join(format!("cabby-xtask-forbidden-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("src")).expect("temp dir");
        std::fs::write(dir.join("src/clean.rs"), "const URL: &str = \"https://example.com\";\n").expect("write");
        std::fs::write(dir.join("src/dirty.rs"), "// a\nconst URL: &str = \"https://telemetry.anycable.io\";\n")
            .expect("write");
        std::fs::write(dir.join("notes.md"), "telemetry.anycable.io\n").expect("write");
        let hits = forbidden_strings(&dir).expect("scan");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(hits, vec![format!("{}:2: telemetry.anycable.io", dir.join("src/dirty.rs").display())]);
    }
}
