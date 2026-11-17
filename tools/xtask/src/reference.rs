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
    let tags = if reference.join("vendorlib/go-mruby/libmruby.a").exists() { "mrb gops" } else { "gops" };
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
    Ok(())
}
