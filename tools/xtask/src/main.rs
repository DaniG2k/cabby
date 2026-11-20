//! `cargo xtask`: maintenance tasks for the Cabby migration (SPEC.md §4, §5, §9).

mod asan;
mod inventory;
mod manifest;
mod reference;
mod runner;

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(about = "Cabby maintenance tasks")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Clone/check out the pinned upstream reference into compat/reference.
    ReferenceCheckout,
    /// Build the Go reference binary and the test bridges into target/.
    ReferenceBuild,
    /// Build libmruby 1.2.0 for the reference (`mrb` tag) in the pinned Ruby 2.7
    /// image, as upstream CI does (go-mruby's Rakefile needs Ruby < 3).
    ReferenceMruby,
    /// Regenerate compat/test-map.json from the AST inventory, baseline runs and
    /// Rust `/// upstream:` annotations. Manually curated fields are preserved.
    Inventory,
    /// Run the translated suite against a backend and reconcile with the manifest.
    Test {
        #[arg(long, value_enum, default_value = "rust")]
        backend: Backend,
        /// Record new outcomes in the manifest (promote passing cases).
        #[arg(long)]
        record: bool,
        /// Extra filter passed to each test binary.
        filter: Option<String>,
    },
    /// Verify vendored upstream inputs (rpc.proto) against upstream.lock.
    CheckInputs,
    /// Run the mruby binding's tests (cabby-mruby-sys) under AddressSanitizer
    /// (x86_64 Linux; docs/mruby.md).
    Asan,
    /// Print translation / reference / implementation coverage.
    Report {
        /// Also break coverage down per upstream file directory.
        #[arg(long)]
        by_package: bool,
    },
    /// Mark translated cases as reviewed after an independent fidelity review.
    /// Patterns are case IDs, or prefixes ending in `/` or `#` (e.g.
    /// `26a1a703:identity/`); `--except` takes precedence.
    MarkReviewed {
        patterns: Vec<String>,
        #[arg(long)]
        except: Vec<String>,
    },
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq, Debug)]
pub enum Backend {
    Rust,
    Go,
}

impl Backend {
    pub fn as_str(self) -> &'static str {
        match self {
            Backend::Rust => "rust",
            Backend::Go => "go",
        }
    }
}

pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("workspace root")
}

fn main() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::ReferenceCheckout => reference::checkout(),
        Cmd::ReferenceBuild => reference::build(),
        Cmd::ReferenceMruby => reference::build_mruby(),
        Cmd::Inventory => inventory::run(),
        Cmd::Test { backend, record, filter } => runner::run(backend, record, filter.as_deref()),
        Cmd::CheckInputs => reference::check_inputs(),
        Cmd::Asan => asan::run(),
        Cmd::Report { by_package } => manifest::report_with(by_package),
        Cmd::MarkReviewed { patterns, except } => manifest::mark_reviewed(&patterns, &except),
    }
}
