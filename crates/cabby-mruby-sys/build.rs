//! Compiles the vendored mruby 1.2.0 the way upstream's reference build did
//! (`etc/build_config.rb`: gcc toolchain, default gembox, mruby-env), from the
//! checked-in sources and generated files, plus Cabby's C shim. No Ruby or
//! bison is needed. See docs/mruby.md and compat/scripts/vendor-mruby.sh.

use std::path::{Path, PathBuf};

/// Gems of the default gembox compiled into libmruby.a (the command-line tool
/// gems are not part of the library). mruby-compiler and mruby-env are listed
/// separately.
const GEMS_WITH_C: &[&str] = &[
    "mruby-array-ext",
    "mruby-fiber",
    "mruby-hash-ext",
    "mruby-kernel-ext",
    "mruby-math",
    "mruby-numeric-ext",
    "mruby-object-ext",
    "mruby-objectspace",
    "mruby-print",
    "mruby-proc-ext",
    "mruby-random",
    "mruby-range-ext",
    "mruby-sprintf",
    "mruby-string-ext",
    "mruby-struct",
    "mruby-symbol-ext",
    "mruby-time",
];

fn c_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|e| e.expect("directory entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "c"))
        .collect();
    files.sort();
    files
}

fn generated(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .map(|e| e.expect("directory entry").path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            generated(&p, out);
        } else if p.extension().is_some_and(|x| x == "c") {
            out.push(p);
        }
    }
}

fn main() {
    println!("cargo::rustc-check-cfg=cfg(cabby_mruby)");
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=shim");
    println!("cargo::rerun-if-changed=SHA256SUMS");

    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let endian = std::env::var("CARGO_CFG_TARGET_ENDIAN").unwrap_or_default();
    let width = std::env::var("CARGO_CFG_TARGET_POINTER_WIDTH").unwrap_or_default();
    let wanted = std::env::var_os("CARGO_FEATURE_VENDORED").is_some();
    if !wanted || !matches!(os.as_str(), "linux" | "macos") {
        return;
    }
    // The checked-in mrbc bytecode is little-endian and the shim assumes the
    // 16-byte unboxed mrb_value of 64-bit targets.
    assert!(endian == "little" && width == "64", "the vendored mruby supports 64-bit little-endian targets only");

    let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let mruby = root.join("mruby");

    let mut files = Vec::new();
    // Core, with gc.c and vm.c compiled through shim/gc_live.c and
    // shim/vm_live.c (see there).
    files.extend(c_files(&mruby.join("src")).into_iter().filter(|p| !p.ends_with("gc.c") && !p.ends_with("vm.c")));
    files.push(root.join("shim/gc_live.c"));
    files.push(root.join("shim/vm_live.c"));
    files.push(mruby.join("mrbgems/mruby-compiler/core/codegen.c"));
    for gem in GEMS_WITH_C {
        files.extend(c_files(&mruby.join("mrbgems").join(gem).join("src")));
    }
    files.push(root.join("mruby-env/src/env.c"));
    generated(&root.join("generated"), &mut files);
    files.push(root.join("shim/cabby_mruby.c"));

    cc::Build::new()
        .files(&files)
        .include(mruby.join("include"))
        // y.tab.c includes node.h and lex.def from the compiler's sources.
        .include(mruby.join("mrbgems/mruby-compiler/core"))
        // Upstream's flags (libmruby.flags.mak): -std=gnu99 -O3.
        .std("gnu99")
        .opt_level(3)
        .debug(false)
        .warnings(false)
        .cargo_warnings(false)
        .compile("mruby");

    if os == "linux" {
        println!("cargo::rustc-link-lib=m");
    }
    println!("cargo::rustc-cfg=cabby_mruby");
}
