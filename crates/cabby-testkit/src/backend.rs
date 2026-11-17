//! Selects the implementation under test.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// Cabby (this repository).
    Rust,
    /// The pinned Go reference, through the bridge or its binary.
    Go,
}

/// The backend selected by `CABBY_BACKEND` (`rust` by default).
pub fn backend() -> Backend {
    static B: OnceLock<Backend> = OnceLock::new();
    *B.get_or_init(|| match std::env::var("CABBY_BACKEND").as_deref() {
        Ok("go") => Backend::Go,
        Ok("rust") | Err(_) => Backend::Rust,
        Ok(other) => panic!("CABBY_BACKEND must be `rust` or `go`, got {other:?}"),
    })
}
