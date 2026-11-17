//! Facades over the implementation under test, one module per upstream package.
//!
//! Each function runs the Rust implementation or calls the Go reference through
//! the bridge, converting Go's JSON shapes into Cabby types. Facades only marshal;
//! they must not contain behavior of their own (SPEC.md §5).

pub mod benchi;
pub mod protocol;
pub mod ws;
