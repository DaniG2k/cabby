//! Cabby's version and the AnyCable server version it is compatible with.

/// The AnyCable server version Cabby is compatible with: the pinned upstream
/// (`compat/upstream.lock`). Sent as `x-anycable-version`, which clients and
/// AnyCable tooling read as a protocol level, so it never drops to Cabby's `0.x`.
pub const ANYCABLE_COMPAT: &str = "1.6.21";

/// Cabby's own version (the workspace `version`).
pub const CABBY: &str = env!("CARGO_PKG_VERSION");

/// `0.5.0 (AnyCable 1.6.21 compatible)`: the start line (D-035) and, after the
/// program name, `--version` (D-059).
pub fn label() -> String {
    format!("{CABBY} (AnyCable {ANYCABLE_COMPAT} compatible)")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins D-035 and D-059: Cabby's version, then the AnyCable version it is
    /// compatible with.
    #[test]
    fn label_is_the_cabby_version_and_the_compatible_anycable_version() {
        assert_eq!(label(), format!("{} (AnyCable 1.6.21 compatible)", env!("CARGO_PKG_VERSION")));
        assert_eq!(ANYCABLE_COMPAT, "1.6.21");
        assert_eq!(CABBY, env!("CARGO_PKG_VERSION"));
    }
}
