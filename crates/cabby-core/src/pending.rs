//! Typed marker for behavior that has not been implemented yet.
//!
//! Stubs never return plausible values (`docs/translating-tests.md`, "Hard rules"). They panic with a payload
//! starting with [`MARKER`], which the compatibility runner classifies as
//! *pending* rather than *pass* or an ordinary *fail*.

/// Prefix of every pending panic payload. Matched by `cargo xtask test`.
pub const MARKER: &str = "CABBY_PENDING:";

/// Marks unimplemented behavior. The id names the upstream unit being ported,
/// e.g. `pending!("protocol::new_connect_message")`.
#[macro_export]
macro_rules! pending {
    ($id:literal) => {
        ::std::panic!("{}{}", $crate::pending::MARKER, $id)
    };
}

/// Returns the pending id if `message` is a pending marker payload.
pub fn pending_id(message: &str) -> Option<&str> {
    message.find(MARKER).map(|i| {
        let rest = &message[i + MARKER.len()..];
        rest.split(|c: char| c.is_whitespace() || c == '\'' || c == '"').next().unwrap_or(rest)
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn marker_is_recognized_in_panic_output() {
        let line = "thread 'x' panicked at src/lib.rs:1:1:\nCABBY_PENDING:protocol::parse 'y'";
        assert_eq!(super::pending_id(line), Some("protocol::parse"));
        assert_eq!(super::pending_id("assertion failed"), None);
    }
}
