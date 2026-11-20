//! Deeply nested JSON, decoded like Go's `encoding/json`.
//!
//! Go decodes documents nested up to 10000 levels ([`MAX_NESTING_DEPTH`]) and
//! rejects deeper ones with `exceeded max depth`. serde_json stops at 128 levels
//! by default to protect the stack: its decoder, `Value`'s `Drop` and the
//! serializers all recurse once per level. Cabby keeps that limit on ordinary
//! threads and handles the rare deeper message on a short-lived thread with a
//! stack sized for Go's limit ([`with_deep_nesting`]). On that thread, the
//! decoders built with [`deserializer`] (client messages) accept up to Go's limit.
//! A value decoded there must not leave the thread: drop it there.

use std::cell::Cell;

/// Go's `encoding/json` nesting limit (`maxNestingDepth`).
pub const MAX_NESTING_DEPTH: usize = 10_000;

/// Stack of a deep-nesting thread. Measured on a debug build, decoding, handling
/// (a perform that formats the data in its error) and dropping a client message
/// nested 10000 levels deep needs between 8 and 16 MiB; this leaves room for
/// other command paths, and release builds need less. Only the touched pages
/// are committed.
const DEEP_STACK_SIZE: usize = 64 << 20;

thread_local! {
    static DEEP: Cell<bool> = const { Cell::new(false) };
}

/// Whether the current thread may decode beyond serde_json's default limit.
pub fn is_deep_thread() -> bool {
    DEEP.with(Cell::get)
}

/// Whether `err` (a decoding error's text) is serde_json's recursion limit, on
/// a thread that has it: the input may still be within Go's limit.
pub fn hit_default_limit(err: &str) -> bool {
    !is_deep_thread() && err.starts_with("recursion limit exceeded")
}

/// Runs `f` on a thread whose stack fits Go's nesting limit, with deep decoding
/// enabled, and waits for it. `None` when no thread could be started.
pub fn with_deep_nesting<R: Send>(f: impl FnOnce() -> R + Send) -> Option<R> {
    std::thread::scope(|s| {
        let handle = std::thread::Builder::new()
            .name("cabby-deep-json".into())
            .stack_size(DEEP_STACK_SIZE)
            .spawn_scoped(s, || {
                DEEP.with(|d| d.set(true));
                f()
            })
            .ok()?;
        Some(handle.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
    })
}

/// Fails with Go's scanner error when arrays and objects in `raw` (outside
/// strings) nest deeper than `limit`.
fn check_depth(raw: &[u8], limit: usize) -> Result<(), String> {
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for &b in raw {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                if depth > limit {
                    return Err(format!("invalid character '{}' exceeded max depth", b as char));
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

/// A serde_json deserializer over `raw` with the current thread's nesting limit:
/// serde_json's default (128), or Go's on a [`with_deep_nesting`] thread, where
/// a deeper document fails with Go's error text.
pub fn deserializer(raw: &[u8]) -> Result<serde_json::Deserializer<serde_json::de::SliceRead<'_>>, String> {
    let mut de = serde_json::Deserializer::from_slice(raw);
    if is_deep_thread() {
        check_depth(raw, MAX_NESTING_DEPTH)?;
        de.disable_recursion_limit();
    }
    Ok(de)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> String {
        format!("{}{}", "[".repeat(depth), "]".repeat(depth))
    }

    #[test]
    fn depth_is_counted_outside_strings() {
        assert!(check_depth(br#"{"a":"[[[[\"[[["}"#, 1).is_ok());
        assert_eq!(Err("invalid character '[' exceeded max depth".into()), check_depth(b"{\"a\":[]}", 1));
        assert!(check_depth(nested(MAX_NESTING_DEPTH).as_bytes(), MAX_NESTING_DEPTH).is_ok());
        assert!(check_depth(nested(MAX_NESTING_DEPTH + 1).as_bytes(), MAX_NESTING_DEPTH).is_err());
    }

    #[test]
    fn ordinary_threads_keep_the_default_limit() {
        let doc = nested(200);
        let mut de = deserializer(doc.as_bytes()).unwrap();
        let err = <serde_json::Value as serde::Deserialize>::deserialize(&mut de).unwrap_err().to_string();
        assert!(hit_default_limit(&err), "{err}");
    }

    #[test]
    fn deep_threads_decode_up_to_go_limit() {
        let ok = with_deep_nesting(|| {
            let doc = nested(MAX_NESTING_DEPTH);
            let mut de = deserializer(doc.as_bytes()).unwrap();
            let v = <serde_json::Value as serde::Deserialize>::deserialize(&mut de).map(|_| ());
            let too_deep = nested(MAX_NESTING_DEPTH + 1);
            (v.is_ok(), deserializer(too_deep.as_bytes()).err())
        });
        assert_eq!(Some((true, Some("invalid character '[' exceeded max depth".into()))), ok);
    }
}
