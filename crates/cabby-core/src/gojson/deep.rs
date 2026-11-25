//! Deeply nested JSON, decoded like Go's `encoding/json`.
//!
//! Go decodes documents nested up to 10000 levels ([`MAX_NESTING_DEPTH`]) and
//! rejects deeper ones with `exceeded max depth`. serde_json stops at 128 levels
//! by default to protect the stack: its decoder, `Value`'s `Drop` and the
//! serializers all recurse once per level. Cabby keeps that limit on ordinary
//! threads and handles the rare deeper message on a short-lived thread with a
//! stack sized for Go's limit ([`with_deep_nesting`]). On that thread, the
//! decoders built with [`deserializer`] (client messages) accept up to Go's limit.
//! A value decoded there must not leave the thread: drop it there, or keep
//! [`bounded`] of it.
//!
//! At most [`MAX_CONCURRENT_DEEP_THREADS`] such threads run at once (D-056): a
//! message that finds them all busy waits up to [`DEEP_PERMIT_WAIT`] and is
//! otherwise refused ([`DeepNestingError::Busy`]), so many clients sending deep
//! messages together cannot multiply big stacks.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use serde_json::Value;

/// Go's `encoding/json` nesting limit (`maxNestingDepth`).
pub const MAX_NESTING_DEPTH: usize = 10_000;

/// The nesting of a value that any thread may keep: what serde_json decodes by
/// default (127 levels), inside an object of its own (a presence event).
pub const MAX_KEPT_DEPTH: usize = 126;

/// Stack of a deep-nesting thread. Measured on a debug build, decoding, handling
/// (a perform that formats the data in its error) and dropping a client message
/// nested 10000 levels deep needs between 8 and 16 MiB; this leaves room for
/// other command paths, and release builds need less. Only the touched pages
/// are committed.
const DEEP_STACK_SIZE: usize = 64 << 20;

/// How many deep-nesting threads may run at once, process-wide (D-056).
pub const MAX_CONCURRENT_DEEP_THREADS: usize = 4;

/// How long a deep message waits for a deep-nesting thread before it is refused.
pub const DEEP_PERMIT_WAIT: Duration = Duration::from_millis(250);

thread_local! {
    static DEEP: Cell<bool> = const { Cell::new(false) };
}

/// Why [`with_deep_nesting`] did not run its closure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeepNestingError {
    /// [`MAX_CONCURRENT_DEEP_THREADS`] were busy for [`DEEP_PERMIT_WAIT`].
    Busy,
    /// The thread could not be started.
    Spawn,
}

impl std::fmt::Display for DeepNestingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Busy => "too many deeply nested messages in progress",
            Self::Spawn => "failed to start a thread for a deeply nested message",
        })
    }
}

/// A counting semaphore with a bounded wait (no fairness needed: waiters only
/// ever wait [`DEEP_PERMIT_WAIT`]).
struct Limiter {
    in_use: Mutex<usize>,
    released: Condvar,
    max: usize,
}

struct Permit<'a>(&'a Limiter);

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut in_use = self.0.in_use.lock().unwrap_or_else(PoisonError::into_inner);
        *in_use -= 1;
        drop(in_use);
        self.0.released.notify_one();
    }
}

impl Limiter {
    const fn new(max: usize) -> Self {
        Self { in_use: Mutex::new(0), released: Condvar::new(), max }
    }

    fn acquire(&self, wait: Duration) -> Option<Permit<'_>> {
        let deadline = Instant::now() + wait;
        let mut in_use = self.in_use.lock().unwrap_or_else(PoisonError::into_inner);
        while *in_use >= self.max {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            in_use = self.released.wait_timeout(in_use, left).unwrap_or_else(PoisonError::into_inner).0;
        }
        *in_use += 1;
        Some(Permit(self))
    }

    fn run<R: Send>(&self, wait: Duration, f: impl FnOnce() -> R + Send) -> Result<R, DeepNestingError> {
        let _permit = self.acquire(wait).ok_or(DeepNestingError::Busy)?;
        std::thread::scope(|s| {
            let handle = std::thread::Builder::new()
                .name("cabby-deep-json".into())
                .stack_size(DEEP_STACK_SIZE)
                .spawn_scoped(s, || {
                    DEEP.with(|d| d.set(true));
                    f()
                })
                .map_err(|_| DeepNestingError::Spawn)?;
            Ok(handle.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
        })
    }
}

static DEEP_THREADS: Limiter = Limiter::new(MAX_CONCURRENT_DEEP_THREADS);

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
/// enabled, and waits for it. Fails when [`MAX_CONCURRENT_DEEP_THREADS`] stay
/// busy for [`DEEP_PERMIT_WAIT`], or when no thread could be started.
pub fn with_deep_nesting<R: Send>(f: impl FnOnce() -> R + Send) -> Result<R, DeepNestingError> {
    DEEP_THREADS.run(DEEP_PERMIT_WAIT, f)
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

/// Whether arrays and objects in `value` nest deeper than `limit` (looks at no
/// more than `limit` levels).
fn nests_deeper(value: &Value, limit: usize) -> bool {
    match value {
        Value::Array(items) => limit == 0 || items.iter().any(|v| nests_deeper(v, limit - 1)),
        Value::Object(map) => limit == 0 || map.values().any(|v| nests_deeper(v, limit - 1)),
        _ => false,
    }
}

/// A copy of `value` that may leave a deep-nesting thread: nested deeper than
/// [`MAX_KEPT_DEPTH`], it becomes its JSON text, a string (as other data nested
/// too deep does, D-011).
pub fn bounded(value: &Value) -> Value {
    if nests_deeper(value, MAX_KEPT_DEPTH) { Value::String(super::to_string(value)) } else { value.clone() }
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
        assert_eq!(Ok((true, Some("invalid character '[' exceeded max depth".into()))), ok);
    }

    /// Deviation D-073: a value kept beyond its deep-nesting thread is its JSON
    /// text when it nests deeper than 126 levels.
    #[test]
    fn deviation_d073_kept_values_are_bounded() {
        fn objects(depth: usize) -> String {
            format!("{}1{}", r#"{"a":"#.repeat(depth), "}".repeat(depth))
        }
        // (whether it became a string, its text), read on the deep thread.
        let kept = |text: String| {
            with_deep_nesting(|| {
                let mut de = deserializer(text.as_bytes()).unwrap();
                match bounded(&<Value as serde::Deserialize>::deserialize(&mut de).unwrap()) {
                    Value::String(text) => (true, text),
                    json => (false, crate::gojson::to_string(&json)),
                }
            })
            .unwrap()
        };
        for doc in [nested, objects] {
            for depth in [1, 125, 126] {
                assert_eq!((false, doc(depth)), kept(doc(depth)), "{depth} levels stay JSON");
            }
            for depth in [127, 128, 4_999, 5_000, 5_001, 10_000] {
                assert_eq!((true, doc(depth)), kept(doc(depth)), "{depth} levels become text");
            }
        }
    }

    /// Deviation D-056: at most `max` deep threads run at once; a call that finds
    /// them busy for the whole wait is refused, and a permit freed during the
    /// wait is taken.
    #[test]
    fn concurrent_deep_threads_are_capped() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier, mpsc};

        const MAX: usize = 3;
        const CALLERS: usize = 12;
        let limiter = Arc::new(Limiter::new(MAX));
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Barrier::new(MAX + 1));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));

        // `MAX` callers hold their threads until released.
        let holders: Vec<_> = (0..MAX)
            .map(|_| {
                let (limiter, running, peak, entered, release_rx) =
                    (limiter.clone(), running.clone(), peak.clone(), entered.clone(), release_rx.clone());
                std::thread::spawn(move || {
                    limiter.run(Duration::from_secs(30), || {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        assert!(is_deep_thread());
                        entered.wait();
                        release_rx.lock().unwrap().recv().unwrap();
                        running.fetch_sub(1, Ordering::SeqCst);
                    })
                })
            })
            .collect();
        entered.wait();

        // Everyone else finds the threads busy and is refused after the wait.
        let refused: Vec<_> = (0..CALLERS - MAX)
            .map(|_| {
                let limiter = limiter.clone();
                std::thread::spawn(move || limiter.run(Duration::from_millis(50), || ()))
            })
            .collect();
        for r in refused {
            assert_eq!(Err(DeepNestingError::Busy), r.join().unwrap());
        }

        // A waiter gets the permit a holder frees.
        let limiter2 = limiter.clone();
        let waiter = std::thread::spawn(move || limiter2.run(Duration::from_secs(30), || 7));
        release_tx.send(()).unwrap();
        assert_eq!(Ok(7), waiter.join().unwrap());

        for _ in 1..MAX {
            release_tx.send(()).unwrap();
        }
        for h in holders {
            assert_eq!(Ok(()), h.join().unwrap());
        }
        assert_eq!(MAX, peak.load(Ordering::SeqCst));
        assert_eq!(0, *limiter.in_use.lock().unwrap());
        // Permits come back after a panicking closure too.
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| limiter.run(Duration::ZERO, || panic!("x"))));
        assert!(r.is_err());
        assert_eq!(0, *limiter.in_use.lock().unwrap());
    }
}
