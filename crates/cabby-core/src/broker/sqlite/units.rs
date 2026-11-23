//! Every unit conversion of the SQLite broker (spec §4). History timestamps
//! and stream deadlines are unix seconds, as `Entry::timestamp` and the memory
//! broker's stream deadline; session expiries are unix milliseconds, as the
//! memory broker's session expiration queue. Offsets are `u64` in memory and
//! `INTEGER` (`i64`) in SQLite.

use std::time::SystemTime;

use crate::broker::{Clock, unix_millis, unix_secs};

/// Unix seconds of `t`.
pub(super) fn secs(t: SystemTime) -> i64 {
    unix_secs(t)
}

/// Unix milliseconds of `t`.
pub(super) fn millis(t: SystemTime) -> i64 {
    unix_millis(t)
}

pub(super) fn now_secs(clock: &dyn Clock) -> i64 {
    secs(clock.now())
}

pub(super) fn now_millis(clock: &dyn Clock) -> i64 {
    millis(clock.now())
}

/// When a stream last written at `ts_secs` may be dropped: ten history TTLs
/// later, as `Memstream::append` computes it.
pub(super) fn stream_deadline(ts_secs: i64, history_ttl: i64) -> i64 {
    ts_secs.saturating_add(history_ttl.saturating_mul(10))
}

/// The oldest timestamp still within `history_ttl` at `now_secs`: older
/// entries are expired, as `Memstream::expire` drops them.
pub(super) fn history_cutoff(now_secs: i64, history_ttl: i64) -> i64 {
    now_secs.saturating_sub(history_ttl)
}

/// An offset as an SQLite integer (offsets beyond `i64::MAX` don't occur).
pub(super) fn offset_to_sql(offset: u64) -> i64 {
    i64::try_from(offset).unwrap_or(i64::MAX)
}

/// A stored offset; `None` for a negative (damaged) value.
pub(super) fn offset_from_sql(seq: i64) -> Option<u64> {
    u64::try_from(seq).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::broker::test_clock::{T0, TestClock};
    use crate::broker::{Broker, Config, Memory};

    #[test]
    fn clock_readings_are_unix_seconds_and_milliseconds() {
        let clock = TestClock::new();
        clock.advance(Duration::from_millis(1_500));
        assert_eq!(T0 + 1, now_secs(clock.as_ref()));
        assert_eq!(T0 * 1000 + 1_500, now_millis(clock.as_ref()));
    }

    #[test]
    fn stream_deadline_matches_the_memory_broker() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 2;
        let memory = Memory::new(None, None, &config, clock.clone());
        memory.add("s", "a");

        let deadline = stream_deadline(T0, config.history_ttl);
        clock.advance(Duration::from_secs((deadline - T0).unsigned_abs()));
        memory.expire();
        assert!(memory.history_since("s", 0).unwrap().is_some(), "kept at its deadline");

        clock.advance(Duration::from_secs(1));
        memory.expire();
        assert_eq!(None, memory.history_since("s", 0).unwrap(), "dropped after it");
    }

    #[test]
    fn history_cutoff_matches_the_memory_broker() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 10;
        let memory = Memory::new(None, None, &config, clock.clone());
        memory.add("s", "a");

        clock.advance(Duration::from_secs(10));
        memory.expire();
        assert_eq!(T0, history_cutoff(T0 + 10, 10));
        assert_eq!(1, memory.history_since("s", 0).unwrap().unwrap_or_default().len(), "ts == cutoff is kept");

        clock.advance(Duration::from_secs(1));
        memory.expire();
        assert!(T0 < history_cutoff(T0 + 11, 10));
        assert_eq!(0, memory.history_since("s", 0).unwrap().unwrap_or_default().len(), "ts < cutoff expires");
    }

    #[test]
    fn offsets_convert_to_sql_integers() {
        assert_eq!(5, offset_to_sql(5));
        assert_eq!(i64::MAX, offset_to_sql(u64::MAX));
        assert_eq!(Some(5), offset_from_sql(5));
        assert_eq!(None, offset_from_sql(-1));
    }
}
