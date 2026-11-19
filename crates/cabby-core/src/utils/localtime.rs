//! Wall-clock time in the local time zone, for log timestamps.
//!
//! Go formats log times in `time.Local`, which it reads once at startup from `TZ`
//! or `/etc/localtime`. Cabby does the same: the system zone is loaded on first use
//! and kept (a zone change needs a restart, as in Go). Without a readable zone the
//! time is UTC, as Go falls back to UTC.

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use jiff::Timestamp;
use jiff::tz::TimeZone;

/// A broken-down local time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalTime {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub millis: u32,
    /// Offset from UTC in seconds (east positive).
    pub offset: i32,
}

impl LocalTime {
    /// The current local time.
    pub fn now() -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let secs = i64::try_from(now.as_secs()).unwrap_or(0);
        Self::at(secs, now.subsec_millis(), offset_at(secs))
    }

    /// The time `unix_secs` (plus `millis`) at a UTC offset of `offset` seconds.
    pub fn at(unix_secs: i64, millis: u32, offset: i32) -> Self {
        let local = unix_secs + i64::from(offset);
        let (year, month, day) = civil_from_days(local.div_euclid(86_400));
        let rem = local.rem_euclid(86_400) as u32;
        LocalTime { year, month, day, hour: rem / 3600, minute: rem % 3600 / 60, second: rem % 60, millis, offset }
    }

    /// Go's RFC 3339 offset suffix: `Z` for UTC, otherwise `+07:00` / `-03:30`.
    pub fn rfc3339_offset(&self) -> String {
        if self.offset == 0 {
            return "Z".to_owned();
        }
        let sign = if self.offset < 0 { '-' } else { '+' };
        let abs = self.offset.unsigned_abs();
        format!("{sign}{:02}:{:02}", abs / 3600, abs % 3600 / 60)
    }
}

/// The local zone's UTC offset at `unix_secs`, in seconds.
fn offset_at(unix_secs: i64) -> i32 {
    static ZONE: OnceLock<TimeZone> = OnceLock::new();
    let zone = ZONE.get_or_init(|| TimeZone::try_system().unwrap_or(TimeZone::UTC));
    Timestamp::from_second(unix_secs).map_or(0, |ts| zone.to_offset(ts).seconds())
}

/// Days since 1970-01-01 to a proleptic Gregorian date (H. Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn breaks_down_times_at_an_offset() {
        // 2026-11-18T11:06:40Z
        let utc = LocalTime::at(1_795_000_000, 7, 0);
        assert_eq!((utc.year, utc.month, utc.day, utc.hour, utc.minute, utc.second), (2026, 11, 18, 11, 6, 40));
        assert_eq!(utc.rfc3339_offset(), "Z");

        // Bangkok (+07:00) crosses midnight.
        let bkk = LocalTime::at(1_795_000_000 + 13 * 3600, 0, 7 * 3600);
        assert_eq!((bkk.year, bkk.month, bkk.day, bkk.hour), (2026, 11, 19, 7));
        assert_eq!(bkk.rfc3339_offset(), "+07:00");

        // Negative half-hour offsets and dates before the epoch.
        let nfld = LocalTime::at(0, 0, -(3 * 3600 + 1800));
        assert_eq!((nfld.year, nfld.month, nfld.day, nfld.hour, nfld.minute), (1969, 12, 31, 20, 30));
        assert_eq!(nfld.rfc3339_offset(), "-03:30");

        // Leap day.
        let leap = LocalTime::at(951_786_061, 0, 0);
        assert_eq!((leap.year, leap.month, leap.day), (2000, 2, 29));
    }
}
