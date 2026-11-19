//! Structured-log interval writer (upstream `metrics/printer.go`).

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use cabby_core::logger::{Attr, Level, LogValue, format_json, format_text};

use super::{IntervalWriter, Metrics, MetricsError};

/// Logs the interval snapshot with sorted keys, optionally filtered.
#[derive(Debug)]
pub struct BasePrinter {
    filter: Option<HashSet<String>>,
}

impl BasePrinter {
    pub fn new(filter: Option<Vec<String>>) -> Self {
        Self { filter: filter.map(|f| f.into_iter().collect()) }
    }

    /// The attributes of one printed snapshot: sorted by name, filtered.
    pub fn snapshot_attrs(&self, metrics: &Metrics) -> Vec<Attr> {
        let mut snapshot: Vec<(String, u64)> = metrics.interval_snapshot().into_iter().collect();
        snapshot.sort();
        snapshot
            .into_iter()
            .filter(|(k, _)| self.filter.as_ref().is_none_or(|f| f.contains(k)))
            .map(|(k, v)| Attr::new(&k, LogValue::Uint(v)))
            .collect()
    }
}

/// Log output format (upstream `logger.InitLogger` formats).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// `text`: tint's format (`2006-01-02 15:04:05.000 INF msg key=value`), no colors.
    Text,
    /// `json`: slog's JSON handler.
    Json,
}

/// The process logger: where every component's records go.
struct Logger {
    /// `None` until [`init_logger`]: slog's default logger (stderr, text
    /// handler format, info and above).
    format: Option<LogFormat>,
    min: Level,
    /// Attributes prepended to every record (upstream `slog.With("nodeid", id)`).
    base: Vec<Attr>,
    discard: bool,
}

fn logger() -> &'static RwLock<Logger> {
    static LOGGER: OnceLock<RwLock<Logger>> = OnceLock::new();
    LOGGER.get_or_init(|| RwLock::new(Logger { format: None, min: Level::Info, base: Vec::new(), discard: false }))
}

fn rank(level: Level) -> u8 {
    match level {
        Level::Debug => 0,
        Level::Info => 1,
        Level::Warn => 2,
        Level::Error => 3,
    }
}

/// Upstream `logger.InitLogger(format, level)`: records at `level` and above
/// go to stdout in `format` (`text` or `json`), with the same error texts for
/// unknown values.
pub fn init_logger(format: &str, level: &str) -> Result<(), MetricsError> {
    let min = match level {
        "debug" => Level::Debug,
        "info" => Level::Info,
        "warn" => Level::Warn,
        "error" => Level::Error,
        _ => {
            return Err(MetricsError(format!(
                "unknown log level: {level}.\nAvailable levels are: debug, info, warn, error"
            )));
        }
    };
    let format = match format {
        "text" => LogFormat::Text,
        "json" => LogFormat::Json,
        _ => {
            return Err(MetricsError(format!("unknown log format: {format}.\nAvaialable formats are: text, json")));
        }
    };
    let mut l = logger().write().unwrap_or_else(|e| e.into_inner());
    l.format = Some(format);
    l.min = min;
    l.discard = false;
    Ok(())
}

/// Sets the attributes prepended to every record (upstream `slog.With(...)` on
/// the default logger).
pub fn set_log_attrs(attrs: Vec<Attr>) {
    logger().write().unwrap_or_else(|e| e.into_inner()).base = attrs;
}

/// Drops every record from now on (upstream `slog.New(slog.DiscardHandler)`).
pub fn discard_logs() {
    logger().write().unwrap_or_else(|e| e.into_inner()).discard = true;
}

/// Whether records at `level` are written.
pub fn log_enabled(level: Level) -> bool {
    let l = logger().read().unwrap_or_else(|e| e.into_inner());
    !l.discard && rank(level) >= rank(l.min)
}

/// Writes one record to the process log.
pub(crate) fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let l = logger().read().unwrap_or_else(|e| e.into_inner());
    if l.discard || rank(level) < rank(l.min) {
        return;
    }
    let all: Vec<Attr>;
    let attrs = if l.base.is_empty() {
        attrs
    } else {
        all = l.base.iter().chain(attrs).cloned().collect();
        &all
    };
    match l.format {
        None => eprint!("{}", format_text(level, msg, attrs)),
        Some(LogFormat::Json) => write_stdout(&format_json(level, msg, attrs)),
        Some(LogFormat::Text) => write_stdout(&format_tint(level, msg, attrs)),
    }
}

fn write_stdout(line: &str) {
    use std::io::Write;
    // One write per record, so lines of concurrent records do not interleave.
    let _ = std::io::stdout().lock().write_all(line.as_bytes());
}

/// tint's text format without colors: `<time> <LVL> <msg> <key=value>...`, the
/// time as `2006-01-02 15:04:05.000` (UTC, where Go uses the local time zone).
pub fn format_tint(level: Level, msg: &str, attrs: &[Attr]) -> String {
    let mut out = String::with_capacity(96);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let (y, m, d) = civil_from_days(i64::try_from(secs / 86_400).unwrap_or(0));
    let rem = secs % 86_400;
    let _ = write!(
        out,
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03} ",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        now.subsec_millis()
    );
    out.push_str(match level {
        Level::Debug => "DBG",
        Level::Info => "INF",
        Level::Warn => "WRN",
        Level::Error => "ERR",
    });
    out.push(' ');
    if !msg.is_empty() {
        out.push_str(msg);
        out.push(' ');
    }
    for a in attrs {
        tint_attr(&mut out, "", a);
    }
    out.pop();
    out.push('\n');
    out
}

fn tint_attr(out: &mut String, prefix: &str, a: &Attr) {
    if let LogValue::Group(attrs) = &a.value {
        let prefix = if a.key.is_empty() { prefix.to_owned() } else { format!("{prefix}{}.", a.key) };
        for inner in attrs {
            tint_attr(out, &prefix, inner);
        }
        return;
    }
    tint_string(out, &format!("{prefix}{}", a.key));
    out.push('=');
    match &a.value {
        LogValue::String(s) => tint_string(out, s),
        other => tint_string(out, &other.to_string()),
    }
    out.push(' ');
}

/// tint `appendString` with quoting: Go `strconv.Quote` when the value is empty
/// or holds spaces, `"`, `=` or unprintable characters.
fn tint_string(out: &mut String, s: &str) {
    let quote = s.is_empty() || s.chars().any(|c| c.is_whitespace() || c == '"' || c == '=' || c.is_control());
    if quote {
        let _ = write!(out, "{s:?}");
    } else {
        out.push_str(s);
    }
}

/// Days since 1970-01-01 to a proleptic Gregorian date (H. Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).unwrap_or(1);
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

impl IntervalWriter for BasePrinter {
    fn run(&self, interval: u64) -> Result<(), MetricsError> {
        let mut attrs = vec![Attr::new("context", LogValue::String("metrics".into()))];
        attrs.push(Attr::new("interval", LogValue::Uint(interval)));
        if let Some(filter) = &self.filter {
            let mut fields: Vec<&str> = filter.iter().map(String::as_str).collect();
            fields.sort_unstable();
            attrs.push(Attr::new("fields", LogValue::String(fields.join(","))));
        }
        log(Level::Info, "log metrics", &attrs);
        Ok(())
    }

    fn stop(&self) {}

    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        let mut attrs = vec![Attr::new("context", LogValue::String("metrics".into()))];
        attrs.extend(self.snapshot_attrs(metrics));
        log(Level::Info, "", &attrs);
        Ok(())
    }
}

#[cfg(test)]
mod logger_tests {
    use super::*;

    /// Pins D-023: tint's text format without colors, timestamps in UTC.
    #[test]
    fn deviation_d023_text_logs_use_utc_and_no_colors() {
        let line = format_tint(
            Level::Info,
            "Starting AnyCable",
            &[
                Attr::new("nodeid", LogValue::String("abc".into())),
                Attr::new("x", LogValue::String("a b".into())),
                Attr::new("n", LogValue::Uint(2)),
            ],
        );
        let (time, rest) = line.split_at(23);
        assert_eq!(b' ', time.as_bytes()[10]);
        assert_eq!(" INF Starting AnyCable nodeid=abc x=\"a b\" n=2\n", rest);
        assert_eq!(" WRN k=v\n", &format_tint(Level::Warn, "", &[Attr::new("k", LogValue::String("v".into()))])[23..]);
    }

    #[test]
    fn unknown_levels_and_formats_fail_like_upstream() {
        assert_eq!(
            "unknown log level: trace.\nAvailable levels are: debug, info, warn, error",
            init_logger("text", "trace").unwrap_err().0
        );
        assert_eq!(
            "unknown log format: xml.\nAvaialable formats are: text, json",
            init_logger("xml", "info").unwrap_err().0
        );
    }
}
