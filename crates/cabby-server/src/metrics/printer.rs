//! Structured-log interval writer (upstream `metrics/printer.go`).

use std::collections::HashSet;

use cabby_core::logger::{Attr, Level, LogValue, format_text};

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

/// Writes one record to the process log (stderr, slog text format, info and
/// above). Interim sink until the server's logger is wired in.
pub(crate) fn log(level: Level, msg: &str, attrs: &[Attr]) {
    if level == Level::Debug {
        return;
    }
    eprint!("{}", format_text(level, msg, attrs));
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
