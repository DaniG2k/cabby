//! Scenario entry points (upstream `scenarios/<name>/main.go`).
//!
//! Each scenario is a testable `run(args, stdout, stderr, ..) -> exit code`, like
//! upstream. Flags keep upstream names, Go `flag` package syntax (`-c 10`, `--c=10`,
//! boolean flags without a value), Go duration syntax (`500ms`, `1s`, `2m`) and the
//! key=value stdout contract. Two flags are new because the server is external:
//!
//! * `--sut <path>`: server binary to launch (Cabby or `anycable-go`); defaults to
//!   `$CABBY_BENCH_SUT`;
//! * `--server-arg <flag>` (repeatable): extra server CLI flags.
//!
//! `soak` is a Cabby addition without an upstream counterpart (memory stability,
//! [`soak`]).
//!
//! `--protocol=pusher` (both upstream scenarios) switches clients, publisher and server to the
//! Pusher protocol ([`crate::pusher`]).
//!
//! Go-runtime flags have no meaning for an external Rust harness and are not
//! accepted: stress_publications' `--cpu-profile`, `--mem-profile`,
//! `--block-profile`, `--mutex-profile` and reconnect_floods' `--pprof` (profile the
//! server process with external tools instead).

use std::collections::HashMap;
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::cancel::CancelToken;

pub mod reconnect_floods;
pub mod soak;
pub mod stress_publications;

/// Environment variable naming the default server binary (`--sut`).
pub const SUT_ENV: &str = "CABBY_BENCH_SUT";

/// Parses a Go `time.ParseDuration` string (`300ms`, `1.5s`, `2m`, `-1ns`).
/// Negative durations are an error here; see [`parse_go_duration_ns`].
pub fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let ns = parse_go_duration_ns(s)?;
    u64::try_from(ns).map(Duration::from_nanos).map_err(|_| format!("time: negative duration {s:?}"))
}

/// Parses a Go duration into signed nanoseconds.
pub fn parse_go_duration_ns(s: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration {s:?}");
    let (neg, mut rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut total: f64 = 0.0;
    while !rest.is_empty() {
        let num_end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
        let num = &rest[..num_end];
        if num.is_empty() || num == "." || num.matches('.').count() > 1 {
            return Err(invalid());
        }
        rest = &rest[num_end..];
        let unit_end = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let scale: f64 = match unit {
            "" => return Err(format!("time: missing unit in duration {s:?}")),
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            u => return Err(format!("time: unknown unit {u:?} in duration {s:?}")),
        };
        let v: f64 = num.parse().map_err(|_| invalid())?;
        total += v * scale;
    }
    if total > i64::MAX as f64 {
        return Err(invalid());
    }
    let ns = total.round() as i64;
    Ok(if neg { -ns } else { ns })
}

/// Formats a duration like Go's `time.Duration.String` (`1m30s`, `500ms`, `0s`).
pub(crate) fn go_duration_string(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns == 0 {
        return "0s".into();
    }
    if ns < 1_000 {
        return format!("{ns}ns");
    }
    if ns < 1_000_000 {
        return format!("{}µs", trim_float(ns as f64 / 1e3));
    }
    if ns < 1_000_000_000 {
        return format!("{}ms", trim_float(ns as f64 / 1e6));
    }
    let total_s = ns as f64 / 1e9;
    let h = (ns / 3_600_000_000_000) as u64;
    let m = ((ns / 60_000_000_000) % 60) as u64;
    let s = total_s - (h * 3600 + m * 60) as f64;
    let mut out = String::new();
    if h > 0 {
        out.push_str(&format!("{h}h"));
    }
    if h > 0 || m > 0 {
        out.push_str(&format!("{m}m"));
    }
    out.push_str(&format!("{}s", trim_float(s)));
    out
}

fn trim_float(v: f64) -> String {
    let s = format!("{v:.9}");
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_owned()
}

/// Exit code for an interrupted run (SIGINT/SIGTERM).
pub const EXIT_INTERRUPTED: i32 = 130;
/// Exit code for flag errors.
pub const EXIT_USAGE: i32 = 2;

// ------------------------------------------------------------------ flags

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Int,
    Float,
    Bool,
    Duration,
    Str,
    /// A repeatable string flag.
    List,
}

#[derive(Clone, Copy)]
pub(crate) struct FlagDef {
    pub name: &'static str,
    pub kind: Kind,
    /// Default as Go prints it in the usage (`""` for the zero value).
    pub default: &'static str,
    pub usage: &'static str,
}

/// Parse errors of [`parse`]: `-h`/`-help`, or a message (usage already printed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FlagError {
    Help,
    Invalid(String),
}

/// Parsed flag values: the raw text of each flag that was set.
#[derive(Debug, Default)]
pub(crate) struct Parsed {
    values: HashMap<&'static str, Vec<String>>,
}

impl Parsed {
    fn raw(&self, name: &str) -> Option<&str> {
        self.values.get(name).and_then(|v| v.last()).map(String::as_str)
    }

    pub fn int(&self, name: &str, default: i64) -> i64 {
        self.raw(name).and_then(parse_int).unwrap_or(default)
    }

    pub fn float(&self, name: &str, default: f64) -> f64 {
        self.raw(name).and_then(|v| v.parse().ok()).unwrap_or(default)
    }

    pub fn boolean(&self, name: &str) -> bool {
        self.raw(name).and_then(parse_bool).unwrap_or(false)
    }

    pub fn duration_ns(&self, name: &str, default: i64) -> i64 {
        self.raw(name).and_then(|v| parse_go_duration_ns(v).ok()).unwrap_or(default)
    }

    pub fn duration(&self, name: &str, default: Duration) -> Duration {
        let d = i64::try_from(default.as_nanos()).unwrap_or(i64::MAX);
        Duration::from_nanos(u64::try_from(self.duration_ns(name, d)).unwrap_or(0))
    }

    pub fn string(&self, name: &str) -> String {
        self.raw(name).unwrap_or_default().to_owned()
    }

    pub fn list(&self, name: &str) -> Vec<String> {
        self.values.get(name).cloned().unwrap_or_default()
    }
}

/// Go `strconv.ParseInt(s, 0, 64)`: decimal, `0x`, `0o`/leading `0`, `0b`, `_`.
fn parse_int(s: &str) -> Option<i64> {
    let (neg, body) = match s.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (radix, digits) = if let Some(d) = body.strip_prefix("0x").or_else(|| body.strip_prefix("0X")) {
        (16, d)
    } else if let Some(d) = body.strip_prefix("0b").or_else(|| body.strip_prefix("0B")) {
        (2, d)
    } else if let Some(d) = body.strip_prefix("0o").or_else(|| body.strip_prefix("0O")) {
        (8, d)
    } else if body.len() > 1 && body.starts_with('0') {
        (8, &body[1..])
    } else {
        (10, body)
    };
    let digits = digits.replace('_', "");
    if digits.is_empty() {
        return None;
    }
    let v = i128::from_str_radix(&digits, radix).ok()?;
    let v = if neg { -v } else { v };
    i64::try_from(v).ok()
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

fn type_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Int => "int",
        Kind::Float => "float",
        Kind::Bool => "",
        Kind::Duration => "duration",
        Kind::Str | Kind::List => "string",
    }
}

/// Go `flag.PrintDefaults` output, preceded by `Usage of <name>:`.
pub(crate) fn write_usage(w: &mut dyn Write, name: &str, defs: &[FlagDef]) {
    let mut sorted: Vec<&FlagDef> = defs.iter().collect();
    sorted.sort_by_key(|d| d.name);
    let mut out = format!("Usage of {name}:\n");
    for d in sorted {
        let mut line = format!("  -{}", d.name);
        let t = type_name(d.kind);
        if !t.is_empty() {
            line.push(' ');
            line.push_str(t);
        }
        line.push_str(if line.len() <= 4 { "\t" } else { "\n    \t" });
        line.push_str(&d.usage.replace('\n', "\n    \t"));
        if !d.default.is_empty() {
            match d.kind {
                Kind::Str => line.push_str(&format!(" (default {:?})", d.default)),
                _ => line.push_str(&format!(" (default {})", d.default)),
            }
        }
        out.push_str(&line);
        out.push('\n');
    }
    let _ = w.write_all(out.as_bytes());
}

/// Parses `args` with Go `flag` package rules (`ContinueOnError`); on errors the
/// message and the usage go to `stderr`.
pub(crate) fn parse(
    name: &str,
    defs: &[FlagDef],
    args: &[String],
    stderr: &mut dyn Write,
) -> Result<Parsed, FlagError> {
    let mut parsed = Parsed::default();
    let fail = |msg: String, stderr: &mut dyn Write| {
        let _ = writeln!(stderr, "{msg}");
        write_usage(stderr, name, defs);
        FlagError::Invalid(msg)
    };
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg.len() < 2 || !arg.starts_with('-') {
            break;
        }
        let mut body = &arg[1..];
        if let Some(b) = body.strip_prefix('-') {
            if b.is_empty() {
                break; // "--" terminates the flags
            }
            body = b;
        }
        if body.is_empty() || body.starts_with('-') || body.starts_with('=') {
            return Err(fail(format!("bad flag syntax: {arg}"), stderr));
        }
        i += 1;
        let (fname, inline) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v.to_owned())),
            None => (body, None),
        };
        let Some(def) = defs.iter().find(|d| d.name == fname) else {
            if fname == "help" || fname == "h" {
                write_usage(stderr, name, defs);
                return Err(FlagError::Help);
            }
            return Err(fail(format!("flag provided but not defined: -{fname}"), stderr));
        };
        let value = if def.kind == Kind::Bool {
            match inline {
                Some(v) => {
                    if parse_bool(&v).is_none() {
                        return Err(fail(format!("invalid boolean value {v:?} for -{fname}: parse error"), stderr));
                    }
                    v
                }
                None => "true".to_owned(),
            }
        } else {
            match inline {
                Some(v) => v,
                None if i < args.len() => {
                    i += 1;
                    args[i - 1].clone()
                }
                None => return Err(fail(format!("flag needs an argument: -{fname}"), stderr)),
            }
        };
        let valid = match def.kind {
            Kind::Int => parse_int(&value).is_some(),
            Kind::Float => value.parse::<f64>().is_ok(),
            Kind::Duration => parse_go_duration_ns(&value).is_ok(),
            Kind::Bool | Kind::Str | Kind::List => true,
        };
        if !valid {
            return Err(fail(format!("invalid value {value:?} for flag -{fname}: parse error"), stderr));
        }
        parsed.values.entry(def.name).or_default().push(value);
    }
    Ok(parsed)
}

/// The two flags every scenario adds for the external server.
pub(crate) const SUT_FLAGS: [FlagDef; 2] = [
    FlagDef {
        name: "sut",
        kind: Kind::Str,
        default: "",
        usage: "server binary to launch: cabby or anycable-go (default $CABBY_BENCH_SUT)",
    },
    FlagDef {
        name: "server-arg",
        kind: Kind::List,
        default: "",
        usage: "extra server CLI flag, e.g. --server-arg=--broker=memory (repeatable)",
    },
];

/// `--protocol`: the protocol the clients and the publisher speak ([`crate::pusher::Protocol`]).
pub(crate) const PROTOCOL_FLAG: FlagDef = FlagDef {
    name: "protocol",
    kind: Kind::Str,
    default: "actioncable",
    usage: "client protocol: actioncable, or pusher (protocol 7, public channels, broadcasts over the signed Pusher HTTP API)",
};

/// The server binary: `--sut`, the test seam, or `$CABBY_BENCH_SUT`.
pub(crate) fn resolve_sut(flag: Option<PathBuf>, seam: Option<PathBuf>) -> Option<PathBuf> {
    flag.or(seam).or_else(|| std::env::var_os(SUT_ENV).filter(|v| !v.is_empty()).map(PathBuf::from))
}

// ------------------------------------------------------------------ UI

/// Lifecycle log lines on stderr, only when interactive (not `--non-interactive`
/// and the process's stderr is a terminal). Otherwise the key=value summary on
/// stdout is the only output, as upstream.
pub(crate) struct Ui {
    interactive: bool,
    start: Instant,
}

impl Ui {
    pub fn new(non_interactive: bool, start: Instant) -> Self {
        Self { interactive: !non_interactive && std::io::stderr().is_terminal(), start }
    }

    pub fn log(&self, w: &mut dyn Write, msg: &str) {
        if self.interactive {
            let _ = writeln!(w, "[{:6.2}s] {msg}", self.start.elapsed().as_secs_f64());
        }
    }
}

// ------------------------------------------------------------------ signals

fn active_run() -> &'static Mutex<Option<CancelToken>> {
    static ACTIVE: OnceLock<Mutex<Option<CancelToken>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(None))
}

/// Routes SIGINT/SIGTERM to `token` for the duration of a run: the first signal
/// cancels the run (which then prints its summary); a signal while no run is
/// active, or after the run was already cancelled, exits with 130.
pub(crate) struct SignalScope;

impl SignalScope {
    pub fn install(token: &CancelToken) -> Self {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            crate::runtime::rt().spawn(async {
                use tokio::signal::unix::{SignalKind, signal};
                let (Ok(mut int), Ok(mut term)) = (signal(SignalKind::interrupt()), signal(SignalKind::terminate()))
                else {
                    return;
                };
                loop {
                    tokio::select! {
                        _ = int.recv() => {}
                        _ = term.recv() => {}
                    }
                    let active = active_run().lock().unwrap_or_else(|e| e.into_inner()).clone();
                    match active {
                        Some(t) if !t.is_cancelled() => t.cancel(),
                        _ => std::process::exit(EXIT_INTERRUPTED),
                    }
                }
            });
        });
        *active_run().lock().unwrap_or_else(|e| e.into_inner()) = Some(token.clone());
        SignalScope
    }
}

impl Drop for SignalScope {
    fn drop(&mut self) {
        active_run().lock().unwrap_or_else(|e| e.into_inner()).take();
    }
}

// ------------------------------------------------------------------ generator health

/// Timer period of the generator starvation probe.
const LAG_PROBE: Duration = Duration::from_millis(5);

/// Watches the load generator itself during a measured window: CPU time of the
/// harness process and the lateness of a timer task on the harness runtime (a
/// starved runtime delivers messages, pings and timeouts late).
pub(crate) struct GeneratorMonitor {
    lag: std::sync::Arc<crate::histogram::Histogram>,
    stop: tokio_util::sync::CancellationToken,
    cpu_start: Duration,
    started: Instant,
}

/// What [`GeneratorMonitor`] saw.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GeneratorReport {
    /// Harness runtime worker threads.
    pub threads: usize,
    /// CPUs the harness may run on.
    pub cpus: usize,
    /// Average harness CPU cores busy over the window.
    pub cpu_cores: f64,
    /// How late a 5 ms timer fired (p99, max).
    pub lag_p99: Duration,
    pub lag_max: Duration,
}

impl GeneratorReport {
    /// The generator could not keep up: its timers fired 5 ms or more late at p99,
    /// or it kept 90% or more of its worker threads (at most its CPUs) busy.
    ///
    /// CPU time alone overstates the need when workers are mostly idle: waking a
    /// parked worker for each message costs more than the message. Fewer workers
    /// (`CABBY_BENCH_THREADS`) lower that overhead.
    pub fn starved(&self) -> bool {
        let capacity = self.threads.min(self.cpus).max(1) as f64;
        self.lag_p99 >= LAG_PROBE || self.cpu_cores >= 0.9 * capacity
    }

    pub(crate) fn write(&self, out: &mut String, saturated: bool) {
        out.push_str(&format!("generator_threads={}\n", self.threads));
        out.push_str(&format!("generator_cpus={}\n", self.cpus));
        out.push_str(&format!("generator_cpu_cores={:.2}\n", self.cpu_cores));
        out.push_str(&format!("generator_loop_lag_p99_ms={:.2}\n", ms(self.lag_p99)));
        out.push_str(&format!("generator_loop_lag_max_ms={:.2}\n", ms(self.lag_max)));
        out.push_str(&format!("generator_saturated={saturated}\n"));
    }
}

impl GeneratorMonitor {
    pub fn start() -> Self {
        let lag = std::sync::Arc::new(crate::histogram::Histogram::new());
        let stop = tokio_util::sync::CancellationToken::new();
        let (h, s) = (std::sync::Arc::clone(&lag), stop.clone());
        crate::runtime::rt().spawn(async move {
            loop {
                let t = tokio::time::Instant::now();
                tokio::select! {
                    () = s.cancelled() => return,
                    () = tokio::time::sleep(LAG_PROBE) => {}
                }
                h.record(t.elapsed().saturating_sub(LAG_PROBE));
            }
        });
        Self { lag, stop, cpu_start: crate::resources::cpu_time("self").unwrap_or_default(), started: Instant::now() }
    }

    pub fn finish(&self) -> GeneratorReport {
        self.stop.cancel();
        let cpu = crate::resources::cpu_time("self").unwrap_or_default().saturating_sub(self.cpu_start);
        let lag = self.lag.snapshot();
        GeneratorReport {
            threads: crate::runtime::worker_threads(),
            cpus: std::thread::available_parallelism().map_or(1, |n| n.get()),
            cpu_cores: cpu.as_secs_f64() / self.started.elapsed().as_secs_f64().max(1e-9),
            lag_p99: lag.quantile(0.99),
            lag_max: lag.max,
        }
    }
}

impl Drop for GeneratorMonitor {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// `value` as `MiB` with one decimal.
pub(crate) fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 20) as f64
}

/// Milliseconds of `d`.
pub(crate) fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_durations() {
        assert_eq!(Ok(Duration::from_millis(300)), parse_go_duration("300ms"));
        assert_eq!(Ok(Duration::from_millis(1500)), parse_go_duration("1.5s"));
        assert_eq!(Ok(Duration::from_secs(150)), parse_go_duration("2m30s"));
        assert_eq!(Ok(-1), parse_go_duration_ns("-1ns"));
        assert_eq!(Ok(0), parse_go_duration_ns("0"));
        assert!(parse_go_duration("1").is_err());
        assert!(parse_go_duration("1x").is_err());
        assert!(parse_go_duration("").is_err());
        assert_eq!("2m0s", go_duration_string(Duration::from_secs(120)));
        assert_eq!("500ms", go_duration_string(Duration::from_millis(500)));
        assert_eq!("1h0m0s", go_duration_string(Duration::from_secs(3600)));
    }

    #[test]
    fn go_flag_syntax() {
        let defs = [
            FlagDef { name: "c", kind: Kind::Int, default: "", usage: "clients" },
            FlagDef { name: "q", kind: Kind::Bool, default: "", usage: "quiet" },
            FlagDef { name: "d", kind: Kind::Duration, default: "1s", usage: "d" },
            FlagDef { name: "x", kind: Kind::List, default: "", usage: "x" },
        ];
        let args: Vec<String> =
            ["-c", "10", "--q", "-d=2s", "--x", "a", "-x=b", "rest", "-c", "5"].iter().map(|s| s.to_string()).collect();
        let mut err = Vec::new();
        let p = parse("t", &defs, &args, &mut err).expect("parsed");
        assert_eq!(10, p.int("c", 0));
        assert!(p.boolean("q"));
        assert_eq!(Duration::from_secs(2), p.duration("d", Duration::ZERO));
        assert_eq!(vec!["a".to_owned(), "b".to_owned()], p.list("x"));
        let bad: Vec<String> = ["-nope"].iter().map(|s| s.to_string()).collect();
        assert!(matches!(parse("t", &defs, &bad, &mut err), Err(FlagError::Invalid(_))));
        assert!(String::from_utf8_lossy(&err).contains("Usage of t:"));
    }
}
