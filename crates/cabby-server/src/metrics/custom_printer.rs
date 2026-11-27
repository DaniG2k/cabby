//! The mruby metrics log formatter (upstream `metrics/custom_printer.go`):
//! `--metrics_log_formatter=<file.rb>` defines `MetricsFormatter.call(data)`,
//! which turns each interval snapshot into the logged line.
//!
//! An mruby VM is bound to one thread, while interval writers are shared by the
//! metrics task, so the printer owns a thread that holds the VM (the default
//! engine of that thread, as upstream uses its process-wide default engine)
//! and is woken over a channel. The snapshot waiting to be formatted is kept in
//! one slot: a snapshot written while the formatter is still busy replaces the
//! one waiting (D-124), so a slow `call` holds at most one.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use cabby_core::logger::{Attr, Level, LogValue};

use super::{IntervalWriter, Metrics, MetricsError, log};
use crate::mrb;

enum Msg {
    Run {
        interval: u64,
        reply: mpsc::Sender<Result<(), String>>,
    },
    /// A snapshot is waiting in the printer's slot.
    Print,
}

type Snapshot = HashMap<String, u64>;

/// The newest snapshot not formatted yet.
type Slot = Arc<Mutex<Option<Snapshot>>>;

/// Where a formatted line goes: the log (tests replace it).
type Emit = Arc<dyn Fn(&str) + Send + Sync>;

/// Why `--metrics_log_formatter` fails in a build without the `mruby` feature (D-062).
pub const NO_MRUBY: &str = "--metrics_log_formatter needs mruby, and this cabby was built without it (build with --features mruby; see docs/mruby.md)";

/// Upstream `RubyPrinter`.
pub struct RubyPrinter {
    path: String,
    /// Holds at most one message: a `Print` waiting means the slot is read.
    tx: Mutex<Option<mpsc::SyncSender<Msg>>>,
    pending: Slot,
    emit: Emit,
}

impl std::fmt::Debug for RubyPrinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RubyPrinter").field("path", &self.path).finish_non_exhaustive()
    }
}

impl RubyPrinter {
    /// Upstream `NewCustomPrinter`: fails only when mruby is not compiled in
    /// (upstream says `unsupported`; Cabby names the feature, D-062).
    pub fn new(path: &str) -> Result<RubyPrinter, MetricsError> {
        if !mrb::supported() {
            return Err(MetricsError(NO_MRUBY.into()));
        }
        let emit: Emit = Arc::new(|line: &str| log(Level::Info, line, &[]));
        Ok(RubyPrinter { path: path.to_owned(), tx: Mutex::new(None), pending: Slot::default(), emit })
    }

    fn send(&self, msg: Msg) -> bool {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        tx.as_ref().is_some_and(|tx| tx.send(msg).is_ok())
    }

    /// Puts `snapshot` in the slot, replacing one not formatted yet, and wakes
    /// the VM thread unless a wakeup is already waiting.
    fn offer(&self, snapshot: Snapshot) {
        *self.pending.lock().unwrap_or_else(|e| e.into_inner()) = Some(snapshot);
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(tx) = tx.as_ref() {
            // Full: a `Print` is waiting and will take this snapshot.
            let _ = tx.try_send(Msg::Print);
        }
    }
}

/// The VM side: loads the script, then formats the snapshot in `pending` on
/// each wakeup until the printer is dropped.
fn serve(path: String, rx: mpsc::Receiver<Msg>, pending: Slot, emit: Emit) {
    let mut module: Option<mrb::MrbValue> = None;
    let engine = mrb::default_engine();
    for msg in rx {
        match msg {
            Msg::Run { interval, reply } => {
                let loaded = load(&engine, &path);
                if loaded.is_ok() {
                    log(
                        Level::Info,
                        &format!("Log metrics every {interval}s using a custom Ruby formatter from {path}"),
                        &[],
                    );
                }
                module = loaded.as_ref().ok().cloned();
                let _ = reply.send(loaded.map(|_| ()));
            }
            Msg::Print => {
                let snapshot = pending.lock().unwrap_or_else(|e| e.into_inner()).take();
                if let (Some(module), Some(snapshot)) = (&module, snapshot)
                    && let Some(line) = print(&engine, module, snapshot)
                {
                    emit(&line);
                }
            }
        }
    }
}

/// Upstream `Run`: `LoadFile`, then the `MetricsFormatter` module (whose
/// absence aborts the Go process; here it is an error).
fn load(engine: &mrb::Engine, path: &str) -> Result<mrb::MrbValue, String> {
    engine.load_file(&PathBuf::from(path)).map_err(|e| e.to_string())?;
    let vm = engine.vm();
    let module = vm.module("MetricsFormatter").ok_or_else(|| "MetricsFormatter module is not defined".to_owned())?;
    let value = module.mrb_value(&vm);
    // Keep the module reachable from Rust's side for the printer's lifetime.
    value.gc_protect();
    Ok(value)
}

/// Upstream `Print`: the snapshot as a Ruby hash of `String => Int`, formatted
/// by `MetricsFormatter.call`: the line to log, or `None` when the call failed
/// (logged). The arena is restored afterwards so a long-running printer does
/// not accumulate objects.
fn print(engine: &mrb::Engine, module: &mrb::MrbValue, snapshot: Snapshot) -> Option<String> {
    let vm = engine.vm();
    let ai = vm.arena_save();
    let result = (|| {
        let rhash = engine.eval("{}")?;
        let hash = rhash.hash();
        for (k, v) in snapshot {
            // Go: mruby.Int(v) (the uint64 converted to int).
            hash.set(&mrb::Value::String(k), &mrb::Value::Int(v as i64))?;
        }
        module.call("call", &[mrb::Value::Mrb(rhash)])
    })();
    let line = match result {
        Ok(line) => Some(line.to_string()),
        Err(e) => {
            log(Level::Error, "mruby call failed", &[Attr::new("error", LogValue::String(e.message))]);
            None
        }
    };
    vm.arena_restore(ai);
    line
}

impl IntervalWriter for RubyPrinter {
    fn run(&self, interval: u64) -> Result<(), MetricsError> {
        let (tx, rx) = mpsc::sync_channel(1);
        let (path, pending, emit) = (self.path.clone(), Arc::clone(&self.pending), Arc::clone(&self.emit));
        std::thread::Builder::new()
            .name("mruby-printer".into())
            .spawn(move || serve(path, rx, pending, emit))
            .map_err(|e| MetricsError(format!("starting the mruby printer: {e}")))?;
        *self.tx.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);

        let (reply, done) = mpsc::channel();
        if !self.send(Msg::Run { interval, reply }) {
            return Err(MetricsError("the mruby printer stopped".into()));
        }
        match done.recv() {
            Ok(r) => r.map_err(MetricsError),
            Err(_) => Err(MetricsError("the mruby printer stopped".into())),
        }
    }

    /// Upstream `Stop` does nothing; the VM thread ends with the printer.
    fn stop(&self) {}

    /// Hands the interval snapshot to the VM thread; a snapshot it has not
    /// formatted yet is replaced (D-124).
    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        self.offer(metrics.interval_snapshot());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A formatter that raises on every call keeps the printer's VM usable:
    /// each print fails cleanly. Before the mruby binding unwound and checked
    /// what a raise leaves behind, such a formatter crashed the process after
    /// about 1,400 to 4,200 prints (review finding M4).
    #[cfg(feature = "mruby")]
    #[test]
    fn a_formatter_that_always_raises_survives_10000_prints() {
        let path = std::env::temp_dir().join(format!("cabby-raising-formatter-{}.rb", std::process::id()));
        std::fs::write(
            &path,
            "module MetricsFormatter\n  def self.call(data)\n    data.each { |k, v| raise \"bad #{k}\" }\n  end\nend\n",
        )
        .expect("script written");
        let engine = mrb::Engine::new();
        let module = load(&engine, &path.to_string_lossy()).expect("formatter loads");
        let _ = std::fs::remove_file(&path);

        let names = [
            "clients_num",
            "clients_uniq_num",
            "client_msg_total",
            "failed_client_msg_total",
            "broadcast_msg_total",
            "failed_broadcast_msg_total",
            "broadcast_streams_num",
            "rpc_call_total",
            "rpc_error_total",
            "rpc_retries_total",
            "rpc_pending_num",
            "failed_auths_total",
            "goroutines_num",
            "disconnect_queue_size",
            "server_msg_total",
            "failed_server_msg_total",
            "data_sent_total",
            "data_rcvd_total",
            "mem_sys_bytes",
        ];
        for i in 0..10_000_u64 {
            let snapshot: HashMap<String, u64> = names.iter().map(|n| ((*n).to_owned(), i)).collect();
            print(&engine, &module, snapshot);
        }
        assert_eq!(engine.eval("1 + 41").expect("the VM is still usable").fixnum(), 42);
        let data = engine.eval("{'k' => 1}").expect("hash");
        let e = module.call("call", &[mrb::Value::Mrb(data)]).expect_err("raises");
        assert_eq!(e.message, "bad k");
        assert!(e.file.ends_with(".rb") && e.line == 3, "{}:{}", e.file, e.line);
    }

    #[cfg(feature = "mruby")]
    fn script(name: &str, source: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cabby-{name}-{}.rb", std::process::id()));
        std::fs::write(&path, source).expect("script written");
        path
    }

    /// A formatter script whose top-level code runs for a long time (here a
    /// loop of about a second) leaves a one-worker runtime running, and the
    /// metrics shut down while it still runs. Before, `Metrics::run` waited for
    /// the script on the runtime worker, so a script that never finished
    /// stopped the whole server on a one-CPU host.
    #[cfg(feature = "mruby")]
    #[test]
    fn a_slow_formatter_script_blocks_no_runtime_worker() {
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use crate::metrics::worker_tests::{assert_beats, heartbeat, one_worker};

        let rt = one_worker();
        let path = script(
            "formatter-with-a-slow-load",
            "t = Time.now\nwhile Time.now - t < 1.0\nend\nmodule MetricsFormatter\n  def self.call(data)\n    'formatted'\n  end\nend\n",
        );
        let printer: Arc<dyn IntervalWriter> = Arc::new(RubyPrinter::new(&path.to_string_lossy()).expect("printer"));
        let metrics = Arc::new(Metrics::new(vec![printer], 1));
        let beats = heartbeat(&rt);

        let (done, ran) = mpsc::channel();
        let m = Arc::clone(&metrics);
        rt.spawn(async move {
            let _ = done.send(m.run().await);
        });
        assert_beats(&beats, 20);
        assert!(ran.try_recv().is_err(), "run waits for the script");

        let started = Instant::now();
        rt.block_on(metrics.shutdown()).expect("shutdown");
        assert_eq!(Ok(Ok(())), ran.recv_timeout(Duration::from_secs(2)), "run returns once shut down");
        assert!(started.elapsed() < Duration::from_millis(500), "shutdown took {:?}", started.elapsed());
        let _ = std::fs::remove_file(&path);
    }

    /// A formatter script that fails to load still fails `Metrics::run` with
    /// its error, which stops the server at startup, as upstream.
    #[cfg(feature = "mruby")]
    #[test]
    fn a_formatter_script_that_fails_to_load_fails_the_metrics_run() {
        use std::sync::Arc;

        use crate::metrics::worker_tests::one_worker;

        let rt = one_worker();
        for (name, source, error) in [
            ("formatter-raising-at-load", "raise 'formatter is broken'\n", "formatter is broken"),
            ("formatter-without-the-module", "x = 1\n", "MetricsFormatter module is not defined"),
        ] {
            let path = script(name, source);
            let printer: Arc<dyn IntervalWriter> =
                Arc::new(RubyPrinter::new(&path.to_string_lossy()).expect("printer"));
            let metrics = Metrics::new(vec![printer], 1);

            let e = rt.block_on(metrics.run()).expect_err("the script does not load");

            assert!(e.0.contains(error), "{name}: {}", e.0);
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Metrics with one gauge set to `value`.
    fn gauge_metrics(value: u64) -> Metrics {
        use crate::metrics::Instrumenter as _;
        let m = Metrics::new(Vec::new(), 0);
        m.register_gauge("n", "");
        m.gauge("n").expect("gauge").set(value as i64);
        m
    }

    /// Pins D-124: while the VM thread is busy (here it never reads), each
    /// write replaces the snapshot waiting in the slot, and at most one wakeup
    /// waits: the newest snapshot is the one formatted next. Before, every
    /// snapshot was queued on an unbounded channel.
    #[test]
    fn deviation_d124_writes_to_a_busy_printer_keep_the_newest_snapshot() {
        let (tx, rx) = mpsc::sync_channel(1);
        let printer = RubyPrinter {
            path: "formatter.rb".into(),
            tx: Mutex::new(Some(tx)),
            pending: Slot::default(),
            emit: Arc::new(|_: &str| {}),
        };
        for value in 0..1000 {
            printer.write(&gauge_metrics(value)).expect("write");
        }
        assert!(matches!(rx.try_recv(), Ok(Msg::Print)), "one wakeup waits");
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)), "and only one");
        let pending = printer.pending.lock().unwrap().take();
        assert_eq!(Some(999), pending.and_then(|s| s.get("n").copied()), "the newest snapshot waits");
    }

    /// D-124 with the VM: a formatter whose `call` takes 0.3 s formats the
    /// snapshot it was given, then the newest of those written meanwhile, and
    /// none of the others.
    #[cfg(feature = "mruby")]
    #[test]
    fn deviation_d124_a_slow_formatter_formats_the_newest_snapshot() {
        use std::time::{Duration, Instant};

        let path = script(
            "slow-formatter",
            "module MetricsFormatter\n  def self.call(data)\n    t = Time.now\n    while Time.now - t < 0.3\n    end\n    data['n'].to_s\n  end\nend\n",
        );
        let (lines, formatted) = mpsc::channel::<String>();
        let lines = Mutex::new(lines);
        let mut printer = RubyPrinter::new(&path.to_string_lossy()).expect("printer");
        printer.emit = Arc::new(move |line: &str| {
            let _ = lines.lock().unwrap_or_else(|e| e.into_inner()).send(line.to_owned());
        });
        printer.run(1).expect("the formatter loads");
        let _ = std::fs::remove_file(&path);

        printer.write(&gauge_metrics(0)).expect("write");
        // The VM thread has taken the first snapshot: it is formatting it.
        let deadline = Instant::now() + Duration::from_secs(2);
        while printer.pending.lock().unwrap().is_some() {
            assert!(Instant::now() < deadline, "the first snapshot is taken");
            std::thread::sleep(Duration::from_millis(5));
        }
        for value in 1..=20 {
            printer.write(&gauge_metrics(value)).expect("write");
        }
        let next = || formatted.recv_timeout(Duration::from_secs(2)).ok();
        assert_eq!((Some("0".to_owned()), Some("20".to_owned())), (next(), next()));
        assert_eq!(None, formatted.recv_timeout(Duration::from_millis(500)).ok(), "no other snapshot is formatted");
    }

    /// Pins D-062: without the `mruby` feature, the formatter's error names the
    /// feature. Runs only in a build without mruby (`cargo test -p cabby-server`).
    #[cfg(not(feature = "mruby"))]
    #[test]
    fn deviation_d062_formatter_without_mruby_names_the_feature() {
        let e = RubyPrinter::new("formatter.rb").expect_err("no mruby in this build");
        assert_eq!(
            e.0,
            "--metrics_log_formatter needs mruby, and this cabby was built without it (build with --features mruby; see docs/mruby.md)"
        );
    }
}
