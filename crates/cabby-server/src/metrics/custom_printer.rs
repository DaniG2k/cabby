//! The mruby metrics log formatter (upstream `metrics/custom_printer.go`):
//! `--metrics_log_formatter=<file.rb>` defines `MetricsFormatter.call(data)`,
//! which turns each interval snapshot into the logged line.
//!
//! An mruby VM is bound to one thread, while interval writers are shared by the
//! metrics task, so the printer owns a thread that holds the VM (the default
//! engine of that thread, as upstream uses its process-wide default engine)
//! and receives snapshots over a channel.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::mpsc;

use cabby_core::logger::{Attr, Level, LogValue};

use super::{IntervalWriter, Metrics, MetricsError, log};
use crate::mrb;

enum Msg {
    Run { interval: u64, reply: mpsc::Sender<Result<(), String>> },
    Print(HashMap<String, u64>),
}

/// Why `--metrics_log_formatter` fails in a build without the `mruby` feature (D-062).
pub const NO_MRUBY: &str = "--metrics_log_formatter needs mruby, and this cabby was built without it (build with --features mruby; see docs/mruby.md)";

/// Upstream `RubyPrinter`.
pub struct RubyPrinter {
    path: String,
    tx: Mutex<Option<mpsc::Sender<Msg>>>,
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
        Ok(RubyPrinter { path: path.to_owned(), tx: Mutex::new(None) })
    }

    fn send(&self, msg: Msg) -> bool {
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        tx.as_ref().is_some_and(|tx| tx.send(msg).is_ok())
    }
}

/// The VM side: loads the script, then formats snapshots until the printer
/// is dropped.
fn serve(path: String, rx: mpsc::Receiver<Msg>) {
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
            Msg::Print(snapshot) => {
                if let Some(module) = &module {
                    print(&engine, module, snapshot);
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
/// by `MetricsFormatter.call`. The arena is restored afterwards so a
/// long-running printer does not accumulate objects.
fn print(engine: &mrb::Engine, module: &mrb::MrbValue, snapshot: HashMap<String, u64>) {
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
    match result {
        Ok(line) => log(Level::Info, &line.to_string(), &[]),
        Err(e) => log(Level::Error, "mruby call failed", &[Attr::new("error", LogValue::String(e.message))]),
    }
    vm.arena_restore(ai);
}

impl IntervalWriter for RubyPrinter {
    fn run(&self, interval: u64) -> Result<(), MetricsError> {
        let (tx, rx) = mpsc::channel();
        let path = self.path.clone();
        std::thread::Builder::new()
            .name("mruby-printer".into())
            .spawn(move || serve(path, rx))
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

    fn write(&self, metrics: &Metrics) -> Result<(), MetricsError> {
        self.send(Msg::Print(metrics.interval_snapshot()));
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
