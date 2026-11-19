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
    /// (`unsupported`, as upstream without the `mrb` build tag).
    pub fn new(path: &str) -> Result<RubyPrinter, MetricsError> {
        if !mrb::supported() {
            return Err(MetricsError("unsupported".into()));
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
