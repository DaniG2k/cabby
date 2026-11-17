//! The scripting engine used by Cabby features (upstream package `mrb`).

use std::fmt;
use std::path::Path;

use cabby_core::pending;

use super::{Exception, Mrb, MrbValue, NotSend, ParserError};

/// Whether mruby scripting is compiled in (upstream: the `mrb` build tag on
/// Linux/macOS; Cabby: the planned `mruby` cargo feature, see `docs/mruby.md`).
pub fn supported() -> bool {
    pending!("mrb::supported")
}

/// `MRUBY_DESCRIPTION` of the default engine, trimmed (`Version`); upstream
/// returns `unknown` when scripting is not supported.
pub fn version() -> Result<String, MrbError> {
    pending!("mrb::version")
}

/// One VM plus the lock that serializes loading into it (`Engine`). Clones share
/// the VM.
#[derive(Debug, Clone)]
pub struct Engine {
    _e: NotSend,
}

/// The engine shared by features that do not need their own VM
/// (`DefaultEngine`). Upstream keeps one per process; since a VM is
/// thread-bound, Cabby keeps one per thread that asks for it.
pub fn default_engine() -> Engine {
    pending!("mrb::default_engine")
}

/// Errors of [`Engine::load_file`] and [`Engine::load_string`].
#[derive(Debug)]
pub enum MrbError {
    Io(std::io::Error),
    Parse(ParserError),
    Exception(Exception),
    /// Generating the random file name failed (upstream: nanoid error).
    FileName(String),
}

impl fmt::Display for MrbError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("mrb::MrbError::fmt")
    }
}

impl std::error::Error for MrbError {}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    /// `NewEngine`.
    pub fn new() -> Engine {
        pending!("mrb::Engine::new")
    }

    /// The VM (`Engine.VM`).
    pub fn vm(&self) -> Mrb {
        pending!("mrb::Engine::vm")
    }

    /// Reads and loads a Ruby file (`LoadFile`).
    pub fn load_file(&self, _path: &Path) -> Result<(), MrbError> {
        pending!("mrb::Engine::load_file")
    }

    /// Parses `code` under a random `<nanoid>.rb` file name and runs it
    /// (`LoadString`).
    pub fn load_string(&self, _code: &str) -> Result<(), MrbError> {
        pending!("mrb::Engine::load_string")
    }

    /// Evaluates `code` and returns its value (`Eval`).
    pub fn eval(&self, _code: &str) -> Result<MrbValue, Exception> {
        pending!("mrb::Engine::eval")
    }
}
