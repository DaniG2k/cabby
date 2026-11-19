//! The scripting engine used by Cabby features (upstream package `mrb`).

use std::fmt;
use std::path::Path;

use super::{CompileContext, Exception, Mrb, MrbValue, Parser, ParserError};

/// Whether mruby scripting is compiled in (upstream: the `mrb` build tag on
/// Linux/macOS; Cabby: the `mruby` cargo feature, on by default, on Linux and
/// macOS).
pub fn supported() -> bool {
    cabby_mruby_sys::available()
}

/// `MRUBY_DESCRIPTION` of the default engine, trimmed (`Version`); upstream
/// returns `unknown` when scripting is not supported.
pub fn version() -> Result<String, MrbError> {
    if !supported() {
        return Ok("unknown".to_owned());
    }
    let v = default_engine().eval("MRUBY_DESCRIPTION").map_err(MrbError::Exception)?;
    Ok(v.to_string().trim().to_owned())
}

/// One VM (`Engine`). Clones share the VM. Upstream serializes loading with a
/// mutex; a Cabby engine is bound to one thread, so there is nothing to lock.
#[derive(Debug, Clone)]
pub struct Engine {
    vm: Mrb,
}

thread_local! {
    static DEFAULT_ENGINE: Engine = Engine::new();
}

/// The engine shared by features that do not need their own VM
/// (`DefaultEngine`). Upstream keeps one per process; since a VM is
/// thread-bound, Cabby keeps one per thread that asks for it.
pub fn default_engine() -> Engine {
    DEFAULT_ENGINE.with(Engine::clone)
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MrbError::Io(e) => e.fmt(f),
            MrbError::Parse(e) => e.fmt(f),
            MrbError::Exception(e) => e.fmt(f),
            MrbError::FileName(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for MrbError {}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

/// The text of Go's `*fs.PathError` (`open <path>: no such file or directory`):
/// the OS message without Rust's ` (os error N)` suffix, lower-cased as Go's
/// errno texts are.
fn go_path_error(op: &str, path: &Path, e: &std::io::Error) -> std::io::Error {
    let text = e.to_string();
    let text = text.split(" (os error").next().unwrap_or(&text);
    let mut chars = text.chars();
    let text: String = chars.next().map(|c| c.to_lowercase().chain(chars).collect()).unwrap_or_default();
    std::io::Error::new(e.kind(), format!("{op} {}: {text}", path.display()))
}

/// go-nanoid's default `Nanoid()`: 21 characters of `_-0-9a-zA-Z`.
fn nanoid() -> Result<String, String> {
    const ALPHABET: &[u8; 64] = b"_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let mut bytes = [0u8; 21];
    getrandom::fill(&mut bytes).map_err(|e| e.to_string())?;
    Ok(bytes.iter().map(|b| char::from(ALPHABET[usize::from(b & 63)])).collect())
}

impl Engine {
    /// `NewEngine`.
    pub fn new() -> Engine {
        Engine { vm: Mrb::new() }
    }

    /// The VM (`Engine.VM`).
    pub fn vm(&self) -> Mrb {
        self.vm.clone()
    }

    /// Reads and loads a Ruby file (`LoadFile`).
    pub fn load_file(&self, path: &Path) -> Result<(), MrbError> {
        let contents = std::fs::read(path).map_err(|e| MrbError::Io(go_path_error("open", path, &e)))?;
        self.load_string(&String::from_utf8_lossy(&contents))
    }

    /// Parses `code` under a random `<nanoid>.rb` file name and runs it
    /// (`LoadString`).
    pub fn load_string(&self, code: &str) -> Result<(), MrbError> {
        let mut ctx = CompileContext::new(&self.vm);
        let filename = nanoid().map_err(MrbError::FileName)?;
        ctx.set_filename(&format!("{filename}.rb"));

        let mut parser = Parser::new(&self.vm);
        parser.parse(code, Some(&ctx)).map_err(MrbError::Parse)?;
        let parsed = parser.generate_code();
        self.vm.run(&parsed.into(), None).map_err(MrbError::Exception)?;
        Ok(())
    }

    /// Evaluates `code` and returns its value (`Eval`).
    pub fn eval(&self, code: &str) -> Result<MrbValue, Exception> {
        self.vm.load_string(code)
    }
}
