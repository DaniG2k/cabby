//! Parsing and compiling Ruby code (go-mruby `parser.go`, `context.go`).

use std::fmt;

use cabby_core::pending;

use super::{Mrb, MrbValue, NotSend};

/// A parser (`*Parser`, `mrb_parser_state`), created with captured errors.
#[derive(Debug)]
pub struct Parser {
    _p: NotSend,
}

/// A parser warning or error (`ParserMessage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserMessage {
    pub col: i64,
    pub line: i64,
    pub message: String,
}

/// Parse errors (`ParserError`). Go returns the warnings next to the error;
/// they are kept here in `warnings`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParserError {
    pub errors: Vec<ParserMessage>,
    pub warnings: Vec<ParserMessage>,
}

/// `Error()`: `Ruby parse error!\n\n` followed by `line L:C: message` lines.
impl fmt::Display for ParserError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("mrb::ParserError::fmt")
    }
}

impl std::error::Error for ParserError {}

impl Parser {
    /// `NewParser`.
    pub fn new(_m: &Mrb) -> Parser {
        pending!("mrb::Parser::new")
    }

    pub fn close(self) {
        pending!("mrb::Parser::close")
    }

    /// Parses `code`, returning its warnings (empty where Go returns nil).
    pub fn parse(&mut self, _code: &str, _ctx: Option<&CompileContext>) -> Result<Vec<ParserMessage>, ParserError> {
        pending!("mrb::Parser::parse")
    }

    /// Compiles the last parse into a proc (`GenerateCode`).
    pub fn generate_code(&mut self) -> MrbValue {
        pending!("mrb::Parser::generate_code")
    }
}

/// Compile options such as the file name used in backtraces (`*CompileContext`).
#[derive(Debug)]
pub struct CompileContext {
    _c: NotSend,
}

impl CompileContext {
    /// `NewCompileContext`.
    pub fn new(_m: &Mrb) -> CompileContext {
        pending!("mrb::CompileContext::new")
    }

    pub fn close(self) {
        pending!("mrb::CompileContext::close")
    }

    /// The file name; empty when unset (`Filename`).
    pub fn filename(&self) -> String {
        pending!("mrb::CompileContext::filename")
    }

    pub fn set_filename(&mut self, _name: &str) {
        pending!("mrb::CompileContext::set_filename")
    }

    pub fn capture_errors(&mut self, _yes: bool) {
        pending!("mrb::CompileContext::capture_errors")
    }
}
