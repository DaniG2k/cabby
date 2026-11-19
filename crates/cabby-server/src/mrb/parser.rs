//! Parsing and compiling Ruby code (go-mruby `parser.go`, `context.go`).

use std::fmt;

use cabby_mruby_sys as sys;

use super::{Mrb, MrbValue};

/// A parser (`*Parser`, `mrb_parser_state`), created with captured errors.
#[derive(Debug)]
pub struct Parser {
    p: sys::Parser,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Ruby parse error!\n\n")?;
        for e in &self.errors {
            writeln!(f, "line {}:{}: {}", e.line, e.col, e.message)?;
        }
        Ok(())
    }
}

impl std::error::Error for ParserError {}

fn messages(list: Vec<sys::ParserMessage>) -> Vec<ParserMessage> {
    list.into_iter().map(|m| ParserMessage { col: m.col, line: m.line, message: m.message }).collect()
}

impl Parser {
    /// `NewParser`.
    pub fn new(m: &Mrb) -> Parser {
        Parser { p: sys::Parser::new(&m.vm) }
    }

    pub fn close(self) {
        drop(self.p);
    }

    /// Parses `code`, returning its warnings (empty where Go returns nil).
    pub fn parse(&mut self, code: &str, ctx: Option<&CompileContext>) -> Result<Vec<ParserMessage>, ParserError> {
        let (errors, warnings) = self.p.parse(code, ctx.map(|c| &c.c));
        let warnings = messages(warnings);
        if errors.is_empty() { Ok(warnings) } else { Err(ParserError { errors: messages(errors), warnings }) }
    }

    /// Compiles the last parse into a proc (`GenerateCode`). The value is nil
    /// when there is nothing to run: the last parse failed (go-mruby generates
    /// code from the partial tree) or code generation failed (go-mruby
    /// crashes).
    pub fn generate_code(&mut self) -> MrbValue {
        MrbValue { v: self.p.generate_code() }
    }
}

/// Compile options such as the file name used in backtraces (`*CompileContext`).
#[derive(Debug)]
pub struct CompileContext {
    c: sys::Context,
}

impl CompileContext {
    /// `NewCompileContext`.
    pub fn new(m: &Mrb) -> CompileContext {
        CompileContext { c: sys::Context::new(&m.vm) }
    }

    pub fn close(self) {
        drop(self.c);
    }

    /// The file name; empty when unset (`Filename`).
    pub fn filename(&self) -> String {
        self.c.filename()
    }

    pub fn set_filename(&mut self, name: &str) {
        self.c.set_filename(name);
    }

    pub fn capture_errors(&mut self, yes: bool) {
        self.c.capture_errors(yes);
    }
}
