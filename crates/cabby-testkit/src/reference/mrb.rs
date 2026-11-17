//! Facade for upstream package `mrb` (the scripting engine). Values returned by
//! the engine are go-mruby values of [`super::mruby`].

use cabby_server::mrb as rs;
use serde::Deserialize;
use serde_json::json;

use super::mruby::{Exception, MrbValue, go_exception};
use crate::mruby_bridge as bridge;
use crate::{Backend, backend};

/// `*mrb.Engine`.
#[derive(Debug, Clone)]
pub enum Engine {
    Rust(rs::Engine),
    Go(u64),
}

#[derive(Deserialize)]
struct GoResult {
    value: Option<u64>,
    exc: Option<serde_json::Value>,
    error: Option<String>,
}

impl Engine {
    /// `NewEngine()`.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Engine {
        match backend() {
            Backend::Rust => Engine::Rust(rs::Engine::new()),
            Backend::Go => Engine::Go(bridge::call("mrb.NewEngine", json!({}))),
        }
    }

    /// `LoadString(code)`; the error is Go's `err.Error()` text.
    pub fn load_string(&self, code: &str) -> Result<(), String> {
        match self {
            Engine::Rust(e) => e.load_string(code).map_err(|e| e.to_string()),
            Engine::Go(h) => {
                let r: GoResult = bridge::call("mrb.Engine.LoadString", json!({"h": h, "code": code}));
                match (r.exc, r.error) {
                    (Some(exc), _) => Err(go_exception(exc).error()),
                    (None, Some(e)) => Err(e),
                    (None, None) => Ok(()),
                }
            }
        }
    }

    /// `Eval(code)`.
    pub fn eval(&self, code: &str) -> Result<MrbValue, Exception> {
        match self {
            Engine::Rust(e) => e.eval(code).map(MrbValue::Rust).map_err(Exception::from),
            Engine::Go(h) => {
                let r: GoResult = bridge::call("mrb.Engine.Eval", json!({"h": h, "code": code}));
                if let Some(exc) = r.exc {
                    return Err(go_exception(exc));
                }
                if let Some(e) = r.error {
                    panic!("mrb.Engine.Eval returned a non-exception error: {e}");
                }
                Ok(MrbValue::Go(r.value.expect("Eval returned a nil *MrbValue without error")))
            }
        }
    }
}
