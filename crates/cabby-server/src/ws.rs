//! WebSocket transport (upstream package `ws`).

use cabby_core::pending;

pub use cabby_core::encoders::{FrameType, SentFrame};
pub use cabby_core::logger::TomlError;

/// Origin policy built from the comma-separated `allowed_origins` setting.
#[derive(Debug, Clone)]
pub struct OriginCheck {
    _origins: String,
}

/// Builds the origin policy (upstream `ws.CheckOrigin`).
pub fn check_origin(origins: &str) -> OriginCheck {
    OriginCheck { _origins: origins.to_owned() }
}

impl OriginCheck {
    /// Decides a handshake given its `Origin` header (`None` when absent).
    pub fn allows(&self, _origin: Option<&str>) -> bool {
        pending!("ws::check_origin")
    }
}

/// WebSocket connection settings (upstream `ws.Config`, TOML table `[ws]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub paths: Vec<String>,
    pub read_buffer_size: i64,
    pub write_buffer_size: i64,
    pub max_message_size: i64,
    pub write_timeout: i64,
    pub max_pending_size: u64,
    pub enable_compression: bool,
    /// Not part of the TOML section (upstream tag `toml:"-"`).
    pub allowed_origins: String,
}

impl Config {
    /// Defaults (upstream `ws.NewConfig`).
    pub fn new() -> Self {
        pending!("ws::new_config")
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("ws::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), TomlError> {
        pending!("ws::config_decode_toml")
    }
}
