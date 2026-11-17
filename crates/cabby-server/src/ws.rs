//! WebSocket transport (upstream package `ws`).

use cabby_core::pending;
use cabby_core::toml_config::string_list;

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

cabby_core::toml_section!(Config, "ws.Config" {
    "paths" => paths,
    "read_buffer_size" => read_buffer_size,
    "write_buffer_size" => write_buffer_size,
    "max_message_size" => max_message_size,
    "write_timeout" => write_timeout,
    "max_pending_size" => max_pending_size,
    "enable_compression" => enable_compression,
});

impl Config {
    /// Defaults (upstream `ws.NewConfig`).
    pub fn new() -> Self {
        Self {
            paths: vec!["/cable".to_owned()],
            read_buffer_size: 1024,
            write_buffer_size: 1024,
            max_message_size: 65536,
            // 1 MB
            max_pending_size: 1024 * 1024,
            write_timeout: 2,
            ..Default::default()
        }
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# WebSocket endpoint paths\n");
        out.push_str(&format!("paths = {}\n", string_list(&self.paths)));
        out.push_str("# Read buffer size\n");
        out.push_str(&format!("read_buffer_size = {}\n", self.read_buffer_size));
        out.push_str("# Write buffer size\n");
        out.push_str(&format!("write_buffer_size = {}\n", self.write_buffer_size));
        out.push_str("# Maximum message size\n");
        out.push_str(&format!("max_message_size = {}\n", self.max_message_size));
        out.push_str("# Write timeout (seconds)\n");
        out.push_str(&format!("write_timeout = {}\n", self.write_timeout));
        out.push_str("# Maximum pending size\n");
        if self.max_pending_size > 0 {
            out.push_str(&format!("max_pending_size = {}\n", self.max_pending_size));
        } else {
            out.push_str("# max_pending_size = 1048576\n");
        }
        if self.enable_compression {
            out.push_str("# Enable compression (per-message deflate)\n");
            out.push_str("enable_compression = true\n");
            out.push_str("# enable_compression = true\n");
        }
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| TomlError(e.0))?;
        Ok(())
    }
}
