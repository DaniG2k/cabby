//! Logging settings (upstream `logger/config.go`).

use serde::Deserialize;

/// Logging settings (upstream `logger.Config`, TOML table `[logging]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    /// `level`
    pub log_level: String,
    /// `format`
    pub log_format: String,
    /// `debug`
    pub debug: bool,
}

/// A TOML decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TomlError(pub String);

/// The keys present in a TOML document; absent keys keep their current values, as
/// with `toml.Decode` into an initialized struct. Unknown keys are ignored.
#[derive(Deserialize)]
struct Overrides {
    level: Option<String>,
    format: Option<String>,
    debug: Option<bool>,
}

impl Config {
    /// Defaults (upstream `logger.NewConfig`).
    pub fn new() -> Self {
        Config { log_level: "info".into(), log_format: "text".into(), debug: false }
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str("# Logging level (debug, info, warn, error)\n");
        out.push_str(&format!("level = \"{}\"\n", self.log_level));
        out.push_str("# Logs formatting (e.g., 'text' or 'json')\n");
        out.push_str(&format!("format = \"{}\"\n", self.log_format));
        out.push_str("# Enable debug (verbose) logging\n");
        out.push_str(if self.debug { "debug = true\n" } else { "# debug = true\n" });
        out.push('\n');
        out
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), TomlError> {
        let o: Overrides = toml::from_str(toml).map_err(|e| TomlError(e.to_string()))?;
        if let Some(level) = o.level {
            self.log_level = level;
        }
        if let Some(format) = o.format {
            self.log_format = format;
        }
        if let Some(debug) = o.debug {
            self.debug = debug;
        }
        Ok(())
    }
}
