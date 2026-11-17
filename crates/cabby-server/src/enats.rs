//! Embedded NATS server (upstream package `enats`).
//!
//! Status: interfaces only; every behavior is `pending!`. The embedded-NATS
//! strategy is a separate milestone (SPEC.md §3); translated tests that need a
//! NATS server use an external `nats-server` for now.

use cabby_core::pending;
use cabby_core::toml_config::string_list;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnatsError(pub String);

impl std::fmt::Display for EnatsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EnatsError {}

/// Upstream `enats.Config`. `Default` is Go's zero value (`var c Config`), which
/// upstream tests use directly; defaults come from [`Config::new`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    pub enabled: bool,
    pub debug: bool,
    pub trace: bool,
    pub name: String,
    pub service_addr: String,
    pub cluster_addr: String,
    pub cluster_name: String,
    pub gateway_addr: String,
    pub gateway_advertise: String,
    pub gateways: Vec<String>,
    pub routes: Vec<String>,
    pub jetstream: bool,
    pub store_dir: String,
    /// Seconds.
    pub jetstream_ready_timeout: i64,
    /// Bytes; 0 means the server default.
    pub max_payload: i64,
}

cabby_core::toml_section!(Config, "enats.Config" {
    "enabled" => enabled,
    "debug" => debug,
    "trace" => trace,
    "name" => name,
    "service_addr" => service_addr,
    "cluster_addr" => cluster_addr,
    "cluster_name" => cluster_name,
    "gateway_addr" => gateway_addr,
    "gateway_advertise" => gateway_advertise,
    "gateways" => gateways,
    "routes" => routes,
    "jetstream" => jetstream,
    "jetstream_store_dir" => store_dir,
    "jetstream_ready_timeout" => jetstream_ready_timeout,
    "max_payload" => max_payload,
});

impl Config {
    /// Upstream `enats.NewConfig()`.
    pub fn new() -> Self {
        Self {
            service_addr: crate::nats::DEFAULT_URL.to_owned(),
            cluster_name: "anycable-cluster".to_owned(),
            // seconds
            jetstream_ready_timeout: 30,
            ..Default::default()
        }
    }

    pub fn to_toml(&self) -> String {
        fn flag(out: &mut String, key: &str, on: bool) {
            out.push_str(if on { "" } else { "# " });
            out.push_str(key);
            out.push_str(" = true\n");
        }
        fn optional(out: &mut String, key: &str, value: &str) {
            if value.is_empty() {
                out.push_str(&format!("# {key} = \"\"\n"));
            } else {
                out.push_str(&format!("{key} = \"{value}\"\n"));
            }
        }
        fn list(out: &mut String, key: &str, values: &[String]) {
            if values.is_empty() {
                out.push_str(&format!("# {key} = []\n"));
            } else {
                out.push_str(&format!("{key} = {}\n", string_list(values)));
            }
        }

        let mut out = String::new();
        flag(&mut out, "enabled", self.enabled);
        out.push_str("#\n# Verbose  logging settings\n");
        flag(&mut out, "debug", self.debug);
        flag(&mut out, "trace", self.trace);
        out.push_str("#\n# Service name\n");
        out.push_str(&format!("name = \"{}\"\n", self.name));
        out.push_str("#\n# Service address\n");
        out.push_str(&format!("service_addr = \"{}\"\n", self.service_addr));
        out.push_str("#\n# Cluster configuration\n#\n");
        optional(&mut out, "cluster_addr", &self.cluster_addr);
        if self.cluster_name.is_empty() {
            out.push_str("# cluster_name = \"\"\n");
        } else if self.cluster_addr.is_empty() {
            out.push_str(&format!("# cluster_name = \"{}\"\n", self.cluster_name));
        } else {
            out.push_str(&format!("cluster_name = \"{}\"\n", self.cluster_name));
        }
        optional(&mut out, "gateway_addr", &self.gateway_addr);
        optional(&mut out, "gateway_advertise", &self.gateway_advertise);
        list(&mut out, "gateways", &self.gateways);
        list(&mut out, "routes", &self.routes);
        out.push_str("#\n# JetStream configuration\n#\n");
        flag(&mut out, "jetstream", self.jetstream);
        optional(&mut out, "jetstream_store_dir", &self.store_dir);
        if self.jetstream {
            out.push_str(&format!("jetstream_ready_timeout = {}\n", self.jetstream_ready_timeout));
        } else {
            out.push_str(&format!("# jetstream_ready_timeout = {}\n", self.jetstream_ready_timeout));
        }
        out.push_str("#\n# Maximum message payload size\n#\n");
        if self.max_payload > 0 {
            out.push_str(&format!("max_payload = {}\n", self.max_payload));
        } else {
            out.push_str("# max_payload = 1048576\n");
        }
        out
    }

    /// Decodes TOML into `self`, keeping fields the document omits.
    pub fn decode_toml(&mut self, toml: &str) -> Result<(), EnatsError> {
        cabby_core::toml_config::decode(self, toml).map_err(|e| EnatsError(e.0))?;
        Ok(())
    }
}

/// Embedded NATS service (upstream `enats.Service`).
pub struct Service {
    _private: (),
}

impl Service {
    pub fn new(_config: &Config) -> Self {
        pending!("enats::new_service")
    }

    pub fn start(&self) -> Result<(), EnatsError> {
        pending!("enats::start")
    }

    pub fn wait_ready(&self) -> Result<(), EnatsError> {
        pending!("enats::wait_ready")
    }

    pub fn shutdown(&self) -> Result<(), EnatsError> {
        pending!("enats::shutdown")
    }
}
