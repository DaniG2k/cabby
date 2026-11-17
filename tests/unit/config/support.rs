//! Test fixtures: temporary directories (Go `t.TempDir()`) and environment maps.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use cabby_server::config::EnvMap;

/// A fresh directory removed on drop (Go `t.TempDir()`).
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("cabby-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("creating a temporary directory");
        TempDir(dir)
    }

    /// Writes `contents` to `<dir>/<name>` and returns the path.
    pub fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, contents).expect("require.NoError(os.WriteFile(...))");
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Builds an environment from `KEY, value` pairs (upstream `setEnv(pairs...)`).
pub fn env(pairs: &[(&str, &str)]) -> EnvMap {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}
