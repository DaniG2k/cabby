//! Container health probe: exits 0 when `GET <health path>` on the local
//! server answers 200 (see `cabby_server::healthcheck`).

use std::process::ExitCode;

fn main() -> ExitCode {
    match cabby_server::healthcheck::run(|k| std::env::var(k).ok()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cabby-healthcheck: {e}");
            ExitCode::FAILURE
        }
    }
}
