//! `cabby` executable (upstream `cmd/anycable-go/main.go`).
//!
//! The process arguments and environment are read here only; the library takes
//! them explicitly.

use std::process::ExitCode;

use cabby_core::utils::LocalTime;
use cabby_server::cli;
use cabby_server::config::EnvMap;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args_os().map(|a| a.to_string_lossy().into_owned()).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let env: EnvMap = std::env::vars_os()
        .map(|(k, v)| (k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
        .collect();

    let config = match cli::new_config_from_cli(&args, &env, vec![]) {
        Err(err) => {
            // Go `log.Fatalf("%v", err)`.
            eprintln!("{} {err}", log_timestamp());
            return ExitCode::from(1);
        }
        Ok((_, true)) => return ExitCode::SUCCESS,
        Ok((config, false)) => config,
    };

    match cli::run(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            println!("{err}");
            ExitCode::from(1)
        }
    }
}

/// The Go `log` package prefix (`2006/01/02 15:04:05`) in the local time zone.
fn log_timestamp() -> String {
    format_log_timestamp(&LocalTime::now())
}

fn format_log_timestamp(t: &LocalTime) -> String {
    format!("{:04}/{:02}/{:02} {:02}:{:02}:{:02}", t.year, t.month, t.day, t.hour, t.minute, t.second)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go's `log` prefix, in the local zone.
    #[test]
    fn fatal_log_timestamp_is_local() {
        assert_eq!(format_log_timestamp(&LocalTime::at(0, 0, 0)), "1970/01/01 00:00:00");
        assert_eq!(format_log_timestamp(&LocalTime::at(951_786_061, 0, 0)), "2000/02/29 01:01:01");
        // 2026-11-18T11:06:40Z in Bangkok.
        assert_eq!(format_log_timestamp(&LocalTime::at(1_795_000_000, 0, 7 * 3600)), "2026/11/18 18:06:40");
    }
}
