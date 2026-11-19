//! `cabby` executable (upstream `cmd/anycable-go/main.go`).
//!
//! The process arguments and environment are read here only; the library takes
//! them explicitly.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

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

/// The Go `log` package prefix (`2006/01/02 15:04:05`), in UTC where Go uses the
/// local time zone (deviation D-014).
fn log_timestamp() -> String {
    format_log_timestamp(SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()))
}

fn format_log_timestamp(secs: u64) -> String {
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let rem = secs % 86_400;

    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);

    format!("{year:04}/{month:02}/{day:02} {:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins D-014: the fatal error prefix is the UTC time in Go's `log` format.
    #[test]
    fn deviation_d014_fatal_log_timestamp_is_utc() {
        assert_eq!(format_log_timestamp(0), "1970/01/01 00:00:00");
        // 2000-02-29T01:01:01Z
        assert_eq!(format_log_timestamp(951_786_061), "2000/02/29 01:01:01");
        // 2026-11-18T11:06:40Z
        assert_eq!(format_log_timestamp(1_795_000_000), "2026/11/18 11:06:40");
    }
}
