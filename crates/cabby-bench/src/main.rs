//! `cabby-bench` executable: `cabby-bench <scenario> [flags]`, where scenario is
//! `stress_publications` or `reconnect_floods` (upstream: one binary per scenario).
//! Exit codes and the key=value stdout contract follow the upstream scenarios.

fn main() {
    let mut args = std::env::args().skip(1);
    let scenario = args.next().unwrap_or_default();
    let rest: Vec<String> = args.collect();
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();
    let code = match scenario.as_str() {
        "stress_publications" => cabby_bench::scenarios::stress_publications::run(
            &rest,
            &mut stdout,
            &mut stderr,
            cabby_bench::scenarios::stress_publications::RunOpts::default(),
        ),
        "reconnect_floods" => cabby_bench::scenarios::reconnect_floods::run(&rest, &mut stdout, &mut stderr),
        _ => {
            eprintln!("usage: cabby-bench <stress_publications|reconnect_floods> [flags]");
            2
        }
    };
    std::process::exit(code);
}
