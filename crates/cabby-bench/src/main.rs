//! `cabby-bench` executable: `cabby-bench <scenario> [flags]`, where scenario is
//! `stress_publications`, `reconnect_floods` (upstream: one binary per scenario) or
//! `soak` (Cabby's memory-stability run).
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
        "soak" => cabby_bench::scenarios::soak::run(&rest, &mut stdout, &mut stderr),
        _ => {
            eprintln!("usage: cabby-bench <stress_publications|reconnect_floods|soak> [flags]  (-h: scenario flags)");
            2
        }
    };
    let _ = std::io::Write::flush(&mut stdout);
    std::process::exit(code);
}
