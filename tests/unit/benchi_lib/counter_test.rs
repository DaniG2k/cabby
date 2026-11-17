//! upstream file: etc/benchi/lib/counter_test.go

use cabby_testkit::reference::benchi::Accumulator;

/// upstream: etc/benchi/lib/counter_test.go#TestAccumulator_RaceClean
/// note: adaptation: go test -race is replaced by Sync types (data races are compile errors) plus the exact-total check under 100 concurrent threads.
///
/// Runtime-specific adaptation: upstream relies on `go test -race` to flag data races.
/// Safe Rust rules them out at compile time (`Accumulator: Sync`); the test keeps the
/// concurrent workload and the lost-update check (the total must be exact).
#[test]
fn test_accumulator_race_clean() {
    let acc = Accumulator::new();

    const THREADS: usize = 100;
    const BUMPS_PER_THREAD: usize = 1000;

    std::thread::scope(|scope| {
        for g in 0..THREADS {
            let acc = &acc;
            scope.spawn(move || {
                let id = (g % 10).to_string(); // 10 distinct client IDs
                for _ in 0..BUMPS_PER_THREAD {
                    acc.bump(&id);
                }
            });
        }
    });

    let snap = acc.snapshot();
    let total: i64 = snap.values().sum();
    assert_eq!((THREADS * BUMPS_PER_THREAD) as i64, total);
}

/// upstream: etc/benchi/lib/counter_test.go#TestAccumulator_SnapshotIsCopy
/// note: Go: the map Snapshot() returns stays behind a bridge handle and the writes go to it (op map.Set), so non-sharing is really observed.
#[test]
fn test_accumulator_snapshot_is_copy() {
    let acc = Accumulator::new();
    acc.bump("a");
    acc.bump("a");
    acc.bump("b");

    let mut snap1 = acc.snapshot_to_mutate();
    snap1.insert("a", 999);
    snap1.insert("c", 42);

    let snap2 = acc.snapshot();
    assert_eq!(Some(&2), snap2.get("a"), "Snapshot must not share storage with future snapshots");
    assert_eq!(Some(&1), snap2.get("b"));
    assert!(!snap2.contains_key("c"), "mutating one snapshot must not leak into the next");
}
