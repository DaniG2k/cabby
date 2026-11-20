//! Client messages nested deeply, observed from outside (review 2026-11, Low):
//! Go's `encoding/json` decodes documents nested up to 10000 levels and rejects
//! deeper ones, which ends the session's reader (upstream `Serve` returns on a
//! decode error). Cabby used to stop reading at serde_json's 128 levels.

use std::time::Duration;

use cabby_testkit::cable_client::{CableClient, ClientError, Options};
use cabby_testkit::sut::Sut;
use serde_json::json;

/// Go's `encoding/json` nesting limit.
const GO_MAX_DEPTH: usize = 10_000;

fn connect(sut: &Sut) -> CableClient {
    let opts = Options { ignore: vec!["ping".into()], ..Options::default() };
    let mut c = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    assert_eq!(Some("welcome"), c.receive().expect("welcome")["type"].as_str());
    c
}

/// A `message` command whose `data` makes the whole document `depth` levels deep
/// (the command object is the first level).
fn nested_message(depth: usize) -> String {
    let identifier = json!({"channel": "$pubsub", "stream_name": "deep"}).to_string();
    let inner = depth - 1;
    format!(
        r#"{{"command":"message","identifier":{},"data":{}{}}}"#,
        serde_json::to_string(&identifier).expect("identifier"),
        "[".repeat(inner),
        "]".repeat(inner)
    )
}

fn subscribe_public(c: &mut CableClient, stream: &str) {
    let identifier = json!({"channel": "$pubsub", "stream_name": stream}).to_string();
    c.send(&json!({"command": "subscribe", "identifier": identifier}));
}

/// external-for: encoders/json_test.go#TestJSONEncoder/.Decode
/// note: Messages nested 129 and 10000 levels deep are decoded and handled (the perform fails: its data is not a string); the session keeps reading, so a following $pubsub subscription is confirmed.
#[test]
fn messages_nested_up_to_go_limit_are_processed() {
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let mut c = connect(&sut);

    for depth in [129, 1000, GO_MAX_DEPTH] {
        c.send_raw(&nested_message(depth));
        let stream = format!("after-{depth}");
        subscribe_public(&mut c, &stream);
        let ack = c.receive().unwrap_or_else(|e| panic!("no reply after a message {depth} levels deep: {e}"));
        assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
        assert!(ack["identifier"].as_str().unwrap_or_default().contains(&stream), "unexpected reply {ack}");
    }
}

/// Threads of process `pid` named `name` (Linux procfs; 0 elsewhere).
fn threads_named(pid: u32, name: &str) -> usize {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else { return 0 };
    tasks
        .filter_map(Result::ok)
        .filter(|t| std::fs::read_to_string(t.path().join("comm")).is_ok_and(|c| c.trim_end() == name))
        .count()
}

/// Cabby's cap on concurrent deep-nesting threads (`MAX_CONCURRENT_DEEP_THREADS`).
const MAX_DEEP_THREADS: usize = 4;

/// regression-for: encoders/json_test.go#TestJSONEncoder/.Decode
/// note: Deviation D-056: many sessions sending messages nested 10000 levels deep at once all keep working (a following subscription is confirmed). On Cabby, at most 4 deep-decoding threads (`cabby-deep-json`, 64 MiB stacks) run at once; a deep message that finds them busy for 250 ms is dropped with a warning instead of being handled, and its session keeps reading. Go decodes every one on its goroutine (the thread check does not apply).
#[test]
fn deviation_d056_concurrent_deep_messages_are_capped() {
    const SESSIONS: usize = 24;
    const MESSAGES: usize = 3;
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let pid = sut.process.pid();
    let mut clients: Vec<CableClient> = (0..SESSIONS).map(|_| connect(&sut)).collect();

    // Count the deep threads while the messages are being handled.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler = {
        let done = done.clone();
        std::thread::spawn(move || {
            let mut peak = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                peak = peak.max(threads_named(pid, "cabby-deep-json"));
                std::thread::yield_now();
            }
            peak
        })
    };

    let message = nested_message(GO_MAX_DEPTH);
    for _ in 0..MESSAGES {
        for c in &mut clients {
            c.send_raw(&message);
        }
    }
    for (i, c) in clients.iter_mut().enumerate() {
        let stream = format!("after-{i}");
        subscribe_public(c, &stream);
        // Generous: 72 messages 10,000 levels deep are decoded 4 at a time, which
        // takes a while in debug builds on a loaded machine; the cap is what's tested.
        let ack = c
            .receive_within(Duration::from_secs(60))
            .unwrap_or_else(|e| panic!("session {i}: no reply after deep messages: {e}"));
        assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "session {i}: {ack}");
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let peak = sampler.join().expect("sampler");

    if cabby_testkit::backend() == cabby_testkit::Backend::Rust {
        // At most MAX_DEEP_THREADS decodes run at once: a slot is released only
        // after `join` returns. `pthread_join` returns when the exiting thread has
        // signalled its exit, slightly before the kernel drops its
        // /proc/<pid>/task entry, so under load the sampler can see one finishing
        // thread next to MAX_DEEP_THREADS new ones.
        assert!(peak <= MAX_DEEP_THREADS + 1, "{peak} deep-nesting threads at once");
    }
}

/// external-for: encoders/json_test.go#TestJSONEncoder/.Decode
/// note: A message nested 10001 levels deep fails to decode (Go: `exceeded max depth`), which ends the session's reader as any decode error does: a following subscription gets no reply within AnyT's 0.5s window.
#[test]
fn messages_nested_deeper_than_go_limit_stop_the_reader() {
    let sut = Sut::start(&["--public", "--broadcast_adapter=http", "--disconnect_mode=never"]);
    let mut c = connect(&sut);

    c.send_raw(&nested_message(GO_MAX_DEPTH + 1));
    subscribe_public(&mut c, "after");
    match c.receive_within(Duration::from_millis(500)) {
        Err(ClientError::Timeout) => {}
        other => panic!("expected no reply, got {other:?}"),
    }
}
