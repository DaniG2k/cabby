//! Cabby's identity as a product, observed through the server binary: nothing
//! phones home (D-058), and Cabby's own configuration names (D-061).

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use cabby_testkit::fake_http::HttpRecorder;
use cabby_testkit::sut::Sut;

const IO_DEADLINE: Duration = Duration::from_secs(5);

/// Plain HTTP/1.1 GET; returns the response status code.
fn get_status(sut: &Sut, path: &str) -> u16 {
    let result = (|| -> std::io::Result<String> {
        let mut stream = TcpStream::connect_timeout(&sut.addr(), IO_DEADLINE)?;
        stream.set_read_timeout(Some(IO_DEADLINE))?;
        stream.set_write_timeout(Some(IO_DEADLINE))?;
        write!(stream, "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", sut.addr())?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        Ok(line)
    })();
    let line = result.unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("GET {path} failed: {e}\nSUT output:\n{}", sut.process.output())
    });
    line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or_else(|| panic!("bad status line {line:?}"))
}

/// Waits (bounded) until the server's output contains `needle`; returns the output.
#[allow(dead_code)] // used by the D-061 scenario
fn wait_output(sut: &Sut, needle: &str) -> String {
    let until = Instant::now() + IO_DEADLINE;
    loop {
        let out = sut.process.output();
        if out.contains(needle) || Instant::now() >= until {
            return out;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Pins D-058: with telemetry enabled and its endpoint pointing at a local
/// recorder, the server sends nothing and logs nothing about telemetry. Go
/// passes too: the reference binary is built without a telemetry token.
/// external-for: telemetry/telemetry_test.go#TestTracking
#[test]
fn deviation_d058_no_telemetry_requests() {
    let recorder = HttpRecorder::start();
    let url = recorder.url();
    let mut sut = Sut::try_start(
        &["--noauth"],
        &[
            ("ANYCABLE_DISABLE_TELEMETRY", "false"),
            ("ANYCABLE_TELEMETRY_URL", url.as_str()),
            ("ANYCABLE_TELEMETRY_DEBUG", "1"),
        ],
    )
    .unwrap_or_else(|e| panic!("{e:#}"));
    assert_eq!(200, get_status(&sut, "/health"));
    // A boot event would be posted right after start: give it time to arrive.
    std::thread::sleep(Duration::from_millis(500));
    sut.process.stop(Duration::from_secs(5)).unwrap_or_else(|e| panic!("stopping the server: {e:#}"));
    let out = sut.process.output();
    assert_eq!(recorder.requests().len(), 0, "telemetry requests were sent; output:\n{out}");
    assert!(!out.to_lowercase().contains("telemetry"), "{out}");
}
