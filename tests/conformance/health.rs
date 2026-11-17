//! Health endpoint (`--health-path`), observed externally: `GET` answers 200.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

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

/// external-for: server/health_handler_test.go#TestHealthHandler
#[test]
fn health_endpoint_answers_ok() {
    let sut = Sut::start(&["--noauth"]);
    assert_eq!(200, get_status(&sut, "/health"));

    let custom = Sut::start(&["--noauth", "--health-path=/healthz"]);
    assert_eq!(200, get_status(&custom, "/healthz"));
}
