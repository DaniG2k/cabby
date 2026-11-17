//! Management API publish endpoint (`--api_secret`; served on the main port),
//! observed externally: request method, bearer authentication and acceptance.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use cabby_testkit::sut::Sut;

const SECRET: &str = "api-s3cret";
const PAYLOAD: &str = r#"{"stream":"test_stream","data":"test_data"}"#;

/// Sends one HTTP/1.1 request and returns the response status code.
fn status(sut: &Sut, method: &str, path: &str, headers: &[(&str, &str)], body: &str) -> u16 {
    let io = || -> std::io::Result<String> {
        let mut stream = TcpStream::connect_timeout(&sut.addr(), Duration::from_secs(5))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
            sut.addr(),
            body.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        req.push_str(body);
        stream.write_all(req.as_bytes())?;
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line)?;
        Ok(line)
    };
    let line = io().unwrap_or_else(|e| {
        sut.process.propagate_pending();
        panic!("API request failed: {e}\nSUT output:\n{}", sut.process.output())
    });
    line.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or_else(|| panic!("bad status line {line:?}"))
}

fn start() -> Sut {
    Sut::start(&["--noauth", &format!("--api_secret={SECRET}")])
}

/// external-for: api/publish_test.go#TestPublishHandler/Accepts_when_authorization_header_is_valid
/// external-for: api/auth_test.go#TestAuthenticator/Authenticate_with_valid_Bearer_token
#[test]
fn publish_accepts_valid_bearer_token() {
    let sut = start();
    let bearer = format!("Bearer {SECRET}");
    let headers = [("Authorization", bearer.as_str()), ("Content-Type", "application/json")];
    assert_eq!(201, status(&sut, "POST", "/api/publish", &headers, PAYLOAD));
}

/// external-for: api/auth_test.go#TestAuthenticator/Authenticate_rejects_missing_Authorization_header
/// external-for: api/auth_test.go#TestAuthenticator/Authenticate_rejects_invalid_token
/// external-for: api/auth_test.go#TestAuthenticator/Authenticate_rejects_non-Bearer_auth
#[test]
fn publish_rejects_bad_credentials() {
    let sut = start();
    assert_eq!(401, status(&sut, "POST", "/api/publish", &[], PAYLOAD));
    assert_eq!(401, status(&sut, "POST", "/api/publish", &[("Authorization", "Bearer wrong-secret")], PAYLOAD));
    assert_eq!(401, status(&sut, "POST", "/api/publish", &[("Authorization", "Basic dXNlcjpwYXNz")], PAYLOAD));
}

/// external-for: api/publish_test.go#TestPublishHandler/Rejects_non-POST_requests
#[test]
fn publish_rejects_non_post_requests() {
    let sut = start();
    let bearer = format!("Bearer {SECRET}");
    assert_eq!(422, status(&sut, "GET", "/api/publish", &[("Authorization", bearer.as_str())], ""));
}
