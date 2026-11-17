//! upstream file: sse/connection_test.go
//!
//! The connection writes to an `httptest.ResponseRecorder`; `testResponse` and
//! `testStatus` are the upstream test helpers reading it.

use std::time::{Duration, Instant};

use cabby_testkit::reference::sse::Connection;

/// `ws.CloseNormalClosure`.
const CLOSE_NORMAL_CLOSURE: i32 = 1000;

/// upstream: sse/connection_test.go#TestConnection_Write
#[test]
fn test_connection_write() {
    let c = Connection::new();

    let msg = b"hello, world!";
    let err = c.write(msg, Instant::now() + Duration::from_secs(1));

    assert!(err.is_ok(), "{err:?}");

    assert!(c.test_response().is_empty());

    c.established();

    assert_eq!(200, c.test_status());
    assert_eq!("hello, world!\n\n", c.test_response());
}

mod test_connection_close {
    use super::*;

    /// upstream: sse/connection_test.go#TestConnection_Close/Close_cancels_the_context
    /// note: Go has no accessor for the unexported `c.done`; on Go it is probed indirectly (a closed connection drops a later Write, so Established leaves the body nil). The second Close runs on Go too.
    #[test]
    fn close_cancels_the_context() {
        // Create a new connection
        let c = Connection::new();

        let ctx = c.context();

        c.close(CLOSE_NORMAL_CLOSURE, "bye");

        // `<-ctx.Done()`, bounded.
        assert!(ctx.wait_done(Duration::from_secs(2)), "context not cancelled");
        assert!(c.is_done());

        c.close(CLOSE_NORMAL_CLOSURE, "bye");
    }
}

/// upstream: sse/connection_test.go#TestConnection_WriteBinary
#[test]
fn test_connection_write_binary() {
    let c = Connection::new();

    let msg = [0x01u8, 0x02, 0x03];
    let err = c.write_binary(&msg, Instant::now() + Duration::from_secs(1));

    assert!(err.is_err());
    // `[]byte(nil)`: nothing was ever written to the recorder.
    assert_eq!(None, c.body_bytes());
}

/// upstream: sse/connection_test.go#TestConnection_Read
#[test]
fn test_connection_read() {
    let c = Connection::new();

    let (msg, err) = c.read();

    assert!(err.is_some());
    assert_eq!(None, msg);
}

/// upstream: sse/connection_test.go#TestConnection_Descriptor
#[test]
fn test_connection_descriptor() {
    let c = Connection::new();

    assert!(c.descriptor_is_none());
}
