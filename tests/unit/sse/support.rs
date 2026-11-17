//! Test doubles for `sse/handler_test.go`.

use std::collections::HashMap;
use std::sync::Mutex;

use cabby_core::common::{CommandResult, ConnectResult, SessionEnv};
use cabby_server::pusher_sse_deps::{CallResult, Controller, Error};

/// `mocks.Controller` with testify semantics: calls must match an expectation set
/// with `on_*` (argument `mock.Anything` where upstream uses it), unexpected calls
/// fail. `Shutdown` and `Disconnect` are stubbed to return nil, as in
/// `TestSSEHandler`.
#[derive(Default)]
pub struct MockController {
    authenticate: Mutex<HashMap<String, ConnectResult>>,
    subscribe: Mutex<HashMap<(String, String, String), CommandResult>>,
    subscribe_calls: Mutex<Vec<(String, String, String)>>,
}

impl MockController {
    /// `On("Authenticate", mock.Anything, sid, mock.Anything).Return(res, nil)`.
    pub fn on_authenticate(&self, sid: &str, res: ConnectResult) {
        self.authenticate.lock().unwrap().insert(sid.to_owned(), res);
    }

    /// `On("Subscribe", mock.Anything, sid, mock.Anything, ids, identifier).Return(res, nil)`.
    pub fn on_subscribe(&self, sid: &str, ids: &str, identifier: &str, res: CommandResult) {
        self.subscribe.lock().unwrap().insert((sid.to_owned(), ids.to_owned(), identifier.to_owned()), res);
    }

    /// `AssertCalled(t, "Subscribe", mock.Anything, sid, mock.Anything, ids, identifier)`.
    pub fn assert_called_subscribe(&self, sid: &str, ids: &str, identifier: &str) {
        let key = (sid.to_owned(), ids.to_owned(), identifier.to_owned());
        let calls = self.subscribe_calls.lock().unwrap();
        assert!(calls.contains(&key), "Subscribe{key:?} was not called; calls: {calls:?}");
    }
}

impl Controller for MockController {
    fn start(&self) -> Result<(), Error> {
        panic!("mocks.Controller: unexpected call Start()")
    }

    fn shutdown(&self) -> Result<(), Error> {
        Ok(())
    }

    fn authenticate(&self, sid: &str, _env: &SessionEnv) -> CallResult<ConnectResult> {
        match self.authenticate.lock().unwrap().get(sid) {
            Some(res) => CallResult { result: Some(res.clone()), error: None },
            None => panic!("mocks.Controller: unexpected call Authenticate(_, {sid:?}, _)"),
        }
    }

    fn subscribe(&self, sid: &str, _env: &SessionEnv, ids: &str, channel: &str) -> CallResult<CommandResult> {
        let key = (sid.to_owned(), ids.to_owned(), channel.to_owned());
        self.subscribe_calls.lock().unwrap().push(key.clone());
        match self.subscribe.lock().unwrap().get(&key) {
            Some(res) => CallResult { result: Some(res.clone()), error: None },
            None => panic!("mocks.Controller: unexpected call Subscribe(_, {sid:?}, _, {ids:?}, {channel:?})"),
        }
    }

    fn unsubscribe(&self, sid: &str, _: &SessionEnv, ids: &str, channel: &str) -> CallResult<CommandResult> {
        panic!("mocks.Controller: unexpected call Unsubscribe(_, {sid:?}, _, {ids:?}, {channel:?})")
    }

    fn perform(&self, sid: &str, _: &SessionEnv, _: &str, channel: &str, _: &str) -> CallResult<CommandResult> {
        panic!("mocks.Controller: unexpected call Perform(_, {sid:?}, _, _, {channel:?}, _)")
    }

    fn disconnect(&self, _: &str, _: &SessionEnv, _: &str, _: &[String]) -> Result<(), Error> {
        Ok(())
    }
}
