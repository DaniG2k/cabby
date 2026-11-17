//! Pusher feature scenarios: public channels with REST triggers, private
//! channel client events, presence channels and the channel users API. The
//! Ruby `Pusher::Client` (pusher-http-ruby < 2.0.4) is replaced by the signed
//! REST requests of `cabby_testkit::pusher_auth`.

use std::time::Duration;

use cabby_testkit::pusher_auth::{channel_auth, get_channel_users, now_timestamp, signed_target};
use cabby_testkit::raw_http;
use cabby_testkit::wsdirector::{Client, Connection, Expect, Protocol, StepError};
use serde_json::json;

use crate::support::*;

fn pusher_client(url: &str, socket_id: &str) -> Result<Client, StepError> {
    wsd(
        url,
        Protocol::Base,
        Connection { headers: vec![("x-request-id".into(), socket_id.into())], ..Default::default() },
    )
}

fn established() -> Expect {
    Expect::Partial(json!({"event": "pusher:connection_established"}))
}

/// upstream: features/pusher.testfile#scenario
/// note: `sleep 3` replaced by waiting for pusher_internal:subscription_succeeded before the REST trigger
/// The upstream "sleep 3 # Wait a bit to ensure connection and subscription"
/// becomes waiting for `pusher_internal:subscription_succeeded` before the
/// REST trigger.
#[test]
fn pusher() {
    let sut = launch(
        &["--pusher_app_id=my-test-id", "--public_streams", "--pusher_secret=oaugc1", "--pusher_app_key=detox"],
        &[],
    );
    let url = ws_url(&sut, "/app/detox");

    let result = (|| -> Result<(), StepError> {
        let mut c = wsd(&url, Protocol::Base, Connection::default())?;
        c.receive(&established())?;
        c.send(&json!({"event": "pusher:subscribe", "data": {"channel": "test-pusher-channel", "auth": ""}}));
        c.receive(&Expect::Partial(json!({"event": "pusher_internal:subscription_succeeded"})))?;

        // pusher_client.trigger("test-pusher-channel", "test-event", {"message":"Hello from Pusha"})
        let body =
            br#"{"name":"test-event","channels":["test-pusher-channel"],"data":"{\"message\":\"Hello from Pusha\"}"}"#;
        let target = signed_target("POST", "/apps/my-test-id/events", "detox", "oaugc1", &now_timestamp(), Some(body));
        let res = raw_http::request(sut.addr(), "POST", &target, &[("Content-Type", "application/json")], Some(body))
            .map_err(|e| StepError::Client(format!("trigger: {e:#}")))?;
        if res.status != 200 {
            return Err(StepError::Client(format!("trigger returned {}: {}", res.status, res.text())));
        }

        c.receive(&Expect::Partial(json!({"event": "test-event", "channel": "test-pusher-channel"})))?;
        Ok(())
    })();

    assert_groups(&[("default", result)]);
}

/// upstream: features/pusher-private.testfile#scenario
/// note: wait_all barriers become sequential steps
#[test]
fn pusher_private() {
    let sut = launch(
        &["--pusher_app_id=my-test-id", "--pusher_app_key=ballard", "--public_streams", "--pusher_secret=oaugc1"],
        &[],
    );
    let url = ws_url(&sut, "/app/ballard");
    let subscribe = |c: &mut Client, socket_id: &str| -> Result<(), StepError> {
        c.receive(&established())?;
        c.send(&json!({
            "event": "pusher:subscribe",
            "data": {"channel": "private-test", "auth": channel_auth("ballard", "oaugc1", &format!("{socket_id}:private-test"))},
        }));
        c.receive(&Expect::Partial(json!({"event": "pusher_internal:subscription_succeeded"}))).map(drop)
    };

    let mut publisher = pusher_client(&url, "2025.1234");
    let mut subscriber = pusher_client(&url, "2025.4343");
    let publisher_result = publisher.as_mut().map_err(|e| e.clone()).and_then(|c| subscribe(c, "2025.1234"));
    let subscriber_result = subscriber.as_mut().map_err(|e| e.clone()).and_then(|c| subscribe(c, "2025.4343"));
    // wait_all: both are subscribed.
    let (publisher_result, subscriber_result) = match (publisher_result, subscriber_result, publisher, subscriber) {
        (Ok(()), Ok(()), Ok(mut p), Ok(mut s)) => {
            p.send(&json!({
                "event": "client-forgotten",
                "channel": "private-test",
                "data": {"body": "funeral_without_applause"},
            }));
            let got = s.receive(&Expect::Exact(json!({
                "event": "client-forgotten",
                "channel": "private-test",
                "data": {"body": "funeral_without_applause"},
            })));
            (Ok(()), got.map(drop))
        }
        (p, s, ..) => (p, s),
    };

    assert_groups(&[("publisher", publisher_result), ("subscriber", subscriber_result)]);
}

/// upstream: features/pusher-presence.testfile#scenario
/// note: wait_all barriers become sequential steps; channel users polled once per second, at most 6 times (inherent polling)
/// The two `wait_all` barriers order the clients: the second connects after
/// the first has joined. The channel users are then polled over the REST API
/// once per second, at most 6 times (upstream: `sleep 1` per poll until both
/// users are seen or more than 5 s have passed), while both stay connected.
#[test]
fn pusher_presence() {
    let api_port = cabby_testkit::ports::free_port();
    let api_flag = format!("--pusher_api_port={api_port}");
    let mut sut = launch(
        &[
            "--pusher_app_id=my-test-id",
            "--pusher_app_key=ballard",
            "--public_streams",
            "--pusher_secret=oaugc1",
            &api_flag,
            "--broker=memory",
        ],
        &[],
    );
    sut.process.wait_tcp(local(api_port), READY_DEADLINE).unwrap_or_else(|e| panic!("{e:#}"));
    let url = ws_url(&sut, "/app/ballard");
    let join = |c: &mut Client, socket_id: &str, user: &str, name: &str, succeeded: &str| -> Result<(), StepError> {
        let channel_data = json!({"user_id": user, "user_info": {"name": name}}).to_string();
        c.receive(&established())?;
        c.send(&json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-test",
                "auth": channel_auth("ballard", "oaugc1", &format!("{socket_id}:presence-test:{channel_data}")),
                "channel_data": channel_data,
            },
        }));
        c.receive(&Expect::Partial(json!({"event": "pusher_internal:subscription_succeeded", "data": succeeded})))
            .map(drop)
    };

    let mut first = pusher_client(&url, "2025.123");
    let first_joined = first.as_mut().map_err(|e| e.clone()).and_then(|c| {
        join(c, "2025.123", "312", "Vova", r#"{"presence":{"count":1,"ids":["312"],"hash":{"312":{"name":"Vova"}}}}"#)
    });
    // wait_all
    let mut second = pusher_client(&url, "2025.321");
    let second_result = second.as_mut().map_err(|e| e.clone()).and_then(|c| {
        join(
            c,
            "2025.321",
            "42",
            "Lolo",
            r#"{"presence":{"count":2,"ids":["312","42"],"hash":{"312":{"name":"Vova"},"42":{"name":"Lolo"}}}}"#,
        )
    });
    let first_result = first_joined.and_then(|()| {
        first.as_mut().map_err(|e| e.clone())?.receive(&Expect::Partial(json!({
            "event": "pusher_internal:member_added",
            "channel": "presence-test",
            "data": r#"{"user_id":"42","user_info":{"name":"Lolo"}}"#,
        })))
    });

    let mut user_ids = std::collections::BTreeSet::new();
    for attempt in 0..6 {
        if attempt > 0 {
            std::thread::sleep(Duration::from_secs(1));
        }
        let users = get_channel_users("my-test-id", "detox", "oaugc1", "presence-test", |method, target| {
            let res = raw_http::request(local(api_port), method, target, &[], None).expect("channel users request");
            (res.status, res.body)
        });
        user_ids.extend(users.unwrap_or_else(|e| panic!("channel_users: {e}")));
        if user_ids.len() == 2 {
            break;
        }
    }

    assert_groups(&[("first", first_result.map(drop)), ("second", second_result)]);
    let expected: std::collections::BTreeSet<String> = ["312".to_owned(), "42".to_owned()].into();
    assert_eq!(expected, user_ids, "Channel users fetched over API don't match");
    drop((first, second));
}
