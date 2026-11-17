//! upstream file: rpc/grpc_test.go
//!
//! Upstream runs mock gRPC servers on fixed ports (50051, 50151, 50152) with
//! `MaxConcurrentStreams(1)`; here each test runs [`FakeRpc`]s on free ports with
//! the same stream limit and points `config.host` at them.

use cabby_core::common::{SessionEnv, Status};
use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, FakeRpcOptions, Method, Reply};
use cabby_testkit::reference::metrics::Metrics;
use cabby_testkit::reference::rpc::*;

use super::helpers::{map, strings};

fn run_grpc_server() -> FakeRpc {
    FakeRpc::start_with(FakeRpcOptions { grpc_max_concurrent_streams: Some(1) })
}

fn session() -> SessionEnv {
    SessionEnv {
        url: "http://test.cable".into(),
        headers: Some(map(&[("cookie", "token=secret;")])),
        ..Default::default()
    }
}

fn counter(metrics: &Metrics, name: &str) -> u64 {
    metrics.counter(name).expect("registered counter").value()
}

mod test_grpc_integration {
    use super::*;

    /// The upstream mock service: Connect is fast and successful; Disconnect is
    /// slow (held at `slow` here, upstream `.After(1s)`); Command emulates
    /// exhaustion once, then succeeds.
    fn service(slow: &cabby_testkit::sync::Gate) -> FakeRpc {
        let service = run_grpc_server();
        service
            .on(Method::Connect)
            .reply(protos::ConnectionResponse { status: protos::Status::Success as i32, ..Default::default() });
        service
            .on(Method::Disconnect)
            .held_by(slow)
            .reply(protos::DisconnectResponse { status: protos::Status::Success as i32, ..Default::default() });
        service.on(Method::Command).once().reply(Reply::status(Code::ResourceExhausted, "Request limit exceeded."));
        service.on(Method::Command).reply(protos::CommandResponse {
            status: protos::Status::Success as i32,
            transmissions: strings(&["ok"]),
            ..Default::default()
        });
        service
    }

    /// upstream: rpc/grpc_test.go#TestGRPCIntegration/Success
    #[test]
    fn success() {
        let slow = cabby_testkit::sync::Gate::new();
        let service = service(&slow);
        // Upstream's Disconnect replies after 1s (`.After(1 * time.Second)`): a
        // slow Disconnect still succeeds under the default timeouts (no request
        // timeout, 3s command timeout). The reply is released after that delay;
        // the elapsed time is the behavior here, not a synchronization point.
        let releaser = {
            let slow = slow.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                slow.open();
            })
        };
        let metrics = Metrics::new(0);

        let mut config = new_config();
        config.host = service.grpc_target();
        let controller = Controller::new(&metrics, &config, Dial::Config).expect("controller");
        controller.start().expect("start");

        let rpc_calls = counter(&metrics, metric_names::RPC_CALLS);

        let out = controller.authenticate(&Ctx::background(), "test", &session());

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);

        let err = controller.disconnect(&Ctx::background(), "test", &session(), "", &[]);
        assert_eq!(Ok(()), err);

        assert_eq!(rpc_calls + 2, counter(&metrics, metric_names::RPC_CALLS));
        let _ = controller.shutdown();
        releaser.join().expect("releaser thread");
    }

    fn timeout_case() -> (Metrics, [u64; 3], Result<(), RpcError>) {
        let slow = cabby_testkit::sync::Gate::new();
        let service = service(&slow);
        let metrics = Metrics::new(0);

        let mut config = new_config();
        config.host = service.grpc_target();
        config.request_timeout = 500;
        config.command_timeout = 500;
        let controller = Controller::new(&metrics, &config, Dial::Config).expect("controller");
        controller.start().expect("start");

        let before = [
            counter(&metrics, metric_names::RPC_CALLS),
            counter(&metrics, metric_names::RPC_TIMEOUTS),
            counter(&metrics, metric_names::RPC_FAILURES),
        ];

        let err = controller.disconnect(&Ctx::background(), "test", &session(), "", &[]);
        let _ = controller.shutdown();
        (metrics, before, err)
    }

    /// upstream: rpc/grpc_test.go#TestGRPCIntegration/Timeout
    #[test]
    fn timeout() {
        let (metrics, [rpc_calls, rpc_timeouts, rpc_errors], err) = timeout_case();

        // Upstream `require.Error(t, err, "deadline exceeded")`: non-nil check only
        // (the string is the failure message); the code is checked by
        // `timeout_is_deadline_exceeded`.
        assert!(err.is_err(), "deadline exceeded");

        assert_eq!(rpc_calls + 1, counter(&metrics, metric_names::RPC_CALLS));
        assert_eq!(rpc_timeouts + 1, counter(&metrics, metric_names::RPC_TIMEOUTS));
        assert_eq!(rpc_errors + 1, counter(&metrics, metric_names::RPC_FAILURES));
    }

    /// regression-for: rpc/grpc_test.go#TestGRPCIntegration/Timeout
    /// The intended check: the call fails with DEADLINE_EXCEEDED.
    #[test]
    fn timeout_is_deadline_exceeded() {
        let (_, _, err) = timeout_case();
        match err {
            Err(RpcError::Status(s)) => assert_eq!(Code::DeadlineExceeded, s.code),
            other => panic!("expected a DeadlineExceeded status, got {other:?}"),
        }
    }

    /// upstream: rpc/grpc_test.go#TestGRPCIntegration/Exhausted
    #[test]
    fn exhausted() {
        let slow = cabby_testkit::sync::Gate::new();
        let service = service(&slow);
        let metrics = Metrics::new(0);

        let mut config = new_config();
        config.host = service.grpc_target();
        let controller = Controller::new(&metrics, &config, Dial::Config).expect("controller");
        controller.start().expect("start");

        let rpc_calls = counter(&metrics, metric_names::RPC_CALLS);
        let rpc_retries = counter(&metrics, metric_names::RPC_RETRIES);

        let out = controller.perform(&Ctx::background(), "test", &session(), "", "test", "data");

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
        assert_eq!(strings(&["ok"]), res.transmissions);

        assert_eq!(rpc_calls + 1, counter(&metrics, metric_names::RPC_CALLS));
        assert_eq!(rpc_retries + 1, counter(&metrics, metric_names::RPC_RETRIES));
        let _ = controller.shutdown();
    }
}

fn multi_host() -> (FakeRpc, FakeRpc, Controller) {
    let service = run_grpc_server();
    let service2 = run_grpc_server();
    // Upstream also delays each reply by 10µs (`.After(10 * time.Microsecond)`)
    // and counts calls in `.Run` callbacks; the delay asserts nothing and is
    // dropped, and the fakes' call logs replace the counters.
    for s in [&service, &service2] {
        s.on(Method::Connect)
            .reply(protos::ConnectionResponse { status: protos::Status::Success as i32, ..Default::default() });
    }

    let mut c = new_config();
    c.host = format!("grpc-list://{},{}", service.grpc_target(), service2.grpc_target());
    let controller = Controller::new(&Metrics::new(0), &c, Dial::Config).expect("controller");
    controller.start().expect("start");

    // Warm up the gRPC client (init channel connections)
    let out = controller.authenticate(&Ctx::background(), "test", &session());
    assert!(out.error.is_none(), "{:?}", out.error);
    (service, service2, controller)
}

/// upstream: rpc/grpc_test.go#TestGRPCMultiHost
///
/// Upstream requires `controller.clientState.ActiveConns() == 2` within 2s before
/// counting calls. Go exposes that count only through the `grpc_active_conn_num`
/// gauge, which the controller refreshes 10s after `Start` (and every 10s after
/// that), so the wait allows 15s.
#[test]
fn test_grpc_multi_host() {
    let (service, service2, controller) = multi_host();

    super::helpers::eventually("both gRPC subchannels did not become ready", ACTIVE_CONNS_DEADLINE, || {
        controller.active_conns() == 2
    });

    let service1_before = service.call_count(Method::Connect);
    let service2_before = service2.call_count(Method::Connect);

    for _ in 0..10 {
        let out = controller.authenticate(&Ctx::background(), "test", &session());

        assert!(out.error.is_none(), "{:?}", out.error);
        let res = out.result.expect("result");
        assert_eq!(Status::Success, res.status);
    }

    assert!(service.call_count(Method::Connect) - service1_before > 0);
    assert!(service2.call_count(Method::Connect) - service2_before > 0);
    let _ = controller.shutdown();
}

/// Upstream: 2s. Go's exported view refreshes 10s after start (see above).
const ACTIVE_CONNS_DEADLINE: std::time::Duration = std::time::Duration::from_secs(15);
