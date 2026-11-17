//! upstream file: config/presets_test.go
//!
//! Upstream sets process environment variables (`setEnv`) around each test; here
//! the environment is an explicit map (against Go: the environment of a dedicated
//! bridge process), so nothing global is mutated. Tests without `setEnv` use an
//! empty environment.

use cabby_server::config::EnvMap;
use cabby_testkit::reference::config::{load_presets, new_config, presets};

use crate::support::env;

fn fly_env() -> EnvMap {
    env(&[("FLY_APP_NAME", "any-test"), ("FLY_REGION", "mag"), ("FLY_ALLOC_ID", "1234")])
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// upstream: config/presets_test.go#TestNoPresets
#[test]
fn test_no_presets() {
    let mut config = new_config();

    let err = load_presets(&mut config, &EnvMap::new());

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("localhost", config.server.host);
}

/// upstream: config/presets_test.go#TestFlyPresets
#[test]
fn test_fly_presets() {
    let mut env = fly_env();
    env.insert("ANYCABLE_FLY_RPC_APP_NAME".into(), "anycable-web".into());

    let mut config = new_config();

    config.server.port = 8989;

    assert_eq!(strings(&["fly"]), presets(&config, &env));

    let err = load_presets(&mut config, &env);

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("0.0.0.0", config.server.host);
    assert_eq!(8989, config.server.port);
    assert_eq!(8989, config.http_broadcast.port);
    assert!(config.embedded_nats.enabled);
    assert_eq!("nats", config.pubsub_adapter);
    assert_eq!("", config.broker.adapter);
    assert!(config.embedded_nats.name.contains("fly-mag-1234-"), "{}", config.embedded_nats.name);
    assert_eq!("nats://0.0.0.0:4222", config.embedded_nats.service_addr);
    assert_eq!("nats://0.0.0.0:5222", config.embedded_nats.cluster_addr);
    assert_eq!("any-test-mag-cluster", config.embedded_nats.cluster_name);
    assert_eq!(strings(&["nats://mag.any-test.internal:5222"]), config.embedded_nats.routes);
    assert_eq!("dns:///mag.anycable-web.internal:50051", config.rpc.host);
}

/// upstream: config/presets_test.go#TestFlyPresets_When_RedisConfigured
#[test]
fn test_fly_presets_when_redis_configured() {
    let env = fly_env();

    let mut config = new_config();
    config.redis.url = "redis://some.cloud.redis:6379/1".into();

    assert_eq!(strings(&["fly"]), presets(&config, &env));

    let err = load_presets(&mut config, &env);

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("0.0.0.0", config.server.host);
    assert!(!config.embedded_nats.enabled);
    assert_eq!(strings(&["http", "redis"]), config.broadcast_adapters);
    assert_eq!("redis", config.pubsub_adapter);
    assert_eq!("nats://0.0.0.0:4222", config.embedded_nats.service_addr);
    assert_eq!("nats://0.0.0.0:5222", config.embedded_nats.cluster_addr);
    assert_eq!("any-test-mag-cluster", config.embedded_nats.cluster_name);
    assert_eq!(strings(&["nats://mag.any-test.internal:5222"]), config.embedded_nats.routes);
    // It stays default
    assert_eq!("localhost:50051", config.rpc.host);
}

/// upstream: config/presets_test.go#TestHerokuPresets
#[test]
fn test_heroku_presets() {
    let env = env(&[("HEROKU_DYNO_ID", "web.42"), ("HEROKU_APP_ID", "herr-cable"), ("PORT", "4321")]);

    let mut config = new_config();

    assert_eq!(strings(&["heroku"]), presets(&config, &env));

    let err = load_presets(&mut config, &env);

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("0.0.0.0", config.server.host);
    assert_eq!(4321, config.http_broadcast.port);
}

/// upstream: config/presets_test.go#TestBroker
#[test]
fn test_broker() {
    let mut config = new_config();
    config.user_presets = Some(strings(&["broker"]));
    config.broadcast_adapters = strings(&["http"]);

    assert_eq!(strings(&["broker"]), presets(&config, &EnvMap::new()));

    let err = load_presets(&mut config, &EnvMap::new());

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("memory", config.broker.adapter);
    assert_eq!(strings(&["http"]), config.broadcast_adapters);
    assert_eq!("", config.pubsub_adapter);
}

/// upstream: config/presets_test.go#TestBrokerWhenRedisConfigured
#[test]
fn test_broker_when_redis_configured() {
    let mut config = new_config();
    config.user_presets = Some(strings(&["broker"]));
    config.redis.url = "redis://localhost:6379/1".into();

    assert_eq!(strings(&["broker"]), presets(&config, &EnvMap::new()));

    let err = load_presets(&mut config, &EnvMap::new());

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("memory", config.broker.adapter);
    assert_eq!(strings(&["http", "redisx", "redis"]), config.broadcast_adapters);
    assert_eq!("redis", config.pubsub_adapter);
}

/// upstream: config/presets_test.go#TestBrokerWhenENATSConfigured
#[test]
fn test_broker_when_enats_configured() {
    let mut config = new_config();
    config.user_presets = Some(strings(&["broker"]));
    config.embedded_nats.enabled = true;

    assert_eq!(strings(&["broker"]), presets(&config, &EnvMap::new()));

    let err = load_presets(&mut config, &EnvMap::new());

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("nats", config.broker.adapter);
    assert_eq!(strings(&["http", "nats"]), config.broadcast_adapters);
    assert_eq!("nats", config.pubsub_adapter);
}

/// upstream: config/presets_test.go#TestFlyWithBrokerPresets
#[test]
fn test_fly_with_broker_presets() {
    let env = fly_env();

    let mut config = new_config();
    config.user_presets = Some(strings(&["fly", "broker"]));
    config.server.port = 8989;

    assert_eq!(strings(&["fly", "broker"]), presets(&config, &env));

    let err = load_presets(&mut config, &env);

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("0.0.0.0", config.server.host);
    assert_eq!(8989, config.server.port);
    assert_eq!(8989, config.http_broadcast.port);
    assert!(config.embedded_nats.enabled);
    assert_eq!("nats", config.pubsub_adapter);
    assert_eq!("nats", config.broker.adapter);
    assert_eq!(strings(&["http", "nats"]), config.broadcast_adapters);
    assert_eq!("nats://0.0.0.0:4222", config.embedded_nats.service_addr);
    assert_eq!("nats://0.0.0.0:5222", config.embedded_nats.cluster_addr);
    assert_eq!("any-test-mag-cluster", config.embedded_nats.cluster_name);
    assert_eq!(strings(&["nats://mag.any-test.internal:5222"]), config.embedded_nats.routes);
    assert_eq!("localhost:50051", config.rpc.host);
}

/// upstream: config/presets_test.go#TestOverrideSomePresetSettings
#[test]
fn test_override_some_preset_settings() {
    let env = fly_env();

    let mut config = new_config();

    assert_eq!(strings(&["fly"]), presets(&config, &env));

    config.embedded_nats.service_addr = "nats://0.0.0.0:1234".into();

    let err = load_presets(&mut config, &env);

    assert!(err.is_ok(), "{err:?}");

    assert_eq!("0.0.0.0", config.server.host);
    assert_eq!("nats://0.0.0.0:1234", config.embedded_nats.service_addr);
}

/// upstream: config/presets_test.go#TestExplicitOverImplicitPresets
#[test]
fn test_explicit_over_implicit_presets() {
    let env = fly_env();

    let mut config = new_config();
    config.user_presets = Some(vec![]);

    assert_eq!(Vec::<String>::new(), presets(&config, &env));
}
