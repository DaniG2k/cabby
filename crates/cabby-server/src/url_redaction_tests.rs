//! No part of a password in error texts, log values or `Debug` output derived
//! from Redis, NATS, HTTP and Postgres URLs, whatever the URL looks like
//! (D-080, D-093).

use cabby_core::nats::NatsConfig;
use cabby_core::utils::redact_refused_url;

use crate::http_client::Origin;
use crate::redis::url as redis_url;
use crate::redis::{RedisConfig, pubsub_start_message};

/// Distinctive passwords with the characters that end or split a URL, or
/// that a URL must escape.
const PASSWORDS: &[&str] = &[
    "Qx7mZp9Lw",
    "Qx7m@Zp9Lw",
    "Qx7m@Zp9@Lw",
    "Qx7m:Zp9Lw",
    "Qx7m/Zp9Lw",
    "Qx7m?Zp9Lw",
    "Qx7m#Zp9Lw",
    "Qx7m%Zp9Lw",
    "Qx7m%zzLw",
    "Qx7m%41Zp9",
    "Qx7m Zp9 Lw",
    "Qx7m\u{e9}\u{6f22}Zp9Lw",
    "Qx7m,Zp9Lw",
    "Qx7m,Zp9L,wK4j",
    "Qx7m[Zp9Lw",
    "Qx7m]Zp9Lw",
    "[Qx7mZp9Lw",
    "Qx7m://Zp9Lw",
    "Qx7m\"Zp9\\Lw",
    "Qx7m&Zp9=Lw",
    "4821,Qx7mZp",
    "4821,Qx7m,Zp9L",
    "4821#Qx7mZp",
    "4821?Qx7mZp",
    "4821/Qx7mZp",
    "4821/6@Qx7m",
    "4821]Qx7mZp",
    "4821,[Qx7m:Zp9L",
    "482199#Qx7mZp",
    "4821,Qx7mZp:x,y",
    "4821,Qx7mZp/x/y",
    "4821,Qx7mZp9L%zz",
    "4821,Qx7m://Zp9L",
    "4821,Qx7mZp?ab=Zp9L,y",
];

/// The kind of a URL: which code paths read it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Redis,
    Nats,
    Http,
    Postgres,
}

/// URL shapes; `{p}` is the password.
const TEMPLATES: &[(Kind, &str)] = &[
    // Redis, also as Sentinel lists.
    (Kind::Redis, "redis://:{p}@host"),
    (Kind::Redis, "redis://alice:{p}@host"),
    (Kind::Redis, "redis://alice:{p}@host:6379/1"),
    (Kind::Redis, "rediss://alice:{p}@host"),
    (Kind::Redis, "rediss://:{p}@host:6380?skip_verify=true"),
    (Kind::Redis, "valkey://alice:{p}@host"),
    (Kind::Redis, "redis://alice:{p}@host:bad"),
    (Kind::Redis, "redis://alice:{p}@host:65536x/0"),
    (Kind::Redis, "redis://alice:{p}@[::1]:6379"),
    (Kind::Redis, "redis://alice:{p}@[::1]:bad"),
    (Kind::Redis, "redis://alice:{p}@[::1"),
    (Kind::Redis, "redis://alice:{p}@host/x"),
    (Kind::Redis, "redis://alice:{p}@host/1/2"),
    (Kind::Redis, "redis://alice:{p}@host?db=x"),
    (Kind::Redis, "redis://alice:{p}@host?dial_timeout=bogus"),
    (Kind::Redis, "redis://alice:{p}@host/%zz"),
    (Kind::Redis, "redis://alice:{p}@ho\nst"),
    (Kind::Redis, "unix://alice:{p}@/tmp/redis.sock"),
    (Kind::Redis, "alice:{p}@host"),
    (Kind::Redis, "alice:{p}@host:6379"),
    (Kind::Redis, ":{p}@host"),
    (Kind::Redis, "bogus://alice:{p}@host"),
    (Kind::Redis, "redis://alice:{p}@h1:26379,redis://alice:{p}@h2:26379"),
    (Kind::Redis, "h1:26379,alice:{p}@h2:26379"),
    (Kind::Redis, "redis://h1:26379,redis://:{p}@h2:26379"),
    (Kind::Redis, "redis://:{p}@h1:26379,h2:26379"),
    // NATS.
    (Kind::Nats, "nats://alice:{p}@host:4222"),
    (Kind::Nats, "tls://alice:{p}@host:4222"),
    (Kind::Nats, "ws://alice:{p}@host:8080"),
    (Kind::Nats, "nats://alice:{p}@host:bad"),
    (Kind::Nats, "nats://alice:{p}@[::1]:4222"),
    (Kind::Nats, "nats://alice:{p}@[::1"),
    (Kind::Nats, "alice:{p}@host:4222"),
    (Kind::Nats, "bogus://alice:{p}@host"),
    (Kind::Nats, "nats://h1:4222,nats://alice:{p}@h2:4222"),
    (Kind::Nats, "nats://alice:{p}@h1:4222, nats://alice:{p}@h2:4222"),
    // HTTP (RPC and broadcasts).
    (Kind::Http, "http://alice:{p}@host:3000/rpc"),
    (Kind::Http, "https://:{p}@host/rpc?x=1"),
    (Kind::Http, "http://alice:{p}@host:bad/rpc"),
    (Kind::Http, "http://alice:{p}@[::1]:3000/rpc"),
    (Kind::Http, "http://alice:{p}@[::1/rpc"),
    (Kind::Http, "alice:{p}@host:3000/rpc"),
    (Kind::Http, "ftp://alice:{p}@host"),
    // Postgres.
    (Kind::Postgres, "postgres://alice:{p}@db:5432/app"),
    (Kind::Postgres, "postgresql://alice:{p}@db/app?sslmode=bogus"),
    (Kind::Postgres, "postgres://alice:{p}@db:bad/app"),
    (Kind::Postgres, "postgres://alice:{p}@[::1]:5432/app"),
    (Kind::Postgres, "postgres://alice:{p}@[::1/app"),
    (Kind::Postgres, "postgres://db/app?user=alice&password={p}"),
    (Kind::Postgres, "alice:{p}@db/app"),
    (Kind::Postgres, "mysql://alice:{p}@db/app"),
    (Kind::Postgres, "host=db user=alice password={p} dbname=app"),
    (Kind::Postgres, "host=db password='{p}' dbname=app"),
];

/// What a password must not leave in a text: any 4 or more consecutive characters of it.
fn pieces(password: &str) -> Vec<String> {
    let chars: Vec<char> = password.chars().collect();
    let mut out = Vec::new();
    for start in 0..chars.len() {
        for end in start + 4..=chars.len() {
            out.push(chars[start..end].iter().collect());
        }
    }
    out
}

/// Every text Cabby makes from `raw`: errors, log values and `Debug` output.
fn texts(kind: Kind, raw: &str) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if kind != Kind::Postgres {
        out.push(("redact_refused_url", redact_refused_url(raw)));
    }
    match kind {
        Kind::Redis => {
            if let Err(e) = redis_url::parse(raw) {
                out.push(("redis url parse", e));
            }
            if let Err(e) = redis_url::parse_rueidis(raw) {
                out.push(("redis parse_rueidis", e));
            }
            for (url, sentinels) in [(raw, ""), ("mymaster", raw)] {
                let mut c = RedisConfig::new();
                c.url = url.to_owned();
                c.sentinels = sentinels.to_owned();
                match c.to_client_options() {
                    Err(e) => out.push(("redis config", e.to_string())),
                    Ok(_) => out.push(("redis start log", pubsub_start_message("Starting Redis pub/sub", &c))),
                }
            }
        }
        Kind::Nats => {
            if let Err(e) = cabby_core::nats::client::server_addrs(raw) {
                out.push(("nats servers", e.to_string()));
            }
            let nats = NatsConfig { servers: raw.to_owned(), ..NatsConfig::new() };
            out.push(("nats start log", nats.filtered_servers()));
        }
        Kind::Http => match Origin::parse(raw) {
            Err(e) => out.push(("http origin", e)),
            Ok(o) => {
                out.push(("http origin raw", o.raw().to_owned()));
                out.push(("http origin url", o.url("connect")));
                out.push(("http origin debug", format!("{o:?}")));
                out.push(("rpc start log", crate::rpc::logged_host(raw)));
            }
        },
        Kind::Postgres => match crate::postgres::connect::parse(raw) {
            Err(e) => out.push(("postgres", e)),
            Ok((_, config)) => out.push(("postgres description", crate::postgres::connect::describe(&config))),
        },
    }
    out
}

/// Pins D-093 (and D-080) for odd URLs: no text made from a URL contains 4 or
/// more consecutive characters of its password.
#[test]
fn deviation_d093_no_part_of_a_password_in_url_texts() {
    let mut leaks = Vec::new();
    for (kind, template) in TEMPLATES {
        for password in PASSWORDS {
            let raw = template.replace("{p}", password);
            let pieces = pieces(password);
            for (what, text) in texts(*kind, &raw) {
                if let Some(piece) = pieces.iter().find(|p| text.contains(p.as_str())) {
                    leaks.push(format!("{what}: {raw:?} -> {text:?} (contains {piece:?})"));
                }
            }
        }
    }
    assert!(leaks.is_empty(), "{} texts contain part of a password:\n{}", leaks.len(), leaks.join("\n"));
}

fn redis_start_log(url: &str, sentinels: &str) -> Result<String, String> {
    let mut c = RedisConfig::new();
    c.url = url.to_owned();
    c.sentinels = sentinels.to_owned();
    c.to_client_options().map(|_| pubsub_start_message("Starting Redis pub/sub", &c)).map_err(|e| e.to_string())
}

/// Pins D-093: the texts of URLs that may hold part of a password (a `#`, `?`
/// or `/` in it after a part that reads as a port, or a comma in it).
#[test]
fn deviation_d093_texts_of_urls_that_may_hold_part_of_a_password() {
    // Redis: the hosts of such URLs are `***` in the start log; other hosts are shown.
    assert_eq!(Ok("Starting Redis pub/sub: ***".to_owned()), redis_start_log("redis://alice:4821#s3cret@host", ""));
    assert_eq!(
        Ok("Starting Redis pub/sub (sentinels): [*** s1:26379 s2:26379]".to_owned()),
        redis_start_log("mymaster", "redis://:4821,s3cret@s1:26379,redis://s2:26379")
    );
    assert_eq!(
        Ok("Starting Redis pub/sub (cluster): [h1:6379 h2:6379]".to_owned()),
        redis_start_log("redis://:pw@h1,redis://:pw@h2", "")
    );
    assert_eq!(
        Err("invalid port in Redis address".to_owned()),
        redis_start_log("redis://alice:482199#s3cret@host", "")
    );
    assert_eq!(
        Err("invalid port in Redis address \"host:99999\"".to_owned()),
        redis_start_log("redis://alice:s3cret@host:99999", "")
    );
    assert_eq!(
        Err("parse \"host:bad\": invalid port after host".to_owned()),
        redis_start_log("redis://alice:4821,s3cr://et@host:bad", "")
    );
    assert_eq!(
        Err("parse \"redis://\": invalid port after host".to_owned()),
        redis_start_log("redis://h1:6379,redis://alice:s3cret,ZQX@host", "")
    );

    // HTTP: the URL as error texts show it, in `Debug` and error texts too.
    let origin = Origin::parse("http://alice:4821/s3cret@host:3000/rpc").unwrap();
    assert_eq!("http://host:3000/rpc", origin.raw());
    assert_eq!("http://host:3000/rpc/connect", origin.url("connect"));
    assert!(!format!("{origin:?}").contains("s3cret") && !format!("{origin:?}").contains("4821"));
    let origin = Origin::parse("http://alice:s3cret@host:3000/rpc").unwrap();
    assert_eq!("http://host:3000/rpc/connect", origin.url("connect"));

    // RPC start log.
    assert_eq!("http://host:3000/rpc", crate::rpc::logged_host("http://alice:4821?s3cret@host:3000/rpc"));
    // An `@` after the host may end a password, wherever it is: a URL without
    // user info and with an `@` in a query value is shown from it too.
    assert_eq!("http://b", crate::rpc::logged_host("http://host:3000/rpc?x=a@b"));
    assert_eq!(
        "http://***:s***t@host:3000/rpc?x=***",
        crate::rpc::logged_host("http://alice:s3cret@host:3000/rpc?x=a@b")
    );
    assert_eq!("http://host:3000/rpc", crate::rpc::logged_host("http://host:3000/rpc"));
}

/// A `/` and an `@` in a Postgres password: the user info ends at the `@` before
/// the host, as tokio-postgres reads it, so the description names the host only.
#[test]
fn a_postgres_password_with_a_slash_and_an_at_names_the_host_only() {
    let (_, config) = crate::postgres::connect::parse("postgres://alice:4821/6@s3cret@db:5432/app").unwrap();
    assert_eq!(Some(&b"4821/6@s3cret"[..]), config.get_password());
    assert_eq!("db:5432/app", crate::postgres::connect::describe(&config));
}
