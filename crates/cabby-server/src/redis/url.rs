//! Redis URL parsing with the semantics upstream relies on: Go's `net/url.Parse`
//! (the parts rueidis reads), rueidis `ParseURL`, and Go's `time.ParseDuration`.

use std::time::Duration;

use cabby_core::utils::redact_refused_url;

/// The parts of a Go `*url.URL` that rueidis reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GoUrl {
    pub scheme: String,
    /// `None`: no userinfo (`u.User == nil`).
    pub user: Option<(String, Option<String>)>,
    /// `host[:port]` as written (unescaped).
    pub host: String,
    pub path: String,
    pub query: Vec<(String, String)>,
}

impl GoUrl {
    /// `u.Hostname()`: the host without port and IPv6 brackets.
    pub fn hostname(&self) -> String {
        let (host, _) = split_host_port_lenient(&self.host);
        host
    }

    fn query_get(&self, key: &str) -> Option<&str> {
        self.query.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    fn query_all<'a>(&'a self, key: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.query.iter().filter(move |(k, _)| k == key).map(|(_, v)| v.as_str())
    }
}

/// `u.Hostname()` / `u.Port()` split (no validation).
fn split_host_port_lenient(hostport: &str) -> (String, String) {
    if let Some(rest) = hostport.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        let host = rest[..end].to_owned();
        let port = rest[end + 1..].strip_prefix(':').unwrap_or("").to_owned();
        return (host, port);
    }
    match hostport.rfind(':') {
        Some(i) => (hostport[..i].to_owned(), hostport[i + 1..].to_owned()),
        None => (hostport.to_owned(), String::new()),
    }
}

/// Go `net.SplitHostPort`; `None` where Go returns an error.
fn split_host_port(hostport: &str) -> Option<(String, String)> {
    if let Some(rest) = hostport.strip_prefix('[') {
        let end = rest.find(']')?;
        let port = rest[end + 1..].strip_prefix(':')?;
        return Some((rest[..end].to_owned(), port.to_owned()));
    }
    let i = hostport.rfind(':')?;
    let host = &hostport[..i];
    if host.contains(':') {
        return None; // too many colons
    }
    Some((host.to_owned(), hostport[i + 1..].to_owned()))
}

/// Go `net.JoinHostPort`.
pub(crate) fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

fn unhex(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// An invalid percent escape (Go `url.EscapeError`).
struct EscapeError(String);

impl EscapeError {
    /// Go's text, or without the escape.
    fn reason(&self, quote: bool) -> String {
        if quote { format!("invalid URL escape {:?}", self.0) } else { "invalid URL escape".to_owned() }
    }
}

/// Go `url.unescape` (percent-decoding; `+` becomes a space in query components).
fn unescape(s: &str, query: bool) -> Result<String, EscapeError> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = b.get(i + 1).copied().and_then(unhex).zip(b.get(i + 2).copied().and_then(unhex));
                let Some((h, l)) = hex else {
                    let end = (i + 3).min(b.len());
                    return Err(EscapeError(String::from_utf8_lossy(&b[i..end]).into_owned()));
                };
                out.push(h << 4 | l);
                i += 3;
            }
            b'+' if query => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Go `url.ParseQuery` as read through `u.Query()` (malformed pairs are skipped).
fn parse_query(raw: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() || pair.contains(';') {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if let (Ok(k), Ok(v)) = (unescape(k, true), unescape(v, true)) {
            out.push((k, v));
        }
    }
    out
}

/// Go `getScheme`: `(scheme, rest)`.
fn get_scheme(raw: &str) -> Result<(String, &str), String> {
    for (i, c) in raw.char_indices() {
        match c {
            'a'..='z' | 'A'..='Z' => {}
            '0'..='9' | '+' | '-' | '.' if i > 0 => {}
            ':' if i == 0 => return Err("missing protocol scheme".to_owned()),
            ':' => return Ok((raw[..i].to_ascii_lowercase(), &raw[i + 1..])),
            // Invalid character: no valid scheme, the whole string is a path.
            _ => return Ok((String::new(), raw)),
        }
    }
    Ok((String::new(), raw))
}

/// Go `validOptionalPort`.
fn valid_optional_port(port: &str) -> bool {
    match port.strip_prefix(':') {
        None => port.is_empty(),
        Some(digits) => digits.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// Go `parseHost` (bracketed IPv6 and port validation). Errors quote what is
/// invalid, as Go's do, when `quote`.
fn parse_host(host: &str, quote: bool) -> Result<String, String> {
    let invalid_port = |colon_port: &str| {
        let port = if quote { format!(" {colon_port:?}") } else { String::new() };
        Err(format!("invalid port{port} after host"))
    };
    if host.starts_with('[') {
        let Some(end) = host.rfind(']') else { return Err("missing ']' in host".to_owned()) };
        let colon_port = &host[end + 1..];
        if !valid_optional_port(colon_port) {
            return invalid_port(colon_port);
        }
    } else if let Some(i) = host.rfind(':') {
        let colon_port = &host[i..];
        if !valid_optional_port(colon_port) {
            return invalid_port(colon_port);
        }
    }
    unescape(host, false).map_err(|e| e.reason(quote))
}

/// Whether errors may quote what comes after the user and password of `raw`.
/// Not when part of them may be there: when an `@` follows the authority (a
/// `/`, `?` or `#` in the password ended it early), or when there is no `@` and
/// something other than a port follows the host's colon.
fn quotable(raw: &str) -> bool {
    let Some(at) = raw.rfind('@') else { return redact_refused_url(raw) == raw };
    let Some(start) = raw.find("//").map(|i| i + 2) else { return false };
    raw[start..].find(['/', '?', '#']).is_none_or(|end| at < start + end)
}

/// Go `url.Parse` (the subset rueidis reads). Errors read like Go's
/// (`parse "<url>": <reason>`), without credentials (D-080).
pub(crate) fn parse(raw: &str) -> Result<GoUrl, String> {
    parse_listed(raw, None)
}

/// [`parse`] of a URL of a list. When it may hold part of a password, errors
/// show it as `shown` (from `cabby_core::utils::url_list`) and quote nothing
/// of it (D-093).
fn parse_listed(raw: &str, shown: Option<&str>) -> Result<GoUrl, String> {
    let wrap = |e: String| format!("parse {:?}: {e}", shown.map_or_else(|| redact_refused_url(raw), str::to_owned));
    let quote = shown.is_none() && quotable(raw);
    let without_fragment = raw.split_once('#').map_or(raw, |(u, _)| u);
    if without_fragment.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(wrap("net/url: invalid control character in URL".to_owned()));
    }
    let (scheme, rest) = get_scheme(without_fragment).map_err(wrap)?;
    let (rest, raw_query) = if rest.ends_with('?') && rest.matches('?').count() == 1 {
        (&rest[..rest.len() - 1], "")
    } else {
        rest.split_once('?').unwrap_or((rest, ""))
    };
    let mut url = GoUrl { scheme, query: parse_query(raw_query), ..Default::default() };

    let authority_form = !url.scheme.is_empty() || !rest.starts_with("///");
    let path_part = if let Some(after) = rest.strip_prefix("//").filter(|_| authority_form) {
        let (authority, path) = match after.find('/') {
            Some(i) => (&after[..i], &after[i..]),
            None => (after, ""),
        };
        let host_part = match authority.rfind('@') {
            Some(i) => {
                let userinfo = &authority[..i];
                let decode = |s: &str| unescape(s, false).map_err(|e| wrap(e.reason(false)));
                url.user = Some(match userinfo.split_once(':') {
                    Some((u, p)) => (decode(u)?, Some(decode(p)?)),
                    None => (decode(userinfo)?, None),
                });
                &authority[i + 1..]
            }
            None => authority,
        };
        url.host = parse_host(host_part, quote).map_err(wrap)?;
        path
    } else {
        if url.scheme.is_empty() && rest.split('/').next().is_some_and(|seg| seg.contains(':')) {
            return Err(wrap("first path segment in URL cannot contain colon".to_owned()));
        }
        rest
    };
    url.path = unescape(path_part, false).map_err(|e| wrap(e.reason(quote)))?;
    Ok(url)
}

/// What rueidis `ParseURL` yields (plus what Cabby needs to connect).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ParsedRedisUrl {
    pub init_address: Vec<String>,
    pub username: String,
    pub password: String,
    pub select_db: i64,
    pub dial_timeout: Duration,
    /// `write_timeout` query parameter (rueidis `ConnWriteTimeout`); zero when unset.
    pub write_timeout: Duration,
    /// `rediss://` (rueidis sets a TLS config).
    pub tls: bool,
    /// `skip_verify` query parameter (only with TLS).
    pub skip_verify: bool,
    /// TLS server name (the URL host).
    pub server_name: String,
    /// `unix://` socket path in `init_address`.
    pub unix: bool,
    pub master_set: String,
    pub client_name: String,
    /// For each of `init_address`: whether it comes from a URL that may hold part
    /// of a password, so texts leave it out (D-093).
    pub hidden: Vec<bool>,
}

/// rueidis `ParseURL`, with its error texts (without credentials, D-080).
#[cfg(test)]
pub(crate) fn parse_rueidis(raw: &str) -> Result<ParsedRedisUrl, String> {
    parse_rueidis_listed(raw, None)
}

/// [`parse_rueidis`] of a URL of a list; see [`parse_listed`] (D-093).
pub(crate) fn parse_rueidis_listed(raw: &str, shown: Option<&str>) -> Result<ParsedRedisUrl, String> {
    let u = parse_listed(raw, shown)?;
    let quote = shown.is_none() && quotable(raw);
    let invalid = |what: &str, value: &str| {
        if quote { format!("redis: invalid {what}: {value}") } else { format!("redis: invalid {what}") }
    };
    let invalid_quoted = |what: &str, value: &str| invalid(what, &format!("{value:?}"));
    let mut opt = ParsedRedisUrl::default();
    let parse_addr = |hostport: &str| -> (String, String) {
        let (mut host, mut port) = split_host_port(hostport).unwrap_or_default();
        if host.is_empty() {
            host = u.hostname();
        }
        if host.is_empty() {
            host = "localhost".to_owned();
        }
        if port.is_empty() {
            port = "6379".to_owned();
        }
        let addr = join_host_port(&host, &port);
        (host, addr)
    };
    match u.scheme.as_str() {
        "unix" => {
            opt.unix = true;
            opt.init_address = vec![u.path.trim().to_owned()];
        }
        "rediss" | "valkeys" => opt.tls = true,
        "redis" | "valkey" => {}
        _ if shown.is_some() => return Err("redis: invalid URL scheme".to_owned()),
        other => return Err(format!("redis: invalid URL scheme: {other}")),
    }
    if opt.init_address.is_empty() {
        let (host, addr) = parse_addr(&u.host);
        opt.init_address = vec![addr];
        if opt.tls {
            opt.server_name = host;
        }
    }
    if let Some((user, pass)) = &u.user {
        opt.username = user.clone();
        opt.password = pass.clone().unwrap_or_default();
    }
    if u.scheme != "unix" {
        let ps: Vec<&str> = u.path.split('/').collect();
        if ps.len() == 2 {
            opt.select_db = go_atoi(ps[1]).ok_or_else(|| invalid_quoted("database number", ps[1]))?;
        } else if ps.len() > 2 {
            return Err(invalid("URL path", &u.path));
        }
    }
    if let Some(db) = u.query_get("db") {
        opt.select_db = go_atoi(db).ok_or_else(|| invalid_quoted("database number", db))?;
    }
    if let Some(t) = u.query_get("dial_timeout") {
        opt.dial_timeout = parse_duration(t).ok_or_else(|| invalid_quoted("dial timeout", t))?;
    }
    if let Some(t) = u.query_get("write_timeout") {
        opt.write_timeout = parse_duration(t).ok_or_else(|| invalid_quoted("write timeout", t))?;
    }
    for addr in u.query_all("addr") {
        let (_, addr) = parse_addr(addr);
        opt.init_address.push(addr);
    }
    if opt.tls
        && let Some(v) = u.query_get("skip_verify")
    {
        opt.skip_verify =
            if v.is_empty() { true } else { go_parse_bool(v).ok_or_else(|| invalid_quoted("skip verify", v))? };
    }
    opt.client_name = u.query_get("client_name").unwrap_or("").to_owned();
    opt.master_set = u.query_get("master_set").unwrap_or("").to_owned();
    // The host of a URL that starts with the rest of a password follows its `@`.
    let hidden = cabby_core::utils::userinfo_may_be_cut(raw) || (shown.is_some() && !raw.contains('@'));
    opt.hidden = vec![hidden; opt.init_address.len()];
    Ok(opt)
}

/// Go `strconv.Atoi` (decimal, optional sign).
fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// Go `strconv.ParseBool`.
fn go_parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// Go `time.ParseDuration`; negative durations are clamped to zero.
pub(crate) fn parse_duration(s: &str) -> Option<Duration> {
    let (neg, mut rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if rest == "0" {
        return Some(Duration::ZERO);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total: f64 = 0.0;
    while !rest.is_empty() {
        let num_end = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
        let num = &rest[..num_end];
        if num.is_empty() || num == "." || num.matches('.').count() > 1 {
            return None;
        }
        let value: f64 = num.parse().ok()?;
        rest = &rest[num_end..];
        let unit_end = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let nanos_per = match &rest[..unit_end] {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1e3,
            "ms" => 1e6,
            "s" => 1e9,
            "m" => 60e9,
            "h" => 3600e9,
            _ => return None,
        };
        rest = &rest[unit_end..];
        total += value * nanos_per;
    }
    if total > i64::MAX as f64 {
        return None;
    }
    Some(if neg { Duration::ZERO } else { Duration::from_nanos(total as u64) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_like_go() {
        let u = parse("redis://user:pass@localhost:6379/1?dial_timeout=30s").unwrap();
        assert_eq!("redis", u.scheme);
        assert_eq!(Some(("user".to_owned(), Some("pass".to_owned()))), u.user);
        assert_eq!("localhost:6379", u.host);
        assert_eq!("/1", u.path);
        assert_eq!(vec![("dial_timeout".to_owned(), "30s".to_owned())], u.query);

        let u = parse("redis://:data_pass@mymaster").unwrap();
        assert_eq!(Some((String::new(), Some("data_pass".to_owned()))), u.user);
        assert_eq!("mymaster", u.host);

        assert!(parse("redis://u@host:abc").unwrap_err().contains("invalid port \":abc\" after host"));
        assert!(parse("://x").unwrap_err().contains("missing protocol scheme"));
    }

    #[test]
    fn rueidis_parse_url() {
        let o = parse_rueidis("redis://localhost").unwrap();
        assert_eq!(vec!["localhost:6379"], o.init_address);
        let o = parse_rueidis("rediss://[::1]:7000/3?skip_verify&addr=other:7001&addr=7002").unwrap();
        // As in Go, an address without a port takes the URL host and the default port.
        assert_eq!(vec!["[::1]:7000", "other:7001", "[::1]:6379"], o.init_address);
        assert_eq!(3, o.select_db);
        assert!(o.tls && o.skip_verify);
        assert_eq!("::1", o.server_name);
        assert_eq!("redis: invalid URL scheme: invalid", parse_rueidis("invalid://").unwrap_err());
        assert_eq!("redis: invalid database number: \"\"", parse_rueidis("redis://h/").unwrap_err());
        assert_eq!("redis: invalid URL path: /1/2", parse_rueidis("redis://h/1/2").unwrap_err());
        assert_eq!("m", parse_rueidis("redis://h?master_set=m").unwrap().master_set);
    }

    /// Pins D-080: errors leave out the user and password, wherever the URL is
    /// cut, and otherwise read like Go's.
    #[test]
    fn deviation_d080_errors_leave_out_credentials() {
        for (raw, want) in [
            ("redis://u:s3cret@host:bad", r#"parse "redis://host:bad": invalid port ":bad" after host"#),
            ("redis://:s3cret/ZQX@host:6379", r#"parse "redis://host:6379": invalid port after host"#),
            ("redis://:s3cret?ZQX@host:6379", r#"parse "redis://host:6379": invalid port after host"#),
            ("redis://:s3cret#ZQX@host:6379/1", r#"parse "redis://host:6379/1": invalid port after host"#),
            ("redis://:s3cret%zz@host:6379/1", r#"parse "redis://host:6379/1": invalid URL escape"#),
            ("redis://u:s3cret@host/%zz", r#"parse "redis://host/%zz": invalid URL escape "%zz""#),
            ("redis://:1/s3cret%zz@host", r#"parse "redis://host": invalid URL escape"#),
            ("redis://u:s3cret@ho\nst", r#"parse "redis://ho\nst": net/url: invalid control character in URL"#),
            ("u_s:s3cret@host", r#"parse "host": first path segment in URL cannot contain colon"#),
            (":s3cret@host", r#"parse "host": missing protocol scheme"#),
            // A comma in the password cuts it in a list of URLs.
            ("redis://u:s3cret", r#"parse "redis://u": invalid port after host"#),
            ("redis://:1/s3cret@host", "redis: invalid database number"),
            ("redis://:1/s3cret/ZQX@host", "redis: invalid URL path"),
            ("redis://u:s3cret@host/x", r#"redis: invalid database number: "x""#),
            ("redis://u:s3cret@host/1/2", "redis: invalid URL path: /1/2"),
            ("redis://u:s3cret@host?dial_timeout=x", r#"redis: invalid dial timeout: "x""#),
        ] {
            let err = parse_rueidis(raw).unwrap_err();
            assert_eq!(want, err, "{raw}");
            assert!(!err.contains("s3cret") && !err.contains("ZQX"), "{raw}: {err}");
        }
    }

    #[test]
    fn go_durations() {
        assert_eq!(Some(Duration::from_secs(30)), parse_duration("30s"));
        assert_eq!(Some(Duration::from_millis(1500)), parse_duration("1.5s"));
        assert_eq!(Some(Duration::from_secs(5400)), parse_duration("1h30m"));
        assert_eq!(Some(Duration::ZERO), parse_duration("0"));
        assert_eq!(None, parse_duration("30"));
        assert_eq!(None, parse_duration(""));
        assert_eq!(None, parse_duration("5x"));
    }
}
