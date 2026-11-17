//! A port of the parts of Go's `net/url` that `FilteredURL` relies on: `url.Parse`
//! (which inputs are rejected and how the URL is split) and `URL.String()` (how it is
//! reassembled). Kept faithful so filtered URLs read exactly as upstream logs them.

use std::fmt;
use std::net::{IpAddr, Ipv6Addr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Path,
    Host,
    Zone,
    UserPassword,
    Fragment,
}

/// Go `shouldEscape` (net/url, `gen_encoding_table.go`).
fn should_escape(c: u8, mode: Mode) -> bool {
    if c.is_ascii_alphanumeric() {
        return false;
    }
    if matches!(mode, Mode::Host | Mode::Zone)
        && matches!(
            c,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'['
                | b']'
                | b'<'
                | b'>'
                | b'"'
        )
    {
        return false;
    }
    match c {
        b'-' | b'_' | b'.' | b'~' => return false,
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?' | b'@' => match mode {
            Mode::Path => return c == b'?',
            Mode::UserPassword => return matches!(c, b'@' | b'/' | b'?' | b':'),
            Mode::Fragment => return false,
            Mode::Host | Mode::Zone => {}
        },
        _ => {}
    }
    // Everything else is escaped, except a few sub-delims in fragments.
    !(mode == Mode::Fragment && matches!(c, b'!' | b'(' | b')' | b'*'))
}

fn unhex(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        _ => c - b'A' + 10,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ParseError(String);

fn err<T>(msg: impl Into<String>) -> Result<T, ParseError> {
    Err(ParseError(msg.into()))
}

/// Go `unescape` (no query-component mode: `+` is never special here).
fn unescape(s: &str, mode: Mode) -> Result<Vec<u8>, ParseError> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                if i + 2 >= b.len() || !b[i + 1].is_ascii_hexdigit() || !b[i + 2].is_ascii_hexdigit() {
                    let end = (i + 3).min(b.len());
                    return err(format!("invalid URL escape {:?}", String::from_utf8_lossy(&b[i..end])));
                }
                let v = (unhex(b[i + 1]) << 4) | unhex(b[i + 2]);
                let esc = &s[i..i + 3];
                if mode == Mode::Host && unhex(b[i + 1]) < 8 && esc != "%25" {
                    return err(format!("invalid URL escape {esc:?}"));
                }
                if mode == Mode::Zone && esc != "%25" && v != b' ' && should_escape(v, Mode::Host) {
                    return err(format!("invalid URL escape {esc:?}"));
                }
                out.push(v);
                i += 3;
            }
            c => {
                if matches!(mode, Mode::Host | Mode::Zone) && c < 0x80 && should_escape(c, mode) {
                    return err(format!("invalid character {:?} in host name", c as char));
                }
                out.push(c);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn unescape_str(s: &str, mode: Mode) -> Result<String, ParseError> {
    // Decoded bytes need not be UTF-8; Go strings hold them as is.
    unescape(s, mode).map(|b| String::from_utf8_lossy(&b).into_owned())
}

/// Go `escape`.
fn escape(s: &str, mode: Mode) -> String {
    const UPPER: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &c in s.as_bytes() {
        if should_escape(c, mode) {
            out.push('%');
            out.push(UPPER[(c >> 4) as usize] as char);
            out.push(UPPER[(c & 15) as usize] as char);
        } else {
            out.push(c as char);
        }
    }
    out
}

/// Go `validEncoded`.
fn valid_encoded(s: &str, mode: Mode) -> bool {
    s.bytes().all(|c| {
        matches!(
            c,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
                | b'['
                | b']'
                | b'%'
        ) || !should_escape(c, mode)
    })
}

/// Go `validOptionalPort`.
fn valid_optional_port(port: &str) -> bool {
    match port.strip_prefix(':') {
        None => port.is_empty(),
        Some(digits) => digits.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// Go `validUserinfo`.
fn valid_userinfo(s: &str) -> bool {
    s.chars().all(|r| {
        r.is_ascii_alphanumeric()
            || matches!(
                r,
                '-' | '.'
                    | '_'
                    | ':'
                    | '~'
                    | '!'
                    | '$'
                    | '&'
                    | '\''
                    | '('
                    | ')'
                    | '*'
                    | '+'
                    | ','
                    | ';'
                    | '='
                    | '%'
                    | '@'
            )
    })
}

/// Go `netip.ParseAddr` for the content of an IP literal: an IPv6 address with an
/// optional non-empty zone. Returns whether it is valid IPv6 (not IPv4).
fn valid_ipv6_literal(s: &str) -> Result<(), ParseError> {
    let (addr, zone) = match s.split_once('%') {
        Some((a, z)) => (a, Some(z)),
        None => (s, None),
    };
    if zone == Some("") {
        return err("invalid host: zone must be a non-empty string");
    }
    if addr.parse::<Ipv6Addr>().is_ok() {
        return Ok(());
    }
    if zone.is_none() && matches!(addr.parse::<IpAddr>(), Ok(IpAddr::V4(_))) {
        return err("invalid IP-literal");
    }
    err(format!("invalid host: ParseAddr({s:?}): invalid IP address"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct UserInfo {
    pub username: String,
    pub password: Option<String>,
}

/// A parsed URL (Go `url.URL`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct GoUrl {
    pub scheme: String,
    pub opaque: String,
    pub user: Option<UserInfo>,
    pub host: String,
    pub path: String,
    pub raw_path: String,
    pub omit_host: bool,
    pub force_query: bool,
    pub raw_query: String,
    pub fragment: String,
    pub raw_fragment: String,
}

/// Go `getScheme`.
fn get_scheme(raw: &str) -> Result<(&str, &str), ParseError> {
    for (i, c) in raw.bytes().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' => {
                if i == 0 {
                    return Ok(("", raw));
                }
            }
            b':' => {
                if i == 0 {
                    return err("missing protocol scheme");
                }
                return Ok((&raw[..i], &raw[i + 1..]));
            }
            _ => return Ok(("", raw)),
        }
    }
    Ok(("", raw))
}

impl GoUrl {
    /// Go `url.Parse`.
    pub fn parse(raw: &str) -> Result<GoUrl, ParseError> {
        let (u, frag) = raw.split_once('#').unwrap_or((raw, ""));
        let mut url = Self::parse_no_fragment(u)?;
        if !frag.is_empty() {
            url.fragment = unescape_str(frag, Mode::Fragment)?;
            url.raw_fragment =
                if escape(&url.fragment, Mode::Fragment) == frag { String::new() } else { frag.to_owned() };
        }
        Ok(url)
    }

    fn parse_no_fragment(raw: &str) -> Result<GoUrl, ParseError> {
        if raw.bytes().any(|b| b < b' ' || b == 0x7f) {
            return err("net/url: invalid control character in URL");
        }
        let mut url = GoUrl::default();
        if raw == "*" {
            url.path = "*".into();
            return Ok(url);
        }

        let (scheme, rest) = get_scheme(raw)?;
        url.scheme = scheme.to_ascii_lowercase();

        let mut rest = rest;
        if rest.ends_with('?') && rest.matches('?').count() == 1 {
            url.force_query = true;
            rest = &rest[..rest.len() - 1];
        } else if let Some((r, q)) = rest.split_once('?') {
            rest = r;
            url.raw_query = q.to_owned();
        }

        if !rest.starts_with('/') {
            if !url.scheme.is_empty() {
                url.opaque = rest.to_owned();
                return Ok(url);
            }
            let segment = rest.split('/').next().unwrap_or_default();
            if segment.contains(':') {
                return err("first path segment in URL cannot contain colon");
            }
        }

        if (!url.scheme.is_empty() || !rest.starts_with("///")) && rest.starts_with("//") {
            let authority = &rest[2..];
            let (authority, path) = match authority.find('/') {
                Some(i) => (&authority[..i], &authority[i..]),
                None => (authority, ""),
            };
            let (user, host) = parse_authority(&url.scheme, authority)?;
            url.user = user;
            url.host = host;
            rest = path;
        } else if !url.scheme.is_empty() && rest.starts_with('/') {
            url.omit_host = true;
        }

        url.path = unescape_str(rest, Mode::Path)?;
        url.raw_path = if escape(&url.path, Mode::Path) == rest { String::new() } else { rest.to_owned() };
        Ok(url)
    }

    /// Go `URL.EscapedPath`.
    fn escaped_path(&self) -> String {
        if !self.raw_path.is_empty()
            && valid_encoded(&self.raw_path, Mode::Path)
            && unescape_str(&self.raw_path, Mode::Path).is_ok_and(|p| p == self.path)
        {
            return self.raw_path.clone();
        }
        if self.path == "*" {
            return "*".into();
        }
        escape(&self.path, Mode::Path)
    }

    /// Go `URL.EscapedFragment`.
    fn escaped_fragment(&self) -> String {
        if !self.raw_fragment.is_empty()
            && valid_encoded(&self.raw_fragment, Mode::Fragment)
            && unescape_str(&self.raw_fragment, Mode::Fragment).is_ok_and(|f| f == self.fragment)
        {
            return self.raw_fragment.clone();
        }
        escape(&self.fragment, Mode::Fragment)
    }
}

/// Go `parseAuthority`.
fn parse_authority(scheme: &str, authority: &str) -> Result<(Option<UserInfo>, String), ParseError> {
    let at = authority.rfind('@');
    let host = parse_host(scheme, at.map_or(authority, |i| &authority[i + 1..]))?;
    let Some(i) = at else { return Ok((None, host)) };
    let userinfo = &authority[..i];
    if !valid_userinfo(userinfo) {
        return err("net/url: invalid userinfo");
    }
    let user = match userinfo.split_once(':') {
        None => UserInfo { username: unescape_str(userinfo, Mode::UserPassword)?, password: None },
        Some((u, p)) => UserInfo {
            username: unescape_str(u, Mode::UserPassword)?,
            password: Some(unescape_str(p, Mode::UserPassword)?),
        },
    };
    Ok((Some(user), host))
}

/// Go `parseHost`.
fn parse_host(scheme: &str, host: &str) -> Result<String, ParseError> {
    match host.rfind('[') {
        Some(i) if i > 0 => err("invalid IP-literal"),
        Some(_) => {
            let Some(close) = host.rfind(']') else { return err("missing ']' in host") };
            let colon_port = &host[close + 1..];
            if !valid_optional_port(colon_port) {
                return err(format!("invalid port {colon_port:?} after host"));
            }
            let colon_port = unescape_str(colon_port, Mode::Host)?;
            let hostname = &host[1..close];
            let unescaped = match hostname.find("%25") {
                Some(z) => unescape_str(&hostname[..z], Mode::Host)? + &unescape_str(&hostname[z..], Mode::Zone)?,
                None => unescape_str(hostname, Mode::Host)?,
            };
            valid_ipv6_literal(&unescaped)?;
            Ok(format!("[{unescaped}]{colon_port}"))
        }
        None => {
            if let Some(first) = host.find(':') {
                let last = host.rfind(':').expect("a colon");
                // Go enforces strict colons for http(s) only (go.dev/issue/75223).
                let i = if last != first && scheme != "http" && scheme != "https" { last } else { first };
                let colon_port = &host[i..];
                if !valid_optional_port(colon_port) {
                    return err(format!("invalid port {colon_port:?} after host"));
                }
            }
            unescape_str(host, Mode::Host)
        }
    }
}

impl fmt::Display for GoUrl {
    /// Go `URL.String()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = String::new();
        if !self.scheme.is_empty() {
            buf.push_str(&self.scheme);
            buf.push(':');
        }
        if !self.opaque.is_empty() {
            buf.push_str(&self.opaque);
        } else {
            if (!self.scheme.is_empty() || !self.host.is_empty() || self.user.is_some())
                && !(self.omit_host && self.host.is_empty() && self.user.is_none())
            {
                if !self.host.is_empty() || !self.path.is_empty() || self.user.is_some() {
                    buf.push_str("//");
                }
                if let Some(u) = &self.user {
                    buf.push_str(&escape(&u.username, Mode::UserPassword));
                    if let Some(p) = &u.password {
                        buf.push(':');
                        buf.push_str(&escape(p, Mode::UserPassword));
                    }
                    buf.push('@');
                }
                if !self.host.is_empty() {
                    buf.push_str(&escape(&self.host, Mode::Host));
                }
            }
            let mut path = self.escaped_path();
            if self.omit_host && self.host.is_empty() && self.user.is_none() && path.starts_with("//") {
                buf.push_str("%2F");
                path.remove(0);
            }
            if !path.is_empty() && !path.starts_with('/') && !self.host.is_empty() {
                buf.push('/');
            }
            if buf.is_empty() && path.split('/').next().unwrap_or_default().contains(':') {
                buf.push_str("./");
            }
            buf.push_str(&path);
        }
        if self.force_query || !self.raw_query.is_empty() {
            buf.push('?');
            buf.push_str(&self.raw_query);
        }
        if !self.fragment.is_empty() {
            buf.push('#');
            buf.push_str(&self.escaped_fragment());
        }
        f.write_str(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(s: &str) -> String {
        GoUrl::parse(s).map(|u| u.to_string()).unwrap_or_else(|e| format!("error: {}", e.0))
    }

    #[test]
    fn parses_and_reassembles_like_go() {
        assert_eq!("wss://example.com/cable?a=1#frag", roundtrip("wss://example.com/cable?a=1#frag"));
        assert_eq!("http://[::1]:80/x", roundtrip("http://[::1]:80/x"));
        assert_eq!("error: missing ']' in host", roundtrip("http://[::1"));
        assert_eq!("error: invalid IP-literal", roundtrip("http://[127.0.0.1]/"));
        assert_eq!("error: invalid port \":abc\" after host", roundtrip("http://host:abc/"));
        assert_eq!("error: first path segment in URL cannot contain colon", roundtrip("1a:b"));
        assert_eq!("mailto:x@y", roundtrip("mailto:x@y"));
        assert_eq!("/a%20b", roundtrip("/a b"));
        assert_eq!("/a%2Fb", roundtrip("/a%2Fb"));
        assert_eq!("http://u:p%40@h", roundtrip("http://u:p@@h"));
        assert_eq!("error: missing protocol scheme", roundtrip(":x"));
    }
}
