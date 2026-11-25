//! Leaving credentials out of error texts.

/// A URL that could not be parsed, for an error text: without its user and
/// password, and without anything else that may be part of them.
///
/// Such a URL may not be well formed (a `/`, `?` or `#` in the password ends the
/// authority early), so everything from the start of the authority up to the
/// last `@` is left out. Without an `@`, what follows the host's colon is left
/// out unless it is a port: it may be a password too (a comma in one splits the
/// URL where URL lists are accepted).
pub fn redact_refused_url(raw: &str) -> String {
    let (prefix, rest) = raw.split_at(authority_start(raw));
    if let Some(at) = rest.rfind('@') {
        return format!("{prefix}{}", &rest[at + 1..]);
    }
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    let host_end = match authority.strip_prefix('[') {
        Some(v6) => v6.find(']').map_or(authority.len(), |end| end + 2),
        None => authority.find(':').unwrap_or(authority.len()),
    };
    match authority[host_end..].strip_prefix(':') {
        Some(port) if !port.bytes().all(|b| b.is_ascii_digit()) => format!("{prefix}{}", &authority[..host_end]),
        _ => raw.to_owned(),
    }
}

/// Where the authority starts: after `scheme://`, else after any slashes.
fn authority_start(raw: &str) -> usize {
    let is_scheme = |s: &str| {
        s.starts_with(|c: char| c.is_ascii_alphabetic())
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
    };
    match raw.split_once("://") {
        Some((scheme, _)) if is_scheme(scheme) => scheme.len() + 3,
        _ => raw.len() - raw.trim_start_matches('/').len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_are_left_out_of_refused_urls() {
        for (raw, want) in [
            ("redis://u:s3cret@host:bad", "redis://host:bad"),
            ("redis://:s3cret/ZQX@host:6379/1", "redis://host:6379/1"),
            ("redis://:s3cret#ZQX@host:6379", "redis://host:6379"),
            ("redis://:s3cret?ZQX@host:6379", "redis://host:6379"),
            ("redis://:s3cret@ZQX@host:6379", "redis://host:6379"),
            ("bogus://u:s3cret@h:4222", "bogus://h:4222"),
            (":s3cret@host", "host"),
            ("u:s3cret://ZQX@host", "host"),
            ("//u:s3cret@host/x", "//host/x"),
            // No `@`: what is not a port may be a password.
            ("redis://:s3cret", "redis://"),
            ("redis://u:s3cret:ZQX", "redis://u"),
            ("redis://u:s3cret/1?x=y", "redis://u"),
            ("redis://[::1]:s3cret", "redis://[::1]"),
            // No credentials.
            ("redis://host:6379/1?db=x", "redis://host:6379/1?db=x"),
            ("redis://[::1]:6379/x", "redis://[::1]:6379/x"),
            ("redis://[::1", "redis://[::1"),
            ("redis://host/a:b", "redis://host/a:b"),
            ("://x", "://x"),
            ("", ""),
        ] {
            assert_eq!(want, redact_refused_url(raw), "{raw}");
        }
    }
}
