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

/// Whether the user info of `raw` may have been cut short (D-080): its
/// authority has no `@`, but an `@` follows it. A `/`, `?` or `#` in a password
/// ends the authority early; when what precedes it reads as a port, the URL
/// parses, with part of the password as the port and the rest in the path,
/// query or fragment.
pub fn userinfo_may_be_cut(raw: &str) -> bool {
    let rest = &raw[authority_start(raw)..];
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    !rest[..end].contains('@') && rest[end..].contains('@')
}

/// The URLs of a comma-separated list, each with, when it may hold part of a
/// password, how texts show it (D-093). A comma in a password splits it into
/// several URLs: the pieces before the one with its `@` read as hosts, and that
/// one starts with the rest of the password. So a URL may hold part of a
/// password when its user info may be cut ([`userinfo_may_be_cut`]; shown as
/// [`redact_refused_url`] shows it), when it has no `@` while a later URL of the
/// list has one (shown as its scheme only), and when it has an `@` and follows
/// such a URL (shown as what follows its last `@`).
pub fn url_list(list: &str) -> Vec<(&str, Option<String>)> {
    let pieces: Vec<&str> = list.split(',').collect();
    let mut out = Vec::with_capacity(pieces.len());
    let mut after_split = false;
    for (i, &listed) in pieces.iter().enumerate() {
        // Shown without the spaces around it (NATS lists allow them).
        let piece = listed.trim();
        let has_at = piece.contains('@');
        let split = !has_at && pieces[i + 1..].iter().any(|p| p.contains('@'));
        let shown = if split {
            Some(piece[..authority_start(piece)].to_owned())
        } else if has_at && after_split {
            piece.rfind('@').map(|at| piece[at + 1..].to_owned())
        } else if userinfo_may_be_cut(piece) {
            Some(redact_refused_url(piece))
        } else {
            None
        };
        after_split = split;
        out.push((listed, shown));
    }
    out
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

    #[test]
    fn deviation_d093_cut_user_info_is_recognized() {
        for (raw, cut) in [
            ("redis://u:4821#s3cret@host", true),
            ("redis://u:4821?s3cret@host", true),
            ("http://u:4821/s3cret@host/rpc", true),
            ("u:4821#s3cret@host", true),
            ("redis://u:s3cret@host/0?client_name=a@b", false),
            ("redis://host:6379", false),
            ("redis://host:6379/0?client_name=a@b", true),
        ] {
            assert_eq!(cut, userinfo_may_be_cut(raw), "{raw}");
        }
    }

    #[test]
    fn deviation_d093_list_pieces_around_a_comma_in_a_password_are_hidden() {
        let shown = |list: &str| url_list(list).into_iter().map(|(p, s)| (p.to_owned(), s)).collect::<Vec<_>>();
        let some = |s: &str| Some(s.to_owned());
        assert_eq!(
            vec![
                ("redis://u:4821".to_owned(), some("redis://")),
                ("s3cret".to_owned(), some("")),
                ("ZQX://u@host:1".to_owned(), some("host:1")),
                ("redis://h2".to_owned(), None),
            ],
            shown("redis://u:4821,s3cret,ZQX://u@host:1,redis://h2")
        );
        assert_eq!(
            vec![("redis://:pw@h1".to_owned(), None), ("redis://:pw@h2".to_owned(), None)],
            shown("redis://:pw@h1,redis://:pw@h2")
        );
        assert_eq!(vec![("h1:26379".to_owned(), None), ("h2:26379".to_owned(), None)], shown("h1:26379,h2:26379"));
        assert_eq!(
            vec![("redis://u:4821#s3cret@host".to_owned(), some("redis://host"))],
            shown("redis://u:4821#s3cret@host")
        );
    }
}
