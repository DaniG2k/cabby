//! JSON string encoding as Go's `encoding/json` writes it (HTML-safe escaping), so
//! identifiers, commands and broadcast bodies are byte-for-byte what upstream sends.

/// Appends `s` as a JSON string literal.
pub(crate) fn push_str(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c if (c as u32) < 0x20 => {
                let b = c as u32 as u8;
                out.push_str("\\u00");
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0xF) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// `s` as a JSON string literal.
pub(crate) fn string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    push_str(&mut out, s);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_like_go() {
        // Backslashes shown as '/' to keep the expected escapes readable.
        let got = string("a\"b\\c\n<>&\u{1}").replace('\\', "/");
        assert_eq!(r#""a/"b//c/n/u003c/u003e/u0026/u0001""#, got);
    }
}
