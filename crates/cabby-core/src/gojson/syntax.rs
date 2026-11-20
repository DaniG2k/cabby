//! Go's JSON syntax errors (`encoding/json` scanner): `json.Unmarshal` checks the
//! input with this scanner before decoding, so a syntax error is reported with
//! the scanner's text whatever the target type.

/// Go's `maxNestingDepth`.
const MAX_DEPTH: usize = 10000;

#[derive(Clone, Copy, PartialEq)]
enum Parse {
    ObjectKey,
    ObjectValue,
    ArrayValue,
}

#[derive(Clone, Copy, PartialEq)]
enum State {
    BeginValue,
    BeginValueOrEmpty,
    BeginStringOrEmpty,
    BeginString,
    EndValue,
    EndTop,
    InString,
    InStringEsc,
    InStringEscU(u8),
    Neg,
    Zero,
    One,
    Dot,
    Dot0,
    E,
    ESign,
    E0,
    Literal(&'static [u8], usize),
}

/// Go `quoteChar`.
fn quote_char(c: u8) -> String {
    match c {
        b'\'' => r"'\''".to_owned(),
        b'"' => "'\"'".to_owned(),
        b'\\' => r"'\\'".to_owned(),
        0x07 => r"'\a'".to_owned(),
        0x08 => r"'\b'".to_owned(),
        0x0c => r"'\f'".to_owned(),
        b'\n' => r"'\n'".to_owned(),
        b'\r' => r"'\r'".to_owned(),
        b'\t' => r"'\t'".to_owned(),
        0x0b => r"'\v'".to_owned(),
        0x20..=0x7e => format!("'{}'", c as char),
        0x00..=0x1f | 0x7f => format!(r"'\x{c:02x}'"),
        // Go converts the byte to the rune of the same value (Latin-1).
        0x80..=0xa0 | 0xad => format!(r"'\u{:04x}'", u32::from(c)),
        _ => format!("'{}'", char::from(c)),
    }
}

fn invalid(c: u8, context: &str) -> String {
    format!("invalid character {} {context}", quote_char(c))
}

fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n')
}

struct Scanner {
    state: State,
    stack: Vec<Parse>,
}

impl Scanner {
    fn push(&mut self, p: Parse) -> Result<(), String> {
        self.stack.push(p);
        if self.stack.len() > MAX_DEPTH {
            return Err("exceeded max depth".to_owned());
        }
        Ok(())
    }

    fn pop(&mut self) {
        self.stack.pop();
        self.state = if self.stack.is_empty() { State::EndTop } else { State::EndValue };
    }

    fn begin_value(&mut self, c: u8) -> Result<(), String> {
        self.state = match c {
            c if is_space(c) => return Ok(()),
            b'{' => {
                self.push(Parse::ObjectKey)?;
                State::BeginStringOrEmpty
            }
            b'[' => {
                self.push(Parse::ArrayValue)?;
                State::BeginValueOrEmpty
            }
            b'"' => State::InString,
            b'-' => State::Neg,
            b'0' => State::Zero,
            b'1'..=b'9' => State::One,
            b't' => State::Literal(b"true", 1),
            b'f' => State::Literal(b"false", 1),
            b'n' => State::Literal(b"null", 1),
            _ => return Err(invalid(c, "looking for beginning of value")),
        };
        Ok(())
    }

    fn end_value(&mut self, c: u8) -> Result<(), String> {
        let Some(&top) = self.stack.last() else {
            self.state = State::EndTop;
            return self.step(c);
        };
        if is_space(c) {
            self.state = State::EndValue;
            return Ok(());
        }
        match top {
            Parse::ObjectKey if c == b':' => {
                *self.stack.last_mut().expect("top") = Parse::ObjectValue;
                self.state = State::BeginValue;
            }
            Parse::ObjectKey => return Err(invalid(c, "after object key")),
            Parse::ObjectValue if c == b',' => {
                *self.stack.last_mut().expect("top") = Parse::ObjectKey;
                self.state = State::BeginString;
            }
            Parse::ObjectValue if c == b'}' => self.pop(),
            Parse::ObjectValue => return Err(invalid(c, "after object key:value pair")),
            Parse::ArrayValue if c == b',' => self.state = State::BeginValue,
            Parse::ArrayValue if c == b']' => self.pop(),
            Parse::ArrayValue => return Err(invalid(c, "after array element")),
        }
        Ok(())
    }

    fn step(&mut self, c: u8) -> Result<(), String> {
        match self.state {
            State::BeginValue => self.begin_value(c),
            State::BeginValueOrEmpty => {
                if is_space(c) {
                    Ok(())
                } else if c == b']' {
                    self.pop();
                    Ok(())
                } else {
                    self.begin_value(c)
                }
            }
            State::BeginStringOrEmpty if is_space(c) => Ok(()),
            State::BeginStringOrEmpty if c == b'}' => {
                self.pop();
                Ok(())
            }
            State::BeginStringOrEmpty | State::BeginString => {
                if is_space(c) {
                    Ok(())
                } else if c == b'"' {
                    self.state = State::InString;
                    Ok(())
                } else {
                    Err(invalid(c, "looking for beginning of object key string"))
                }
            }
            State::EndValue => self.end_value(c),
            State::EndTop => {
                if is_space(c) {
                    Ok(())
                } else {
                    Err(invalid(c, "after top-level value"))
                }
            }
            State::InString => {
                match c {
                    b'"' => self.state = State::EndValue,
                    b'\\' => self.state = State::InStringEsc,
                    c if c < 0x20 => return Err(invalid(c, "in string literal")),
                    _ => {}
                }
                Ok(())
            }
            State::InStringEsc => {
                self.state = match c {
                    b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => State::InString,
                    b'u' => State::InStringEscU(0),
                    _ => return Err(invalid(c, "in string escape code")),
                };
                Ok(())
            }
            State::InStringEscU(n) => {
                if !c.is_ascii_hexdigit() {
                    return Err(invalid(c, "in \\u hexadecimal character escape"));
                }
                self.state = if n == 3 { State::InString } else { State::InStringEscU(n + 1) };
                Ok(())
            }
            State::Neg => {
                self.state = match c {
                    b'0' => State::Zero,
                    b'1'..=b'9' => State::One,
                    _ => return Err(invalid(c, "in numeric literal")),
                };
                Ok(())
            }
            State::One if c.is_ascii_digit() => Ok(()),
            State::One | State::Zero => match c {
                b'.' => {
                    self.state = State::Dot;
                    Ok(())
                }
                b'e' | b'E' => {
                    self.state = State::E;
                    Ok(())
                }
                _ => self.end_value(c),
            },
            State::Dot => {
                if !c.is_ascii_digit() {
                    return Err(invalid(c, "after decimal point in numeric literal"));
                }
                self.state = State::Dot0;
                Ok(())
            }
            State::Dot0 => match c {
                c if c.is_ascii_digit() => Ok(()),
                b'e' | b'E' => {
                    self.state = State::E;
                    Ok(())
                }
                _ => self.end_value(c),
            },
            State::E if c == b'+' || c == b'-' => {
                self.state = State::ESign;
                Ok(())
            }
            State::E | State::ESign => {
                if !c.is_ascii_digit() {
                    return Err(invalid(c, "in exponent of numeric literal"));
                }
                self.state = State::E0;
                Ok(())
            }
            State::E0 => {
                if c.is_ascii_digit() {
                    Ok(())
                } else {
                    self.end_value(c)
                }
            }
            State::Literal(word, i) => {
                if c != word[i] {
                    let name = std::str::from_utf8(word).unwrap_or_default();
                    return Err(invalid(c, &format!("in literal {name} (expecting {})", quote_char(word[i]))));
                }
                self.state = if i + 1 == word.len() { State::EndValue } else { State::Literal(word, i + 1) };
                Ok(())
            }
        }
    }
}

/// Go's `checkValid`: the syntax error `json.Unmarshal` reports for `raw`, if any.
pub fn syntax_error(raw: &[u8]) -> Option<String> {
    let mut s = Scanner { state: State::BeginValue, stack: Vec::new() };
    for &c in raw {
        if let Err(e) = s.step(c) {
            return Some(e);
        }
    }
    // Go `eof`: a pending top-level number ends with the input.
    if s.state != State::EndTop && s.step(b' ').is_err() {
        return Some("unexpected end of JSON input".to_owned());
    }
    if s.state != State::EndTop {
        return Some("unexpected end of JSON input".to_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::syntax_error;

    #[test]
    fn reports_go_scanner_errors() {
        let cases: &[(&[u8], Option<&str>)] = &[
            (b"{\"a\": 1}", None),
            (b" 12 ", None),
            (b"12", None),
            (b"this is not json", Some("invalid character 'h' in literal true (expecting 'r')")),
            (b"", Some("unexpected end of JSON input")),
            (b"{\"a\"", Some("unexpected end of JSON input")),
            (b"{\"a\" 1}", Some("invalid character '1' after object key")),
            (b"{\"a\":1 2}", Some("invalid character '2' after object key:value pair")),
            (b"[1 2]", Some("invalid character '2' after array element")),
            (b"{1:2}", Some("invalid character '1' looking for beginning of object key string")),
            (b"x", Some("invalid character 'x' looking for beginning of value")),
            (b"{} x", Some("invalid character 'x' after top-level value")),
            (b"1.x", Some("invalid character 'x' after decimal point in numeric literal")),
            (b"1ex", Some("invalid character 'x' in exponent of numeric literal")),
            (b"-x", Some("invalid character 'x' in numeric literal")),
            (b"\"\\x\"", Some("invalid character 'x' in string escape code")),
            (b"\"\\u12x4\"", Some("invalid character 'x' in \\u hexadecimal character escape")),
            (b"\"a\nb\"", Some("invalid character '\\n' in string literal")),
            (b"nul", Some("unexpected end of JSON input")),
            (b"'a'", Some("invalid character '\\'' looking for beginning of value")),
        ];
        for (raw, want) in cases {
            assert_eq!(want.map(str::to_owned), syntax_error(raw), "{}", String::from_utf8_lossy(raw));
        }
    }
}
