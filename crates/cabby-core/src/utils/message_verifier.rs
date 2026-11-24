//! Rails-compatible message verifier (upstream `utils.MessageVerifier`, which follows
//! `ActiveSupport::MessageVerifier` with SHA-256 digests).
//!
//! A message is `base64(json(payload)) + "--" + hex(hmac_sha256(key, base64 part))`.
//! The JSON is encoded the way Go's `encoding/json` does it (sorted object keys, HTML
//! characters escaped as `\u003c`, `\u003e` and `\u0026`, Go float formatting), so
//! tokens match the ones Go and Rails produce byte for byte. Error texts are upstream's.

use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::{GeneralPurpose, GeneralPurposeConfig};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::Value;
use sha2::Sha256;

/// A verification failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct VerifierError(pub String);

/// Go's `base64.StdEncoding`: padded, and lenient about the unused bits of the last symbol.
const GO_STD_ENCODING: GeneralPurpose =
    GeneralPurpose::new(&base64::alphabet::STANDARD, GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true));

/// Rails-compatible `MessageVerifier` (Base64 JSON payload + `--` + HMAC-SHA256 hex digest).
#[derive(Debug, Clone)]
pub struct MessageVerifier {
    key: Vec<u8>,
}

impl MessageVerifier {
    pub fn new(key: &str) -> Self {
        MessageVerifier { key: key.as_bytes().to_vec() }
    }

    /// Signs a JSON-encodable payload.
    pub fn generate(&self, payload: &Value) -> Result<String, VerifierError> {
        let mut json = String::new();
        write_go_json(&mut json, payload);
        let encoded = GO_STD_ENCODING.encode(json);
        let signature = self.sign(encoded.as_bytes());
        Ok(encoded + "--" + &signature)
    }

    /// Verifies and decodes a signed message.
    pub fn verified(&self, msg: &str) -> Result<Value, VerifierError> {
        self.validate(msg).map_err(|e| VerifierError(format!("failed to verify message, cause: {e}")))?;

        let data = msg.split("--").next().unwrap_or_default();
        let json = decode_base64(data)?;
        serde_json::from_slice(&json).map_err(|e| VerifierError(e.to_string()))
    }

    /// Checks the signature only.
    pub fn validate(&self, msg: &str) -> Result<(), VerifierError> {
        if self.key.is_empty() {
            return Err(VerifierError("verification key is empty".into()));
        }
        if msg.is_empty() {
            return Err(VerifierError("message is empty".into()));
        }
        let parts = msg.split("--").count();
        if parts != 2 {
            return Err(VerifierError(format!("message must contain 2 parts, got {parts}")));
        }
        let (data, digest) = msg.split_once("--").expect("two parts");
        if self.verify_signature(data.as_bytes(), digest.as_bytes()) {
            Ok(())
        } else {
            Err(VerifierError("invalid signature".into()))
        }
    }

    /// The lowercase hex HMAC-SHA256 digest of `payload` (upstream `Sign`).
    pub fn sign(&self, payload: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(payload);
        let digest = mac.finalize().into_bytes();
        let mut hex = String::with_capacity(digest.len() * 2);
        for b in digest.iter() {
            let _ = write!(hex, "{b:02x}");
        }
        hex
    }

    /// Constant-time comparison of `digest` with the expected hex digest (upstream
    /// `VerifySignature`).
    pub fn verify_signature(&self, payload: &[u8], digest: &[u8]) -> bool {
        let actual = self.sign(payload);
        crate::utils::constant_time_eq(actual.as_bytes(), digest)
    }
}

/// Go's `base64.StdEncoding.DecodeString`, which skips CR and LF.
fn decode_base64(data: &str) -> Result<Vec<u8>, VerifierError> {
    let cleaned: std::borrow::Cow<'_, str> = if data.contains(['\r', '\n']) {
        data.chars().filter(|c| *c != '\r' && *c != '\n').collect::<String>().into()
    } else {
        data.into()
    };
    GO_STD_ENCODING.decode(cleaned.as_bytes()).map_err(|e| {
        use base64::DecodeError::*;
        let at = match e {
            InvalidByte(offset, _) | InvalidLastSymbol(offset, _) => offset,
            InvalidLength(len) => len,
            InvalidPadding => cleaned.len(),
        };
        VerifierError(format!("illegal base64 data at input byte {at}"))
    })
}

/// Appends `v` encoded like Go's `json.Marshal` of the equivalent `interface{}` value.
fn write_go_json(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let _ = write!(out, "{i}");
            } else if let Some(u) = n.as_u64() {
                let _ = write!(out, "{u}");
            } else {
                write_go_float(out, n.as_f64().unwrap_or_default());
            }
        }
        Value::String(s) => write_go_string(out, s),
        Value::Array(a) => {
            out.push('[');
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_go_json(out, e);
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut entries: Vec<_> = m.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, e)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_go_string(out, k);
                out.push(':');
                write_go_json(out, e);
            }
            out.push('}');
        }
    }
}

/// `encoding/json` float64 formatting: shortest representation, exponent form outside
/// `[1e-6, 1e21)`, exponents without zero padding.
fn write_go_float(out: &mut String, f: f64) {
    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        let e = format!("{f:e}");
        let (mantissa, exp) = e.split_once('e').expect("exponent form");
        if let Some(neg) = exp.strip_prefix('-') {
            let _ = write!(out, "{mantissa}e-{neg}");
        } else {
            let _ = write!(out, "{mantissa}e+{exp}");
        }
    } else {
        let _ = write!(out, "{f}");
    }
}

/// `encoding/json` string escaping with HTML escaping enabled.
fn write_go_string(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' | '\0'..='\u{1f}' => {
                let n = c as u32;
                out.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    out.push(HEX[((n >> shift) & 0xf) as usize] as char);
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn go_json(v: &Value) -> String {
        let mut s = String::new();
        write_go_json(&mut s, v);
        s
    }

    #[test]
    fn encodes_like_go() {
        assert_eq!(
            r#"{"a":[1,-2,1.5],"b":"\u003cx\u003e \u0026 \"q\"\n"}"#,
            go_json(&json!({"b": "<x> & \"q\"\n", "a": [1, -2, 1.5]}))
        );
        assert_eq!("1e+21", go_json(&json!(1e21)));
        assert_eq!("1e-7", go_json(&json!(1e-7)));
        assert_eq!("0.000001", go_json(&json!(0.000001)));
        assert_eq!("100000000000000000000", go_json(&json!(1e20)));
        assert_eq!(r#""\u0001\u2028""#, go_json(&json!("\u{1}\u{2028}")));
    }

    #[test]
    fn reports_upstream_errors() {
        let v = MessageVerifier::new("k");
        assert_eq!("message is empty", v.validate("").unwrap_err().0);
        assert_eq!("message must contain 2 parts, got 3", v.validate("a--b--c").unwrap_err().0);
        assert_eq!("failed to verify message, cause: invalid signature", v.verified("a--b").unwrap_err().0);
        assert_eq!("verification key is empty", MessageVerifier::new("").validate("a--b").unwrap_err().0);
        let signed = v.generate(&json!({"x": [1, "y"]})).unwrap();
        assert_eq!(json!({"x": [1, "y"]}), v.verified(&signed).unwrap());
    }
}
