//! Test-side JWT minting (upstream tests sign tokens with golang-jwt). This is
//! fixture tooling, not the identifier under test: it only produces tokens.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn b64url(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        for i in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * i)) & 63) as usize] as char);
        }
    }
    out
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    const BLOCK: usize = 64;
    let mut k = if key.len() > BLOCK { Sha256::digest(key).to_vec() } else { key.to_vec() };
    k.resize(BLOCK, 0);
    let ipad: Vec<u8> = k.iter().map(|b| b ^ 0x36).collect();
    let opad: Vec<u8> = k.iter().map(|b| b ^ 0x5c).collect();
    let inner = Sha256::new().chain_update(&ipad).chain_update(msg).finalize();
    Sha256::new().chain_update(&opad).chain_update(inner).finalize().to_vec()
}

fn segments(alg: &str, claims: &Value) -> String {
    let header = json!({"alg": alg, "typ": "JWT"});
    format!("{}.{}", b64url(header.to_string().as_bytes()), b64url(claims.to_string().as_bytes()))
}

/// An HS256-signed token (golang-jwt `NewWithClaims(SigningMethodHS256, claims).SignedString(secret)`).
pub fn sign_hs256(claims: &Value, secret: &str) -> String {
    let signing_input = segments("HS256", claims);
    let sig = hmac_sha256(secret.as_bytes(), signing_input.as_bytes());
    format!("{signing_input}.{}", b64url(&sig))
}

/// An `alg: none` token with an empty signature (golang-jwt `SigningMethodNone`).
pub fn unsigned(claims: &Value) -> String {
    format!("{}.", segments("none", claims))
}

/// Unix seconds `offset_secs` from now (golang-jwt tests use `time.Now().Add(..).Unix()`).
pub fn unix_from_now(offset_secs: i64) -> i64 {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock after epoch");
    now.as_secs() as i64 + offset_secs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_matches_rfc4231_case_2() {
        let mac = hmac_sha256(b"Jefe", b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");
    }

    #[test]
    fn b64url_has_no_padding() {
        assert_eq!(b64url(b"f"), "Zg");
        assert_eq!(b64url(b"fo"), "Zm8");
        assert_eq!(b64url(b"foo"), "Zm9v");
        assert_eq!(b64url(&[0xfb, 0xff]), "-_8");
    }
}
