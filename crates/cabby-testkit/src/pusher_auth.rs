//! Client-side Pusher signing, as Pusher client libraries do it (test driver code,
//! independent of the implementation under test):
//! * REST API request signing, mirroring `pusher-http-go` v5 `createRequestURL`;
//! * channel authorization strings (`<key>:<hex hmac>`), as in the upstream
//!   feature scenarios' `pusher_signature`.

use hmac::{Hmac, KeyInit, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;

pub fn hmac_sha256_hex(secret: &str, input: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(input.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn md5_hex(body: &[u8]) -> String {
    hex::encode(Md5::digest(body))
}

/// `<key>:<hmac(secret, string_to_sign)>`.
pub fn channel_auth(key: &str, secret: &str, string_to_sign: &str) -> String {
    format!("{key}:{}", hmac_sha256_hex(secret, string_to_sign))
}

/// Current Unix time in seconds, as a string (`auth_timestamp`).
pub fn now_timestamp() -> String {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("clock after epoch").as_secs().to_string()
}

/// A signed REST request target (`path?query`): the query holds `auth_key`,
/// `auth_timestamp`, `auth_version=1.0`, `body_md5` (when a body is sent) and
/// `auth_signature` over `METHOD\npath\n<sorted unsigned params>`.
pub fn signed_target(
    method: &str,
    path: &str,
    key: &str,
    secret: &str,
    timestamp: &str,
    body: Option<&[u8]>,
) -> String {
    let mut params = vec![
        ("auth_key".to_owned(), key.to_owned()),
        ("auth_timestamp".to_owned(), timestamp.to_owned()),
        ("auth_version".to_owned(), "1.0".to_owned()),
    ];
    if let Some(b) = body {
        params.push(("body_md5".to_owned(), md5_hex(b)));
    }
    params.sort();
    let unsigned = params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let signature = hmac_sha256_hex(secret, &format!("{method}\n{path}\n{unsigned}"));
    params.push(("auth_signature".to_owned(), signature));
    params.sort();
    let query = params.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    format!("{path}?{query}")
}

/// `pusher.Client.GetChannelUsers`: sends a signed `GET /apps/<app>/channels/<ch>/users`
/// through `send(method, target) -> (status, body)` and returns the user IDs. A non-2xx
/// status is an error `Status Code: <code> - <body>`, as in pusher-http-go.
pub fn get_channel_users(
    app_id: &str,
    key: &str,
    secret: &str,
    channel: &str,
    send: impl FnOnce(&str, &str) -> (u16, Vec<u8>),
) -> Result<Vec<String>, String> {
    let path = format!("/apps/{app_id}/channels/{channel}/users");
    let target = signed_target("GET", &path, key, secret, &now_timestamp(), None);
    let (status, body) = send("GET", &target);
    if !(200..300).contains(&status) {
        return Err(format!("Status Code: {status} - {}", String::from_utf8_lossy(&body)));
    }
    let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
    let users = v.get("users").and_then(|u| u.as_array()).cloned().unwrap_or_default();
    Ok(users.iter().map(|u| u.get("id").and_then(|i| i.as_str()).unwrap_or_default().to_owned()).collect())
}
