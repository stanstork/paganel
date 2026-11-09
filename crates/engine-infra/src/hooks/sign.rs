use hmac::{Hmac, Mac};
use sha2::Sha256;

/// `hex(HMAC-SHA256(secret, "{timestamp}.{body}"))`: the value after `sha256=`
/// in `X-Paganel-Signature`.
pub fn signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
    #[allow(clippy::expect_used)]
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC takes a key of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);

    hex::encode(mac.finalize().into_bytes())
}
