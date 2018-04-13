use error::Result;

use base64;
use secp256k1::{key, Message, Secp256k1, Signature};

/// return a bas64 string for the encrypted hash with the priv_key
pub fn sign(hash: &[u8], priv_key: &[u8]) -> Result<String> {
    let s = Secp256k1::new();
    let msg = Message::from_slice(hash)?;
    let priv_key = key::SecretKey::from_slice(&s, priv_key)?;

    //Sign it with the secret key
    let recoverable = s.sign_recoverable(&msg, &priv_key)?;
    let (_, sig) = recoverable.serialize_compact(&s);
    Ok(base64::encode(&sig[..]))
}

/// verify the bas64 string signiture with the hash and pub key (a bas64 string)
pub fn verify(hash: &[u8], b64_sig: &str, b64_pub_key: &str) -> Result<()> {
    let s = Secp256k1::new();
    let msg = Message::from_slice(hash)?;
    let sig = &base64::decode(b64_sig)?;
    let pub_key = key::PublicKey::from_slice(&s, &base64::decode(b64_pub_key)?)?;

    // verify the signature
    let signature = Signature::from_compact(&s, sig)?;
    Ok(s.verify(&msg, &signature, &pub_key)?)
}
