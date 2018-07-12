use base64;
use error::Result;
use failure::ResultExt;
use secp256k1::{key, Message, Secp256k1, Signature};

lazy_static! {
    static ref SECP256K1: Secp256k1 = Secp256k1::new();
}

pub fn init_secp256k1() -> Result<()> {
    // initialize consume too much memory
    let hash = "KLop9582tzXZJbytWjiWLcnpEdvJI7mUymbnUPXweOM=";
    let priv_key = "jQGnkLnZlX2DjBUd8JKgHgw23zSdRL/Azx3foi/WqvE=";
    let sig =
        "YCdh5Q6jOiKQy2R9mQwKJ6tBnq31VFZX2dkb7Ypr+/5z6jj4GLEFT9RtryC4+mSILtKKLeN9YnBmYI4Xa+4tDw==";

    assert_eq!(
        sign(&base64::decode(hash)?, &base64::decode(priv_key)?)?,
        sig
    );
    Ok(())
}

/// return a bas64 string for the encrypted hash with the priv_key
pub fn sign(hash: &[u8], priv_key: &[u8]) -> Result<String> {
    let msg = Message::from_slice(hash)?;
    let priv_key = key::SecretKey::from_slice(&SECP256K1, priv_key)?;

    //Sign it with the secret key
    let recoverable = SECP256K1
        .sign_recoverable(&msg, &priv_key)
        .context("SECP256K1 sign failed")?;
    let (_, sig) = recoverable.serialize_compact(&SECP256K1);
    Ok(base64::encode(&sig[..]))
}

/// verify the bas64 string signiture with the hash and pub key (a bas64 string)
pub fn verify(hash: &[u8], b64_sig: &str, b64_pub_key: &str) -> Result<()> {
    let msg = Message::from_slice(hash)?;
    let sig = &base64::decode(b64_sig)?;
    let pub_key = key::PublicKey::from_slice(&SECP256K1, &base64::decode(b64_pub_key)?)?;

    // verify the signature
    let signature =
        Signature::from_compact(&SECP256K1, sig).context("invalid SECP256K1 signature")?;
    SECP256K1
        .verify(&msg, &signature, &pub_key)
        .context("SECP256K1 verify failed")?;
    Ok(())
}
