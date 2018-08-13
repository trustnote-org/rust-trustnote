use base64;
use error::Result;
use failure::ResultExt;
use secp256k1::{key, Message, Secp256k1, Signature};

lazy_static! {
    // initialize consume too much memory, init it in thread context
    static ref SECP256K1: Secp256k1 = ::std::thread::spawn(|| Secp256k1::new()).join().unwrap();
}

pub trait Signer {
    fn sign(&self, hash: &[u8], address: &str) -> Result<String>;
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

#[test]
fn test_signature() -> Result<()> {
    let hash = "KLop9582tzXZJbytWjiWLcnpEdvJI7mUymbnUPXweOM=";
    let priv_key = "jQGnkLnZlX2DjBUd8JKgHgw23zSdRL/Azx3foi/WqvE=";
    let sig =
        "YCdh5Q6jOiKQy2R9mQwKJ6tBnq31VFZX2dkb7Ypr+/5z6jj4GLEFT9RtryC4+mSILtKKLeN9YnBmYI4Xa+4tDw==";

    assert_eq!(
        sign(&base64::decode(hash)?, &base64::decode(priv_key)?)?,
        sig
    );

    let hash = "uPQs4TwLtDGRAdH8sbIJ1ZyWpEmwHWRAhXpamODZ7Kk=";
    let pub_key = "A0qTjB3ZjHf2yT1EIvLrkVAWY8MPSueNcB4GTlKGo/o6";
    let sig =
        "up+2Fjhnu4OjJeesBPCgoZE+6ReqQDdnqcjhbq2iaulHjlwKYLcwRrD3udSWdHS57ceQeZ+LVPWYBMWBloAgpA==";

    assert_eq!(verify(&base64::decode(hash)?, sig, pub_key)?, ());

    Ok(())
}
