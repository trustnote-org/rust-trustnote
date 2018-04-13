use std;
use base64::{encode, decode};
use secp256k1::{Secp256k1, Message, key, Signature, Error};

pub fn sign(hash: &[u8], priv_key: &[u8]) -> String{
    let s = Secp256k1::new();
    let priv_key = key::SecretKey::from_slice(&s, priv_key).unwrap();

    //Sign it with the secret key
    let msg = Message::from_slice(hash).unwrap();
    let recoverable = s.sign_recoverable(&msg, &priv_key).unwrap();
    let (_, sig) = recoverable.serialize_compact(&s);

    //println!("Signed message {} signature {:?}", encode(&hash), encode(&sig[..]));
    encode(&sig[..])
}

pub fn verify(hash: &[u8], b64_sig: &str, b64_pub_key: &str) -> std::result::Result<(), Error>{
    let sig = &decode(b64_sig).unwrap()[..];

    let s = Secp256k1::new();
    let msg = Message::from_slice(hash).unwrap();
    let pub_key = key::PublicKey::from_slice(&s, &decode(b64_pub_key).unwrap()[..]).unwrap();
    let signature = Signature::from_compact(&s, sig).unwrap();
    let res = s.verify(&msg, &signature, &pub_key);

    //println!("verify {:?}!", res);
    res
}