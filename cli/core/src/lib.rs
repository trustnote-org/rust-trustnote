// #[macro_use]
// extern crate log;
extern crate bitcoin;
#[macro_use]
extern crate lazy_static;
extern crate rand;
extern crate wallet;

use bitcoin::network::constants::Network;
// use bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey, ExtendedPubKey};
use rand::{OsRng, RngCore};
use wallet::error::WalletError;
use wallet::keyfactory::{KeyFactory, Seed};
use wallet::mnemonic::Mnemonic;

lazy_static! {
    // initialize consume too much memory, init it in thread context
    static ref KEY_FACTORY: KeyFactory = ::std::thread::spawn(|| KeyFactory::new()).join().unwrap();
}

/// generate random mnemonic
pub fn mnemonic(passphrase: &str) -> Result<String, WalletError> {
    let mut encrypted = vec![0u8; 32];
    if let Ok(mut rng) = OsRng::new() {
        rng.fill_bytes(encrypted.as_mut_slice());
        let mnemonic = Mnemonic::new(&encrypted, passphrase)?;
        return Ok(mnemonic.to_string());
    }
    Err(WalletError::Generic("can not obtain random source"))
}

/// generator master private key from mnemonic
pub fn master_private_key(mnemonic: &str, salt: &str) -> Result<String, WalletError> {
    let mnemonic = Mnemonic::from(mnemonic)?;
    let seed = Seed::new(&mnemonic, salt);
    Ok(KEY_FACTORY
        .master_private_key(Network::Bitcoin, &seed)?
        .to_string())
}

#[test]
fn test_mnemonic() -> Result<(), WalletError> {
    let mnemonic = mnemonic("")?;
    println!("mnemonic = {}", mnemonic);
    Ok(())
}

#[test]
fn test_master_private_key() -> Result<(), WalletError> {
    let mnemonic = mnemonic("")?;
    let pk = master_private_key(&mnemonic, "")?;
    println!("master_private_key = {}", pk);
    Ok(())
}
