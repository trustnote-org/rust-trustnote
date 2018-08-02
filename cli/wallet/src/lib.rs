// #[macro_use]
// extern crate log;
extern crate bitcoin;
#[macro_use]
extern crate lazy_static;
extern crate rand;
extern crate wallet;

use bitcoin::network::constants::Network;
use bitcoin::util::bip32::{ChildNumber, ExtendedPrivKey, ExtendedPubKey};
use rand::{OsRng, RngCore};
use wallet::error::WalletError;
use wallet::keyfactory::{KeyFactory, Seed};
use wallet::mnemonic::Mnemonic;

pub type Result<T> = ::std::result::Result<T, WalletError>;

lazy_static! {
    // initialize consume too much memory, init it in thread context
    static ref KEY_FACTORY: KeyFactory = ::std::thread::spawn(|| KeyFactory::new()).join().unwrap();
}

/// generate random mnemonic
pub fn mnemonic(passphrase: &str) -> Result<Mnemonic> {
    let mut encrypted = vec![0u8; 32];
    if let Ok(mut rng) = OsRng::new() {
        rng.fill_bytes(encrypted.as_mut_slice());
        let mnemonic = Mnemonic::new(&encrypted, passphrase)?;
        return Ok(mnemonic);
    }
    Err(WalletError::Generic("can not obtain random source"))
}

/// generator master private key from mnemonic
pub fn master_private_key(mnemonic: &Mnemonic, salt: &str) -> Result<ExtendedPrivKey> {
    let seed = Seed::new(&mnemonic, salt);
    Ok(KEY_FACTORY.master_private_key(Network::Bitcoin, &seed)?)
}

/// get extended public key for a known private key
pub fn extended_public_from_private(extended_private_key: &ExtendedPrivKey) -> ExtendedPubKey {
    KEY_FACTORY.extended_public_from_private(extended_private_key)
}

/// get wallet pubkey for a index
pub fn wallet_pub_key(master_prvk: &ExtendedPrivKey, index: u32) -> Result<ExtendedPubKey> {
    let prvk = KEY_FACTORY.private_child(master_prvk, ChildNumber::Hardened(44))?;
    let prvk = KEY_FACTORY.private_child(&prvk, ChildNumber::Hardened(0))?;
    let prvk = KEY_FACTORY.private_child(&prvk, ChildNumber::Hardened(index))?;
    Ok(KEY_FACTORY.extended_public_from_private(&prvk))
}

#[test]
fn test_mnemonic() -> Result<()> {
    let mnemonic = mnemonic("")?;
    println!("mnemonic = {}", mnemonic.to_string());
    Ok(())
}

#[test]
fn test_master_private_key() -> Result<()> {
    let mnemonic = mnemonic("")?;
    let prvk = master_private_key(&mnemonic, "")?;
    println!("master_private_key = {}", prvk.to_string());
    Ok(())
}

#[test]
fn test_extended_public_from_private() -> Result<()> {
    let mnemonic = mnemonic("")?;
    let prvk = master_private_key(&mnemonic, "")?;
    let pubk = extended_public_from_private(&prvk);
    println!("master_private_key = {}", pubk.to_string());
    Ok(())
}

#[test]
fn test_wallet_pub_key() -> Result<()> {
    let mnemonic = mnemonic("")?;
    let prvk = master_private_key(&mnemonic, "")?;
    let index = 0;
    let wallet_pubk = wallet_pub_key(&prvk, index)?;
    println!("wallet_public_key_{} = {}", index, wallet_pubk.to_string());
    Ok(())
}
