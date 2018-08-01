// #[macro_use]
// extern crate log;
extern crate bitcoin;
extern crate rand;
extern crate wallet;

// use bitcoin::network::constants::Network;
use rand::{OsRng, RngCore};
use wallet::error::WalletError;
// use wallet::keyfactory;
use wallet::mnemonic::Mnemonic;

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

#[test]
fn test_mnemonic() -> Result<(), WalletError> {
    let mnemonic = mnemonic("")?;
    println!("mnemonic = {}", mnemonic);
    Ok(())
}
