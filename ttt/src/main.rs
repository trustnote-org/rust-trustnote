#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate fern;
extern crate may;
extern crate serde;
extern crate serde_json;
extern crate trustnote;
extern crate trustnote_wallet_base;

mod config;

use std::sync::Arc;

use clap::App;
use trustnote::*;
use trustnote_wallet_base::Mnemonic;

fn log_init() {
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Error
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S%.3f]"),
                record.level(),
                record.target(),
                message
            ))
        }).level(log_lvl)
        .chain(std::io::stdout())
        .apply()
        .unwrap();

    debug!("log init done!");
}

fn connect_to_remote(peers: &[String]) -> Result<Arc<network::wallet::WalletConn>> {
    for peer in peers {
        match network::wallet::create_outbound_conn(&peer) {
            Err(e) => {
                error!(" fail to connected: {}, err={}", peer, e);
                continue;
            }
            Ok(c) => return Ok(c),
        }
    }
    bail!("failed to connect remote hub");
}

fn info() -> Result<()> {
    let settings = config::get_settings();
    let mnemonic = Mnemonic::from(&settings.mnemonic)?;
    let prvk = trustnote_wallet_base::master_private_key(&mnemonic, "")?;
    let wallet = 0;

    println!("mnemonic = {}", mnemonic.to_string());
    println!("wallet_private_key = {}", prvk.to_string());

    let wallet_pubk = trustnote_wallet_base::wallet_pubkey(&prvk, wallet)?;
    println!("wallet_public_key = {}", wallet_pubk.to_string());

    let wallet_id = trustnote_wallet_base::wallet_id(&wallet_pubk)?;
    println!("wallet_id= {}", wallet_id);

    let wallet_address = trustnote_wallet_base::wallet_address(&wallet_pubk, false, 0)?;
    println!("wallet_0/0_address = {}", wallet_address);

    let device_address = trustnote_wallet_base::device_address(&prvk)?;
    println!("device_address = {}", device_address);
    Ok(())
}

fn main() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size);
    let mut db_path = ::std::env::current_dir()?;
    db_path.push(config::DB_PATH);
    db::set_db_path(db_path);

    log_init();

    let yml = load_yaml!("ttt.yml");
    let m = App::from_yaml(yml).get_matches();

    //Info
    if let Some(_info) = m.subcommand_matches("info") {
        return info();
    }

    //Log
    if let Some(_log) = m.subcommand_matches("log") {
        println!("Wallet History");
        return Ok(());
    }

    let settings = config::get_settings();
    let _conn = connect_to_remote(&settings.hub_url)?;

    //Sync
    if let Some(sync) = m.subcommand_matches("sync") {
        if let Some(mnemonic) = sync.value_of("mnemonic") {
            println!("Init wallet with mnemonic {}", mnemonic);
        } else {
            println!("Init wallet with random mnemonic");
        }
    }

    //Send
    if let Some(send) = m.subcommand_matches("send") {
        if let Some(address) = send.value_of("address") {
            if let Some(amount) = send.value_of("amount") {
                println!("Pay to address {} amount {}", address, amount);
            }
        }
    }

    use std::io::Read;
    ::std::io::stdin().read(&mut [0; 1]).unwrap();

    Ok(())
}
