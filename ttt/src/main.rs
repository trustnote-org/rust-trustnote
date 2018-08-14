#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate failure;
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

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Local, TimeZone};
use clap::App;
use composer;
use failure::ResultExt;
use trustnote::network::wallet::WalletConn;
use trustnote::*;
use trustnote_wallet_base::{Base64KeyExt, ExtendedPrivKey, ExtendedPubKey, Mnemonic};

use trustnote::signature::Signer;

struct WalletInfo {
    #[allow(dead_code)]
    master_prvk: ExtendedPrivKey,
    wallet_pubk: ExtendedPubKey,
    device_address: String,
    wallet_0_id: String,
    _00_address: String,
    _00_address_pubk: ExtendedPubKey,
    _00_address_prvk: ExtendedPrivKey,
}

impl WalletInfo {
    fn from_mnemonic(mnemonic: &str) -> Result<WalletInfo> {
        let wallet = 0;
        let mnemonic = Mnemonic::from(&mnemonic)?;
        let master_prvk = trustnote_wallet_base::master_private_key(&mnemonic, "")?;
        let device_address = trustnote_wallet_base::device_address(&master_prvk)?;
        let wallet_pubk = trustnote_wallet_base::wallet_pubkey(&master_prvk, wallet)?;
        let wallet_0_id = trustnote_wallet_base::wallet_id(&wallet_pubk);
        let _00_address = trustnote_wallet_base::wallet_address(&wallet_pubk, false, 0)?;
        let _00_address_prvk =
            trustnote_wallet_base::wallet_address_prvkey(&master_prvk, 0, false, 0)?;
        let _00_address_pubk =
            trustnote_wallet_base::wallet_address_pubkey(&wallet_pubk, false, 0)?;

        Ok(WalletInfo {
            master_prvk,
            wallet_pubk,
            device_address,
            wallet_0_id,
            _00_address,
            _00_address_pubk,
            _00_address_prvk,
        })
    }
}

impl Signer for WalletInfo {
    fn sign(&self, hash: &[u8], address: &str) -> Result<String> {
        if address != self._00_address {
            bail!("invalid address for wallet to sign");
        }

        trustnote_wallet_base::sign(hash, &self._00_address_prvk)
    }
}

fn init_log(verbosity: u64) {
    let log_lvl = match verbosity {
        0 => log::LevelFilter::Off,
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Info,
        _ => log::LevelFilter::Debug,
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

// TODO: src database is get from trustnote config which is not clear
fn init_database() -> Result<()> {
    // init the settings first, trustnote lib need this settings file
    let _settings = config::get_settings();

    let mut db_path = ::std::env::current_dir()?;
    db_path.push("trustnote_light.sqlite");
    db::set_db_path(db_path);
    let _db = db::DB_POOL.get_connection();
    Ok(())
}

fn init(verbosity: u64) -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size);

    init_log(verbosity);
    init_database()?;
    Ok(())
}

fn connect_to_remote(peers: &[String]) -> Result<Arc<WalletConn>> {
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

fn info(wallet_info: &WalletInfo) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();
    let db = db::DB_POOL.get_connection();
    let balance = wallet::get_balance(&db, &wallet_info._00_address)? as f32 / 1000_000.0;
    println!("\ncurrent wallet info:\n");
    println!("device_address: {}", wallet_info.device_address);
    println!("wallet_public_key: {}", wallet_info.wallet_pubk.to_string());
    println!("└──wallet_id(0): {}", wallet_info.wallet_0_id);
    println!("   └──address(0/0): {}", wallet_info._00_address);
    println!("      ├── path: /m/44'/0'/0'/0/0");
    println!("      ├── pubkey: {}", address_pubk);
    println!("      └── balance: {:.3}MN", balance);

    Ok(())
}

// save wallet address in database
fn update_wallet_address(wallet_info: &WalletInfo) -> Result<()> {
    use trustnote_wallet_base::Base64KeyExt;

    wallet::update_wallet_address(
        &db::DB_POOL.get_connection(),
        &wallet_info.device_address,
        &wallet_info.wallet_0_id,
        &wallet_info._00_address,
        &wallet_info._00_address_pubk.to_base64_key(),
    )?;
    Ok(())
}

fn sync(ws: &WalletConn, wallet_info: &WalletInfo) -> Result<()> {
    update_wallet_address(&wallet_info)?;
    match ws.refresh_history() {
        Ok(_) => println!("refresh history done"),
        Err(e) => bail!("refresh history failed, err={:?}", e),
    }
    // TODO: print get history statistics
    Ok(())
}

fn history_log(wallet_info: &WalletInfo, index: Option<usize>) -> Result<()> {
    let histories =
        wallet::read_transaction_history(&db::DB_POOL.get_connection(), &wallet_info._00_address)?;

    if let Some(index) = index {
        if index == 0 || index > histories.len() {
            bail!("invalid transaction index");
        }

        let history = &histories[index - 1];
        if history.amount > 0 {
            println!("FROM     : {}", history.address_from);
        } else {
            println!("TO       : {}", history.address_to);
        }
        println!("UNIT     : {}", history.unit);
        println!("AMOUNT   : {:.3} MN", history.amount as f64 / 1_000_000.0);
        println!(
            "DATE     : {}",
            Local.timestamp_millis(history.timestamp).naive_local()
        );
        println!("CONFIRMED: {}", history.confirmations);
    } else {
        for (id, history) in histories.iter().enumerate() {
            println!(
                "#{:<4} {:>10.3} MN  \t{}",
                id + 1,
                history.amount as f64 / 1_000_000.0,
                Local.timestamp_millis(history.timestamp).naive_local()
            );
        }
    }

    Ok(())
}

fn pause() {
    use std::io::Read;
    ::std::io::stdin().read(&mut [0; 1]).unwrap();
}

fn send_payment(
    ws: &Arc<WalletConn>,
    text: Option<&str>,
    address_amount: &HashMap<&str, f64>,
    wallet_info: &WalletInfo,
) -> Result<()> {
    let payment = ws.prepare_payment(address_amount, text, &wallet_info._00_address)?;
    let joint = composer::compose_joint(payment, wallet_info)?;
    ws.post_joint(&joint)?;
    println!("FROM  : {}", wallet_info._00_address);
    println!("TO    : ");
    for address in address_amount.keys() {
        println!("        {}", address);
    }
    println!("UNIT  : {:?}", joint.unit.unit);
    println!("AMOUNT: ");
    for amount in address_amount.values() {
        println!("        {}", amount);
    }
    println!("DATE  : {}", time::now());
    Ok(())
}

fn main() -> Result<()> {
    let yml = load_yaml!("ttt.yml");
    let m = App::from_yaml(yml).get_matches();

    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    let mut settings = config::get_settings();
    let mut wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;

    //Info
    if let Some(_info) = m.subcommand_matches("info") {
        return info(&wallet_info);
    }

    //Log
    if let Some(log) = m.subcommand_matches("log") {
        let v = value_t!(log.value_of("v"), usize);
        match v {
            Ok(v) => {
                return history_log(&wallet_info, Some(v));
            }
            Err(clap::Error {
                kind: clap::ErrorKind::ArgumentNotFound,
                ..
            }) => {
                return history_log(&wallet_info, None);
            }
            Err(e) => e.exit(),
        }
    }

    let ws = connect_to_remote(&settings.hub_url)?;

    //Sync
    if let Some(sync_arg) = m.subcommand_matches("sync") {
        if let Some(mnemonic) = sync_arg.value_of("MNEMONIC") {
            config::update_mnemonic(mnemonic)?;
            // re_init settings
            settings = config::get_settings();
            wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
        }
        //TODO: regist an event to handle_just_saying from hub?
        return sync(&ws, &wallet_info);
    }

    //Send
    if let Some(send) = m.subcommand_matches("send") {
        let mut address_amount = HashMap::new();
        if let Some(pay) = send.values_of("pay") {
            //TODO: Some syntax check for address and amount
            let v = pay.collect::<Vec<_>>();
            for arg in v.chunks(2) {
                address_amount.insert(arg[0], arg[1].parse::<f64>().context("invalid amount arg")?);
            }
        }

        let text = send.value_of("text");

        send_payment(&ws, text, &address_amount, &wallet_info)?;
    }

    pause();
    Ok(())
}
