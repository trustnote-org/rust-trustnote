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
extern crate rusqlite;
extern crate serde;
extern crate serde_json;
extern crate trustnote;
extern crate trustnote_wallet_base;

mod config;

use std::sync::Arc;

use chrono::{Local, TimeZone};
use clap::App;
use composer;
use failure::ResultExt;
use rusqlite::Connection;
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

fn init(verbosity: u64) -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config().set_stack_size(stack_size);

    init_log(verbosity);
    db::use_wallet_db();

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

fn info(db: &Connection, wallet_info: &WalletInfo) -> Result<()> {
    let address_pubk = wallet_info._00_address_pubk.to_base64_key();
    let (unstable_balance, stable_balance) = wallet::get_balance(&db, &wallet_info._00_address)?;
    let total = (unstable_balance + stable_balance) as f64 / 1000_000.0;
    let stable = stable_balance as f64 / 1000_000.0;
    let pending = unstable_balance as f64 / 1000_000.0;

    println!("\ncurrent wallet info:\n");
    println!("device_address: {}", wallet_info.device_address);
    println!("wallet_public_key: {}", wallet_info.wallet_pubk.to_string());
    println!("└──wallet_id(0): {}", wallet_info.wallet_0_id);
    println!("   └──address(0/0): {}", wallet_info._00_address);
    println!("      ├── path: /m/44'/0'/0'/0/0");
    println!("      ├── pubkey: {}", address_pubk);
    println!("      └── balance: {:.6}", total);
    println!("          ├── stable: {:.6}", stable);
    println!("          └── pending: {:.6}", pending);

    Ok(())
}

// save wallet address in database
fn update_wallet_address(db: &Connection, wallet_info: &WalletInfo) -> Result<()> {
    use trustnote_wallet_base::Base64KeyExt;

    wallet::update_wallet_address(
        db,
        &wallet_info.device_address,
        &wallet_info.wallet_0_id,
        &wallet_info._00_address,
        &wallet_info._00_address_pubk.to_base64_key(),
    )?;
    Ok(())
}

// we need to sync witnesses from hub if necessary
fn check_witnesses(ws: &WalletConn, db: &db::Database) -> Result<()> {
    let witnesses = db.get_my_witnesses()?;

    // if the data base is empty we should wait until
    if witnesses.is_empty() {
        let witnesses = ws.get_witnesses()?;
        db.insert_witnesses(&witnesses)?;
    } else {
        assert_eq!(witnesses.len(), trustnote::config::COUNT_WITNESSES);
    }
    Ok(())
}

fn sync(ws: &WalletConn, db: &db::Database, wallet_info: &WalletInfo) -> Result<()> {
    update_wallet_address(db, wallet_info)?;
    check_witnesses(ws, db)?;
    match ws.refresh_history(db) {
        Ok(_) => info!("refresh history done"),
        Err(e) => bail!("refresh history failed, err={:?}", e),
    }
    Ok(())
}

fn history_log(
    db: &Connection,
    wallet_info: &WalletInfo,
    index: Option<usize>,
    max: usize,
) -> Result<()> {
    let histories = wallet::read_transaction_history(db, &wallet_info._00_address)?;

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
        println!("AMOUNT   : {:.6} MN", history.amount as f64 / 1_000_000.0);
        println!(
            "DATE     : {}",
            Local.timestamp_millis(history.timestamp).naive_local()
        );
        println!("CONFIRMED: {}", history.confirmations);
    } else {
        for (id, history) in histories.iter().enumerate() {
            if id > max - 1 {
                break;
            }
            println!(
                "#{:<4} {:>10.6} MN  \t{}",
                id + 1,
                history.amount as f64 / 1_000_000.0,
                Local.timestamp_millis(history.timestamp).naive_local()
            );
        }
    }

    Ok(())
}

fn send_payment(
    ws: &Arc<WalletConn>,
    db: &Connection,
    text: Option<&str>,
    address_amount: &Vec<(&str, f64)>,
    wallet_info: &WalletInfo,
) -> Result<()> {
    let payment = wallet::prepare_payment(ws, address_amount, text, &wallet_info._00_address)?;
    let joint = composer::compose_joint(db, payment, wallet_info)?;
    ws.post_joint(&joint)?;

    println!("FROM  : {}", wallet_info._00_address);
    println!("TO    : ");
    for (address, amount) in address_amount {
        println!("      address : {}, amount : {}", address, amount);
    }
    println!("UNIT  : {}", joint.unit.unit.unwrap());
    println!("TEXT  : {}", text.unwrap_or(""));
    println!(
        "DATE  : {}",
        Local.timestamp_millis(time::now() as i64).naive_local()
    );
    Ok(())
}

fn main() -> Result<()> {
    let yml = load_yaml!("ttt.yml");
    let m = App::from_yaml(yml).get_matches();

    let verbosity = m.occurrences_of("verbose");
    init(verbosity)?;

    // init command
    if let Some(init_arg) = m.subcommand_matches("init") {
        if let Some(mnemonic) = init_arg.value_of("MNEMONIC") {
            config::update_mnemonic(mnemonic)?;
        }
        // create settings
        let settings = config::get_settings();
        settings.show_config();
        // every init would remove the local database
        ::std::fs::remove_file(trustnote::config::get_database_path(true)).ok();
        return Ok(());
    }

    let settings = config::get_settings();
    let wallet_info = WalletInfo::from_mnemonic(&settings.mnemonic)?;
    let db = db::DB_POOL.get_connection();
    let ws = connect_to_remote(&settings.hub_url)?;
    // other commad would just sync data first
    sync(&ws, &db, &wallet_info)?;

    //Info
    if let Some(_info) = m.subcommand_matches("info") {
        return info(&db, &wallet_info);
    }

    //Log
    if let Some(log) = m.subcommand_matches("log") {
        let n = value_t!(log.value_of("n"), usize)?;

        let v = value_t!(log.value_of("v"), usize);
        match v {
            Ok(v) => {
                return history_log(&db, &wallet_info, Some(v), n);
            }
            Err(clap::Error {
                kind: clap::ErrorKind::ArgumentNotFound,
                ..
            }) => {
                return history_log(&db, &wallet_info, None, n);
            }
            Err(e) => e.exit(),
        }
    }

    //Send
    if let Some(send) = m.subcommand_matches("send") {
        let mut address_amount = Vec::new();
        if let Some(pay) = send.values_of("pay") {
            let v = pay.collect::<Vec<_>>();
            for arg in v.chunks(2) {
                if !::object_hash::is_chash_valid(arg[0]) {
                    bail!("invalid address, please check");
                }
                let amount = arg[1].parse::<f64>().context("invalid amount arg")?;
                if amount > std::u64::MAX as f64 || amount < 0.000001 {
                    bail!("invalid amount, please check");
                }
                address_amount.push((arg[0], amount));
            }
        }

        let text = send.value_of("text");
        return send_payment(&ws, &db, text, &address_amount, &wallet_info);
    }

    if let Some(balance) = m.subcommand_matches("balance") {
        let (unstable_balance, stable_balance) =
            wallet::get_balance(&db, &wallet_info._00_address)?;

        if let Some(_s) = balance.values_of("s") {
            println!("{:.6}", stable_balance as f64 / 1000_000.0);
            return Ok(());
        }

        if let Some(_p) = balance.values_of("p") {
            println!("{:.6}", unstable_balance as f64 / 1000_000.0);
            return Ok(());
        }

        println!(
            "{:.6}",
            (stable_balance + unstable_balance) as f64 / 1000_000.0
        );
        return Ok(());
    }

    Ok(())
}
