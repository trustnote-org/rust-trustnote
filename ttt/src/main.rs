#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
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

use clap::App;
use trustnote::*;

fn log_init() {
    // TODO: need to implement async logs
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Info
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
    let _settings = config::get_settings();

    let yml = load_yaml!("ttt.yml");
    let m = App::from_yaml(yml).get_matches();

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

    //Info
    if let Some(_info) = m.subcommand_matches("info") {
        println!("Info for this wallet");
    }

    //Log
    if let Some(_log) = m.subcommand_matches("log") {
        println!("Wallet History");
    }

    Ok(())
}
