#[macro_use]
extern crate log;
extern crate chrono;
#[macro_use]
extern crate clap;
extern crate fern;
extern crate may;
extern crate serde_json;
extern crate trustnote;
extern crate trustnote_cli;

use clap::App;
use trustnote::*;

fn log_init() {
    // TODO: need to implement async logs
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
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
        })
        .level(log_lvl)
        .chain(std::io::stdout())
        .apply()
        .unwrap();

    info!("log init done!");
}

fn main() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x1000
    };
    may::config().set_stack_size(stack_size);

    log_init();
    // config::show_config();

    let yml = load_yaml!("ttt.yml");
    let m = App::from_yaml(yml).get_matches();

    //Init
    if let Some(init) = m.subcommand_matches("init") {
        if let Some(mnemonic) = init.value_of("mnemonic") {
            println!("Init wallet with mnemonic {}", mnemonic);
        } else {
            println!("Init wallet with random mnemonic");
        }
    }

    //Pay
    if let Some(pay) = m.subcommand_matches("pay") {
        if let Some(address) = pay.value_of("address") {
            if let Some(amount) = pay.value_of("amount") {
                println!("Pay to address {} amount {}", address, amount);
            }
        }
    }

    //Info
    if let Some(_info) = m.subcommand_matches("info") {
        println!("Info for this wallet");
    }

    //Balance
    if let Some(_balance) = m.subcommand_matches("balance") {
        let balance = 0;
        println!("Wallet Balance : {}", balance);
    }

    //History
    if let Some(_history) = m.subcommand_matches("history") {
        println!("Wallet History");
    }

    Ok(())
}
