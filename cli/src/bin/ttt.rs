#[macro_use]
extern crate log;
extern crate chrono;
extern crate fern;
extern crate may;
extern crate serde_json;
extern crate trustnote;
extern crate trustnote_cli;

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
        }).level(log_lvl)
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
    info!("ttt cli!\n\n");
    Ok(())
}
