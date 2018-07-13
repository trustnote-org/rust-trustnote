#[macro_use]
extern crate log;
extern crate base64;
extern crate fern;
extern crate trustnote;

#[macro_use]
extern crate may;

use trustnote::*;

fn start_ws_server() -> Result<::may::coroutine::JoinHandle<()>> {
    use network::hub::WSS;
    use network::WsServer;

    let port = config::get_hub_server_port();

    let server = WsServer::start(("0.0.0.0", port), |c| {
        WSS.add_inbound(c);
    });
    println!("Websocket server running on ws://0.0.0.0:{}", port);

    Ok(server)
}

fn show_config() -> Result<()> {
    println!(
        "witnesses = {:?}",
        config::CONFIG.read()?.get::<Vec<String>>("witnesses")?
    );
    Ok(())
}

fn log_init() {
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    };
    // Configure logger at runtime
    fern::Dispatch::new()
    // Perform allocation-free log formatting
    // .format(|out, message, record| {
    //     out.finish(format_args!(
    //         "{}[{}][{}] {}",
    //         chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
    //         record.target(),
    //         record.level(),
    //         message
    //     ))
    // })
    // Add blanket level filter -
    .level(log_lvl)
    // - and per-module overrides
    // .level_for("hyper", log::LevelFilter::Info)
    // Output to stdout, files, and other Dispatch configurations
    .chain(std::io::stdout())
    // .chain(fern::log_file("output.log")?)
    // Apply globally
    .apply().unwrap();

    // and log using log crate macros!
    info!("log init done!");
}

fn test_ws_client() -> Result<()> {
    use network::hub;
    hub::create_outbound_conn(config::get_remote_hub_url())?;
    hub::start_catchup()?;
    Ok(())
}

fn network_clean() {
    // remove all the actors
    network::hub::WSS.close_all();
}

// the main test logic that run in coroutine context
fn main_run() -> Result<()> {
    network::hub::start_purge_jonk_joints_timer();
    let _server = start_ws_server();
    test_ws_client()?;
    Ok(())
}

fn pause() {
    use std::io::{self, Read};
    io::stdin().read(&mut [0]).ok();
}

fn main() -> Result<()> {
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x1000
    };
    may::config()
        .set_stack_size(stack_size)
        .set_io_workers(4)
        .set_workers(2);
    signature::init_secp256k1()?;
    log_init();
    show_config()?;
    // run the network stuff in coroutine context
    go!(|| main_run().unwrap()).join().unwrap();
    pause();
    network_clean();
    info!("bye from main!\n\n");
    Ok(())
}
