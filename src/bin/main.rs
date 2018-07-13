#[macro_use]
extern crate log;
extern crate base64;
extern crate fern;
extern crate trustnote;

#[macro_use]
extern crate may;

use trustnote::*;

fn log_init() {
    let log_lvl = if cfg!(debug_assertions) {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Warn
    };

    fern::Dispatch::new()
    .format(|out, message, record| {
        out.finish(format_args!(
            "[{}][{}] {}",
            record.level(),
            record.target(),
            message
        ))
    })
    .level(log_lvl)
    .chain(std::io::stdout())
    // .chain(fern::log_file("output.log")?)
    .apply().unwrap();

    info!("log init done!");
}

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

fn connect_to_remote() -> Result<()> {
    use network::hub;
    hub::create_outbound_conn(config::get_remote_hub_url())?;
    hub::start_catchup()?;
    Ok(())
}

fn network_cleanup() {
    // remove all the actors
    network::hub::WSS.close_all();
}

// the hub server logic that run in coroutine context
fn run_hub_server() -> Result<()> {
    network::hub::start_purge_jonk_joints_timer();
    let _server = start_ws_server();
    connect_to_remote()?;
    Ok(())
}

fn pause() {
    use std::io::{self, Read};
    io::stdin().read(&mut [0]).ok();
}

fn main() {
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x1000
    };
    may::config()
        .set_stack_size(stack_size)
        .set_io_workers(4)
        .set_workers(2);
    log_init();
    config::show_config();
    // run the network stuff in coroutine context
    go!(|| run_hub_server().unwrap()).join().unwrap();
    pause();
    network_cleanup();
    info!("bye from main!\n\n");
}
