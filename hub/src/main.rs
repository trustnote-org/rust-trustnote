#[macro_use]
extern crate log;
extern crate chrono;
extern crate fern;
#[macro_use]
extern crate trustnote;
#[macro_use]
extern crate may;
extern crate may_signal;
extern crate serde_json;

mod timer;
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
    let peers = config::get_remote_hub_url();

    for peer in peers {
        if let Err(e) = network::hub::create_outbound_conn(&peer) {
            error!(" fail to connected: {}, err={}", peer, e);
        }
    }

    go!(move || if let Err(e) = network::hub::start_catchup() {
        error!("catchup error: {}", e);
        error!("back_trace={}", e.backtrace());
        ::std::process::abort();
    });
    Ok(())
}

fn network_cleanup() {
    network::hub::WSS.close_all();
}

// register golbal event handlers
fn register_event_handlers() {
    use main_chain::MciStableEvent;
    use utils::event::Event;

    MciStableEvent::add_handler(|v| t!(network::hub::notify_watchers_about_stable_joints(v.mci)));
}

// the hub server logic that run in coroutine context
fn run_hub_server() -> Result<()> {
    register_event_handlers();
    let _server = start_ws_server();
    connect_to_remote()?;
    timer::start_global_timers();
    Ok(())
}

#[allow(dead_code)]
fn test_read_joint() -> Result<()> {
    fn pause() {
        use std::io::{self, Read};
        io::stdin().read(&mut [0]).ok();
    }

    fn print_joint(unit: &str) -> Result<()> {
        let db = db::DB_POOL.get_connection();
        let joint = storage::read_joint_directly(&db, &unit.to_string())?;
        println!("joint = {}", serde_json::to_string_pretty(&joint)?);
        Ok(())
    }

    print_joint("V/NuDxzT7VFa/AqfBsAZ8suG4uj3u+l0kXOLE+nP+dU=")?;
    print_joint("g9HQWWTdz8n9+KRYFxOyHNEH7kp7N4j1vU7F1VIpEC8=")?;
    pause();
    Ok(())
}

fn main() -> Result<()> {
    // init default coroutine settings
    let stack_size = if cfg!(debug_assertions) {
        0x4000
    } else {
        0x2000
    };
    may::config()
        .set_stack_size(stack_size)
        .set_io_workers(0)
        .set_workers(1);

    log_init();
    config::show_config();

    // uncomment it to test read joint from db
    // test_read_joint()?;

    go!(|| run_hub_server().unwrap()).join().unwrap();

    // wait user input a ctrl_c to exit
    may_signal::ctrl_c().recv().unwrap();

    // close all the connections
    network_cleanup();
    info!("bye from main!\n\n");
    Ok(())
}
