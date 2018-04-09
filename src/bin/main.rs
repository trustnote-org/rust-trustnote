extern crate trustnote;

use trustnote::*;

fn main() {
    let server = network::run_websocket_server(("0.0.0.0", config::WS_PORT));
    println!("Websocket server running on ws://0.0.0.0:{}", config::WS_PORT);

    server.join().unwrap();
}
