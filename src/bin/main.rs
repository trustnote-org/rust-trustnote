extern crate trustnote;

use trustnote::*;

fn main() {
    let db = db::Database::new().unwrap();

    let names = db.test().expect("failed to query database");

    for name in names {
        println!("name = {}", name);
    }

    let _server = network::run_websocket_server(("0.0.0.0", config::WS_PORT));
    println!(
        "Websocket server running on ws://0.0.0.0:{}",
        config::WS_PORT
    );

    let mut client = network::WsClient::new(("127.0.0.1", config::WS_PORT)).unwrap();
    client.send_message("hello world".into()).unwrap();
    loop {
        let msg = client.recv_message().unwrap();
        println!("recv {}", msg);
    }
    // server.join().unwrap();
}
