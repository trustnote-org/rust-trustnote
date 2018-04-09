use std::net::ToSocketAddrs;

use may::net::TcpListener;
use may::coroutine::JoinHandle;
use tungstenite::server::accept;

pub fn run_websocket_server<T: ToSocketAddrs>(address: T) -> JoinHandle<()> {
    let address = address
        .to_socket_addrs()
        .expect("invalid address")
        .next()
        .expect("can't resolve address");

    go!(move || {
        let listener = TcpListener::bind(address).unwrap();
        for stream in listener.incoming() {
            go!(move || -> () {
                let mut websocket = accept(stream.unwrap()).unwrap();

                loop {
                    let msg = websocket.read_message().unwrap();

                    // Just echo back everything that the client sent to us
                    if msg.is_binary() || msg.is_text() {
                        websocket.write_message(msg).unwrap();
                    }
                }
            });
        }
    })
}
