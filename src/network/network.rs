use std::io::{Read, Write};
use std::net::ToSocketAddrs;

use error::Result;
use may::coroutine::JoinHandle;
use may::net::{TcpListener, TcpStream};
use may_actor::{Actor, DriverActor};
use serde_json::{self, Value};
use tungstenite::protocol::Role;
use tungstenite::server::accept;
use tungstenite::{Message, WebSocket};

macro_rules! t {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
            }
        }
    };
}

macro_rules! t_c {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
                continue;
            }
        }
    };
}

pub trait Connection<S> {
    fn new(s: WebSocket<S>) -> Self;
    fn send_json(&mut self, value: &Value) -> Result<()>;
    fn on_message(&mut self, msg: Value) -> Result<()>;
    fn on_request(&mut self, msg: Value) -> Result<()>;
    fn on_response(&mut self, msg: Value) -> Result<()>;
}

// the recevier back ground coroutine logic
pub fn connection_receiver<S, C>(mut ws: WebSocket<S>, actor: DriverActor<C>)
where
    S: Read + Write + Send + 'static,
    C: Connection<S> + Send + 'static,
{
    loop {
        let msg = match ws.read_message() {
            Ok(msg) => msg,
            Err(e) => {
                error!("{}", e.to_string());
                break;
            }
        };

        let msg = match msg {
            Message::Text(s) => s,
            _ => {
                error!("only text ws packet are supported");
                continue;
            }
        };
        info!("receive msg: {}", msg);

        let value: Value = t_c!(serde_json::from_str(&msg));
        let msg_type = value[0].clone();
        let msg_type = t_c!(msg_type.as_str().ok_or("no msg type"));

        match msg_type {
            "justsaying" => actor.call(|ws| t!(ws.on_message(value))),
            "request" => actor.call(|ws| t!(ws.on_request(value))),
            "response" => actor.call(|ws| t!(ws.on_response(value))),
            s => {
                error!("unkonw msg type: {}", s);
                continue;
            }
        }
    }
}

pub fn run_ws_server<C, A>(address: A) -> JoinHandle<()>
where
    A: ToSocketAddrs,
    C: Connection<TcpStream>,
{
    let address = address
        .to_socket_addrs()
        .expect("invalid address")
        .next()
        .expect("can't resolve address");

    go!(move || {
        let listener = TcpListener::bind(address).unwrap();
        // for stream in listener.incoming() {
        while let Ok((stream, _)) = listener.accept() {
            let r = t_c!(stream.try_clone());
            let ws = Connection::new(t_c!(accept(stream)));
            let recv = WebSocket::from_raw_socket(r, Role::Server);

            let inbound = Actor::drive_new(ws, move |actor| connection_receiver(recv, actor));
            let mut g = super::INBOUND_CONN.write().unwrap();
            g.push(inbound);
        }
    })
}
