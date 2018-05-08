use std::io::{Read, Write};
use std::net::ToSocketAddrs;

use error::Result;
use may::coroutine::JoinHandle;
use may::net::{TcpListener, TcpStream};
use may::sync::RwLock;
use may_actor::{Actor, DriverActor};
use native_tls::{TlsConnector, TlsStream};
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use tungstenite::server::accept;
use tungstenite::{Message, WebSocket};
use url::Url;

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

pub type TrustnoteConn = Actor<WsConnection<TcpStream>>;

lazy_static! {
    pub static ref INBOUND_CONN: RwLock<Vec<TrustnoteConn>> = RwLock::new(Vec::new());
    pub static ref OUTBOUND_CONN: RwLock<Vec<TrustnoteConn>> = RwLock::new(Vec::new());
}

fn connection_receiver<T: Read + Write + Send + 'static>(
    mut ws: WebSocket<T>,
    actor: DriverActor<WsConnection<T>>,
) {
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

        let mut value: Value = t_c!(serde_json::from_str(&msg));
        let msg_type = value[0].take();
        let content = value[1].take();

        let msg_type = t_c!(msg_type.as_str().ok_or("no msg type"));

        match msg_type {
            "justsaying" => actor.call(|ws| t!(ws.on_message(content))),
            "request" => actor.call(|ws| t!(ws.on_request(content))),
            "response" => actor.call(|ws| t!(ws.on_response(content))),
            s => {
                error!("unkonw msg type: {}", s);
                continue;
            }
        }
    }
}

pub fn run_ws_server<T: ToSocketAddrs>(address: T) -> JoinHandle<()> {
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
            let ws = WsConnection {
                conn: t_c!(accept(stream)),
            };
            let recv = WebSocket::from_raw_socket(r, Role::Server);

            let inbound = Actor::drive_new(ws, move |actor| connection_receiver(recv, actor));
            let mut g = INBOUND_CONN.write().unwrap();
            g.push(inbound);
        }
    })
}

pub struct WsConnection<T: Read + Write> {
    // this half is only used for send message
    // the other receive half is within the actor driver
    conn: WebSocket<T>,
}

impl<T: Read + Write> Drop for WsConnection<T> {
    fn drop(&mut self) {
        self.conn.close(None).ok();
    }
}

impl<T: Read + Write> WsConnection<T> {
    pub fn send_json(&mut self, value: &Value) -> Result<()> {
        let msg = serde_json::to_string(value)?;
        self.conn.write_message(Message::Text(msg))?;
        Ok(())
    }

    pub fn on_message(&mut self, msg: Value) -> Result<()> {
        println!("recv a message: {}", msg);
        Ok(())
    }

    pub fn on_request(&mut self, msg: Value) -> Result<()> {
        println!("recv a request: {}", msg);
        Ok(())
    }

    pub fn on_response(&mut self, msg: Value) -> Result<()> {
        println!("recv a resonse: {}", msg);
        Ok(())
    }
}

// impl<T: Read + Write> WsConnection<T> {
//     pub fn say_hello(&mut self, )
// }

pub fn new_ws<A: ToSocketAddrs>(address: A) -> Result<TrustnoteConn> {
    let stream = TcpStream::connect(address)?;
    let r_stream = stream.try_clone()?;

    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);

    let (conn, _) = client(req, stream)?;
    let r_ws = WebSocket::from_raw_socket(r_stream, Role::Client);
    let outbound = Actor::drive_new(WsConnection { conn }, move |actor| {
        connection_receiver(r_ws, actor)
    });

    let mut g = OUTBOUND_CONN.write().unwrap();
    g.push(outbound.clone());
    Ok(outbound)
}

pub fn new_wss(host: &str) -> Result<WsConnection<TlsStream<TcpStream>>> {
    let stream = TcpStream::connect((host, 443))?;
    let connector = TlsConnector::builder()?.build()?;
    let stream = connector.connect(host, stream)?;
    let url = format!("wss://{}/", host);
    let url = Url::parse(&url)?;
    let req = Request::from(url);

    let (conn, _) = client(req, stream)?;
    Ok(WsConnection { conn })
}
