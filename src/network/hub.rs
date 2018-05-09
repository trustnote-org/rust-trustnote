use std::io::{Read, Write};
use std::net::ToSocketAddrs;

use error::Result;
use may::net::TcpStream;
use may_actor::Actor;
use native_tls::{TlsConnector, TlsStream};
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use tungstenite::{Message, WebSocket};
use url::Url;

use network::Connection;

use may::sync::RwLock;

lazy_static! {
    pub static ref INBOUND_CONN: RwLock<Vec<HubConn>> = RwLock::new(Vec::new());
    pub static ref OUTBOUND_CONN: RwLock<Vec<HubConn>> = RwLock::new(Vec::new());
}

#[derive(Clone)]
pub struct HubConn(pub Actor<HubConnImpl<TcpStream>>);

impl HubConn {
    // just a simple example interface
    pub fn send_message(&self, msg: Value) {
        self.0
            .call(move |me| me.send_json(&json!(["justsaying", msg])).unwrap());
    }
}

pub struct HubConnImpl<T: Read + Write> {
    // this half is only used for send message
    // the other receive half is within the actor driver
    conn: WebSocket<T>,
}

impl<T: Read + Write> Drop for HubConnImpl<T> {
    fn drop(&mut self) {
        self.conn.close(None).ok();
    }
}

impl<T: Read + Write> Connection<T> for HubConnImpl<T> {
    fn new(s: WebSocket<T>) -> Self {
        HubConnImpl { conn: s }
    }

    fn send_json(&mut self, value: &Value) -> Result<()> {
        let msg = serde_json::to_string(value)?;
        self.conn.write_message(Message::Text(msg))?;
        Ok(())
    }

    fn on_message(&mut self, msg: Value) -> Result<()> {
        println!("recv a message: {}", msg);
        Ok(())
    }

    fn on_request(&mut self, msg: Value) -> Result<()> {
        println!("recv a request: {}", msg);
        Ok(())
    }

    fn on_response(&mut self, msg: Value) -> Result<()> {
        println!("recv a resonse: {}", msg);
        Ok(())
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<HubConn> {
    let stream = TcpStream::connect(address)?;
    let r_stream = stream.try_clone()?;

    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);

    let (conn, _) = client(req, stream)?;
    let r_ws = WebSocket::from_raw_socket(r_stream, Role::Client);
    let actor = Actor::drive_new(HubConnImpl { conn }, move |actor| {
        super::network::connection_receiver(r_ws, actor)
    });

    let outbound = HubConn(actor);
    let mut g = OUTBOUND_CONN.write().unwrap();
    g.push(outbound.clone());
    Ok(outbound)
}

// wss is not supported yet. ref #49
#[allow(dead_code)]
pub fn new_wss(host: &str) -> Result<HubConnImpl<TlsStream<TcpStream>>> {
    let stream = TcpStream::connect((host, 443))?;
    let connector = TlsConnector::builder()?.build()?;
    let stream = connector.connect(host, stream)?;
    let url = format!("wss://{}/", host);
    let url = Url::parse(&url)?;
    let req = Request::from(url);

    let (conn, _) = client(req, stream)?;
    Ok(HubConnImpl { conn })
}
