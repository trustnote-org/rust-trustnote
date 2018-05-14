use std::io::{Read, Write};
use std::net::ToSocketAddrs;

use config;
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
    #[inline]
    pub fn with<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut HubConnImpl<TcpStream>) -> R + Send,
        R: Send,
    {
        self.0.with(f)
    }

    pub fn send_version(&self) -> Result<()> {
        // TODO: read these things from config
        self.0.with(|me| {
            me.send_just_saying(
                "version",
                json!({
                    "protocol_version": config::VERSION, 
	                "alt": config::ALT, 
		            "library": "rust-trustnote", 
		            "library_version": "0.1.0", 
		            "program": "rust-trustnote-hub", 
		            "program_version": "0.1.0"
                }),
            )
        })
    }
}

pub struct HubConnImpl<T: Read + Write> {
    // this half is only used for send message
    // the other receive half is within the actor driver
    conn: WebSocket<T>,
    peer: String,
}

impl<T: Read + Write> Drop for HubConnImpl<T> {
    fn drop(&mut self) {
        self.conn.close(None).ok();
    }
}

impl<T: Read + Write> Connection<T> for HubConnImpl<T> {
    fn new(s: WebSocket<T>) -> Self {
        // TODO: need to add peer init
        let peer = "peer".to_owned();
        HubConnImpl { conn: s, peer }
    }

    fn send_json(&mut self, value: Value) -> Result<()> {
        let msg = serde_json::to_string(&value)?;
        info!("SENDING to {}: {}", self.peer, msg);
        self.conn.write_message(Message::Text(msg))?;
        Ok(())
    }

    fn on_message(&mut self, mut msg: Value) -> Result<()> {
        let mut content = msg[1].take();
        let subject = content["subject"].take();
        let body = content["body"].take();
        match subject.as_str().unwrap_or("none") {
            "version" => self.on_version(body)?,
            subject => bail!("on_message unkown subject: {}", subject),
        }
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

impl<T: Read + Write> HubConnImpl<T> {
    pub fn close(&self) {
        unimplemented!()
    }
}

impl<T: Read + Write> HubConnImpl<T> {
    fn on_version(&mut self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            let err_msg = format!("Incompatible versions, mine {}", config::VERSION);
            self.send_error(json!(err_msg))?;
            self.close();
            return Ok(());
        }

        if version["alt"].as_str() != Some(config::ALT) {
            let err_msg = format!("Incompatible alt, mine {}", config::ALT);
            self.send_error(json!(err_msg))?;
            self.close();
            return Ok(());
        }

        info!("got peer version: {}", version);
        Ok(())
    }
}

impl<T: Read + Write> HubConnImpl<T> {
    pub fn send_message(&mut self, kind: &str, content: Value) -> Result<()> {
        self.send_json(json!([kind, &content]))
    }

    pub fn send_just_saying(&mut self, subject: &str, body: Value) -> Result<()> {
        self.send_message("justsaying", json!({"subject": subject, "body": body}))
    }

    pub fn send_error(&mut self, error: Value) -> Result<()> {
        self.send_just_saying("error", error)
    }

    pub fn send_info(&mut self, info: Value) -> Result<()> {
        self.send_just_saying("info", info)
    }

    pub fn send_result(&mut self, result: Value) -> Result<()> {
        self.send_just_saying("result", result)
    }

    pub fn send_error_result(&mut self, unit: &str, error: &str) -> Result<()> {
        self.send_result(json!({"unit": unit, "result": "error", "error": error}))
    }

    pub fn send_response(&mut self, tag: &str, response: Value) -> Result<()> {
        self.send_message("response", json!({"tag": tag, "response": response}))
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<HubConn> {
    let stream = TcpStream::connect(address)?;
    let r_stream = stream.try_clone()?;

    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);

    let (conn, _) = client(req, stream)?;
    let r_ws = WebSocket::from_raw_socket(r_stream, Role::Client);
    let actor = Actor::drive_new(HubConnImpl::new(conn), move |actor| {
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
    Ok(HubConnImpl::new(conn))
}
