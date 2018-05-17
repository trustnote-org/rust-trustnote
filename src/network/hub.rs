use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use config;
use error::Result;
use may::coroutine;
use may::net::TcpStream;
use may::sync::RwLock;
use network::{Sender, Server, WsConnection};
use serde_json::Value;
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

// global Ws connections
lazy_static! {
    pub static ref WSS: WsConnections = WsConnections::new();
}

fn start_heartbeat(ws: Weak<HubConn>) {
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    go!(move || loop {
        coroutine::sleep(Duration::from_millis(3000 + n));
        let ws = match ws.upgrade() {
            Some(ws) => ws,
            None => return,
        };
        if ws.get_last_recv_tm().elapsed() < Duration::from_secs(5) {
            continue;
        }
        // heartbeat failed so just close the connnection
        if ws.send_heartbeat().is_err() {
            ws.close();
        }
    });
}

// global request has no specific ws connections, just find a proper one should be fine
pub struct WsConnections {
    inbound: RwLock<Vec<Arc<HubConn>>>,
    outbound: RwLock<Vec<Arc<HubConn>>>,
    next_inbound: AtomicUsize,
    next_outbound: AtomicUsize,
}

impl WsConnections {
    fn new() -> Self {
        WsConnections {
            inbound: RwLock::new(Vec::new()),
            outbound: RwLock::new(Vec::new()),
            next_inbound: AtomicUsize::new(0),
            next_outbound: AtomicUsize::new(0),
        }
    }

    pub fn add_inbound(&self, inbound: Arc<HubConn>) {
        let ws = Arc::downgrade(&inbound);
        start_heartbeat(ws);
        let mut g = self.inbound.write().unwrap();
        g.push(inbound);
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) {
        let ws = Arc::downgrade(&outbound);
        start_heartbeat(ws);
        let mut g = self.outbound.write().unwrap();
        g.push(outbound);
    }

    pub fn close_all(&self) {
        let mut g = self.outbound.write().unwrap();
        g.clear();
        let mut g = self.inbound.write().unwrap();
        g.clear();
    }

    pub fn close(&self, conn: &Arc<WsConnection>) {
        // find out the actor and remove it
        let mut g = self.outbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].is_same_connection(&conn) {
                g.swap_remove(i);
                return;
            }
        }

        let mut g = self.inbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].is_same_connection(&conn) {
                g.swap_remove(i);
                return;
            }
        }
    }

    pub fn get_next_inbound(&self) -> Arc<HubConn> {
        let g = self.inbound.read().unwrap();
        let len = g.len();
        assert_ne!(len, 0);
        let idx = self.next_inbound.fetch_add(1, Ordering::Relaxed) % len;
        g[idx].clone()
    }

    pub fn get_next_outbound(&self) -> Arc<HubConn> {
        let g = self.outbound.read().unwrap();
        let len = g.len();
        assert_ne!(len, 0);
        let idx = self.next_outbound.fetch_add(1, Ordering::Relaxed) % len;
        g[idx].clone()
    }
}

#[derive(Clone)]
pub struct HubServer;

impl Server for HubServer {
    fn on_message(&self, ws: Arc<WsConnection>, mut msg: Value) -> Result<()> {
        let mut content = msg[1].take();
        let subject = content["subject"].take();
        let body = content["body"].take();
        match subject.as_str().unwrap_or("none") {
            "version" => self.on_version(ws, body)?,
            subject => bail!("on_message unkown subject: {}", subject),
        }
        Ok(())
    }

    fn on_request(&self, ws: Arc<WsConnection>, mut msg: Value) -> Result<Value> {
        let mut content = msg[1].take();
        let command = content["command"].take();
        let body = content["params"].take();
        // let tag = content["tag"].take();

        let response = match command.as_str().unwrap_or("none") {
            "heartbeat" => self.on_heartbeat(ws, body)?,
            "subscribe" => self.on_subscribe(ws, body)?,
            command => bail!("on_request unkown command: {}", command),
        };
        Ok(response)
    }
}

impl HubServer {
    fn on_version(&self, ws: Arc<WsConnection>, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
            WSS.close(&ws)
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
            // TODO:
            // HubConn(ws).close();
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_heartbeat(&self, _ws: Arc<WsConnection>, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, _ws: Arc<WsConnection>, param: Value) -> Result<Value> {
        if param.is_null() {
            bail!("no params");
        }
        let _subscription_id = param["subscription_id"]
            .as_str()
            .ok_or(format_err!("no subscription_id"))?;

        // TODO:
        // ws.set_subscribed();
        Ok(json!("subscribed"))
    }
}

pub struct HubConn {
    ws: Arc<WsConnection>,
    // indicate if this connection is a subscribed peer
    is_subscribed: AtomicBool,
    is_source: AtomicBool,
}

impl HubConn {
    pub fn new(ws: Arc<WsConnection>) -> Self {
        HubConn {
            ws: ws,
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
        }
    }

    pub fn is_subscribed(&self) -> bool {
        self.is_subscribed.load(Ordering::Relaxed)
    }

    pub fn set_subscribed(&self) {
        self.is_subscribed.store(true, Ordering::Relaxed);
    }

    pub fn is_source(&self) -> bool {
        self.is_source.load(Ordering::Relaxed)
    }

    pub fn set_source(&self) {
        self.is_source.store(true, Ordering::Relaxed);
    }
}

impl Deref for HubConn {
    type Target = WsConnection;
    fn deref(&self) -> &WsConnection {
        &self.ws
    }
}

impl HubConn {
    pub fn send_version(&self) -> Result<()> {
        // TODO: read these things from config
        self.send_just_saying(
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
    }

    pub fn send_subscribe(&self) -> Result<()> {
        use object_hash;
        // TODO: this is used to detect self-connect
        let subscription_id = object_hash::gen_random_string(30);
        // let last_mci = storage::read_last_main_chain_index()?;
        let rsp = self.send_request(
            "subscribe",
            json!({ "subscription_id": subscription_id, "last_mci": 100}),
        )?;

        println!("subscribe rsp={}", rsp);
        Ok(())
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", Value::Null)?;
        Ok(())
    }

    // remove self from global
    pub fn close(&self) {
        WSS.close(&self.ws);
    }
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<HubConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => format!("{}", addr),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;
    // let ws
    let ws = WsConnection::new(conn, HubServer, peer, Role::Client)?;

    let outbound = Arc::new(HubConn::new(ws));
    WSS.add_outbound(outbound.clone());
    Ok(outbound)
}
