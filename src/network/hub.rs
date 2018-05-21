use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use config;
use error::Result;
use may::coroutine;
use may::net::TcpStream;
use may::sync::RwLock;
use serde_json::Value;
use storage;
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

pub struct HubData {
    // indicate if this connection is a subscribed peer
    is_subscribed: AtomicBool,
    is_source: AtomicBool,
}

pub type HubConn = WsConnection<HubData>;

// global Ws connections
lazy_static! {
    pub static ref WSS: WsConnections = WsConnections::new();
}

fn init_connection(ws: &Arc<HubConn>) {
    use rand::{thread_rng, Rng};

    t!(ws.send_version());
    t!(ws.send_subscribe());
    t!(ws.send_hub_challenge());

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    let ws = Arc::downgrade(ws);
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
        let rsp = ws.send_heartbeat();
        if rsp.is_err() {
            error!("heartbeat err= {}", rsp.unwrap_err());
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
        init_connection(&inbound);
        let mut g = self.inbound.write().unwrap();
        g.push(inbound);
    }

    pub fn add_outbound(&self, outbound: Arc<HubConn>) {
        init_connection(&outbound);
        let mut g = self.outbound.write().unwrap();
        g.push(outbound);
    }

    pub fn close_all(&self) {
        let mut g = self.outbound.write().unwrap();
        g.clear();
        let mut g = self.inbound.write().unwrap();
        g.clear();
    }

    pub fn close(&self, conn: &HubConn) {
        // find out the actor and remove it
        let mut g = self.outbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
                g.swap_remove(i);
                return;
            }
        }

        let mut g = self.inbound.write().unwrap();
        for i in 0..g.len() {
            if g[i].conn_eq(&conn) {
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

impl Server<HubData> for HubData {
    fn new() -> HubData {
        HubData {
            is_subscribed: AtomicBool::new(false),
            is_source: AtomicBool::new(false),
        }
    }

    fn on_message(ws: Arc<HubConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
            subject => bail!("on_message unkown subject: {}", subject),
        }
        Ok(())
    }

    fn on_request(ws: Arc<HubConn>, command: String, params: Value) -> Result<Value> {
        let response = match command.as_str() {
            "heartbeat" => ws.on_heartbeat(params)?,
            "subscribe" => ws.on_subscribe(params)?,
            command => bail!("on_request unkown command: {}", command),
        };
        Ok(response)
    }
}

// internal state access
impl HubConn {
    pub fn is_subscribed(&self) -> bool {
        let data = self.get_data();
        data.is_subscribed.load(Ordering::Relaxed)
    }

    fn set_subscribed(&self) {
        let data = self.get_data();
        data.is_subscribed.store(true, Ordering::Relaxed);
    }

    pub fn is_source(&self) -> bool {
        let data = self.get_data();
        data.is_source.load(Ordering::Relaxed)
    }

    fn set_source(&self) {
        let data = self.get_data();
        data.is_source.store(true, Ordering::Relaxed);
    }
}

// the server side impl
impl HubConn {
    fn on_version(&self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
            self.close();
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
            self.close();
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, param: Value) -> Result<Value> {
        // TODO: is it necessary to detect the self connection? (#63)
        let _subscription_id = param["subscription_id"]
            .as_str()
            .ok_or(format_err!("no subscription_id"))?;

        self.set_subscribed();
        Ok(json!("subscribed"))
    }

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // this is hub, we do nothing here
        // only wallet would save the challenge and save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        Ok(())
    }
}

// the client side impl
impl HubConn {
    fn send_version(&self) -> Result<()> {
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

    fn send_hub_challenge(&self) -> Result<()> {
        use object_hash;
        let challenge = object_hash::gen_random_string(30);
        self.send_just_saying("hub/challenge", json!(challenge))?;
        Ok(())
    }

    fn send_subscribe(&self) -> Result<()> {
        use object_hash;
        // TODO: this is used to detect self-connect (#63)
        let subscription_id = object_hash::gen_random_string(30);
        let db = ::db::DB_POOL.get_connection();
        let last_mci = storage::read_last_main_chain_index(&db)?;
        self.send_request(
            "subscribe",
            json!({ "subscription_id": subscription_id, "last_mci": last_mci}),
        )?;

        self.set_source();
        Ok(())
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", Value::Null)?;
        Ok(())
    }

    // remove self from global
    pub fn close(&self) {
        info!("close connection: {}", self.get_peer());
        WSS.close(self);
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
    let ws = WsConnection::new(conn, HubData::new(), peer, Role::Client)?;

    WSS.add_outbound(ws.clone());
    Ok(ws)
}
