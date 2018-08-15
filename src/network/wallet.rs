use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use super::network::{Sender, Server, WsConnection};
use config;
use error::Result;
use failure::ResultExt;
use joint::Joint;
use light;
use light::LastStableBallAndParentUnitsAndWitnessListUnit;
use light_wallet;
use may::coroutine;
use may::net::TcpStream;
use my_witness;
use rusqlite::Connection;
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

#[derive(Default)]
pub struct WalletData {}

pub type WalletConn = WsConnection<WalletData>;

fn init_connection(ws: &Arc<WalletConn>) -> Result<()> {
    use rand::{thread_rng, Rng};

    ws.send_version()?;

    let mut rng = thread_rng();
    let n: u64 = rng.gen_range(0, 1000);
    let ws_c = Arc::downgrade(ws);

    // start the heartbeat timer for each connection
    go!(move || loop {
        coroutine::sleep(Duration::from_millis(3000 + n));
        let ws = match ws_c.upgrade() {
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
        }
    });

    Ok(())
}

pub fn create_outbound_conn<A: ToSocketAddrs>(address: A) -> Result<Arc<WalletConn>> {
    let stream = TcpStream::connect(address)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;

    let ws = WsConnection::new(conn, WalletData::default(), peer, Role::Client)?;

    init_connection(&ws)?;
    Ok(ws)
}

impl WalletConn {
    fn send_version(&self) -> Result<()> {
        self.send_just_saying(
            "version",
            json!({
                "protocol_version": config::VERSION,
                "alt": config::ALT,
                "library": config::LIBRARY,
                "library_version": config::LIBRARY_VERSION,
                "program": "rust-trustnote-ttt",
                "program_version": "0.1.0"
            }),
        )
    }

    fn send_heartbeat(&self) -> Result<()> {
        self.send_request("heartbeat", &Value::Null)?;
        Ok(())
    }

    pub fn post_joint(&self, joint: &Joint) -> Result<()> {
        self.send_request("post_joint", &serde_json::to_value(joint)?)?;
        Ok(())
    }

    pub fn get_parents_and_last_ball_and_witness_list_unit(
        &self,
    ) -> Result<LastStableBallAndParentUnitsAndWitnessListUnit> {
        let resp = self.send_request(
            "light/get_parents_and_last_ball_and_witness_list_unit",
            &json!({"witnesses": &*my_witness::MY_WITNESSES}),
        )?;

        Ok(serde_json::from_value(resp)?)
    }

    pub fn refresh_history(&self, db: &Connection) -> Result<()> {
        let req_get_history =
            light_wallet::get_history(db).context("prepare_request_for_history failed")?;

        let response_history = self
            .send_request("light/get_history", &serde_json::to_value(req_get_history)?)
            .context("send request get_history failed")?;

        let mut response_history_s: light::HistoryResponse =
            serde_json::from_value(response_history)?;

        light::process_history(&db, &mut response_history_s)
    }

    pub fn get_witnesses(&self) -> Result<Vec<String>> {
        let witnesses = self
            .send_request("get_witnesses", &Value::Null)
            .context("failed to get witnesses")?;
        let witnesses: Vec<String> =
            serde_json::from_value(witnesses).context("failed to parse witnesses")?;
        if witnesses.len() != config::COUNT_WITNESSES {
            bail!(
                "witnesses must contains {} addresses, but we got {}",
                config::COUNT_WITNESSES,
                witnesses.len()
            );
        }
        Ok(witnesses)
    }
}

// the server side impl
impl WalletConn {
    fn on_version(&self, version: Value) -> Result<()> {
        if version["protocol_version"].as_str() != Some(config::VERSION) {
            error!("Incompatible versions, mine {}", config::VERSION);
        }

        if version["alt"].as_str() != Some(config::ALT) {
            error!("Incompatible alt, mine {}", config::ALT);
        }

        info!("got peer version: {}", version);
        Ok(())
    }

    fn on_hub_challenge(&self, param: Value) -> Result<()> {
        // TODO: add special wallet logic here
        // this is hub, we do nothing here
        // only wallet would save the challenge and save the challenge
        // for next login and match
        info!("peer is a hub, challenge = {}", param);
        Ok(())
    }

    fn on_heartbeat(&self, _: Value) -> Result<Value> {
        Ok(Value::Null)
    }

    fn on_subscribe(&self, _param: Value) -> Result<Value> {
        bail!("I'm light, cannot subscribe you to updates");
    }
}

impl Server<WalletData> for WalletData {
    fn on_message(ws: Arc<WalletConn>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            "version" => ws.on_version(body)?,
            "hub/challenge" => ws.on_hub_challenge(body)?,
            subject => error!("on_message unknown subject: {}", subject),
        }
        Ok(())
    }

    fn on_request(ws: Arc<WalletConn>, command: String, params: Value) -> Result<Value> {
        let response = match command.as_str() {
            "heartbeat" => ws.on_heartbeat(params)?,
            "subscribe" => ws.on_subscribe(params)?,
            command => bail!("on_request unknown command: {}", command),
        };
        Ok(response)
    }
}
