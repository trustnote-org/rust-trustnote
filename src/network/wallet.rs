use std::sync::Arc;

use super::network::{Sender, Server, WsConnection};
use error::Result;
use may::{self, net::TcpStream};
use serde_json::{self, Value};
use tungstenite::client::client;
use tungstenite::handshake::client::Request;
use tungstenite::protocol::Role;
use url::Url;

#[derive(Default)]
pub struct WalletData {}

pub fn connect_to_hub(hub: &str) -> Result<()> {
    let stream = TcpStream::connect(hub)?;
    let peer = match stream.peer_addr() {
        Ok(addr) => addr.to_string(),
        Err(_) => "unknown peer".to_owned(),
    };
    let url = Url::parse("wss://localhost/")?;
    let req = Request::from(url);
    let (conn, _) = client(req, stream)?;

    let ws = WsConnection::new(conn, WalletData::default(), peer, Role::Client)?;

    //Just test heartbeat for now
    let response = ws.send_request("heartbeat", &Value::Null)?;
    println!("Response from heartbeat {}", response);

    Ok(())
}

impl Server<WalletData> for WalletData {
    fn on_message(ws: Arc<WsConnection<WalletData>>, subject: String, body: Value) -> Result<()> {
        match subject.as_str() {
            subject => info!(
                "on_message unknown subject: {} body {}",
                subject,
                body.to_string()
            ),
        }
        Ok(())
    }

    fn on_request(
        ws: Arc<WsConnection<WalletData>>,
        command: String,
        params: Value,
    ) -> Result<Value> {
        let response = match command.as_str() {
            command => bail!(
                "on_request unknown command: {} {}",
                command,
                params.to_string()
            ),
        };
        Ok(response)
    }
}

// #[test]
// fn test_wallet_connection() -> Result<()> {
//     may::config()
//         .set_stack_size(0x4000)
//         .set_io_workers(0)
//         .set_workers(1);

//     connect_to_hub("127.0.0.1:6615")?;

//     Ok(())
// }
