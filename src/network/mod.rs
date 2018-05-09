pub mod hub_connection;
mod network;

pub use self::network::{run_ws_server, Connection};

use self::hub_connection::HubConn;
use may::sync::RwLock;

lazy_static! {
    pub static ref INBOUND_CONN: RwLock<Vec<HubConn>> = RwLock::new(Vec::new());
    pub static ref OUTBOUND_CONN: RwLock<Vec<HubConn>> = RwLock::new(Vec::new());
}
