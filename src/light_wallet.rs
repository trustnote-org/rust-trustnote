//use trustnote::network;

use failure::ResultExt;

use db;
use error::Result;
use light;
use my_witness;
use network::wallet::WalletConn;
use rusqlite::Connection;
use serde_json::{self, Value};

pub fn refresh_light_client_history(ws: &WalletConn) -> Result<()> {
    let req_get_history =
        prepare_request_for_history().context("prepare_request_for_history failed")?;
    let response_history_v = ws
        .send_request("light/get_history", &req_get_history)
        .context("send get_history_request failed")?;
    let mut response_history_s: light::HistoryResponse =
        serde_json::from_value(response_history_v)?;

    light::process_history(&mut response_history_s).context("process_history response failed")?;

    Ok(())
}

fn prepare_request_for_history() -> Result<Value> {
    let witnesses = my_witness::read_my_witnesses()?;
    if witnesses.is_empty() {
        bail!("witnesses not found");
    }
    let db = db::DB_POOL.get_connection();
    let addresses = read_my_addresses(&db).context("prepare_request_for_history failed as ")?;
    let requested_joints =
        read_list_of_unstable_units(&db).context("prepare_request_for_history failed as ")?;
    if addresses.is_empty() && requested_joints.is_empty() {
        bail!("prepare_request_for_history failed as addresses and requested_joints are not found");
    }

    let mut req_history = light::HistoryRequest {
        witnesses,
        addresses,
        requested_joints,
        known_stable_units: Vec::new(),
    };
    if req_history.addresses.is_empty() {
        return Ok(serde_json::to_value(req_history)?);
    }

    let addresses_list = req_history
        .addresses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT unit FROM unit_authors JOIN units USING(unit) WHERE is_stable=1 AND address IN({}) \
					UNION \
					SELECT unit FROM outputs JOIN units USING(unit) WHERE is_stable=1 AND address IN({})", 
                    addresses_list, addresses_list);
    let mut stmt = db.prepare_cached(&sql)?;
    let known_stable_units = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if !known_stable_units.is_empty() {
        req_history.known_stable_units = known_stable_units;
    }
    Ok(serde_json::to_value(req_history)?)
}

fn read_my_addresses(db: &Connection) -> Result<Vec<String>> {
    let mut stmt =
            db.prepare_cached("SELECT address FROM my_addresses UNION SELECT shared_address AS address FROM shared_addresses")?;
    let addresses = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if addresses.is_empty() {
        bail!("addresses not found");
    }
    Ok(addresses)
}

fn read_list_of_unstable_units(db: &Connection) -> Result<Vec<String>> {
    let mut stmt = db.prepare_cached("SELECT unit FROM units WHERE is_stable=0")?;
    let units = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if units.is_empty() {
        bail!("unstable_units not found");
    }
    Ok(units)
}
