//use trustnote::network;

use failure::ResultExt;

use rusqlite::Connection;
use serde::ser::Serialize;
use trustnote::error::Result;
use trustnote::my_witness;
use trustnote::*;

struct ws {
    b_refreshing_history: bool,
    b_light_vendor: bool,
}

fn find_outbound_peer_or_connect(_: String) -> Result<ws> {
    unimplemented!();
}
fn refresh_light_client_history() -> Result<()> {
    if !config::IS_LIGHT {
        info!("is not light wallet.");
        return Ok(());
    }
    let light_vendor_url = "".to_string();
    if light_vendor_url.is_empty() {
        info!("refreshLightClientHistory called too early: light_vendor_url not set yet");
        return Ok(());
    }
    info!("refresh_light_started");

    let mut ws = find_outbound_peer_or_connect(light_vendor_url)?; //FIXME: need to impl

    if ws.b_refreshing_history {
        info!("previous refresh not finished yet");
        return Ok(());
    }
    ws.b_refreshing_history = true;
    let req_get_history =
        prepare_request_for_history().context("prepare_request_for_history failed")?;
    let response_history = network::send_request(ws, "light/get_history", req_get_history)
        .context("send get_history_request failed")?;

    let ret = light::process_history(response_history).context("process_history failed")?;

    //unimplemented!();
    Ok(())
}

struct Req_History {
    //#[serde(skip_serializing_if = "is_empty")]
    witnesses: Vec<String>,
    //#[serde(skip_serializing_if = "is_empty")]
    addresses: Vec<String>,
    //#[serde(skip_serializing_if = "is_empty")]
    unstable_units: Vec<String>,
    known_stable_units: Vec<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    last_stable_mci: u32,
    unit: String,
}

fn prepare_request_for_history() -> Result<Req_History> {
    let witnesses = my_witness::read_my_witnesses()?;
    if witnesses.is_empty() {
        bail!("witnesses not found");
    }
    let db = db::DB_POOL.get_connection();
    let addresses = read_my_addresses(&db).context("prepare_request_for_history failed as ")?;
    let unstable_units =
        read_list_of_unstable_units(&db).context("prepare_request_for_history failed as ")?;
    if addresses.is_empty() && unstable_units.is_empty() {
        bail!("prepare_request_for_history failed as addresses and unstable_units are not found");
    }
    let mut req_history = Req_History {
        witnesses,
        addresses,
        unstable_units,
        known_stable_units: vec![],
        last_stable_mci: 0,
        unit: "".to_string(),
    };
    if req_history.addresses.is_empty() {
        return Ok(req_history);
    }
    req_history.last_stable_mci = 0;

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
    Ok(req_history)
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
