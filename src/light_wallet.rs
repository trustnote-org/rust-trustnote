//use trustnote::network;

use failure::ResultExt;

use error::Result;
use light::HistoryRequest;
use my_witness;
use rusqlite::Connection;

pub fn get_history(db: &Connection) -> Result<HistoryRequest> {
    let witnesses = my_witness::MY_WITNESSES.clone();
    if witnesses.is_empty() {
        bail!("witnesses not found");
    }

    let addresses =
        read_my_addresses(db).context("prepare_request_for_history read_my_addresses failed")?;
    let mut requested_joints = read_list_of_unstable_units(db)
        .context("prepare_request_for_history read_list_of_unstable_units failed")?;
    if requested_joints.is_empty() {
        // here we can't give an empty vec, just make up one
        requested_joints.push("v|NuDxzT7VFa/AqfBsAZ8suG4uj3u+l0kXOLE+nP+dU=".to_string());
    }

    let mut req_history = HistoryRequest {
        witnesses,
        addresses,
        requested_joints,
        // here we can't give an empty vec, just make up one
        known_stable_units: vec!["v|NuDxzT7VFa/AqfBsAZ8suG4uj3u+l0kXOLE+nP+dU=".to_string()],
    };

    let addresses_list = req_history
        .addresses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT unit FROM unit_authors JOIN units USING(unit) WHERE is_stable=1 AND address IN({}) \
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
    let mut stmt = db.prepare_cached(
        "SELECT address FROM my_addresses \
         UNION \
         SELECT shared_address AS address FROM shared_addresses",
    )?;
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
        info!("unstable_units not found");
    }
    Ok(units)
}
