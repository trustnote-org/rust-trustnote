use std::collections::HashMap;
use db;
use config;
use error::Result;
use failure::ResultExt;
use graph;
use joint::Joint;
use rusqlite::Connection;
use serde_json::{self, Value};
use storage;
use witness_proof;

const MAX_HISTORY_ITEMS: usize = 1000;

#[derive(Deserialize)]
pub struct HistoryRequest {
    pub known_stable_units: Vec<String>,
    pub witnesses: Vec<String>,
    pub addresses: Vec<String>,
    pub requested_joints: Vec<String>,
}

pub fn prepare_history(history_request: &HistoryRequest, db: &mut Connection) -> Result<Value> {
    if history_request.addresses.is_empty() && history_request.requested_joints.is_empty() {
        bail!("neither addresses nor joints requested");
    }
    if history_request.addresses.is_empty() {
        bail!("no addresses");
    }
    if history_request.known_stable_units.is_empty() {
        bail!("known_stable_units must be non-empty array");
    }
    if history_request.requested_joints.is_empty() {
        bail!("no requested joints");
    }
    if history_request.witnesses.len() != config::COUNT_WITNESSES {
        bail!("wrong number of witnesses");
    }
    let assoc_know_stable_units = history_request
        .known_stable_units
        .iter()
        .map(|s| (s, true))
        .collect::<HashMap<_, _>>();

    #[derive(Serialize)]
    struct Response {
        unstable_mc_joints: Vec<Joint>,            //yes
        witness_change_and_definition: Vec<Joint>, //yes
        // last_ball_unit: String,                    //
        // last_ball_mci: u32,
        joints: Vec<Joint>,
        proofchain_balls: Vec<String>,
    }

    let mut selects = Vec::new();
    let addresses_and_shared_address = add_shared_addresses_of_wallet(&history_request.addresses);
    if !addresses_and_shared_address.is_empty() {
        let address_list = addresses_and_shared_address
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT DISTINCT unit, main_chain_index, level FROM outputs JOIN units USING(unit) \
				WHERE address IN({}) AND (+sequence='good' OR is_stable=1) \
				UNION \
				SELECT DISTINCT unit, main_chain_index, level FROM unit_authors JOIN units USING(unit) \
				WHERE address IN({}) AND (+sequence='good' OR is_stable=1) ", address_list, address_list);
        selects.push(sql);
    }
    if !history_request.requested_joints.is_empty() {
        let unit_list = history_request
            .requested_joints
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT unit, main_chain_index, level FROM units WHERE unit IN({}) AND (+sequence='good' OR is_stable=1) ", unit_list);
        selects.push(sql);
    }
    let sql = format!(
        "{} ORDER BY main_chain_index DESC, level DESC",
        selects.join("UNION ")
    );
    #[derive(Clone)]
    struct TempProps {
        unit: String,
        main_chain_index: Option<u32>,
        level: u32,
    }
    let mut stmt = db.prepare_cached(&sql)?;
    let tmp_rows = stmt
        .query_map(&[], |row| TempProps {
            unit: row.get(0),
            main_chain_index: row.get(1),
            level: row.get(2),
        })?
        .collect::<::std::result::Result<Vec<_>, _>>()?;
    let rows = tmp_rows
        .iter()
        .filter(|s| !assoc_know_stable_units[&s.unit])
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Value::Null);
    }
    if rows.len() > MAX_HISTORY_ITEMS {
        bail!("your history is too large, consider switching to a full client");
    }
    let prepare_witness_proof =
        witness_proof::prepare_witness_proof(db, &history_request.witnesses, 0)?;
    let mut later_mci = prepare_witness_proof.last_ball_mci + 1;
    let mut joints = Vec::new();
    let mut proofchain_balls = Vec::new();
    for row in rows {
        match storage::read_joint(db, &row.unit) {
            Ok(j) => {
                joints.push(j);
                if row.main_chain_index > Some(prepare_witness_proof.last_ball_mci)
                    || row.main_chain_index.is_none()
                {
                    continue;
                }
                build_proof_chain(
                    later_mci,
                    row.main_chain_index,
                    &row.unit,
                    &mut proofchain_balls,
                )?;
                later_mci = row.main_chain_index.unwrap();
            }

            Err(_) => error!("prepareJointsWithProofs unit not found {}", row.unit),
        }
    }

    Ok(serde_json::to_value(Response {
        unstable_mc_joints: prepare_witness_proof.unstable_mc_joints,
        witness_change_and_definition: prepare_witness_proof.witness_change_and_definition,
        joints: joints,
        proofchain_balls: Vec::new(),
    })?)
}
fn add_shared_addresses_of_wallet(_addresses: &Vec<String>) -> Vec<String> {
    unimplemented!()
}

// TODO: better use specil struct instead of Value
pub fn prepare_link_proofs(params: Value) -> Result<Value> {
    let units: Vec<String> =
        serde_json::from_value(params).context("prepare_Link_proofs.params is error")?;

    if units.is_empty() {
        bail!("no units array");
    } else if units.len() == 1 {
        bail!("chain of one element");
    }
    let mut chains: Vec<Joint> = Vec::new();

    let db = db::DB_POOL.get_connection();
    for two_units in units.windows(2) {
        create_link_proof(&db, &two_units[0], &two_units[1], &mut chains)?;
    }

    Ok(serde_json::to_value(chains)?)
}

fn create_link_proof(
    db: &Connection,
    later_unit: &String,
    earlier_unit: &String,
    chains: &mut Vec<Joint>,
) -> Result<()> {
    let later_joint = storage::read_joint(&db, later_unit).context("nonserial unit not found?")?;

    let later_mci = later_joint.unit.main_chain_index;
    chains.push(later_joint.clone());

    let laster_ball_unit = later_joint
        .unit
        .last_ball_unit
        .as_ref()
        .ok_or_else(|| format_err!("joint.unit.last_ball_unit is none"))?;

    let unit_props = storage::read_unit_props(&db, laster_ball_unit)?;

    let later_lb_mci = unit_props
        .main_chain_index
        .ok_or_else(|| format_err!("main_chain_index is error"))?;

    let earlier_joint =
        storage::read_joint(&db, earlier_unit).context("nonserial unit not found?")?;

    let earlier_mci = earlier_joint
        .unit
        .main_chain_index
        .ok_or_else(|| format_err!("mci is None"))?;

    if later_mci.is_none() || later_mci < Some(earlier_mci) {
        bail!("not included");
    }

    let earlier_joint_unit = earlier_joint.get_unit_hash();
    if later_lb_mci >= earlier_mci {
        buil_proof_chain(later_lb_mci + 1, earlier_mci, earlier_joint_unit, chains)?;
    } else {
        if !graph::determine_if_included(&db, &earlier_joint_unit, &[later_unit.to_string()])? {
            bail!("not included");
        }
        build_path(&later_joint, &earlier_joint, chains)?;
    }

    Ok(())
}

//TODO:
fn buil_proof_chain(
    _mci: u32,
    _earlier_mci: u32,
    _unit: &String,
    _balls: &mut Vec<Joint>,
) -> Result<()> {
    unimplemented!()
}

//TODO:
fn build_path(
    _later_joint: &Joint,
    _earlier_joint: &Joint,
    _chains: &mut Vec<Joint>,
) -> Result<()> {
    unimplemented!()
}

// TODO: better to return a struct instead of Value
pub fn prepare_parents_and_last_ball_and_witness_list_unit(
    witness: &[String],
    db: &Connection,
) -> Result<Value> {
    if witness.len() != config::COUNT_WITNESSES {
        bail!("wrong number of witnesses");
    }

    if storage::determine_if_witness_and_address_definition_have_refs(db, witness)? {
        bail!("some witnesses have references in their addresses");
    }
    //TODO: impl pickParentUnitsAndLastBall()

    unimplemented!()
}
