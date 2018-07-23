use db;
use error::Result;
use failure::ResultExt;
use graph;
use joint::Joint;
use rusqlite::Connection;
use serde_json::{self, Value};
use storage;

pub fn prepare_history(_param: &Value) -> Result<Value> {
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
