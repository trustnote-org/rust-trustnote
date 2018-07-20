use db;
use error::Result;
use graph;
use joint::Joint;
use serde_json::{self, Value};
use storage;

pub fn prepare_history(_param: &Value) -> Result<Value> {
    unimplemented!()
}

pub fn prepare_link_proofs(params: Value) -> Result<Value> {
    let units: Vec<String> =
        serde_json::from_value(params).expect("prepare_Link_proofs.params is error");
    if units.is_empty() {
        return Err(format_err!("no units array"));
    } else if units.len() == 1 {
        return Err(format_err!("chain of one element"));
    }
    let mut chains: Vec<Joint> = Vec::new();
    for i in 1.. {
        if i > units.len() {
            break;
        }
        create_link_proof(&units[i - 1], &units[i], &mut chains)?;
    }

    Ok(serde_json::to_value(chains)?)
}

fn create_link_proof(
    later_unit: &String,
    earlier_unit: &String,
    chains: &mut Vec<Joint>,
) -> Result<()> {
    let db = db::DB_POOL.get_connection();
    let later_joint = storage::read_joint(&db, later_unit)
        .or_else(|e| bail!("nonserial unit not found?, err={}", e))?;

    let later_mci = later_joint.unit.main_chain_index;
    chains.push(later_joint.clone());

    let unit_props = storage::read_unit_props(
        &db,
        &later_joint
            .unit
            .last_ball_unit
            .as_ref()
            .expect("joint.unit.last_ball_unit is none"),
    )?;

    let later_lb_mci = unit_props
        .main_chain_index
        .expect("main_chain_index is error");

    let earlier_joint = storage::read_joint(&db, earlier_unit)
        .or_else(|e| bail!("nonserial unit not found?, err={}", e))?;

    let earlier_mci = earlier_joint
        .unit
        .main_chain_index
        .as_ref()
        .expect("mci is error");
    if later_mci.as_ref() < Some(earlier_mci) {
        return Err(format_err!("not included"));
    }

    let earlier_joint_unit = earlier_joint.unit.unit.as_ref().expect("unit is error");
    if &later_lb_mci >= earlier_mci {
        buil_proof_chain(
            &(later_lb_mci + 1),
            earlier_mci,
            &earlier_joint_unit,
            chains,
        )?;
    } else {
        if !graph::determine_if_included_or_equal(
            &db,
            &earlier_joint_unit,
            &[later_unit.to_string()],
        )? {
            return Err(format_err!("not included"));
        }
        build_path(&later_joint, &earlier_joint, chains)?;
    }
    Ok(())
}

//TODO:
fn buil_proof_chain(
    _mci: &u32,
    _earlier_mci: &u32,
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
