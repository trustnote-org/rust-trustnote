use error::{Result, TrustnoteError};
use joint::Joint;
use rusqlite::Connection;
use storage;
use witness_proof;

pub struct CatchupReq {
    last_stable_mci: u32,
    last_known_mci: u32,
    witnesses: Vec<String>,
}

pub struct CatchupChain {
    pub unstable_mc_joints: Vec<Joint>,
    pub stable_last_ball_joints: Vec<Joint>,
    pub witness_change_and_definition: Vec<Joint>,
}

pub fn prepare_catchup_chain(db: &Connection, catchup_req: CatchupReq) -> Result<CatchupChain> {
    let CatchupReq {
        last_stable_mci,
        last_known_mci,
        witnesses,
    } = catchup_req;

    let mut stable_last_ball_joints = Vec::new();

    ensure!(
        last_stable_mci >= last_known_mci,
        "last_stable_mci >= last_known_mci"
    );
    ensure!(witnesses.len() == 12, "invalide witness list");

    let mut stmt = db.prepare_cached(
        "SELECT is_stable FROM units \
         WHERE is_on_main_chain=1 AND main_chain_index=?",
    )?;

    let rows = stmt.query_map(&[&last_known_mci], |row| row.get::<_, u32>(0))?
        .collect::<Vec<_>>();
    ensure!(
        rows.len() == 0 || rows[0].as_ref().unwrap() == &0,
        TrustnoteError::CatchupAlreadyCurrent
    );

    let witness_proof = witness_proof::prepare_witness_proof(db, witnesses, last_stable_mci)?;
    let mut last_ball_unit = witness_proof.last_ball_unit;

    loop {
        let joint = storage::read_joint_with_ball(db, &last_ball_unit)?;
        let joint_last_ball_unit = joint.unit.last_ball_unit.clone();
        stable_last_ball_joints.push(joint);
        let unit_porps = storage::read_unit_props(db, &last_ball_unit)?;
        if unit_porps.main_chain_index <= last_stable_mci {
            break;
        }
        if joint_last_ball_unit.is_none() {
            break;
        }
        // goup
        last_ball_unit = joint_last_ball_unit.unwrap();
    }

    // if last
    Ok(CatchupChain {
        stable_last_ball_joints,
        unstable_mc_joints: witness_proof.unstable_mc_joints,
        witness_change_and_definition: witness_proof.witness_change_and_definition,
    })
}

pub fn purge_handled_balls_from_hash_tree(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT ball FROM hash_tree_balls \
         CROSS JOIN balls USING(ball)",
    )?;
    let balls = stmt.query_map(&[], |row| row.get::<_, String>(0))?;

    let mut stmt = db.prepare_cached("DELETE FROM hash_tree_balls WHERE ball=?")?;
    for ball in balls {
        stmt.execute(&[&ball?])?;
    }
    Ok(())
}
