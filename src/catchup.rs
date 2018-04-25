use error::Result;
use joint::Joint;
use may::sync::Mutex;
use rusqlite::Connection;
use storage;
use witness_proof;

lazy_static! {
    static ref CATCHUP_MUTEX: Mutex<()> = Mutex::new(());
}

#[derive(Serialize, Deserialize)]
pub struct CatchupReq {
    last_stable_mci: u32,
    last_known_mci: u32,
    witnesses: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct CatchupChain {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unstable_mc_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub stable_last_ball_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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
    if rows.len() == 0 || rows[0].as_ref().unwrap() == &0 {
        return Ok(CatchupChain {
            // already current
            status: Some("current".to_owned()),
            unstable_mc_joints: Vec::new(),
            stable_last_ball_joints: Vec::new(),
            witness_change_and_definition: Vec::new(),
        });
    }

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

    Ok(CatchupChain {
        status: None,
        stable_last_ball_joints,
        unstable_mc_joints: witness_proof.unstable_mc_joints,
        witness_change_and_definition: witness_proof.witness_change_and_definition,
    })
}

// return true if alreay current, or else flase
pub fn process_catchup_chain(db: &Connection, catchup_chain: CatchupChain) -> Result<bool> {
    if let Some(s) = catchup_chain.status {
        if s.as_str() == "current" {
            return Ok(true);
        }
    }

    ensure!(
        !catchup_chain.stable_last_ball_joints.is_empty(),
        "stable_last_ball_joints is empty"
    );

    let witness_proof = witness_proof::process_witness_proof(
        db,
        catchup_chain.unstable_mc_joints,
        catchup_chain.witness_change_and_definition,
        true,
    )?;

    let last_ball_units = witness_proof.last_ball_units;
    let assoc_last_ball_by_last_ball_unit = witness_proof.assoc_last_ball_by_last_ball_unit;

    let first_stable_joint = &catchup_chain.stable_last_ball_joints[0];
    // let first_stable_unit = &first_stable_joint.unit;

    let mut last_ball_unit = first_stable_joint.get_unit_hash();
    ensure!(
        last_ball_units.contains(last_ball_unit),
        "first stable unit is not last ball unit of any unstable unit"
    );

    let last_stable_mc_unit_props;
    let mut last_ball = &assoc_last_ball_by_last_ball_unit[last_ball_unit];
    ensure!(
        first_stable_joint.ball.as_ref() == Some(last_ball),
        "last ball and last ball unit do not match"
    );

    let mut chain_balls = Vec::<&String>::new();
    for joint in catchup_chain.stable_last_ball_joints.iter() {
        ensure!(joint.ball.is_some(), "stable but no ball");
        ensure!(joint.has_valid_hashes(), "invalid hash");
        ensure!(
            joint.get_unit_hash() == last_ball_unit,
            "not the last ball unit"
        );
        ensure!(joint.ball.as_ref() == Some(last_ball), "not the last ball");

        let unit = &joint.unit;

        if unit.last_ball_unit.is_some() {
            last_ball = unit.last_ball.as_ref().unwrap();
            last_ball_unit = unit.last_ball_unit.as_ref().unwrap();
        }

        chain_balls.push(joint.ball.as_ref().unwrap());
    }
    // FIXME: use a dqueue to avoid reverse
    chain_balls.reverse();

    // FIXME: is this lock too earlier?
    let _g = CATCHUP_MUTEX.lock().unwrap();
    let mut stmt = db.prepare_cached("SELECT 1 FROM catchup_chain_balls LIMIT 1")?;
    ensure!(!stmt.exists(&[])?, "duplicate catchup_chain_balls");

    // adjust first chain ball if necessary and make sure it is the only stable unit in the entire chain
    let mut stmt = db.prepare_cached(
        "SELECT is_stable, is_on_main_chain, main_chain_index \
         FROM balls JOIN units USING(unit) WHERE ball=?",
    )?;

    let mut rows = stmt.query_map(&[chain_balls[0]], |row| {
        (
            row.get::<_, u32>(0),
            row.get::<_, u32>(1),
            row.get::<_, u32>(2),
        )
    })?;
    let (is_stable, is_on_main_chain, main_chain_index) = match rows.next() {
        None => {
            if storage::is_genesis_ball(chain_balls[0]) {
                return Ok(false);
            }
            bail!("first chain ball {} is not known", chain_balls[0]);
        }
        Some(row) => row?,
    };

    ensure!(
        is_stable == 1,
        "first chain ball {} is not stable",
        chain_balls[0]
    );
    ensure!(
        is_on_main_chain == 1,
        "first chain ball {} is not on mc",
        chain_balls[0]
    );

    last_stable_mc_unit_props = storage::read_last_stable_mc_unit_props(db)?;
    let last_stable_mci = last_stable_mc_unit_props.main_chain_index;
    if main_chain_index > last_stable_mci {
        bail!("first chain ball {} mci is too large", chain_balls[0]);
    }
    if last_stable_mci == main_chain_index {
        return Ok(false);
    }

    // replace to avoid receiving duplicates
    chain_balls[0] = &last_stable_mc_unit_props.ball;
    if chain_balls.len() > 1 {
        return Ok(false);
    }

    let mut stmt =
        db.prepare_cached("SELECT is_stable FROM balls JOIN units USING(unit) WHERE ball=?")?;
    let mut rows = stmt.query_map(&[chain_balls[1]], |row| row.get::<_, u32>(0))?;
    let second_ball_is_stable = match rows.next() {
        None => return Ok(false),
        Some(row) => row?,
    };

    ensure!(
        second_ball_is_stable != 1,
        "second chain ball {} must not be stable",
        chain_balls[1]
    );

    // validation complete, now write the chain for future downloading of hash trees
    let ball_str = chain_balls
        .iter()
        .map(|s| format!("('{}')", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO catchup_chain_balls (ball) VALUES {}", ball_str);
    db.execute(&sql, &[])?;
    Ok(false)
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
