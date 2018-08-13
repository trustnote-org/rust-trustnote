use error::Result;
use joint::Joint;
use may::sync::Mutex;
use rusqlite::Connection;
use spec;
use storage;
use witness_proof;

lazy_static! {
    static ref CATCHUP_MUTEX: Mutex<()> = Mutex::new(());
    static ref HASHTREE_MUTEX: Mutex<()> = Mutex::new(());
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
    #[serde(default)]
    pub unstable_mc_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub stable_last_ball_joints: Vec<Joint>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub witness_change_and_definition_joints: Vec<Joint>,
}

pub fn prepare_catchup_chain(db: &Connection, catchup_req: CatchupReq) -> Result<CatchupChain> {
    let CatchupReq {
        last_stable_mci,
        last_known_mci,
        witnesses,
    } = catchup_req;

    let mut stable_last_ball_joints = Vec::new();

    if last_stable_mci >= last_known_mci && (last_known_mci > 0 || last_stable_mci > 0) {
        bail!("last_stable_mci >= last_known_mci");
    };
    ensure!(witnesses.len() == 12, "invalide witness list");

    let mut stmt = db.prepare_cached(
        "SELECT is_stable FROM units \
         WHERE is_on_main_chain=1 AND main_chain_index=?",
    )?;

    let rows = stmt
        .query_map(&[&last_known_mci], |row| row.get::<_, u32>(0))?
        .collect::<Vec<_>>();
    if rows.is_empty() || rows[0].as_ref().unwrap() == &0 {
        return Ok(CatchupChain {
            // already current
            status: Some("current".to_owned()),
            unstable_mc_joints: Vec::new(),
            stable_last_ball_joints: Vec::new(),
            witness_change_and_definition_joints: Vec::new(),
        });
    }

    let witness_proof = witness_proof::prepare_witness_proof(db, &witnesses, last_stable_mci)?;
    let mut last_ball_unit = witness_proof.last_ball_unit;

    loop {
        let joint = storage::read_joint_with_ball(db, &last_ball_unit)?;
        let joint_last_ball_unit = joint.unit.last_ball_unit.clone();
        stable_last_ball_joints.push(joint);
        let unit_porps = storage::read_unit_props(db, &last_ball_unit)?;
        if unit_porps.main_chain_index <= Some(last_stable_mci) {
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
        witness_change_and_definition_joints: witness_proof.witness_change_and_definition,
    })
}

// return true if alreay current, or else flase
pub fn process_catchup_chain(db: &Connection, catchup_chain: CatchupChain) -> Result<()> {
    if let Some(s) = catchup_chain.status {
        if s.as_str() == "current" {
            return Ok(());
        }
    }

    ensure!(
        !catchup_chain.stable_last_ball_joints.is_empty(),
        "stable_last_ball_joints is empty"
    );

    let witness_proof = witness_proof::process_witness_proof(
        db,
        &catchup_chain.unstable_mc_joints,
        &catchup_chain.witness_change_and_definition_joints,
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

    let mut last_ball = &assoc_last_ball_by_last_ball_unit[last_ball_unit];
    ensure!(
        first_stable_joint.ball.as_ref() == Some(last_ball),
        "last ball and last ball unit do not match"
    );

    let mut chain_balls = Vec::<String>::new();
    for joint in &catchup_chain.stable_last_ball_joints {
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

        chain_balls.push(joint.ball.as_ref().unwrap().clone());
    }
    // FIXME: use a dqueue to avoid reverse
    chain_balls.reverse();

    // FIXME: is this lock too earlier?
    let _g = CATCHUP_MUTEX.lock().unwrap();
    let mut stmt = db.prepare_cached("SELECT 1 FROM catchup_chain_balls LIMIT 1")?;
    ensure!(!stmt.exists(&[])?, "duplicate catchup_chain_balls");

    // adjust first chain ball if necessary and make sure it is the only stable unit in the entire chain
    || -> Result<()> {
        let mut stmt = db.prepare_cached(
            "SELECT is_stable, is_on_main_chain, main_chain_index \
             FROM balls JOIN units USING(unit) WHERE ball=?",
        )?;

        let mut rows = stmt.query_map(&[&chain_balls[0]], |row| {
            (
                row.get::<_, u32>(0),
                row.get::<_, u32>(1),
                row.get::<_, u32>(2),
            )
        })?;

        let (is_stable, is_on_main_chain, main_chain_index) = match rows.next() {
            None => {
                if spec::is_genesis_ball(&chain_balls[0]) {
                    return Ok(());
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

        let last_stable_mc_unit_props = storage::read_last_stable_mc_unit_props(db)?;
        ensure!(
            last_stable_mc_unit_props.is_some(),
            "can't read last stable mc unit props"
        );
        let last_stable_mc_unit_props = last_stable_mc_unit_props.unwrap();
        let last_stable_mci = last_stable_mc_unit_props.main_chain_index;
        if main_chain_index > last_stable_mci {
            bail!("first chain ball {} mci is too large", chain_balls[0]);
        }
        if last_stable_mci == main_chain_index {
            return Ok(());
        }

        // replace to avoid receiving duplicates
        chain_balls[0] = last_stable_mc_unit_props.ball.clone();
        if chain_balls.len() > 1 {
            return Ok(());
        }

        let mut stmt =
            db.prepare_cached("SELECT is_stable FROM balls JOIN units USING(unit) WHERE ball=?")?;
        let mut rows = stmt.query_map(&[&chain_balls[1]], |row| row.get::<_, u32>(0))?;
        let second_ball_is_stable = match rows.next() {
            None => return Ok(()),
            Some(row) => row?,
        };

        ensure!(
            second_ball_is_stable != 1,
            "second chain ball {} must not be stable",
            chain_balls[1]
        );
        Ok(())
    }()?;

    // validation complete, now write the chain for future downloading of hash trees
    let ball_str = chain_balls
        .iter()
        .map(|s| format!("('{}')", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO catchup_chain_balls (ball) VALUES {}", ball_str);
    db.execute(&sql, &[])?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
pub struct HashTreeReq {
    pub from_ball: String,
    pub to_ball: String,
}

#[derive(Serialize, Deserialize)]
pub struct BallProps {
    pub unit: String,
    ball: Option<String>, // this should not be an option
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(default)]
    is_nonserial: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    parent_balls: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    skiplist_balls: Vec<String>,
}

pub fn read_hash_tree(db: &Connection, hash_tree_req: HashTreeReq) -> Result<Vec<BallProps>> {
    let HashTreeReq { from_ball, to_ball } = hash_tree_req;
    let mut from_mci = 0;
    let mut to_mci = 0;

    // --> BEGIN: query props from db
    struct Props {
        is_stable: u32,
        is_on_main_chain: u32,
        main_chain_index: u32,
        ball: String,
    }

    let mut stmt = db.prepare_cached(
        "SELECT is_stable, is_on_main_chain, main_chain_index, ball \
         FROM balls JOIN units USING(unit) WHERE ball IN(?,?)",
    )?;
    let rows = stmt.query_map(&[&from_ball, &to_ball], |row| Props {
        is_stable: row.get(0),
        is_on_main_chain: row.get(1),
        main_chain_index: row.get(2),
        ball: row.get(3),
    })?;

    let mut props = Vec::new();
    for row in rows {
        props.push(row?);
    }
    // --> END query props from db

    ensure!(props.len() == 2, "some balls not found");
    for prop in props {
        ensure!(prop.is_stable == 1, "some balls not stable");
        ensure!(prop.is_on_main_chain == 1, "some balls not on mc");
        if prop.ball == from_ball {
            from_mci = prop.main_chain_index;
        }
        if prop.ball == to_ball {
            to_mci = prop.main_chain_index;
        }
    }

    ensure!(from_mci < to_mci, "from is after to");
    let mut balls = Vec::new();
    let op = if from_mci == 0 { ">=" } else { ">" };
    let sql = format!(
        "SELECT unit, ball, content_hash FROM units LEFT JOIN balls USING(unit) \
         WHERE main_chain_index {} ? AND main_chain_index<=? ORDER BY `level`",
        op
    );
    let mut stmt = db.prepare_cached(&sql)?;
    let rows = stmt.query_map(&[&from_mci, &to_mci], |row| BallProps {
        unit: row.get(0),
        ball: row.get(1),
        content_hash: row.get(2),
        is_nonserial: false,
        parent_balls: Vec::new(),
        skiplist_balls: Vec::new(),
    })?;
    for row in rows {
        let mut ball_prop = row?;
        if ball_prop.ball.is_none() {
            bail!("no ball for unit {}", ball_prop.unit);
        }

        if ball_prop.content_hash.is_some() {
            ball_prop.content_hash = None;
            ball_prop.is_nonserial = true;
        }

        let mut stmt = db.prepare_cached(
            "SELECT ball FROM parenthoods LEFT JOIN balls \
             ON parent_unit=balls.unit WHERE child_unit=? ORDER BY ball",
        )?;
        let rows = stmt.query_map(&[&ball_prop.unit], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            let parent_ball = row?;
            ensure!(parent_ball.is_some(), "some parents have no balls");
            ball_prop.parent_balls.push(parent_ball.unwrap());
        }

        let mut stmt = db.prepare_cached(
            "SELECT ball FROM skiplist_units LEFT JOIN balls \
             ON skiplist_unit=balls.unit WHERE skiplist_units.unit=? ORDER BY ball",
        )?;
        let srows = stmt.query_map(&[&ball_prop.unit], |row| row.get::<_, Option<String>>(0))?;
        for srow in srows {
            let skiplist_ball = srow?;
            ensure!(skiplist_ball.is_some(), "some skiplist units have no balls");
            ball_prop.skiplist_balls.push(skiplist_ball.unwrap());
        }

        balls.push(ball_prop);
    }

    Ok(balls)
}

// this function take a new db connection
pub fn process_hash_tree(db: &mut Connection, balls: Vec<BallProps>) -> Result<()> {
    use object_hash;

    if balls.is_empty() {
        return Ok(());
    }

    let _g = HASHTREE_MUTEX.lock().unwrap();
    let tx = db.transaction()?;

    let mut max_mci = 0;
    let last_ball = balls.last().as_ref().unwrap().ball.clone().unwrap();
    for ball_prop in balls {
        ensure!(ball_prop.ball.is_some(), "no ball");
        if !::spec::is_genesis_unit(&ball_prop.unit) {
            if ball_prop.parent_balls.is_empty() {
                bail!("no parents");
            }
        } else if !ball_prop.parent_balls.is_empty() {
            bail!("genesis with parents?");
        }

        let ball = ball_prop.ball.as_ref().unwrap();
        if ball != &object_hash::get_ball_hash(
            &ball_prop.unit,
            &ball_prop.parent_balls,
            &ball_prop.skiplist_balls,
            ball_prop.is_nonserial,
        ) {
            bail!("wrong ball hash, ball {}, unit {}", ball_prop.unit, ball);
        }

        let parent_balls_set = ball_prop
            .parent_balls
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let skiplist_balls_set = ball_prop
            .skiplist_balls
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let add_ball = || -> Result<()> {
            let mut stmt = tx
                .prepare_cached("INSERT OR IGNORE INTO hash_tree_balls (ball, unit) VALUES(?,?)")?;
            stmt.execute(&[ball, &ball_prop.unit])?;
            Ok(())
        };

        let check_skiplist_ball_exist = || -> Result<()> {
            if ball_prop.skiplist_balls.is_empty() {
                return add_ball();
            }

            // FIXME: only count rows, could use select 1
            let sql = format!(
                "SELECT ball FROM hash_tree_balls \
                 WHERE ball IN({}) UNION SELECT ball FROM balls WHERE ball IN({})",
                skiplist_balls_set, skiplist_balls_set
            );
            let mut stmt = tx.prepare(&sql)?;
            let rows = stmt.query_map(&[], |row| row.get::<_, String>(0))?;
            let mut tmp = Vec::new();
            for row in rows {
                tmp.push(row?);
            }
            if tmp.len() != ball_prop.skiplist_balls.len() {
                bail!("some skiplist balls not found")
            }
            add_ball()
        };

        if !ball_prop.parent_balls.is_empty() {
            check_skiplist_ball_exist()?;
            continue;
        }

        let sql = format!(
            "SELECT ball FROM hash_tree_balls WHERE ball IN({})",
            parent_balls_set,
        );
        let mut stmt = tx.prepare(&sql)?;
        let rows = stmt.query_map(&[], |row| row.get::<_, String>(0))?;
        let mut found_balls = Vec::new();
        for row in rows {
            found_balls.push(row?);
        }
        if found_balls.len() == ball_prop.parent_balls.len() {
            check_skiplist_ball_exist()?;
            continue;
        }

        let missing_balls = ball_prop
            .parent_balls
            .iter()
            .filter(|v| !found_balls.contains(&v))
            .collect::<Vec<_>>();
        let missing_balls_set = missing_balls
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT ball, main_chain_index, is_on_main_chain \
             FROM balls JOIN units USING(unit) WHERE ball IN({})",
            missing_balls_set,
        );
        let mut stmt = tx.prepare(&sql)?;
        let rows2 = stmt.query_map(&[], |row| {
            (
                row.get::<_, String>(0), // ball
                row.get::<_, u32>(1),    // mci
                row.get::<_, u32>(2),    // is_on_mc
            )
        })?;
        let mut missing_ball_props = Vec::new();
        for row in rows2 {
            missing_ball_props.push(row?);
        }

        if missing_ball_props.len() != missing_balls.len() {
            bail!("some parents not found, unit {}", ball_prop.unit);
        }

        for props in missing_ball_props {
            if props.2 == 1 && props.1 > max_mci {
                max_mci = props.1;
            }
        }

        check_skiplist_ball_exist()?;
    }
    {
        let mut stmt = tx.prepare_cached(
            "SELECT ball, main_chain_index \
             FROM catchup_chain_balls LEFT JOIN balls USING(ball) LEFT JOIN units USING(unit) \
             ORDER BY member_index LIMIT 2",
        )?;

        let rows_data: Vec<String> = stmt
            .query_map(&[], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<_>, _>>()?;

        if rows_data.len() != 2 {
            bail!("expecting to have 2 elements in the chain");
        }
        if rows_data[1] != last_ball {
            bail!("tree root doesn't match second chain element");
        }

        let mut stmt = tx.prepare_cached("DELETE FROM catchup_chain_balls WHERE ball=?")?;
        stmt.execute(&[&rows_data[0]])?;
        purge_handled_balls_from_hash_tree(&tx)?;
    }

    tx.commit()?;

    Ok(())
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
