use config;
use db;
use error::Result;
use failure::ResultExt;
use graph;
use joint::Joint;
use rusqlite::Connection;
use serde_json::{self, Value};
use std::collections::HashSet;
use storage;
use witness_proof;

const MAX_HISTORY_ITEMS: usize = 1000;

#[derive(Deserialize)]
pub struct HistoryRequest {
    pub witnesses: Vec<String>,
    pub addresses: Vec<String>,
    pub known_stable_units: Vec<String>,
    pub requested_joints: Vec<String>,
}

#[derive(Serialize)]
pub struct Response {
    unstable_mc_joints: Vec<Joint>,
    witness_change_and_definition: Vec<Joint>,
    joints: Vec<Joint>,
    proofchain_balls: Vec<ProofBalls>,
}

#[derive(Serialize)]
struct ProofBalls {
    ball: Option<String>,
    unit: Option<String>,
    parent_balls: Vec<Option<String>>,
    content_hash: Option<String>,
    is_nonserial: Option<bool>,
    skiplist_balls: Vec<Option<String>>,
}

pub fn prepare_history(db: &Connection, history_request: &HistoryRequest) -> Result<Response> {
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
        .map(|s| s)
        .collect::<HashSet<_>>();
    let mut selects = Vec::new();

    let addresses_and_shared_address =
        add_shared_addresses_of_wallet(db, &history_request.addresses)?;
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
        .filter(|s| !assoc_know_stable_units.contains(&s.unit))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(Response {
            unstable_mc_joints: Vec::new(),
            witness_change_and_definition: Vec::new(),
            joints: Vec::new(),
            proofchain_balls: Vec::new(),
        });
    }
    if rows.len() > MAX_HISTORY_ITEMS {
        bail!("your history is too large, consider switching to a full client");
    }

    let prepare_witness_proof =
        witness_proof::prepare_witness_proof(db, &history_request.witnesses, 0)?;

    let mut joints = Vec::new();
    let mut proofchain_balls = Vec::new();
    let mut later_mci = prepare_witness_proof.last_ball_mci + 1;

    for row in rows {
        match storage::read_joint(db, &row.unit) {
            Ok(joint) => {
                joints.push(joint);
                if row.main_chain_index > Some(prepare_witness_proof.last_ball_mci)
                    || row.main_chain_index.is_none()
                {
                    continue;
                }
                build_proof_chain(
                    db,
                    Some(later_mci),
                    row.main_chain_index,
                    &row.unit,
                    &mut proofchain_balls,
                )?;
                later_mci = row.main_chain_index.unwrap();
            }

            Err(_) => bail!("prepareJointsWithProofs unit not found {}", row.unit),
        }
    }

    Ok(Response {
        unstable_mc_joints: prepare_witness_proof.unstable_mc_joints,
        witness_change_and_definition: prepare_witness_proof.witness_change_and_definition,
        joints,
        proofchain_balls,
    })
}

fn add_shared_addresses_of_wallet(db: &Connection, addresses: &Vec<String>) -> Result<Vec<String>> {
    let mut address_list = addresses.clone();
    loop {
        if address_list.is_empty() {
            return Ok(Vec::new());
        }
        let str_addresses = address_list
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let mut stmt = db.prepare_cached(
            "SELECT DISTINCT shared_address FROM shared_address_signing_paths WHERE address IN(?)",
        )?;
        let rows = stmt
            .query_map(&[&str_addresses], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;

        if rows.is_empty() {
            return Ok(address_list.to_vec());
        }
        let mut diff = Vec::new();
        for row in rows.into_iter() {
            if !address_list.iter().any(|address| address == &row) {
                diff.push(row);
            }
        }
        if diff.is_empty() {
            return Ok(address_list.to_vec());
        }
        address_list.append(&mut diff);
    }
}

pub fn prepare_link_proofs(units: &Vec<String>) -> Result<Vec<Joint>> {
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
    Ok(chains)
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
        // build_proof_chain(
        //     db,
        //     Some(later_lb_mci.unwrap() + 1),
        //     earlier_mci,
        //     earlier_joint_unit,
        //     chains,
        // )?;
    } else {
        if !graph::determine_if_included(&db, &earlier_joint_unit, &[later_unit.to_string()])? {
            bail!("not included");
        }

        build_path(db, later_joint.clone(), earlier_joint.clone(), chains)?;
    }

    Ok(())
}

fn build_proof_chain(
    db: &Connection,
    later_mci: Option<u32>,
    earlier_mci: Option<u32>,
    unit: &String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    if later_mci.is_none() {
        bail!("earlier_mci=null, unit={}", unit);
    }
    if later_mci == earlier_mci {
        return build_last_mile_of_proof_chain(db, earlier_mci, unit, balls);
    }
    build_proof_chain_on_mc(later_mci, earlier_mci, balls)?;
    build_last_mile_of_proof_chain(db, earlier_mci, unit, balls)
}

fn build_last_mile_of_proof_chain(
    db: &Connection,
    earlier_mci: Option<u32>,
    unit: &String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    let mut stmt =
        db.prepare("SELECT unit FROM units WHERE main_chain_index=? AND is_on_main_chain=1")?;
    let rows = stmt
        .query_map(&[&earlier_mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if rows.len() != 1 {
        bail!("no mc unit?");
    }
    if &rows[0] == unit {
        return Ok(());
    }
    find_parent_and_add_ball(db, earlier_mci, rows[0].clone(), balls)
}

fn find_parent_and_add_ball(
    db: &Connection,
    mci: Option<u32>,
    interim_unit: String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    let mut add_ball = |cur_unit: String| -> Result<()> {
        let mut stmt =
            db.prepare("SELECT unit, ball FROM units JOIN balls USING(unit) WHERE unit=?")?;
        struct TempUnit {
            unit: Option<String>,
            ball: Option<String>,
        }
        let rows = stmt
            .query_map(&[&cur_unit], |row| TempUnit {
                unit: row.get(0),
                ball: row.get(1),
            })?
            .collect::<::std::result::Result<Vec<_>, _>>()?;
        if rows.len() != 1 {
            bail!("no unit?");
        }

        let mut stmt =
            db.prepare("SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit WHERE child_unit=? ORDER BY ball")?;
        let parent_rows = stmt
            .query_map(&[&rows[0].unit], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<Option<String>>, _>>()?;
        if parent_rows.iter().any(|row| row.is_none()) {
            bail!("some parents have no balls");
        }

        balls.push(ProofBalls {
            ball: rows[0].ball.clone(),
            unit: rows[0].unit.clone(),
            parent_balls: parent_rows,
            content_hash: None,
            is_nonserial: None,
            skiplist_balls: Vec::new(),
        });
        Ok(())
    };

    let mut interim_unit = interim_unit;
    loop {
        let mut stmt =
        db.prepare("SELECT parent_unit FROM parenthoods JOIN units ON parent_unit=unit WHERE child_unit=? AND main_chain_index=?")?;
        let parents = stmt
            .query_map(&[&interim_unit, &mci], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;
        if parents.contains(&interim_unit) {
            add_ball(interim_unit)?; // push last parent to vector balls
            return Ok(());
        }
        for parent_unit in parents {
            let parent = parent_unit.clone();
            if graph::determine_if_included(db, &interim_unit, &[parent_unit])? {
                add_ball(interim_unit)?; //push curent parent to balls and continue loop
                interim_unit = parent.clone();
                break;
            }
        }
    }
}

fn build_proof_chain_on_mc(
    _later_mci: Option<u32>,
    _earlier_mci: Option<u32>,
    _balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    unimplemented!()
}

//TODO:
fn build_path(
    db: &Connection,
    later_joint: Joint,
    earlier_joint: Joint,
    chains: &mut Vec<Joint>,
) -> Result<()> {
    fn add_joint(db: &Connection, unit: &String, chains: &mut Vec<Joint>) -> Result<Joint> {
        let later_joint = storage::read_joint(&db, &unit)
            .or_else(|e| bail!("nonserial unit not found?, err={}", e))?;
        chains.push(later_joint.clone());
        Ok(later_joint)
    };

    fn go_up(
        db: &Connection,
        later_joint: Joint,
        earlier_joint: Joint,
        chains: &mut Vec<Joint>,
    ) -> Result<()> {
        let mut loop_joint = later_joint.clone();
        struct Tmp {
            main_chain_index: Option<u32>,
            unit: Option<String>,
        }
        loop {
            let sql = format!(
                "SELECT parent.unit, parent.main_chain_index \
                 FROM units AS child JOIN units AS parent ON child.best_parent_unit=parent.unit \
                 WHERE child.unit='{}'",
                loop_joint.unit.unit.as_ref().map_or_else(|| "", |v| v)
            );
            let mut stmt = db.prepare_cached(&sql)?;
            let rows = stmt
                .query_map(&[], |v| Tmp {
                    main_chain_index: v.get(1),
                    unit: v.get(0),
                })?
                .collect::<::std::result::Result<Vec<_>, _>>()?;
            if rows[0].main_chain_index < earlier_joint.unit.main_chain_index {
                return build_path_to_earlier_unit(db, &loop_joint, &earlier_joint, chains);
            }

            let tmp_joint = add_joint(
                db,
                &rows[0].unit.as_ref().map_or_else(|| "", |v| v).to_string(),
                chains,
            )?;
            if tmp_joint.unit.main_chain_index == earlier_joint.unit.main_chain_index {
                build_path_to_earlier_unit(db, &tmp_joint, &earlier_joint, chains)?;
                return Ok(());
            } else {
                loop_joint = tmp_joint;
            }
        }
    };

    fn build_path_to_earlier_unit(
        db: &Connection,
        joint: &Joint,
        earlier_joint: &Joint,
        chains: &mut Vec<Joint>,
    ) -> Result<()> {
        let mut tmp_joint = joint.clone();
        let mut parent_units: Vec<Vec<String>> = Vec::new();
        loop {
            let sql = format!(
                "SELECT unit FROM parenthoods JOIN units ON parent_unit=unit \
                 WHERE child_unit='{}' AND main_chain_index=?",
                tmp_joint.unit.unit.as_ref().map_or_else(|| "", |v| v)
            );

            let mut stmt = db.prepare_cached(&sql)?;
            let loop_parent_units = stmt
                .query_map(&[&tmp_joint.unit.main_chain_index], |v| v.get(0))?
                .collect::<::std::result::Result<Vec<String>, _>>()?;
            if loop_parent_units.is_empty() {
                bail!("no parents with same mci?");
            }

            if loop_parent_units.contains(&earlier_joint.unit.unit.as_ref().expect("unit is none"))
            {
                return Ok(());
            }
            if loop_parent_units.len() == 1 {
                tmp_joint = add_joint(db, &loop_parent_units[0], chains)?;
                continue;
            } else {
                parent_units.push(loop_parent_units.clone());
            }

            //for loop_parent_units in &mut parent_units {
            while parent_units.last().unwrap().is_empty() {
                parent_units.pop();
            }
            if let Some(loop_parent_units) = parent_units.last_mut() {
                while let Some(parent_unit) = loop_parent_units.pop() {
                    if graph::determine_if_included(
                        db,
                        earlier_joint.unit.unit.as_ref().expect("unit is none"),
                        &[parent_unit.to_string()],
                    )? {
                        return Ok(());
                    }
                    if parent_unit.is_empty() {
                        bail!("none of the parents includes earlier unit")
                    }
                    tmp_joint = add_joint(db, &parent_unit, chains)?;
                }
            }
        }
    };

    if later_joint.unit.unit == earlier_joint.unit.unit {
        return Ok(());
    }
    if later_joint.unit.main_chain_index == earlier_joint.unit.main_chain_index {
        return build_path_to_earlier_unit(db, &later_joint, &earlier_joint, chains);
    } else {
        return go_up(db, later_joint, earlier_joint, chains);
    }
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
