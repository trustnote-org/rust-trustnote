use config;
use db;
use error::Result;
use failure::ResultExt;
use graph;
use joint::Joint;
use may::sync::Mutex;
use object_hash;
use parent_composer;
use rusqlite::Connection;
use std::collections::HashMap;
use std::collections::HashSet;
use storage;
use validation::ValidationState;
use witness_proof;

const MAX_HISTORY_ITEMS: usize = 1000;

lazy_static! {
    static ref LIGHT_JOINTS: Mutex<()> = Mutex::new(());
}

#[derive(Serialize, Deserialize)]
pub struct HistoryRequest {
    pub witnesses: Vec<String>,
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub known_stable_units: Vec<String>,
    #[serde(default)]
    pub requested_joints: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct HistoryResponse {
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unstable_mc_joints: Vec<Joint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    witness_change_and_definition_joints: Vec<Joint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    joints: Vec<Joint>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    proofchain_balls: Vec<ProofBalls>,
}

#[derive(Serialize, Deserialize)]
struct ProofBalls {
    ball: String,
    unit: String,
    parent_balls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_nonserial: Option<bool>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    skiplist_balls: Vec<String>,
}

pub fn prepare_history(
    db: &Connection,
    history_request: &HistoryRequest,
) -> Result<HistoryResponse> {
    if history_request.addresses.is_empty() && history_request.requested_joints.is_empty() {
        bail!("neither addresses nor joints requested");
    }

    if history_request.witnesses.len() != config::COUNT_WITNESSES {
        bail!("wrong number of witnesses");
    }

    let known_stable_units = history_request
        .known_stable_units
        .iter()
        .map(|s| s)
        .collect::<HashSet<_>>();
    let mut selects = Vec::new();

    let addresses_and_shared_address =
        add_shared_addresses_of_wallet(db, &history_request.addresses)?;
    if !addresses_and_shared_address.is_empty() {
        let address_list = addresses_and_shared_address
            .into_iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT DISTINCT unit, main_chain_index, level FROM outputs JOIN units USING(unit) \
             WHERE address IN({}) AND (+sequence='good' OR is_stable=1) \
             UNION \
             SELECT DISTINCT unit, main_chain_index, level FROM unit_authors JOIN units USING(unit) \
             WHERE address IN({}) AND (+sequence='good' OR is_stable=1)",
            address_list, address_list);
        selects.push(sql);
    }
    if !history_request.requested_joints.is_empty() {
        let unit_list = history_request
            .requested_joints
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT unit, main_chain_index, level FROM units \
             WHERE unit IN({}) AND (+sequence='good' OR is_stable=1)",
            unit_list
        );
        selects.push(sql);
    }
    let sql = format!(
        "{} ORDER BY main_chain_index DESC, level DESC",
        selects.join(" UNION ")
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
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;
    let rows = tmp_rows
        .into_iter()
        .filter(|s| !known_stable_units.contains(&s.unit))
        .collect::<Vec<_>>();
    if rows.is_empty() {
        return Ok(HistoryResponse {
            unstable_mc_joints: Vec::new(),
            witness_change_and_definition_joints: Vec::new(),
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
                if row.main_chain_index.is_none()
                    || row.main_chain_index > Some(prepare_witness_proof.last_ball_mci)
                {
                    continue;
                }
                build_proof_chain(
                    db,
                    later_mci,
                    row.main_chain_index.unwrap(),
                    row.unit,
                    &mut proofchain_balls,
                )?;
                later_mci = row.main_chain_index.unwrap();
            }

            Err(_) => bail!("prepareJointsWithProofs unit not found {}", row.unit),
        }
    }

    Ok(HistoryResponse {
        unstable_mc_joints: prepare_witness_proof.unstable_mc_joints,
        witness_change_and_definition_joints: prepare_witness_proof.witness_change_and_definition,
        joints,
        proofchain_balls,
    })
}

pub fn process_history(db: &Connection, history: &mut HistoryResponse) -> Result<()> {
    if history.joints.is_empty() {
        info!("no joints need to be added or updated");
        return Ok(());
    }
    if history.unstable_mc_joints.is_empty() {
        bail!("no unstable_mc_joints");
    }

    let witness_proof = witness_proof::process_witness_proof(
        db,
        &history.unstable_mc_joints,
        &history.witness_change_and_definition_joints,
        false,
    ).context("process_history process_witness_proof failed")?;

    let mut known_balls = witness_proof
        .assoc_last_ball_by_last_ball_unit
        .values()
        .cloned()
        .collect::<HashSet<_>>();

    let mut proven_units_non_serial = HashMap::new();

    for ball in &history.proofchain_balls {
        let obj_ball = ball;
        if obj_ball.ball != object_hash::get_ball_hash(
            &obj_ball.unit,
            &obj_ball.parent_balls,
            &obj_ball.skiplist_balls,
            obj_ball.is_nonserial.unwrap_or(false),
        ) {
            bail!("wrong ball hash");
        }
        if !known_balls.contains(&obj_ball.ball) {
            bail!("ball not known");
        }
        for parent_ball in &obj_ball.parent_balls {
            known_balls.insert(parent_ball.to_owned());
        }
        for skiplist_ball in &obj_ball.skiplist_balls {
            known_balls.insert(skiplist_ball.to_owned());
        }

        proven_units_non_serial.insert(
            obj_ball.unit.clone(),
            obj_ball.is_nonserial.unwrap_or(false),
        );
    }
    let joints = &history.joints;
    for joint in joints {
        let unit = &joint.unit;
        if joint.get_unit_hash() != &unit.get_unit_hash() {
            bail!("invalid hash");
        }
        if unit.timestamp.is_none() {
            bail!("no timestamp");
        }
    }
    let _g = LIGHT_JOINTS.lock().unwrap();

    let units_list = joints
        .iter()
        .map(|s| format!("'{}'", s.get_unit_hash()))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT unit, is_stable FROM units WHERE unit IN({})",
        units_list
    );
    let mut stmt = db.prepare_cached(&sql)?;
    let existing_units = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    let mut proven_units = Vec::new();
    let joints_reverse = joints.iter().rev();
    for joint_r in joints_reverse {
        let unit = joint_r.get_unit_hash();
        let sequence = if *proven_units_non_serial.get(unit).unwrap_or(&false) {
            String::from("final-bad")
        } else {
            String::from("good")
        };
        if proven_units_non_serial.contains_key(unit) {
            proven_units.push(unit);
        }
        if existing_units.contains(unit) {
            let mut stmt =
                db.prepare_cached("UPDATE units SET main_chain_index=?, sequence=? WHERE unit=?")?;
            stmt.execute(&[&joint_r.unit.main_chain_index, &sequence, unit])?;
        } else {
            let mut validate_state = ValidationState::new();
            validate_state.sequence = sequence;
            joint_r
                .save(validate_state, true)
                .context("save_joint failed")?;
        }
    }

    fix_is_spent_flag_and_input_address(db)
        .context("fix_is_spent_flag_and_input_address failed")?;
    if proven_units.is_empty() {
        return Ok(());
    }

    let proven_units_list = proven_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "UPDATE units SET is_stable=1, is_free=0 WHERE unit IN({})",
        proven_units_list
    );
    let mut stmt = db.prepare_cached(&sql)?;
    stmt.execute(&[])?;

    Ok(())
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
    later_mci: u32,
    earlier_mci: u32,
    unit: String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    if later_mci > earlier_mci {
        build_proof_chain_on_mc(db, later_mci, earlier_mci, balls)?;
    }

    build_last_mile_of_proof_chain(db, earlier_mci, unit, balls)
}

fn build_last_mile_of_proof_chain(
    db: &Connection,
    earlier_mci: u32,
    unit: String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    let mut stmt = db
        .prepare_cached("SELECT unit FROM units WHERE main_chain_index=? AND is_on_main_chain=1")?;
    let rows = stmt
        .query_map(&[&earlier_mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if rows.len() != 1 {
        bail!("no mc unit?");
    }
    let cur_unit = rows.into_iter().nth(0).unwrap();
    if unit == cur_unit {
        return Ok(());
    }
    find_parent_and_add_ball(db, earlier_mci, cur_unit, unit, balls)
}

fn find_parent_and_add_ball(
    db: &Connection,
    mci: u32,
    mut interim_unit: String,
    dest_unit: String,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    let mut add_ball = |cur_unit: String| -> Result<()> {
        struct TempUnit {
            unit: String,
            ball: String,
        }

        let mut stmt = db.prepare_cached(
            "SELECT unit, ball FROM units JOIN balls USING(unit) \
             WHERE unit=?",
        )?;
        let rows = stmt
            .query_map(&[&cur_unit], |row| TempUnit {
                unit: row.get(0),
                ball: row.get(1),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        if rows.len() != 1 {
            bail!("no unit?");
        }
        let cur_unit = rows.into_iter().nth(0).unwrap();

        let mut stmt = db.prepare_cached(
            "SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit \
             WHERE child_unit=? ORDER BY ball",
        )?;

        let mut parent_balls = Vec::new();
        let parent_rows =
            stmt.query_map(&[&cur_unit.unit], |row| row.get::<_, Option<String>>(0))?;

        for row in parent_rows {
            if let Some(ball) = row? {
                parent_balls.push(ball);
            } else {
                bail!("some parents have no balls");
            }
        }

        balls.push(ProofBalls {
            ball: cur_unit.ball,
            unit: cur_unit.unit,
            parent_balls,
            content_hash: None,
            is_nonserial: None,
            skiplist_balls: Vec::new(),
        });
        Ok(())
    };

    loop {
        let mut stmt = db.prepare_cached(
            "SELECT parent_unit FROM parenthoods JOIN units ON parent_unit=unit \
             WHERE child_unit=? AND main_chain_index=?",
        )?;
        let parents = stmt
            .query_map(&[&interim_unit, &mci], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;
        if parents.contains(&dest_unit) {
            add_ball(dest_unit)?;
            return Ok(());
        }

        let mut is_found = false;
        for parent_unit in parents {
            if graph::determine_if_included(db, &dest_unit, &[parent_unit.clone()])? {
                add_ball(interim_unit)?; //push curent parent to balls and continue loop
                interim_unit = parent_unit;
                is_found = true;
                break;
            }
        }

        if !is_found {
            bail!("no parent that includes target unit");
        }
    }
}

// FIXME: this is only used by private payment which is not used currently
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
                })?.collect::<::std::result::Result<Vec<_>, _>>()?;
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

#[derive(Serialize, Deserialize, Clone)]
pub struct LastStableBallAndParentUnitsAndWitnessListUnit {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_stable_mc_ball: Option<String>,
    pub last_stable_mc_ball_mci: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_stable_mc_ball_unit: Option<String>,
    pub parent_units: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub witness_list_unit: Option<String>,
}

pub fn prepare_parents_and_last_ball_and_witness_list_unit(
    witnesses: &Vec<String>,
) -> Result<LastStableBallAndParentUnitsAndWitnessListUnit> {
    let db = db::DB_POOL.get_connection();
    if witnesses.len() != config::COUNT_WITNESSES {
        bail!("wrong number of witnesses");
    }

    if storage::determine_if_witness_and_address_definition_have_refs(&db, witnesses)? {
        bail!("some witnesses have references in their addresses");
    }

    let parent_composer::LastStableBallAndParentUnits {
        parent_units,
        last_stable_mc_ball,
        last_stable_mc_ball_mci,
        last_stable_mc_ball_unit,
    } = parent_composer::pick_parent_units_and_last_ball(&db, witnesses)
        .context("unable to find parents")?;

    let witness_list_unit =
        storage::find_witness_list_unit(&db, witnesses, last_stable_mc_ball_mci)?;

    Ok(LastStableBallAndParentUnitsAndWitnessListUnit {
        last_stable_mc_ball: last_stable_mc_ball,
        last_stable_mc_ball_mci,
        last_stable_mc_ball_unit: last_stable_mc_ball_unit,
        parent_units,
        witness_list_unit,
    })
}

fn build_proof_chain_on_mc(
    db: &Connection,
    later_mci: u32,
    earlier_mci: u32,
    balls: &mut Vec<ProofBalls>,
) -> Result<()> {
    if earlier_mci > later_mci {
        bail!("earlier > later")
    }

    if earlier_mci == later_mci {
        return Ok(());
    }

    let mut tmp_mci = later_mci - 1;

    loop {
        let mut stmt = db.prepare_cached(
            "SELECT unit, ball, content_hash FROM units JOIN balls USING(unit) \
             WHERE main_chain_index=? AND is_on_main_chain=1",
        )?;
        let tmp_balls = stmt
            .query_map(&[&tmp_mci], |v| ProofBalls {
                unit: v.get(0),
                ball: v.get(1),
                content_hash: v.get(2),
                is_nonserial: None,
                parent_balls: Vec::new(),
                skiplist_balls: Vec::new(),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;
        if tmp_balls.len() != 1 {
            bail!(
                "no prev chain element? mci={}, later_mci={}, earlier_mci={}",
                tmp_mci,
                later_mci,
                earlier_mci
            );
        }

        let mut ball = tmp_balls.into_iter().nth(0).unwrap();
        if ball.content_hash.is_some() {
            ball.is_nonserial = Some(true);
            ball.content_hash = None;
        }

        let mut stmt = db.prepare_cached(
            "SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=balls.unit \
             WHERE child_unit=? ORDER BY ball",
        )?;

        let parent_rows = stmt
            .query_map(&[&ball.unit], |v| v.get(0))?
            .collect::<::std::result::Result<Vec<Option<String>>, _>>()?;
        if parent_rows.iter().any(|row| row.is_none()) {
            bail!("some parents have no balls");
        }

        for parent_row in parent_rows {
            ball.parent_balls.push(parent_row.unwrap());
        }

        struct TmpScrow {
            ball: Option<String>,
            main_chain_index: Option<u32>,
        }

        let mut stmt = db.prepare_cached(
            "SELECT ball, main_chain_index \
             FROM skiplist_units JOIN units ON skiplist_unit=units.unit LEFT JOIN balls \
             ON units.unit=balls.unit WHERE skiplist_units.unit=? ORDER BY ball",
        )?;

        let srows = stmt
            .query_map(&[&ball.unit], |v| TmpScrow {
                ball: v.get(0),
                main_chain_index: v.get(1),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        if srows.iter().any(|s| s.ball.is_none()) {
            bail!("some skiplist units have no balls");
        }

        for scrow in &srows {
            ball.skiplist_balls
                .push(scrow.ball.as_ref().unwrap().to_owned());
        }
        balls.push(ball);

        if tmp_mci == earlier_mci || tmp_mci == 0 {
            return Ok(());
        }

        tmp_mci -= 1;
        for skip in srows.into_iter().rev() {
            if let Some(next_skiplist_mci) = skip.main_chain_index {
                if next_skiplist_mci < tmp_mci && next_skiplist_mci >= earlier_mci {
                    tmp_mci = next_skiplist_mci;
                    break;
                }
            }
        }
    }
}

fn fix_is_spent_flag_and_input_address(db: &Connection) -> Result<()> {
    fix_is_spent_flag(db).context("")?;
    fix_input_address(db).context("")?;

    Ok(())
}

struct TempOutput {
    unit: String,
    message_index: u32,
    output_index: u32,
}
fn fix_is_spent_flag(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT outputs.unit, outputs.message_index, outputs.output_index \
		FROM outputs \
		JOIN inputs ON outputs.unit=inputs.src_unit AND outputs.message_index=inputs.src_message_index \
        AND outputs.output_index=inputs.src_output_index \
		WHERE is_spent=0 AND type='transfer'",
    )?;

    let rows = stmt
        .query_map(&[], |row| TempOutput {
            unit: row.get(0),
            message_index: row.get(1),
            output_index: row.get(2),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    if rows.is_empty() {
        return Ok(());
    }
    for row in rows {
        let mut stmt = db.prepare_cached(
            "UPDATE outputs SET is_spent=1 WHERE unit=? AND message_index=? AND output_index=?",
        )?;
        stmt.execute(&[&row.unit, &row.message_index, &row.output_index])?;
    }

    Ok(())
}

struct TempOutput2 {
    unit: String,
    message_index: u32,
    output_index: u32,
    addr: String,
}
fn fix_input_address(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT outputs.unit, outputs.message_index, outputs.output_index, outputs.address \
         FROM outputs \
         JOIN inputs ON outputs.unit=inputs.src_unit AND \
         outputs.message_index=inputs.src_message_index \
         AND outputs.output_index=inputs.src_output_index \
         WHERE inputs.address IS NULL AND type='transfer'",
    )?;

    let rows = stmt
        .query_map(&[], |row| TempOutput2 {
            unit: row.get(0),
            message_index: row.get(1),
            output_index: row.get(2),
            addr: row.get(3),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    if rows.is_empty() {
        return Ok(());
    }
    for row in rows {
        let mut stmt = db.prepare_cached(
            "UPDATE inputs SET address=? WHERE src_unit=? AND src_message_index=? AND src_output_index=?",
        )?;
        stmt.execute(&[&row.addr, &row.unit, &row.message_index, &row.output_index])?;
    }

    Ok(())
}
