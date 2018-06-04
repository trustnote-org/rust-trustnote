use error::Result;
use joint::Joint;
use rusqlite::Connection;
use serde_json;
use storage;
// use spec::Unit;

#[derive(Debug)]
pub enum CheckNewResult {
    Known,
    KnownUnverified,
    KnownBad,
    New,
}

pub fn check_new_unit(db: &Connection, unit: &String) -> Result<CheckNewResult> {
    if storage::is_known_unit(unit) {
        return Ok(CheckNewResult::Known);
    }

    let mut stmt = db.prepare_cached("SELECT 1 FROM units WHERE unit=?")?;
    if stmt.exists(&[unit])? {
        storage::set_unit_is_known(unit);
        return Ok(CheckNewResult::Known);
    }

    let mut stmt = db.prepare_cached("SELECT 1 FROM unhandled_joints WHERE unit=?")?;
    if stmt.exists(&[unit])? {
        return Ok(CheckNewResult::KnownUnverified);
    }

    let mut stmt = db.prepare_cached("SELECT error FROM known_bad_joints WHERE unit=?")?;
    let mut rows = stmt.query(&[unit])?;
    if let Some(row) = rows.next() {
        let error: String = row?.get_checked(0)?;
        warn!("detect knownbad unit {}, err: {}", unit, error);
        return Ok(CheckNewResult::KnownBad);
    }

    Ok(CheckNewResult::New)
}

pub fn check_new_joint(db: &Connection, joint: &Joint) -> Result<CheckNewResult> {
    let unit = joint.unit.unit.as_ref().expect("miss unit hash in joint");
    let ret = check_new_unit(db, unit)?;
    match ret {
        CheckNewResult::New => {
            let mut stmt = db.prepare_cached("SELECT error FROM known_bad_joints WHERE joint=?")?;
            let joint_hash = joint.get_joint_hash();
            let mut rows = stmt.query(&[&joint_hash])?;
            if let Some(row) = rows.next() {
                let error: String = row?.get_checked(0)?;
                warn!("detect knownbad joint {}, err: {}", joint_hash, error);
                return Ok(CheckNewResult::KnownBad);
            }
        }
        _ => {}
    }
    Ok(ret)
}

pub fn save_unhandled_joint_and_dependencies(
    db: &mut Connection,
    joint: &Joint,
    missing_parent_units: &[String],
    peer: &String,
) -> Result<()> {
    let unit = joint.unit.unit.as_ref().unwrap();
    let tx = db.transaction()?;
    let mut stmt =
        tx.prepare_cached("INSERT INTO unhandled_joints (unit, json, peer) VALUES (?, ?, ?)")?;
    stmt.insert(&[unit, &serde_json::to_string(joint)?, peer])?;
    let missing_units = missing_parent_units
        .iter()
        .map(|parent| format!("('{}', '{}')", unit, parent))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT OR IGNORE INTO dependencies (unit, depends_on_unit) VALUES {}",
        missing_units
    );
    let mut stmt = tx.prepare(&sql)?;
    stmt.execute(&[])?;
    Ok(())
}

pub fn find_lost_joints(db: &Connection) -> Result<Vec<String>> {
    let mut stmt = db.prepare_cached(
        "SELECT DISTINCT depends_on_unit \
		FROM dependencies \
		LEFT JOIN unhandled_joints ON depends_on_unit=unhandled_joints.unit \
		LEFT JOIN units ON depends_on_unit=units.unit \
		WHERE unhandled_joints.unit IS NULL AND units.unit IS NULL AND dependencies.creation_date < \'NOW() + INTERVAL -8 SECOND\'"
        )?;

    let rows = stmt.query_map(&[], |row| row.get(0))?;

    let mut names = Vec::new();
    for depend_result in rows {
        names.push(depend_result?);
    }

    //if names.len() == 0 {
    // return Err(format_err!("No lost joints found"));
    //}

    Ok(names)
}
