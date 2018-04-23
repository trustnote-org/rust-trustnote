use error::Result;
use joint::Joint;
use rusqlite::Connection;
use spec::StaticUnitProperty;

// TODO: need to cache in memory
pub fn get_witness_list(unit_hash: &String, db: &Connection) -> Result<Vec<String>> {
    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let rows = stmt.query_map(&[unit_hash], |row| row.get(0))?;
    let mut names = Vec::new();
    for name_result in rows {
        names.push(name_result?);
    }

    if names.len() != ::config::COUNT_WITNESSES {
        return Err(format_err!(
            "wrong number of witnesses in unit {}",
            unit_hash
        ));
    }
    Ok(names)
}

// TODO: need to cache in memory
pub fn get_static_unit_property(unit_hash: &String, db: &Connection) -> Result<StaticUnitProperty> {
    let mut stmt = db.prepare_cached(
        "SELECT level, witnessed_level, best_parent_unit, witness_list_unit \
         FROM units WHERE unit=?",
    )?;
    let ret = stmt.query_row(&[unit_hash], |row| StaticUnitProperty {
        level: row.get(0),
        witnessed_level: row.get(1),
        best_parent_unit: row.get(2),
        witness_list_unit: row.get(3),
    })?;

    Ok(ret)
}

// TODO: need to cache in memory
pub fn get_unit_authors(unit_hash: &String, db: &Connection) -> Result<Vec<String>> {
    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let rows = stmt.query_map(&[unit_hash], |row| row.get(0))?;
    let mut names = Vec::new();
    for name_result in rows {
        names.push(name_result?);
    }

    if names.len() != ::config::COUNT_WITNESSES {
        return Err(format_err!(
            "wrong number of witnesses in unit {}",
            unit_hash
        ));
    }
    Ok(names)
}

pub fn determine_if_witness_and_address_definition_have_refs(
    db: &Connection,
    witnesses: &[String],
) -> Result<bool> {
    let witness_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT 1 FROM address_definition_changes JOIN definitions USING(definition_chash) \
         WHERE address IN({}) AND has_references=1 \
         UNION \
         SELECT 1 FROM definitions WHERE definition_chash IN({}) AND has_references=1 \
         LIMIT 1",
        witness_list, witness_list
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(&[], |row| row.get::<_, u32>(0))?;
    Ok(rows.count() > 0)
}

pub fn read_joint_with_ball(db: &Connection, unit: &String) -> Result<Joint> {
    let mut joint = read_joint_directly(db, unit)?;
    if joint.ball.is_none() {
        let mut stmt = db.prepare_cached("SELECT ball FROM balls WHERE unit=?")?;
        if let Ok(ball) = stmt.query_row(&[unit], |row| row.get(0)) {
            joint.ball = Some(ball);
        }
    }

    Ok(joint)
}

pub fn read_joint_directly(_db: &Connection, _unit: &String) -> Result<Joint> {
    // TODO: #34
    unimplemented!()
}
