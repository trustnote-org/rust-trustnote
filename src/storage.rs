use error::Result;
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
