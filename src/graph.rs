use error::Result;
use rusqlite::Connection;

pub struct UnitProps {
    pub unit: String,
    pub level: u32,
    pub latest_included_mc_index: Option<u32>,
    pub main_chain_index: Option<u32>,
    pub is_on_main_chain: Option<u32>,
    pub is_free: u32,
}

pub fn compare_units(db: &Connection, unit1: &String, unit2: &String) -> Result<Option<i32>> {
    if unit1 == unit2 {
        return Ok(Some(0));
    }

    let units = [unit1, unit2];
    let unit_list = units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain, is_free \
        FROM units WHERE unit IN({})",
        unit_list
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(&[], |row| UnitProps {
        unit: row.get(0),
        level: row.get(1),
        latest_included_mc_index: row.get(2),
        main_chain_index: row.get(3),
        is_on_main_chain: row.get(4),
        is_free: row.get(5),
    })?;

    let mut unit_props = Vec::new();
    for row in rows {
        unit_props.push(row?);
    }

    ensure!(
        unit_props.len() == 2,
        "Not 2 rows for {} and {}",
        unit1,
        unit2
    );

    let (unit_props1, unit_props2) = if &unit_props[0].unit == unit1 {
        (&unit_props[0], &unit_props[1])
    } else {
        (&unit_props[1], &unit_props[0])
    };

    compare_unit_props(db, unit_props1, unit_props2)
}

pub fn compare_unit_props(
    db: &Connection,
    unit_props1: &UnitProps,
    unit_props2: &UnitProps,
) -> Result<Option<i32>> {
    if unit_props1.unit == unit_props2.unit {
        return Ok(Some(0));
    }
    if unit_props1.level == unit_props2.level {
        return Ok(None);
    }
    if unit_props1.is_free == 1 && unit_props2.is_free == 1 {
        // free units
        return Ok(None);
    }

    // genesis
    if unit_props1.latest_included_mc_index == None {
        return Ok(Some(-1));
    }
    if unit_props2.latest_included_mc_index == None {
        return Ok(Some(1));
    }

    if unit_props1.latest_included_mc_index >= unit_props2.main_chain_index
        && unit_props2.main_chain_index != None
    {
        return Ok(Some(1));
    }
    if unit_props2.latest_included_mc_index >= unit_props1.main_chain_index
        && unit_props1.main_chain_index != None
    {
        return Ok(Some(-1));
    }

    if unit_props1.level <= unit_props2.level
        && unit_props1.latest_included_mc_index <= unit_props2.latest_included_mc_index
        && (unit_props1.main_chain_index <= unit_props2.main_chain_index
            && unit_props1.main_chain_index != None
            && unit_props2.main_chain_index != None
            || unit_props1.main_chain_index == None
            || unit_props2.main_chain_index == None)
        || unit_props1.level >= unit_props2.level
            && unit_props1.latest_included_mc_index >= unit_props2.latest_included_mc_index
            && (unit_props1.main_chain_index >= unit_props2.main_chain_index
                && unit_props1.main_chain_index != None
                && unit_props2.main_chain_index != None
                || unit_props1.main_chain_index == None
                || unit_props2.main_chain_index == None)
    {
        // still can be comparable
    } else {
        return Ok(None);
    }

    let (earlier_unit, later_unit, result_if_found) = if unit_props1.level < unit_props2.level {
        (unit_props1, unit_props2, -1)
    } else {
        (unit_props2, unit_props1, 1)
    };

    // In JS it is okay if main_chain_index === null, the delta will be negative, need to check
    // the None value in rust
    let earlier_unit_delta =
        earlier_unit.main_chain_index.unwrap() - earlier_unit.latest_included_mc_index.unwrap();
    let later_unit_delta =
        later_unit.main_chain_index.unwrap() - later_unit.latest_included_mc_index.unwrap();

    let mut start_units = Vec::new();
    if later_unit_delta > earlier_unit_delta {
        //GoUp()
        start_units.push(later_unit.unit.clone());

        'go_up: loop {
            let start_unit_list = start_units
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \
                 FROM parenthoods JOIN units ON parent_unit=unit \
                 WHERE child_unit IN({})",
                start_unit_list
            );

            let mut stmt = db.prepare(&sql)?;
            let rows = stmt.query_map(&[], |row| UnitProps {
                unit: row.get(0),
                level: row.get(1),
                latest_included_mc_index: row.get(2),
                main_chain_index: row.get(3),
                is_on_main_chain: row.get(4),
                is_free: 0, //is_free is not queried
            })?;

            let mut new_start_units = Vec::new();
            for row in rows {
                let unit = row?;
                if unit.unit == earlier_unit.unit {
                    break 'go_up;
                }

                if unit.is_on_main_chain == Some(0) && unit.level > earlier_unit.level {
                    new_start_units.push(unit.unit.clone());
                }
            }

            if new_start_units.len() > 0 {
                start_units = new_start_units;
            } else {
                return Ok(None);;
            }
        }
    } else {
        // GoDown
        start_units.push(earlier_unit.unit.clone());

        'go_down: loop {
            let start_unit_list = start_units
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain \
                 FROM parenthoods JOIN units ON child_unit=unit \
                 WHERE parent_unit IN({})",
                start_unit_list
            );

            let mut stmt = db.prepare(&sql)?;
            let rows = stmt.query_map(&[], |row| UnitProps {
                unit: row.get(0),
                level: row.get(1),
                latest_included_mc_index: row.get(2),
                main_chain_index: row.get(3),
                is_on_main_chain: row.get(4),
                is_free: 0, //is_free is not queried
            })?;

            let mut new_start_units = Vec::new();
            for row in rows {
                let unit = row?;
                if unit.unit == earlier_unit.unit {
                    break 'go_down;
                }

                if unit.is_on_main_chain == Some(0) && unit.level < later_unit.level {
                    new_start_units.push(unit.unit.clone());
                }
            }

            if new_start_units.len() > 0 {
                start_units = new_start_units;
            } else {
                return Ok(None);
            }
        }
    }

    return Ok(Some(result_if_found));
}
