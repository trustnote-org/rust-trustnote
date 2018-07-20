use error::Result;
use rusqlite::Connection;
use storage;

#[derive(Debug, Clone)]
pub struct UnitProps {
    pub unit: String,
    pub level: u32,
    pub latest_included_mc_index: Option<u32>,
    pub main_chain_index: Option<u32>,
    pub is_on_main_chain: Option<u32>,
    pub is_free: u32,
}

pub fn compare_units(db: &Connection, unit1: &str, unit2: &str) -> Result<Option<i32>> {
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

    let (unit_props1, unit_props2) = if unit_props[0].unit == unit1 {
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
        && (unit_props1.main_chain_index == None
            || unit_props2.main_chain_index == None
            || unit_props1.main_chain_index <= unit_props2.main_chain_index)
        || unit_props1.level >= unit_props2.level
            && unit_props1.latest_included_mc_index >= unit_props2.latest_included_mc_index
            && (unit_props1.main_chain_index == None
                || unit_props2.main_chain_index == None
                || unit_props1.main_chain_index >= unit_props2.main_chain_index)
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

    // can be negative if main_chain_index == None but that doesn't matter
    let earlier_unit_delta = earlier_unit.main_chain_index.unwrap_or(0)
        - earlier_unit.latest_included_mc_index.unwrap_or(0);
    let later_unit_delta =
        later_unit.main_chain_index.unwrap_or(0) - later_unit.latest_included_mc_index.unwrap_or(0);

    let mut start_units = Vec::new();
    if later_unit_delta > earlier_unit_delta {
        //GoUp()
        start_units.push(later_unit.unit.clone());

        loop {
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
                    return Ok(Some(result_if_found));
                }

                if unit.is_on_main_chain == Some(0) && unit.level > earlier_unit.level {
                    new_start_units.push(unit.unit.clone());
                }
            }

            if !new_start_units.is_empty() {
                start_units = new_start_units;
            } else {
                return Ok(None);;
            }
        }
    } else {
        // GoDown
        start_units.push(earlier_unit.unit.clone());

        loop {
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
                    return Ok(Some(result_if_found));
                }

                if unit.is_on_main_chain == Some(0) && unit.level < later_unit.level {
                    new_start_units.push(unit.unit.clone());
                }
            }

            if !new_start_units.is_empty() {
                start_units = new_start_units;
            } else {
                return Ok(None);
            }
        }
    }
}

pub fn determine_if_included(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
) -> Result<bool> {
    if ::spec::is_genesis_unit(&earlier_unit) {
        return Ok(true);
    }

    let (earlier_unit_props, later_units_props) =
        storage::read_props_of_units(db, &earlier_unit, later_units)?;

    if earlier_unit_props.is_free == 1 {
        return Ok(false);
    }

    ensure!(
        !later_units_props.is_empty(),
        "no later unit props were read"
    );

    //spec::UnitProps.latest_included_mc_index and spec::UnitProps.main_chain_index is not Option
    let max_later_limci = later_units_props
        .iter()
        .max_by_key(|props| props.latest_included_mc_index)
        .unwrap()
        .latest_included_mc_index;
    if
    /*earlier_unit_props.main_chain_index.is_some()
        &&*/
    max_later_limci >= earlier_unit_props.main_chain_index {
        return Ok(true);
    }

    let max_later_level = later_units_props
        .iter()
        .max_by_key(|props| props.level)
        .unwrap()
        .level;
    if max_later_level < earlier_unit_props.level {
        return Ok(false);
    }

    let mut start_units = later_units.to_vec();

    loop {
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
            if unit.unit == *earlier_unit {
                return Ok(true);
            }

            if unit.is_on_main_chain == Some(0) && unit.level > earlier_unit_props.level {
                new_start_units.push(unit.unit.clone());
            }
        }

        if !new_start_units.is_empty() {
            new_start_units.sort();
            new_start_units.dedup();
            start_units = new_start_units;
        } else {
            return Ok(false);
        }
    }
}

pub fn determine_if_included_or_equal(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
) -> Result<bool> {
    if later_units.contains(earlier_unit) {
        return Ok(true);
    }

    determine_if_included(db, earlier_unit, later_units)
}

pub fn read_descendant_units_by_authors_before_mc_index(
    db: &Connection,
    earlier_unit: &UnitProps,
    author_addresses: &[String],
    to_main_chain_index: u32,
) -> Result<Vec<String>> {
    let mut units = Vec::new();

    let author_address_list = author_addresses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    ensure!(
        earlier_unit.main_chain_index.is_some(),
        "earlier unit has no main chain index"
    );
    let earlier_unit_mci = earlier_unit.main_chain_index.unwrap();

    //Missing db.forceIndex("byMcIndex") from original js
    let sql = format!(
        "SELECT unit FROM units \
         LEFT JOIN unit_authors USING(unit) \
         WHERE latest_included_mc_index>={} AND main_chain_index>{} \
         AND main_chain_index<={} AND latest_included_mc_index<{} \
         AND address IN({})",
        earlier_unit_mci,
        earlier_unit_mci,
        to_main_chain_index,
        to_main_chain_index,
        author_address_list
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(&[], |row| row.get::<_, String>(0))?;

    for row in rows {
        units.push(row?)
    }

    let mut start_units = Vec::new();
    start_units.push(earlier_unit.unit.clone());

    loop {
        let start_unit_list = start_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "SELECT units.unit, unit_authors.address AS author_in_list \
             FROM parenthoods \
             JOIN units ON child_unit=units.unit \
             LEFT JOIN unit_authors ON unit_authors.unit=units.unit \
             AND address IN({}) \
             WHERE parent_unit IN({}) \
             AND latest_included_mc_index<{} \
             AND main_chain_index<={}",
            author_address_list, start_unit_list, earlier_unit_mci, to_main_chain_index
        );

        //The query fields have different structures
        struct UnitProps {
            unit: String,
            author_in_list: Option<String>,
        }

        let mut stmt = db.prepare(&sql)?;
        let rows = stmt.query_map(&[], |row| UnitProps {
            unit: row.get(0),
            author_in_list: row.get(1),
        })?;

        let mut new_start_units = Vec::new();

        for row in rows {
            let unit = row?;

            new_start_units.push(unit.unit.clone());

            if unit.author_in_list.is_some() {
                units.push(unit.unit.clone());
            }
        }

        if !new_start_units.is_empty() {
            start_units = new_start_units;
        } else {
            return Ok(units);
        }
    }
}
