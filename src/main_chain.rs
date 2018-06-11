use rusqlite::Connection;
// use spec::*;
use error::Result;
use headers_commission;
use paid_witnessing;
use storage;
use graph;

pub fn determin_if_stable_in_laster_units_and_update_stable_mc_flag(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
    is_stable_in_db: u32, // this should be bool, but read from db
) -> bool {
    let _ = (db, earlier_unit, later_units, is_stable_in_db);
    unimplemented!()
}

fn set_content_hash(db: &Connection, unit: &String) -> Result<()> {
    let joint = storage::read_joint(db, unit)?;
    let content_hash = joint.unit.get_unit_content_hash();

    let mut stmt = db.prepare_cached("UPDATE units SET content_hash=? WHERE unit=?")?;
    stmt.execute(&[&content_hash, unit])?;

    Ok(())
}

fn find_stable_conflicting_units(_db: &Connection, _unit_prop: &graph::UnitProps) -> Result<Vec<String>> {
    unimplemented!()
}

pub fn mark_mc_index_stable(db: &Connection, mci: u32) -> Result<()> {
    let mut stmt =
        db.prepare_cached("UPDATE units SET is_stable=1 WHERE is_stable=0 AND main_chain_index=?")?;
    stmt.execute(&[&mci])?;

    //Handle non-serial units
    let mut stmt = db.prepare_cached(
        "SELECT * FROM units WHERE main_chain_index=? AND sequence!='good' ORDER BY unit",
    )?;

    struct UnitTemp {
        unit_props: graph::UnitProps,
        sequence: String,
        content_hash: Option<String>,
    };

    let rows = stmt.query_map(&[&mci], |row| UnitTemp {
        unit_props: graph::UnitProps {
            unit: row.get("unit"),
            level: row.get("level"),
            latest_included_mc_index: row.get("latest_included_mc_index"),
            main_chain_index: row.get("main_chain_index"),
            is_on_main_chain: row.get("is_on_main_chain"),
            is_free: row.get("is_free"),
        },
        sequence: row.get("sequence"),
        content_hash: row.get("content_hash"),
    })?;

    for row in rows {
        let row = row?;
        let unit_prop = row.unit_props;
        let unit = &unit_prop.unit;

        if row.sequence.as_str() == "final-bad" {
            if row.content_hash.is_none() {
                set_content_hash(db, unit)?;
            }

            continue;
        }

        //Temp bad
        ensure!(
            row.content_hash.is_none(),
            "temp-bad and with content_hash?"
        );

        let conflict_units = find_stable_conflicting_units(db, &unit_prop)?;

        let sequence = if conflict_units.len() > 0 {
            String::from("final-bad")
        } else {
            String::from("good")
        };

        info!(
            "unit {} has competitors {:?}, it becomes {}",
            unit, conflict_units, sequence
        );

        let mut stmt = db.prepare_cached("UPDATE units SET sequence=? WHERE unit=")?;
        stmt.execute(&[&sequence, unit])?;

        if sequence.as_str() == "good" {
            let mut stmt = db.prepare_cached("UPDATE inputs SET is_unique=1 WHERE unit=?")?;
            stmt.execute(&[unit])?;
        } else {
            set_content_hash(db, unit)?;
        }
    }

    //TODO: Add balls


    //Update retrievable
    let _min_retrievable_mci = storage::update_min_retrievable_mci_after_stabilizing_mci(db, mci)?;

    //Calculate commissions
    headers_commission::calc_headers_commissions(db)?;
    paid_witnessing::update_paid_witnesses(db)?;

    //No event bus, but should tell network to notify others about stable joint

    Ok(())
}
