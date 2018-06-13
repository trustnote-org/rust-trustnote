use rusqlite::Connection;
// use spec::*;
use error::Result;
use graph;
use headers_commission;
use object_hash;
use paid_witnessing;
use storage;

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

fn get_similar_mcis(mci: u32) -> Vec<u32> {
    let mut similar_mcis = Vec::new();
    let mut devisor = 10;
    loop {
        if mci % devisor == 0 {
            similar_mcis.push(mci - devisor);
            devisor *= 10;
        } else {
            return similar_mcis;
        }
    }
}

fn find_stable_conflicting_units(
    db: &Connection,
    unit_prop: &graph::UnitProps,
) -> Result<Vec<String>> {
    // units come here sorted by original unit,
    //so the smallest original on the same MCI comes first and will become good,
    //all others will become final-bad
    let mut stmt = db.prepare_cached(
        "SELECT competitor_units.* \
        FROM unit_authors AS this_unit_authors \
        JOIN unit_authors AS competitor_unit_authors USING(address) \
        JOIN units AS competitor_units ON competitor_unit_authors.unit=competitor_units.unit \
        JOIN units AS this_unit ON this_unit_authors.unit=this_unit.unit \
        WHERE this_unit_authors.unit=? AND competitor_units.is_stable=1 AND +competitor_units.sequence='good' \
            -- if it were main_chain_index <= this_unit_limci, the competitor would've been included \
            AND (competitor_units.main_chain_index > this_unit.latest_included_mc_index) \
            AND (competitor_units.main_chain_index <= this_unit.main_chain_index)",
    )?;
    let rows = stmt.query_map(&[&unit_prop.unit], |row| graph::UnitProps {
        unit: row.get("unit"),
        level: row.get("level"),
        latest_included_mc_index: row.get("latest_included_mc_index"),
        main_chain_index: row.get("main_chain_index"),
        is_on_main_chain: row.get("is_on_main_chain"),
        is_free: row.get("is_free"),
    })?;

    let mut conflicting_units = Vec::new();
    for row in rows {
        let row = row?;
        if graph::compare_unit_props(db, &row, unit_prop)? == None {
            conflicting_units.push(row.unit);
        }
    }

    Ok(conflicting_units)
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
        ball: Option<String>,
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
        ball: row.get("ball"),
    })?;

    for row in rows {
        let row = row?;
        let unit_props = row.unit_props;
        let unit = &unit_props.unit;

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

        let conflict_units = find_stable_conflicting_units(db, &unit_props)?;

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

    //Add balls
    let mut stmt = db.prepare_cached(
        "SELECT units.*, ball \
         FROM units LEFT JOIN balls USING(unit) \
         WHERE main_chain_index=? ORDER BY level",
    )?;

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
        ball: row.get("ball"),
    })?;

    for row in rows {
        let row = row?;
        let unit_props = row.unit_props;
        let unit = &unit_props.unit;

        //Parent balls
        let mut stmt = db.prepare_cached(
            "SELECT ball FROM parenthoods LEFT JOIN balls ON parent_unit=unit \
             WHERE child_unit=? ORDER BY ball",
        )?;
        let mut ball_rows = stmt.query_map(&[unit], |row| row.get::<_, Option<String>>(0))?;

        let mut parent_balls = Vec::new();
        for ball in ball_rows {
            let ball = ball?;
            ensure!(ball.is_some(), "some parent balls not found for unit {}");
            parent_balls.push(ball.unwrap());
        }

        let similar_mcis = get_similar_mcis(mci);
        let mut skiplist_balls = Vec::new();
        let mut skiplist_units = Vec::new();

        if unit_props.is_on_main_chain == Some(1) && similar_mcis.len() > 0 {
            let similar_mcis_list = similar_mcis
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = format!(
                "SELECT units.unit, ball FROM units LEFT JOIN balls USING(unit) \
                 WHERE is_on_main_chain=1 AND main_chain_index IN({})",
                similar_mcis_list
            );
            let mut stmt = db.prepare(&sql)?;

            struct UnitBall {
                unit: String,
                ball: Option<String>,
            }
            let rows = stmt.query_map(&[], |row| UnitBall {
                unit: row.get(0),
                ball: row.get(1),
            })?;

            for row in rows {
                let row = row?;
                let skiplist_unit = row.unit;
                let skiplist_ball = row.ball;

                ensure!(skiplist_ball.is_some(), "no skiplist ball");

                skiplist_balls.push(skiplist_ball.unwrap());
                skiplist_units.push(skiplist_unit);
            }
        }

        //Add ball
        skiplist_balls.sort();
        let ball = object_hash::get_ball_hash(
            unit,
            &parent_balls,
            &skiplist_balls,
            row.sequence.as_str() == "final-bad",
        );

        if row.ball.is_some() {
            //Already inserted
            let stored_ball = row.ball.unwrap();
            ensure!(
                stored_ball == ball,
                "stored and calculated ball hashes do not match, ball={} unit_props.ball={} unit_props={:?}",
                ball,
                stored_ball,
                unit_props
            );

            continue;
        }

        //Finally, insert ball
        let mut stmt = db.prepare_cached("INSERT INTO balls (ball, unit) VALUES(?,?)")?;
        stmt.execute(&[&ball, unit])?;

        let mut stmt = db.prepare_cached("DELETE FROM hash_tree_balls WHERE ball=?")?;
        stmt.execute(&[&ball])?;

        if skiplist_units.len() > 0 {
            let value_list = skiplist_units
                .iter()
                .map(|s| format!("('{}','{}')", unit, s))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO skiplist_units (unit, skiplist_unit) VALUES {}",
                value_list
            );
            let mut stmt = db.prepare(&sql)?;
            stmt.execute(&[])?;
        }
    }

    //Update retrievable
    let _min_retrievable_mci = storage::update_min_retrievable_mci_after_stabilizing_mci(db, mci)?;

    //Calculate commissions
    headers_commission::calc_headers_commissions(db)?;
    paid_witnessing::update_paid_witnesses(db)?;

    //No event bus, but should tell network to notify others about stable joint

    Ok(())
}
