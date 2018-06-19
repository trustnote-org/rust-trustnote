use error::Result;
use graph;
use headers_commission;
use object_hash;
use paid_witnessing;
use rusqlite::Connection;
use spec::*;
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

fn read_last_stable_mc_unit(_db: &Connection) -> Result<String> {
    unimplemented!();
}

fn find_next_up_main_chain_unit(db: &Connection, unit: Option<&String>) -> Result<String> {
    let unit_props = if unit.is_some() {
        storage::read_static_unit_property(db, unit.unwrap())?
    } else {
        // if unit is None, read free balls
        let mut stmt = db.prepare_cached(
            "SELECT unit AS best_parent_unit, witnessed_level \
             FROM units WHERE is_free=1 \
             ORDER BY witnessed_level DESC, level-witnessed_level ASC, unit ASC LIMIT 1",
        )?;

        let mut row = stmt.query_row(&[], |row| StaticUnitProperty {
            level: 0, //Not queried
            witnessed_level: row.get(1),
            best_parent_unit: row.get(0),
            witness_list_unit: String::new(),
        });
        ensure!(row.is_ok(), "no free units?");

        row.unwrap()
    };

    //Handle unit props
    ensure!(unit_props.best_parent_unit.is_some(), "best parent is null");
    let best_parent_unit = unit_props.best_parent_unit.unwrap();

    info!(
        "unit {:?}, best parent {}, wlevel {}",
        unit, best_parent_unit, unit_props.witnessed_level
    );

    Ok(best_parent_unit)
}

pub fn update_main_chain(db: &Connection, last_unit: Option<&String>) -> Result<()> {
    info!("Will Update MC");

    //Go up from unit to find the last main chain unit and its index
    let last_main_chain_index;
    let last_main_chain_unit;
    let mut unit = last_unit.cloned();
    loop {
        if unit.is_some() && ::spec::is_genesis_unit(unit.as_ref().unwrap()) {
            last_main_chain_index = 0;
            last_main_chain_unit = unit.as_ref().unwrap().clone();
            break;
        } else {
            let best_parent_unit = find_next_up_main_chain_unit(db, unit.as_ref())?;
            let best_parent_unit_props = storage::read_unit_props(db, &best_parent_unit)?;

            if best_parent_unit_props.is_on_main_chain == 1 {
                last_main_chain_index = best_parent_unit_props.main_chain_index;
                last_main_chain_unit = best_parent_unit;
                break;
            }

            let mut stmt = db.prepare_cached(
                "UPDATE units SET is_on_main_chain=1, main_chain_index=NULL WHERE unit=?",
            )?;
            stmt.execute(&[&best_parent_unit])?;

            unit = Some(best_parent_unit);
        }
    }

    if unit.is_some() {
        //let unit = unit.unwrap();
        //Check whether it is rebuilding stable main chain
        info!("checkNotRebuildingStableMainChainAndGoDown {:?}", last_unit);
        let mut stmt = db.prepare_cached(
            "SELECT unit FROM units \
             WHERE is_on_main_chain=1 AND main_chain_index>? AND is_stable=1 LIMIT 1",
        )?;
        let row = stmt.query_row(&[&last_main_chain_index], |row| row.get::<_, String>(0));

        ensure!(
            row.is_err(),
            "removing stable witnessed unit {} from main chain",
            row.unwrap()
        );

        //Go down and update main chain index
        let mut stmt = db.prepare_cached(
            "UPDATE units SET is_on_main_chain=0, main_chain_index=NULL WHERE main_chain_index>?",
        )?;
        stmt.execute(&[&last_main_chain_index])?;

        let mut main_chain_index = last_main_chain_index;
        let main_chain_unit = last_main_chain_unit;

        let mut stmt = db.prepare_cached(
            "SELECT unit FROM units \
             WHERE is_on_main_chain=1 AND main_chain_index IS NULL ORDER BY level",
        )?;
        let rows = stmt
            .query_map(&[], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;

        ensure!(rows.len() > 0, "no unindexed MC units?");

        for row in rows.iter() {
            main_chain_index += 1;
            let mut children_units = Vec::new();
            children_units.push(row.clone());

            //Go up
            loop {
                let children_units_list = children_units
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(",");

                let sql = format!(
                    "SELECT unit \n\
                     FROM parenthoods JOIN units ON parent_unit=unit \n\
                     WHERE child_unit IN({}) AND main_chain_index IS NULL",
                    children_units_list
                );
                let mut stmt = db.prepare(&sql)?;
                let mut children_rows = stmt
                    .query_map(&[], |row| row.get(0))?
                    .collect::<::std::result::Result<Vec<String>, _>>()?;

                if children_rows.len() == 0 {
                    //Update Mc and then break
                    let sql = format!(
                        "UPDATE units SET main_chain_index={} WHERE unit IN({})",
                        main_chain_index, children_units_list
                    );
                    let mut stmt = db.prepare_cached(&sql)?;
                    stmt.execute(&[])?;

                    break;
                } else {
                    //Append children units and continue
                    children_units.append(children_rows.as_mut());
                }
            }
        }
        info!("goDownAndUpdateMainChainIndex done");
    }

    //Update latest included mc index
    info!("Update latest included mc index {}", last_main_chain_index);
    let mut stmt = db.prepare_cached(
        "UPDATE units SET latest_included_mc_index=NULL \
        WHERE main_chain_index>? OR main_chain_index IS NULL",
    )?;
    let affected_rows = stmt.execute(&[&last_main_chain_index])?;

    info!("Update LIMCI=NULL done, matched rows: {}", affected_rows);

    // if these units have other parents, they cannot include later MC units (otherwise, the parents would've been redundant).
    // the 2nd condition in WHERE is the same that was used 1 query ago to NULL limcis.
    let mut stmt = db.prepare_cached(
        "SELECT chunits.unit, punits.main_chain_index \
         FROM units AS punits \
         JOIN parenthoods ON punits.unit=parent_unit \
         JOIN units AS chunits ON child_unit=chunits.unit \
         WHERE punits.is_on_main_chain=1 \
         AND (chunits.main_chain_index > ? OR chunits.main_chain_index IS NULL) \
         AND chunits.latest_included_mc_index IS NULL",
    )?;
    let rows = stmt
        .query_map(&[&last_main_chain_index], |row| (row.get(0), row.get(1)))?
        .collect::<::std::result::Result<Vec<(String, u32)>, _>>()?;

    info!("{} rows", rows.len());

    ensure!(
        rows.len() != 0,
        "no latest_included_mc_index updated, last_mci={}, affected={}",
        last_main_chain_index,
        affected_rows
    );

    for row in rows.iter() {
        info!("{} {}", row.1, row.0);

        let mut stmt =
            db.prepare_cached("UPDATE units SET latest_included_mc_index=? WHERE unit=?")?;
        stmt.execute(&[&row.1, &row.0])?;
    }

    //Propagate latest included mc index
    loop {
        info!("Propagate latest included mc index");
        let mut stmt = db.prepare_cached(
            "SELECT punits.latest_included_mc_index, chunits.unit \
            FROM units AS punits \
            JOIN parenthoods ON punits.unit=parent_unit \
            JOIN units AS chunits ON child_unit=chunits.unit \
            WHERE (chunits.main_chain_index > ? OR chunits.main_chain_index IS NULL) \
                AND (chunits.latest_included_mc_index IS NULL OR chunits.latest_included_mc_index < punits.latest_included_mc_index)",
        )?;
        let rows = stmt
            .query_map(&[&last_main_chain_index], |row| (row.get(0), row.get(1)))?
            .collect::<::std::result::Result<Vec<(u32, String)>, _>>()?;

        if rows.len() == 0 {
            break;
        }

        for row in rows.iter() {
            let mut stmt =
                db.prepare_cached("UPDATE units SET latest_included_mc_index=? WHERE unit=?")?;
            stmt.execute(&[&row.0, &row.1])?;
        }
    }

    //Check all latest include mc indexes are set
    let mut stmt = db.prepare_cached(
        "SELECT unit FROM units \
         WHERE latest_included_mc_index IS NULL AND level!=0",
    )?;
    let rows = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    ensure!(
        rows.len() == 0,
        "{} units have latest_included_mc_index=NULL, e.g. unit {}",
        rows.len(),
        rows[0]
    );

    //Update stable mc flag
    loop {
        info!("Update stable mc flag");
        let last_stable_mc_unit = read_last_stable_mc_unit(db)?;
        let witnesses = storage::read_witness_list(db, &last_stable_mc_unit);

        //Query the children of last stable mc unit

        //Query witness level

        //mark_mc_index_stable(db, first_unstable_mc_index);

        break;
    }

    //Finish
    info!("Done Update MC");

    Ok(())
}
