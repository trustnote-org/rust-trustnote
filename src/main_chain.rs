use config;
use error::Result;
use graph;
use headers_commission;
use joint::WRITER_MUTEX;
use object_hash;
use paid_witnessing;
use rusqlite::Connection;
use spec;
use storage;

pub fn determin_if_stable_in_laster_units(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
) -> Result<bool> {
    if ::spec::is_genesis_unit(&earlier_unit) {
        return Ok(true);
    }

    let (earlier_unit_props, later_units_props) =
        storage::read_props_of_units(db, earlier_unit, later_units)?;

    if earlier_unit_props.is_free == 1 {
        return Ok(false);
    }

    let max_later_limci = later_units_props
        .iter()
        .map(|prop| prop.latest_included_mc_index)
        .max()
        .unwrap_or(None);

    let (best_parent_unit, arr_witnesses) = read_best_parent_and_its_witnesses(db, earlier_unit)?;

    let witnesses_set = arr_witnesses
        .into_iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let mut stmt = db.prepare_cached(
        "SELECT unit, is_on_main_chain, main_chain_index, level \
         FROM units WHERE best_parent_unit=?",
    )?;

    #[derive(Clone)]
    struct TempUnitProp {
        unit: String,
        level: u32,
        main_chain_index: Option<u32>,
        is_on_main_chain: u32,
    };

    let rows = stmt
        .query_map(&[&best_parent_unit], |row| TempUnitProp {
            unit: row.get(0),
            is_on_main_chain: row.get(1),
            main_chain_index: row.get(2),
            level: row.get(3),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    ensure!(!rows.is_empty(), "no best children of {}", best_parent_unit);

    let mc_rows: Vec<TempUnitProp> = rows
        .iter()
        .filter(|r| r.is_on_main_chain == 1)
        .cloned()
        .collect();
    let alt_rows: Vec<TempUnitProp> = rows
        .into_iter()
        .filter(|r| r.is_on_main_chain == 0)
        .collect();

    ensure!(mc_rows.len() == 1, "not a single MC child");
    let mc_unit_prop = mc_rows.into_iter().nth(0).unwrap();

    let first_unstable_mc_unit = mc_unit_prop.unit;
    ensure!(
        first_unstable_mc_unit == *earlier_unit,
        "first unstable MC unit is not our input unit"
    );

    let first_unstable_mc_level = mc_unit_prop.level;
    let alt_branch_root_units: Vec<String> = alt_rows.into_iter().map(|row| row.unit).collect();

    let min_mc_wl = find_min_mc_witnessed_level(db, &witnesses_set, later_units)?;
    if !determine_if_has_alt_branches(db, later_units, &alt_branch_root_units)? {
        return Ok(min_mc_wl >= first_unstable_mc_level);
    }

    let alt_best_children = create_list_of_best_children_included_by_later_units(
        db,
        later_units,
        &alt_branch_root_units,
        max_later_limci.unwrap_or(0),
    )?.into_iter()
    .map(|s| format!("'{}'", s))
    .collect::<Vec<_>>()
    .join(",");

    let sql = format!(
        "SELECT MAX(units.level) AS max_alt_level FROM units \
         LEFT JOIN parenthoods ON units.unit=child_unit \
         LEFT JOIN units AS punits ON parent_unit=punits.unit \
         AND punits.witnessed_level >= units.witnessed_level \
         WHERE units.unit IN({}) AND punits.unit IS NULL AND ( \
         SELECT COUNT(*) FROM unit_witnesses \
         WHERE unit_witnesses.unit IN(units.unit, units.witness_list_unit) \
         AND unit_witnesses.address IN({})) >= {}",
        alt_best_children,
        witnesses_set,
        config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS,
    );

    let mut stmt = db.prepare(&sql)?;
    let rows: Vec<u32> = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<_>, _>>()?;

    ensure!(rows.len() == 1, "not a single max alt level");
    Ok(min_mc_wl >= rows[0])
}

pub fn determin_if_stable_in_laster_units_and_update_stable_mc_flag(
    db: &Connection,
    earlier_unit: &String,
    later_units: &[String],
    is_stable_in_db: bool, // this should be bool, but read from db
) -> Result<bool> {
    let is_stable = determin_if_stable_in_laster_units(db, earlier_unit, later_units)?;
    info!(
        "determineIfStableInLaterUnits {}, {:?}, {}",
        earlier_unit, later_units, is_stable
    );
    if !is_stable {
        return Ok(false);
    }

    if is_stable && is_stable_in_db {
        return Ok(true);
    }

    info!("stable in parents, will wait for write lock");
    let _g = WRITER_MUTEX.lock().unwrap();
    info!("stable in parents, got write lock");
    let last_stable_mci = storage::read_last_stable_mc_index(db)?;
    let prop = storage::read_unit_props(db, earlier_unit)?;
    let new_last_stable_mci = prop.main_chain_index;
    ensure!(
        new_last_stable_mci > Some(last_stable_mci),
        "new last stable mci expected to be higher than existing"
    );

    let mut mci = last_stable_mci;

    while Some(mci) <= new_last_stable_mci {
        mci += 1;
        mark_mc_index_stable(db, mci)?;
    }

    Ok(is_stable)
}

pub fn read_best_parent_and_its_witnesses(
    db: &Connection,
    unit_hash: &String,
) -> Result<(String, Vec<String>)> {
    let prop = storage::read_static_unit_property(db, unit_hash)?;
    let best_parent_unit = prop
        .best_parent_unit
        .ok_or_else(|| format_err!("no best parent set for unit {}", unit_hash))?;
    let arr_witnesses = storage::read_witnesses(db, &best_parent_unit)?;

    Ok((best_parent_unit, arr_witnesses))
}

fn find_min_mc_witnessed_level(
    db: &Connection,
    witnesses_set: &str,
    later_units: &[String],
) -> Result<u32> {
    struct OutputTemp {
        witnessed_level: u32,
        best_parent_unit: String,
        count: u32,
    };

    let later_units_set = later_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "SELECT witnessed_level, best_parent_unit, \
             (SELECT COUNT(*) FROM unit_authors WHERE unit_authors.unit=units.unit AND address IN({})) AS count \
         FROM units \
         WHERE unit IN({}) \
         ORDER BY witnessed_level DESC, \
             level-witnessed_level ASC, \
             unit ASC \
         LIMIT 1", witnesses_set, later_units_set);

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| OutputTemp {
            witnessed_level: row.get(0),
            best_parent_unit: row.get(1),
            count: row.get(2),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    ensure!(!rows.is_empty(), "find_min_mc_witnessed_level: not 1 row");
    let mut count = rows[0].count as usize;
    let mut min_mc_wl = rows[0].witnessed_level;
    let mut start_unit = rows.into_iter().nth(0).unwrap().best_parent_unit;

    while count < config::MAJORITY_OF_WITNESSES {
        let sql = format!(
            "SELECT best_parent_unit, witnessed_level, \
             (SELECT COUNT(*) FROM unit_authors WHERE unit_authors.unit=units.unit AND address IN({})) AS count \
             FROM units WHERE unit=?", witnesses_set);
        let mut stmt = db.prepare(&sql)?;
        let rows = stmt
            .query_map(&[&start_unit], |row| OutputTemp {
                best_parent_unit: row.get(0),
                witnessed_level: row.get(1),
                count: row.get(2),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        ensure!(rows.len() == 1, "findMinMcWitnessedLevel: not 1 row");
        let row = rows.into_iter().nth(0).unwrap();

        if row.count > 0 && row.witnessed_level < min_mc_wl {
            min_mc_wl = row.witnessed_level;
        }
        count += row.count as usize;
        start_unit = row.best_parent_unit;
    }

    Ok(min_mc_wl)
}

fn determine_if_has_alt_branches(
    db: &Connection,
    later_units: &[String],
    alt_branch_root_units: &[String],
) -> Result<bool> {
    if alt_branch_root_units.is_empty() {
        return Ok(false);
    }

    for alt_root_unit in alt_branch_root_units {
        if graph::determine_if_included_or_equal(db, alt_root_unit, later_units)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn create_list_of_best_children_included_by_later_units(
    db: &Connection,
    later_units: &[String],
    alt_branch_root_units: &[String],
    max_later_limci: u32,
) -> Result<Vec<String>> {
    if alt_branch_root_units.is_empty() {
        return Ok(Vec::new());
    }

    let mut best_children = Vec::new();
    let mut filtered_alt_branch_root_units = Vec::new();

    let alt_branch_root_units_set = alt_branch_root_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "SELECT unit, is_free, main_chain_index FROM units WHERE unit IN({})",
        alt_branch_root_units_set
    );

    let mut stmt = db.prepare(&sql)?;

    struct TempUnitProp {
        unit: String,
        is_free: u32,
        main_chain_index: Option<u32>,
    }

    let rows = stmt
        .query_map(&[], |row| TempUnitProp {
            unit: row.get(0),
            is_free: row.get(1),
            main_chain_index: row.get(2),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    ensure!(!rows.is_empty(), "no alt branch root units?");

    for row in rows {
        let is_included =
            if row.main_chain_index.is_some() && row.main_chain_index <= Some(max_later_limci) {
                true
            } else {
                graph::determine_if_included_or_equal(db, &row.unit, later_units)?
            };

        if is_included {
            best_children.push(row.unit.clone());
            filtered_alt_branch_root_units.push(row.unit);
        }
    }

    let mut start_units = filtered_alt_branch_root_units;
    loop {
        let start_units_set = start_units
            .drain(..)
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");

        let sql = format!(
            "SELECT unit, is_free, main_chain_index FROM units WHERE best_parent_unit IN ({})",
            start_units_set
        );

        let mut stmt = db.prepare(&sql)?;

        let rows = stmt
            .query_map(&[], |row| TempUnitProp {
                unit: row.get(0),
                is_free: row.get(1),
                main_chain_index: row.get(2),
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

        if rows.is_empty() {
            break;
        }

        for row in rows {
            let included = if row.main_chain_index.is_some()
                && row.main_chain_index <= Some(max_later_limci)
            {
                true
            } else {
                graph::determine_if_included_or_equal(db, &row.unit, later_units)?
            };

            if included {
                best_children.push(row.unit.clone());
                if row.is_free != 1 {
                    start_units.push(row.unit);
                }
            }
        }
    }

    Ok(best_children)
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

pub struct MciStableEvent {
    pub mci: u32,
}
impl_event!(MciStableEvent);

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
        ball: None,
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

        let sequence = if !conflict_units.is_empty() {
            String::from("final-bad")
        } else {
            String::from("good")
        };

        info!(
            "unit {} has competitors {:?}, it becomes {}",
            unit, conflict_units, sequence
        );

        let mut stmt = db.prepare_cached("UPDATE units SET sequence=? WHERE unit=?")?;
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

        if unit_props.is_on_main_chain == Some(1) && !similar_mcis.is_empty() {
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

        if !skiplist_units.is_empty() {
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

    // trigger mci stable event
    ::utils::event::emit_event(MciStableEvent { mci });

    Ok(())
}

fn read_last_stable_mc_unit(db: &Connection) -> Result<String> {
    let mut stmt = db.prepare_cached(
        "SELECT unit FROM units \
         WHERE is_on_main_chain=1 AND is_stable=1 \
         ORDER BY main_chain_index DESC LIMIT 1",
    )?;

    let row = stmt.query_row(&[], |row| row.get::<_, String>(0));

    ensure!(row.is_ok(), "no units on stable MC?");

    Ok(row.unwrap())
}

fn create_list_of_best_children(db: &Connection, parent_units: Vec<String>) -> Result<Vec<String>> {
    let mut best_children = parent_units.clone();

    if !parent_units.is_empty() {
        //Go down and collect best children
        let mut units_list = parent_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");
        loop {
            let sql = format!(
                "SELECT unit, is_free FROM units WHERE best_parent_unit IN({})",
                units_list
            );
            let mut stmt = db.prepare(&sql)?;

            struct UnitTemp {
                unit: String,
                is_free: u32,
            };

            let rows = stmt
                .query_map(&[], |row| UnitTemp {
                    unit: row.get(0),
                    is_free: row.get(1),
                })?.collect::<::std::result::Result<Vec<_>, _>>()?;

            let mut next_units = Vec::new();
            for row in rows {
                best_children.push(row.unit.clone());

                //It has children, push it to the query of next round
                if row.is_free != 1 {
                    next_units.push(row.unit);
                }
            }

            if next_units.is_empty() {
                break;
            } else {
                units_list = next_units
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(",");
            }
        }
    }

    Ok(best_children)
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

        stmt.query_row(&[], |row| spec::StaticUnitProperty {
            level: 0, //Not queried
            witnessed_level: row.get(1),
            best_parent_unit: row.get(0),
            witness_list_unit: None,
        }).or_else(|e| bail!("no free units, err={}", e))?
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

fn update_latest_included_mc_index(
    db: &Connection,
    last_main_chain_index: u32,
    rebuild_mc: bool,
) -> Result<()> {
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

    if rows.is_empty() && rebuild_mc {
        bail!(
            "no latest_included_mc_index updated, last_mci={}, affected={}",
            last_main_chain_index,
            affected_rows
        );
    }

    for row in &rows {
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

        if rows.is_empty() {
            break;
        }

        for row in &rows {
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
        rows.is_empty(),
        "{} units have latest_included_mc_index=NULL, e.g. unit {}",
        rows.len(),
        rows[0]
    );

    Ok(())
}

fn update_stable_mc_flag(db: &Connection) -> Result<()> {
    loop {
        info!("Update stable mc flag");

        let last_stable_mc_unit = read_last_stable_mc_unit(db)?;
        info!("Last stable mc unit {}", last_stable_mc_unit);

        let witnesses = storage::read_witnesses(db, &last_stable_mc_unit)?;
        let witness_list = witnesses
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");

        //Query the children of last stable mc unit
        let mut stmt = db.prepare_cached(
            "SELECT unit, is_on_main_chain, main_chain_index, level \
             FROM units WHERE best_parent_unit=?",
        )?;

        #[derive(Clone)]
        struct TempUnitProp {
            unit: String,
            level: u32,
            main_chain_index: Option<u32>,
            is_on_main_chain: u32,
        };

        let best_children = stmt
            .query_map(&[&last_stable_mc_unit], |row| TempUnitProp {
                unit: row.get(0),
                is_on_main_chain: row.get(1),
                main_chain_index: row.get(2),
                level: row.get(3),
            })?.collect::<::std::result::Result<Vec<TempUnitProp>, _>>()?;

        ensure!(
            !best_children.is_empty(),
            "no best children of last stable MC unit {}",
            last_stable_mc_unit
        );

        //Current main chain
        let mc_child: Vec<TempUnitProp> = best_children
            .iter()
            .filter(|r| r.is_on_main_chain == 1)
            .cloned()
            .collect();

        ensure!(mc_child.len() == 1, "not a single MC child?");
        //let first_unstable_mc_unit = mc_child[0].unit.clone();
        let first_unstable_mc_index = mc_child[0].main_chain_index.unwrap_or(0);
        let first_unstable_mc_level = mc_child[0].level;

        let alt_children: Vec<TempUnitProp> = best_children
            .into_iter()
            .filter(|r| r.is_on_main_chain == 0)
            .collect();

        //The alternative branch
        let alt_branch_root_units: Vec<String> =
            alt_children.into_iter().map(|row| row.unit).collect();

        //Query main chain witness level
        let min_mc_wl = {
            let mc_end_witnessed_level = {
                let mut stmt = db.prepare_cached(
                    "SELECT witnessed_level FROM units WHERE is_free=1 AND is_on_main_chain=1",
                )?;
                let rows = stmt
                    .query_map(&[], |row| row.get(0))?
                    .collect::<::std::result::Result<Vec<u32>, _>>()?;

                ensure!(rows.len() == 1, "not a single mc wl");

                // this is the level when we collect 7 witnesses if walking up the MC from its end
                rows[0]
            };
            let sql = format!(
                "SELECT MIN(witnessed_level) AS min_mc_wl FROM units \
                 LEFT JOIN unit_authors USING(unit) \
                 WHERE is_on_main_chain=1 AND level>={} AND address IN({})",
                mc_end_witnessed_level, witness_list
            );
            let mut stmt = db.prepare(&sql)?;
            let rows = stmt
                .query_map(&[], |row| row.get(0))?
                .collect::<::std::result::Result<Vec<Option<u32>>, _>>()?;

            ensure!(rows.len() == 1, "not a single min mc wl");

            rows[0].unwrap_or(0)
        };

        let mut stable = false;
        if alt_branch_root_units.is_empty() {
            // no alt branches
            if min_mc_wl >= first_unstable_mc_level {
                stable = true;
            }
        } else {
            let max_alt_level = {
                let alt_best_children_list =
                    create_list_of_best_children(db, alt_branch_root_units)?
                        .iter()
                        .map(|s| format!("'{}'", s))
                        .collect::<Vec<_>>()
                        .join(",");

                // Compose a set S of units that increase WL, that is their own WL is greater than that of every parent.
                // In this set, find max L. Alt WL will never reach it. If min_mc_wl > L, next MC unit is stable.
                // Also filter the set S to include only those units that are conformant with the last stable MC unit.
                let sql = format!(
                    "SELECT MAX(units.level) AS max_alt_level \
                    FROM units \
                    LEFT JOIN parenthoods ON units.unit=child_unit \
                    LEFT JOIN units AS punits ON parent_unit=punits.unit AND punits.witnessed_level >= units.witnessed_level \
                    WHERE units.unit IN({}) AND punits.unit IS NULL AND ( \
                        SELECT COUNT(*) \
                        FROM unit_witnesses \
                        WHERE unit_witnesses.unit IN(units.unit, units.witness_list_unit) AND unit_witnesses.address IN({}) \
                    )>={}",
                    alt_best_children_list,
                    witness_list,
                    config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS
                );
                let mut stmt = db.prepare(&sql)?;
                let rows = stmt
                    .query_map(&[], |row| row.get(0))?
                    .collect::<::std::result::Result<Vec<Option<u32>>, _>>()?;

                ensure!(rows.len() == 1, "not a single max alt level");
                rows[0].unwrap_or(0)
            };

            if min_mc_wl > max_alt_level {
                stable = true;
            }
        }

        if stable {
            //Advanced last stable Mc unit and try next
            mark_mc_index_stable(db, first_unstable_mc_index)?;
        } else {
            break;
        }
    }

    Ok(())
}

fn go_down_and_update_main_chain_index(db: &Connection, last_main_chain_index: u32) -> Result<()> {
    info!("goDownAndUpdateMainChainIndex start");

    let mut stmt = db.prepare_cached(
        "UPDATE units SET is_on_main_chain=0, main_chain_index=NULL WHERE main_chain_index>?",
    )?;
    stmt.execute(&[&last_main_chain_index])?;

    let mut main_chain_index = last_main_chain_index;

    let mut stmt = db.prepare_cached(
        "SELECT unit FROM units \
         WHERE is_on_main_chain=1 AND main_chain_index IS NULL ORDER BY level",
    )?;
    let rows = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    ensure!(!rows.is_empty(), "no unindexed MC units?");

    for row in &rows {
        main_chain_index += 1;
        let mut children_units = Vec::new();
        let mut units = vec![row.clone()];
        children_units.push(row.clone());
        let mut children_units_list = children_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");

        //Go up
        loop {
            let sql = format!(
                "SELECT unit \
                 FROM parenthoods JOIN units ON parent_unit=unit \
                 WHERE child_unit IN({}) AND main_chain_index IS NULL",
                children_units_list
            );
            let mut stmt = db.prepare(&sql)?;
            let mut children_rows = stmt
                .query_map(&[], |row| row.get(0))?
                .collect::<::std::result::Result<Vec<String>, _>>()?;

            if children_rows.is_empty() {
                break;
            } else {
                children_units_list = children_rows
                    .iter()
                    .map(|s| format!("'{}'", s))
                    .collect::<Vec<_>>()
                    .join(",");

                units.append(children_rows.as_mut());
            }
        }

        //Update main chain index
        let unit_list = units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE units SET main_chain_index={} WHERE unit IN({})",
            main_chain_index, unit_list
        );
        let mut stmt = db.prepare_cached(&sql)?;
        stmt.execute(&[])?;
    }

    info!("goDownAndUpdateMainChainIndex done");

    Ok(())
}

fn check_not_rebuild_stable_main_chain(db: &Connection, last_main_chain_index: u32) -> Result<()> {
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

    Ok(())
}

fn go_up_from_unit(db: &Connection, unit: Option<&String>) -> Result<(Option<String>, u32)> {
    let mut unit = unit.cloned();
    let last_main_chain_index;
    loop {
        if unit.is_some() && ::spec::is_genesis_unit(unit.as_ref().unwrap()) {
            last_main_chain_index = 0;
            break;
        } else {
            let best_parent_unit = find_next_up_main_chain_unit(db, unit.as_ref())?;
            let best_parent_unit_props = storage::read_unit_props(db, &best_parent_unit)?;

            if best_parent_unit_props.is_on_main_chain == 1 {
                last_main_chain_index = best_parent_unit_props.main_chain_index.unwrap_or(0);
                break;
            }

            let mut stmt = db.prepare_cached(
                "UPDATE units SET is_on_main_chain=1, main_chain_index=NULL WHERE unit=?",
            )?;
            stmt.execute(&[&best_parent_unit])?;

            unit = Some(best_parent_unit);
        }
    }

    Ok((unit, last_main_chain_index))
}

pub fn update_main_chain(db: &Connection, last_unit: Option<&String>) -> Result<()> {
    info!("Will Update MC");

    //Go up to find the first unit not in main chain and its best_parent's main chain index
    let (unit, last_main_chain_index) = go_up_from_unit(db, last_unit)?;

    if unit.is_some() {
        info!("checkNotRebuildingStableMainChainAndGoDown {:?}", last_unit);
        check_not_rebuild_stable_main_chain(db, last_main_chain_index)?;
        go_down_and_update_main_chain_index(db, last_main_chain_index)?;
    }

    //Update latest included mc index
    let rebuild_mc = unit.is_some();
    update_latest_included_mc_index(db, last_main_chain_index, rebuild_mc)?;

    //Update stable mc flag
    update_stable_mc_flag(db)?;

    //Finish
    info!("Done Update MC");

    Ok(())
}
