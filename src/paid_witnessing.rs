use config;
use error::Result;
use graph;
use mc_outputs;
use rusqlite::Connection;
use storage;

pub fn calc_witness_earnings(
    db: &Connection,
    kind: &String,
    from_main_chain_index: u32,
    to_main_chain_index: u32,
    address: &String,
) -> Result<u32> {
    let mut stmt = db.prepare_cached(
        "SELECT COUNT(*) AS count FROM units \
         WHERE is_on_main_chain=1 AND is_stable=1 \
         AND main_chain_index>=? AND main_chain_index<=?",
    )?;

    let count = stmt
        .query_row(
            &[
                &to_main_chain_index,
                &(to_main_chain_index + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING + 1),
            ],
            |row| row.get(0),
        ).unwrap_or(0);

    ensure!(
        count == config::COUNT_MC_BALLS_FOR_PAID_WITNESSING + 2,
        "not enough stable MC units after to_main_chain_index"
    );

    mc_outputs::calc_earnings(
        db,
        kind,
        from_main_chain_index,
        to_main_chain_index,
        address,
    )
}

pub fn get_max_spendable_mci_for_last_ball_mci(last_ball_mci: u32) -> Option<u32> {
    last_ball_mci.checked_sub(1 + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING)
}

fn read_mc_unit_witnesses(db: &Connection, main_chain_index: u32) -> Result<Vec<String>> {
    let mut stmt = db.prepare_cached(
        "SELECT witness_list_unit, unit FROM units \
         WHERE main_chain_index=? AND is_on_main_chain=1",
    )?;

    struct Row {
        witness_list_unit: Option<String>,
        unit: String,
    }

    let rows = stmt.query_map(&[&main_chain_index], |row| Row {
        witness_list_unit: row.get(0),
        unit: row.get(1),
    })?;

    let mut units = Vec::new();
    for row in rows {
        units.push(row?);
    }

    ensure!(units.len() == 1, "not 1 row on MC {}", main_chain_index);

    let unit = if units[0].witness_list_unit.is_some() {
        units[0].witness_list_unit.as_ref().unwrap().clone()
    } else {
        units[0].unit.clone()
    };

    storage::read_witness_list(db, &unit)
}

fn build_paid_witnesses(
    db: &Connection,
    unit_prop: graph::UnitProps,
    witnesses: &[String],
) -> Result<()> {
    let main_chain_index = unit_prop.main_chain_index.unwrap_or(0);
    let to_main_chain_index = main_chain_index + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING;

    let units = graph::read_descendant_units_by_authors_before_mc_index(
        db,
        &unit_prop,
        &witnesses,
        to_main_chain_index,
    )?;

    let unit = unit_prop.unit;
    let unit_list = units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let witness_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let mut paid_witnesses = Vec::new();
    let value_list;

    if !units.is_empty() {
        let sql = format!(
            "SELECT address, MIN(main_chain_index-{}) AS delay FROM units \
             LEFT JOIN unit_authors USING(unit) \
             WHERE unit IN({}) AND address IN({}) AND +sequence='good' \
             GROUP BY address",
            main_chain_index, unit_list, witness_list
        );

        struct UnitProps {
            address: String,
            delay: u32,
        }

        let mut stmt = db.prepare(&sql)?;
        let rows = stmt.query_map(&[], |row| UnitProps {
            address: row.get(0),
            delay: row.get(1),
        })?;

        for row in rows {
            paid_witnesses.push(row?);
        }
    }

    let mut count_paid_witnesses = paid_witnesses.len() as u32;

    //If the query result is empty or no query at all
    if count_paid_witnesses == 0 {
        count_paid_witnesses = witnesses.len() as u32;
        value_list = witnesses
            .iter()
            .map(|s| format!("('{}', '{}', NULL)", unit, s))
            .collect::<Vec<_>>()
            .join(", ");
    } else {
        value_list = paid_witnesses
            .iter()
            .map(|s| format!("('{}', '{}', {})", unit, s.address, s.delay))
            .collect::<Vec<_>>()
            .join(", ");
    }

    let sql = format!(
        "INSERT INTO paid_witness_events_tmp (unit, address, delay) VALUES {}",
        value_list
    );
    let mut stmt = db.prepare(&sql)?;
    stmt.execute(&[])?;

    //update count paid witnesses
    let mut stmt = db.prepare_cached("UPDATE balls SET count_paid_witnesses=? WHERE unit=?")?;
    stmt.execute(&[&count_paid_witnesses, &unit])?;

    Ok(())
}

fn build_paid_witnesses_for_main_chain_index(db: &Connection, main_chain_index: u32) -> Result<()> {
    info!("updating paid witnesses mci {}", main_chain_index);
    let mut stmt = db.prepare_cached(
        "SELECT COUNT(*) AS count, \
         SUM(CASE WHEN is_stable=1 THEN 1 ELSE 0 END) AS count_on_stable_mc \
         FROM units WHERE is_on_main_chain=1 \
         AND main_chain_index>=? AND main_chain_index<=?",
    )?;

    let (count, count_on_stable_mc) = stmt.query_row(
        &[
            &main_chain_index,
            &(main_chain_index + config::COUNT_MC_BALLS_FOR_PAID_WITNESSING + 1),
        ],
        |row| (row.get::<_, u32>(0), row.get::<_, u32>(1)),
    )?;

    ensure!(
        count == config::COUNT_MC_BALLS_FOR_PAID_WITNESSING + 2,
        "main chain is not long enough yet for MC index {}",
        main_chain_index
    );

    ensure!(
        count_on_stable_mc == count,
        "not enough stable MC units yet after MC index {}: count_on_stable_mc={}, count={}",
        main_chain_index,
        count_on_stable_mc,
        count
    );

    let witnesses = read_mc_unit_witnesses(db, main_chain_index)?;

    let mut stmt = db.prepare_cached(
        "CREATE TEMPORARY TABLE paid_witness_events_tmp ( \
         unit CHAR(44) NOT NULL, \
         address CHAR(32) NOT NULL, \
         delay TINYINT NULL)",
    )?;
    stmt.execute(&[])?;

    struct TablePaidWitnessEventsTmp<'a> {
        db: &'a Connection,
    }
    impl<'a> Drop for TablePaidWitnessEventsTmp<'a> {
        fn drop(&mut self) {
            let _ = self
                .db
                .prepare_cached("DROP TABLE IF EXISTS paid_witness_events_tmp")
                .and_then(|mut stmt| stmt.execute(&[]));
        }
    }
    let _tmp_table = TablePaidWitnessEventsTmp { db };

    //In build_paid_witnesses(), graph::UnitProp only use some of the columns, no need to select * and parse them all
    let mut stmt = db.prepare_cached(
        "SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain, is_free \
        FROM units WHERE main_chain_index=?",
        )?;
    let rows = stmt.query_map(&[&main_chain_index], |row| graph::UnitProps {
        unit: row.get(0),
        level: row.get(1),
        latest_included_mc_index: row.get(2),
        main_chain_index: row.get(3),
        is_on_main_chain: row.get(4),
        is_free: row.get(5),
    })?;

    for row in rows {
        let unit_prop = row?;

        build_paid_witnesses(db, unit_prop, &witnesses)?;
    }

    let mut stmt = db.prepare_cached(
            "INSERT INTO witnessing_outputs (main_chain_index, address, amount) \
            SELECT main_chain_index, address, \
            SUM(CASE WHEN sequence='good' THEN ROUND(1.0*payload_commission/count_paid_witnesses) ELSE 0 END) \
            FROM balls \
            JOIN units USING(unit) \
            JOIN paid_witness_events_tmp USING(unit) \
            WHERE main_chain_index=? \
            GROUP BY address"
        )?;
    stmt.execute(&[&main_chain_index])?;

    Ok(())
}

fn build_paid_witnesses_till_main_chain_index(
    db: &Connection,
    to_main_chain_index: u32,
) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT MIN(main_chain_index) AS min_main_chain_index FROM balls \
         CROSS JOIN units USING(unit) \
         WHERE count_paid_witnesses IS NULL",
    )?;

    let mut main_chain_index = stmt.query_row(&[], |row| row.get(0))?;
    while main_chain_index <= to_main_chain_index {
        build_paid_witnesses_for_main_chain_index(db, main_chain_index)?;
        main_chain_index += 1;
    }

    Ok(())
}

pub fn update_paid_witnesses(db: &Connection) -> Result<()> {
    info!("updating paid witnesses");
    let last_stable_mci = storage::read_last_stable_mc_index(db)?;
    let max_spendable_mc_index = get_max_spendable_mci_for_last_ball_mci(last_stable_mci);

    if max_spendable_mc_index.is_some() {
        build_paid_witnesses_till_main_chain_index(db, max_spendable_mc_index.unwrap())?;
    }

    Ok(())
}
