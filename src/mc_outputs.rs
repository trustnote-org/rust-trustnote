use error::Result;
use rusqlite::Connection;

pub fn read_next_spendable_mc_index(
    db: &Connection,
    kind: &str,
    address: &str,
    conflict_units: &[String],
) -> Result<u32> {
    let sql = if !conflict_units.is_empty() {
        let conflict_units_list = conflict_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT to_main_chain_index FROM inputs CROSS JOIN units USING(unit) \
             WHERE type={} AND address={} AND sequence='good' \
             AND unit NOT IN({}) \
             ORDER BY to_main_chain_index DESC LIMIT 1",
            kind, address, conflict_units_list
        )
    } else {
        format!(
            "SELECT to_main_chain_index FROM inputs CROSS JOIN units USING(unit) \
             WHERE type={} AND address={} AND sequence='good' \
             ORDER BY to_main_chain_index DESC LIMIT 1",
            kind, address
        )
    };

    let mut stmt = db.prepare(&sql)?;
    let mut rows = stmt.query_map(&[], |row| row.get::<_, u32>(0))?;
    let row = rows.next();
    if row.is_none() {
        Ok(0)
    } else {
        Ok(row.unwrap()? + 1)
    }
}

pub fn read_max_spendable_mc_index(db: &Connection, kind: &str) -> Result<u32> {
    let sql = format!(
        "SELECT MAX(main_chain_index) AS max_mc_index FROM {}_outputs",
        kind
    );

    let mut stmt = db.prepare_cached(&sql)?;
    let max_mc_index = stmt.query_row(&[], |row| row.get::<_, u32>(0)).unwrap_or(0);

    Ok(max_mc_index)
}

pub struct McIndexInterval {
    pub from_mci: u32,
    pub to_mci: u32,
    pub accumulated: i64,
    pub has_sufficient: bool,
}

pub fn find_mc_index_interval_to_target_amount(
    db: &Connection,
    kind: &str,
    address: &String,
    max_mci: u32,
    target_amount: u64,
) -> Result<Option<McIndexInterval>> {
    let from_mci = read_next_spendable_mc_index(db, kind, address, &[])?;

    if from_mci > max_mci {
        return Ok(None);
    }

    let mut max_spendable_mci = read_max_spendable_mc_index(db, kind)?;
    if max_spendable_mci == 0 {
        return Ok(None);
    }

    if max_spendable_mci > max_mci {
        max_spendable_mci = max_mci;
    }

    //Original js checks whether there is a overflow
    // if (target_amount === Infinity)
    //     target_amount = 1e15;

    //Original js has another implementation for mysql
    let min_mc_output = if kind == "witnessing" { 11.0 } else { 344.0 };
    let max_count_outputs = (target_amount as f64 / min_mc_output).ceil() as i64;

    let sql = format!(
        "SELECT main_chain_index, amount \
         FROM {}_outputs \
         WHERE is_spent=0 AND address=? AND main_chain_index>=? AND main_chain_index<=? \
         ORDER BY main_chain_index LIMIT ?",
        kind
    );

    struct Row {
        main_chain_index: u32,
        amount: i64,
    }

    let mut stmt = db.prepare_cached(&sql)?;
    let rows = stmt.query_map(
        &[address, &from_mci, &max_spendable_mci, &max_count_outputs],
        |row| Row {
            main_chain_index: row.get(0),
            amount: row.get(1),
        },
    )?;

    let mut outputs = Vec::new();
    for row in rows {
        outputs.push(row?);
    }

    if outputs.is_empty() {
        return Ok(None);
    }

    let mut accumulated = 0;
    let mut to_mci = 0;
    let mut has_sufficient = false;
    for output in outputs {
        accumulated += output.amount;
        to_mci = output.main_chain_index;
        if accumulated as u64 > target_amount {
            has_sufficient = true;
            break;
        }
    }

    Ok(Some(McIndexInterval {
        from_mci,
        to_mci,
        accumulated,
        has_sufficient,
    }))
}

pub fn calc_earnings(
    db: &Connection,
    kind: &str,
    from_main_chain_index: u32,
    to_main_chain_index: u32,
    address: &String,
) -> Result<u32> {
    let sql = format!(
        "SELECT SUM(amount) AS total FROM {}_outputs \
         WHERE main_chain_index>=? AND main_chain_index<=? \
         AND address=?",
        kind
    );

    let mut stmt = db.prepare_cached(&sql)?;
    let total = stmt
        .query_row(
            &[&from_main_chain_index, &to_main_chain_index, address],
            |row| row.get::<_, u32>(0),
        ).unwrap_or(0);

    Ok(total)
}
