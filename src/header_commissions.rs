use error::Result;
use rusqlite::Connection;
use std::collections::HashMap;

struct ChildInfo {
    child_unit: String,
    next_mc_unit: String,
}

struct ChildrenInfo {
    headers_commission: u32,
    children: Vec<ChildInfo>,
}

static mut MAX_SPENDABLE_MCI: Option<u32> = None;

fn init_max_spendable_mci(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT MAX(main_chain_index) AS max_spendable_mci FROM headers_commission_outputs",
    )?;
    let mci = stmt.query_row(&[], |row| row.get(0)).unwrap_or(0);

    unsafe {
        MAX_SPENDABLE_MCI = Some(mci);
    }

    Ok(())
}

fn get_winner_info<'a>(children: &'a mut Vec<ChildInfo>) -> Result<&'a ChildInfo> {
    if children.len() == 1 {
        return Ok(&children[0]);
    }

    use sha1::Sha1;
    children.sort_by_key(|child| {
        let mut m = Sha1::new();
        m.update(child.child_unit.as_bytes());
        m.update(child.next_mc_unit.as_bytes());
        m.digest().to_string()
    });

    Ok(&children[0])
}

pub fn calc_headers_commissions(db: &Connection) -> Result<()> {
    let since_mc_index;
    unsafe {
        if MAX_SPENDABLE_MCI.is_none() {
            init_max_spendable_mci(db)?;
        }
        since_mc_index = MAX_SPENDABLE_MCI.unwrap();
    }

    let sql = format!(
    // chunits is any child unit and contender for headers commission, punits is hc-payer unit
        "SELECT chunits.unit AS child_unit, punits.headers_commission, next_mc_units.unit AS next_mc_unit, punits.unit AS payer_unit \
        FROM units AS chunits \
        JOIN parenthoods ON chunits.unit=parenthoods.child_unit \
        JOIN units AS punits ON parenthoods.parent_unit=punits.unit \
        JOIN units AS next_mc_units ON next_mc_units.is_on_main_chain=1 AND next_mc_units.main_chain_index=punits.main_chain_index+1 \
        WHERE chunits.is_stable=1 \
            AND +chunits.sequence='good' \
            AND punits.main_chain_index>? \
            AND +punits.sequence='good' \
            AND punits.is_stable=1 \
            AND chunits.main_chain_index-punits.main_chain_index<=1 \
            AND next_mc_units.is_stable=1"
    );

    struct Row {
        child_unit: String,
        headers_commission: u32,
        next_mc_unit: String,
        payer_unit: String,
    }

    let mut stmt = db.prepare_cached(&sql)?;
    let rows = stmt.query_map(&[&since_mc_index], |row| Row {
        child_unit: row.get(0),
        headers_commission: row.get(1),
        next_mc_unit: row.get(2),
        payer_unit: row.get(3),
    })?;

    let mut assoc_children_infos = HashMap::<String, ChildrenInfo>::new();
    for row in rows {
        let row = row?;
        let payer_unit = row.payer_unit;
        let child_unit = row.child_unit;

        let info = assoc_children_infos
            .entry(payer_unit)
            .or_insert(ChildrenInfo {
                headers_commission: row.headers_commission,
                children: Vec::new(),
            });

        ensure!(
            info.headers_commission == row.headers_commission,
            "different headers_commission"
        );

        info.children.push(ChildInfo {
            child_unit: child_unit,
            next_mc_unit: row.next_mc_unit,
        })
    }

    //Create a nested HashMap, first key by child_unit then key by payer_unit
    let mut assoc_won_amounts = HashMap::new();
    for (payer_unit, children_info) in assoc_children_infos.iter_mut() {
        let headers_commission = children_info.headers_commission;
        let winner_child_info = get_winner_info(&mut children_info.children);
        let child_unit = &winner_child_info?.child_unit;

        let amount_map = assoc_won_amounts
            .entry(child_unit)
            .or_insert(HashMap::<String, u32>::new());

        amount_map.insert(payer_unit.to_string(), headers_commission);
    }

    if assoc_won_amounts.keys().len() > 0 {
        let winner_units_list = assoc_won_amounts
            .keys()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");

        let sql =
            format!(
            "SELECT \
                unit_authors.unit, \
                unit_authors.address, \
                100 AS earned_headers_commission_share \
            FROM unit_authors \
            LEFT JOIN earned_headers_commission_recipients USING(unit) \
            WHERE unit_authors.unit IN({}) AND earned_headers_commission_recipients.unit IS NULL \
            UNION ALL \
            SELECT \
                unit, \
                address, \
                earned_headers_commission_share \
            FROM earned_headers_commission_recipients \
            WHERE unit IN({})", winner_units_list, winner_units_list);

        let mut stmt = db.prepare(&sql)?;

        struct Row {
            unit: String,
            address: String,
            earned_headers_commission_share: u32,
        }

        let rows = stmt.query_map(&[], |row| Row {
            unit: row.get(0),
            address: row.get(1),
            earned_headers_commission_share: row.get(2),
        })?;

        let mut values = Vec::new();
        for row in rows {
            let row = row?;
            let child_unit = row.unit;

            let entry = assoc_won_amounts.get(&child_unit);
            ensure!(entry.is_some(), "no amount for child unit {}", child_unit);
            let entry = entry.unwrap();

            for payer_unit in entry.keys() {
                let full_amount = entry.get(payer_unit);
                ensure!(
                    full_amount.is_some(),
                    "no amount for child unit {} and payer unit {}",
                    child_unit,
                    payer_unit
                );
                let full_amount = *full_amount.unwrap();

                let amount = if row.earned_headers_commission_share == 100 {
                    full_amount
                } else {
                    (full_amount as f64 * row.earned_headers_commission_share as f64 / 100.0)
                        .round() as u32
                };

                let value = format!("('{}','{}','{}')", payer_unit, row.address, amount);
                values.push(value);
            }
        }

        let value_list = values
            .iter()
            .map(|s| format!("{}", s))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO headers_commission_contributions (unit, address, amount) VALUES {}",
            value_list
        );

        let mut stmt = db.prepare(&sql)?;
        stmt.execute(&[])?;
    }

    let mut stmt = db.prepare_cached(
            "INSERT INTO headers_commission_outputs (main_chain_index, address, amount) \
                SELECT main_chain_index, address, SUM(amount) FROM headers_commission_contributions JOIN units USING(unit) \
                WHERE main_chain_index>? \
                GROUP BY main_chain_index, address")?;
    stmt.execute(&[&since_mc_index])?;

    stmt = db.prepare_cached(
        "SELECT MAX(main_chain_index) AS max_spendable_mci FROM headers_commission_outputs",
    )?;

    unsafe {
        MAX_SPENDABLE_MCI = Some(stmt.query_row(&[], |row| row.get(0))?);
    }

    Ok(())
}

pub fn get_max_spendable_mci_for_last_ball_mci(last_ball_mci: u32) -> Result<u32> {
    Ok(last_ball_mci - 1)
}
