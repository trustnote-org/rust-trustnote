use std::collections::HashMap;
use std::sync::Arc;

use composer::{self, ComposeInfo};
use error::Result;
use network::wallet::WalletConn;
use rusqlite::Connection;
use serde_json;
use spec::Output;

pub fn update_wallet_address(
    db: &Connection,
    device_address: &String,
    wallet_id: &String,
    address: &String,
    address_pubk: &String,
) -> Result<()> {
    let pubk_at_device = format!("$pubkey@{}", device_address);
    let definition_template = serde_json::to_string(&json!(["sig", { "pubkey": pubk_at_device }]))?;
    let mut stmt = db.prepare_cached(
        "INSERT OR IGNORE INTO wallets ('wallet', 'account', 'definition_template') \
         VALUES (?, 0, ?)",
    )?;
    stmt.execute(&[wallet_id, &definition_template])?;

    let mut stmt = db.prepare_cached(
        "INSERT OR IGNORE INTO wallet_signing_paths ('wallet', 'signing_path', 'device_address') \
         VALUES (?, 'r', ?)",
    )?;
    stmt.execute(&[wallet_id, device_address])?;

    let definition = serde_json::to_string(&json!(["sig", { "pubkey": address_pubk }]))?;
    let mut stmt = db.prepare_cached(
        "INSERT OR IGNORE INTO my_addresses ('address', 'wallet', 'is_change', 'address_index', 'definition') \
         VALUES (?, ?, 0, 0, ?)")?;
    stmt.execute(&[address, wallet_id, &definition])?;

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionHistory {
    pub amount: i64,
    pub address_to: String,
    pub address_from: String,
    pub confirmations: bool,
    pub fee: u32,
    pub unit: String,
    pub timestamp: i64,
    pub level: Option<u32>,
    pub mci: Option<u32>,
}

pub fn read_transaction_history(db: &Connection, address: &str) -> Result<Vec<TransactionHistory>> {
    let mut history_transactions = Vec::new();

    let mut stmt = db.prepare_cached(
        "SELECT unit, level, is_stable, sequence, address, \
            strftime('%s', units.creation_date) AS ts, headers_commission+payload_commission AS fee, \
            SUM(amount) AS amount, address AS to_address, NULL AS from_address, main_chain_index AS mci \
        FROM units JOIN outputs USING(unit) \
        WHERE address=? AND asset is NULL \
        GROUP BY unit, address \
        UNION \
        SELECT unit, level, is_stable, sequence, address, \
            strftime('%s', units.creation_date) AS ts, headers_commission+payload_commission AS fee, \
            NULL AS amount, NULL AS to_address, address AS from_address, main_chain_index AS mci \
        FROM units JOIN inputs USING(unit) \
        WHERE address=? AND asset is NULL \
        ORDER BY ts DESC",
    )?;

    #[derive(Debug)]
    struct TempRow {
        unit: String,
        level: Option<u32>,
        is_stable: u32,
        sequence: String,
        address: String,
        timestamp: i64,
        fee: u32,
        amount: Option<i64>,
        to_address: Option<String>,
        from_address: Option<String>,
        mci: Option<u32>,
    };

    let rows = stmt
        .query_map(&[&address, &address], |row| TempRow {
            unit: row.get(0),
            level: row.get(1),
            is_stable: row.get(2),
            sequence: row.get(3),
            address: row.get(4),
            timestamp: row.get::<_, String>(5).parse::<i64>().unwrap() * 1000,
            fee: row.get(6),
            amount: row.get(7),
            to_address: row.get(8),
            from_address: row.get(9),
            mci: row.get(10),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    let mut movements = HashMap::new();
    for row in rows {
        //debug!("{:?}", row);
        struct Movement {
            plus: i64,
            has_minus: bool,
            timestamp: i64,
            level: Option<u32>,
            is_stable: bool,
            sequence: String,
            fee: u32,
            mci: Option<u32>,
            from_address: Option<String>,
            to_address: Option<String>,
            amount: Option<i64>,
        };

        let has_to_address = row.to_address.is_some();
        let has_from_address = row.from_address.is_some();

        let mut movement = movements.entry(row.unit).or_insert(Movement {
            plus: 0,
            has_minus: false,
            timestamp: row.timestamp,
            level: row.level,
            is_stable: row.is_stable > 0,
            sequence: row.sequence,
            fee: row.fee,
            mci: row.mci,
            from_address: row.from_address,
            to_address: row.to_address,
            amount: row.amount,
        });

        if has_to_address {
            movement.plus = movement.plus + row.amount.unwrap_or(0);
        }

        if has_from_address {
            movement.has_minus = true;
        }
    }

    for (unit, movement) in movements.into_iter() {
        //TODO: handle invalid case
        let _sequence = movement.sequence;

        //Receive
        if movement.plus > 0 && !movement.has_minus {
            let mut stmt = db.prepare_cached(
                "SELECT DISTINCT address FROM inputs \
                 WHERE unit=? AND asset is NULL ORDER BY address",
            )?;

            let addresses = stmt
                .query_map(&[&unit], |row| row.get(0))?
                .collect::<::std::result::Result<Vec<String>, _>>()?;

            for address in addresses {
                let transaction = TransactionHistory {
                    amount: movement.amount.unwrap_or(0),
                    address_to: movement
                        .to_address
                        .as_ref()
                        .cloned()
                        .unwrap_or(String::new()),
                    address_from: address,
                    confirmations: movement.is_stable,
                    fee: movement.fee,
                    unit: unit.clone(),
                    timestamp: movement.timestamp,
                    level: movement.level,
                    mci: movement.mci,
                };

                history_transactions.push(transaction);
            }
        } else if movement.has_minus {
            //The amount is none when sending out
            let mut stmt = db.prepare_cached(
                "SELECT address, SUM(amount) AS amount, (address!=?) AS is_external \
                 FROM outputs \
                 WHERE unit=? AND asset is NULL \
                 GROUP BY address",
            )?;

            #[derive(Debug)]
            struct PayeeRows {
                address: Option<String>,
                amount: Option<i64>,
                is_external: bool,
            }

            let payee_rows = stmt
                .query_map(&[&address, &unit], |row| PayeeRows {
                    address: row.get(0),
                    amount: row.get(1),
                    is_external: row.get(2),
                })?.collect::<::std::result::Result<Vec<_>, _>>()?;

            for payee_row in payee_rows {
                //debug!("{:?}", payee_row);

                if !payee_row.is_external {
                    continue;
                }

                let transaction = TransactionHistory {
                    amount: -payee_row.amount.unwrap_or(0),
                    address_to: payee_row.address.unwrap_or(String::new()),
                    address_from: movement
                        .from_address
                        .as_ref()
                        .cloned()
                        .unwrap_or(String::new()),
                    confirmations: movement.is_stable,
                    fee: movement.fee,
                    unit: unit.clone(),
                    timestamp: movement.timestamp,
                    level: movement.level,
                    mci: movement.mci,
                };

                history_transactions.push(transaction);
            }
        }
    }

    //Should sort by level and time, but level is None in light wallet
    history_transactions.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(history_transactions)
}

// return values: first is unstable balance, second is stable balance.
pub fn get_balance(db: &Connection, address: &str) -> Result<(i64, i64)> {
    let mut stmt = db.prepare_cached(
        "SELECT asset, is_stable, SUM(amount) AS balance \
         FROM outputs JOIN units USING(unit) \
         WHERE is_spent=0 AND address=? AND sequence='good' AND asset IS NULL \
         GROUP BY is_stable",
    )?;

    let rows = stmt
        .query_map(&[&address], |row| row.get(2))?
        .collect::<::std::result::Result<Vec<i64>, _>>()?;

    match rows.len() {
        2 => return Ok((rows[0], rows[1])),
        1 => return Ok((0, rows[0])),
        _ => return Ok((0, 0)),
    }
}

pub fn prepare_payment(
    ws: &Arc<WalletConn>,
    address_amount: &Vec<(&str, f64)>,
    text: Option<&str>,
    wallet_info_address: &str,
) -> Result<ComposeInfo> {
    let mut outputs = Vec::new();
    for (address, amount) in address_amount.into_iter() {
        outputs.push(Output {
            address: address.to_string(),
            amount: (amount * 1_000_000.0).round() as i64,
        });
    }
    let amounts = outputs.iter().fold(0, |acc, x| acc + x.amount);
    outputs.push(Output {
        address: wallet_info_address.to_string(),
        amount: 0,
    });

    let light_props = match ws.get_parents_and_last_ball_and_witness_list_unit() {
        Ok(res) => {
            if res.parent_units.is_empty()
                || res.last_stable_mc_ball.is_none()
                || res.last_stable_mc_ball_unit.is_none()
            {
                bail!("invalid parents or last stable mc ball");
            }
            res
        }
        Err(e) => bail!(
            "err : get_parents_and_last_ball_and_witness_list_unit err:{:?}",
            e
        ),
    };

    let messages = if text.is_some() {
        vec![composer::create_text_message(&text.unwrap().to_string())?]
    } else {
        vec![]
    };

    Ok(ComposeInfo {
        paying_addresses: vec![wallet_info_address.to_string()],
        input_amount: amounts as u64,
        signing_addresses: Vec::new(),
        outputs: outputs,
        messages,
        light_props: light_props,
        earned_headers_commission_recipients: Vec::new(),
        witnesses: Vec::new(),
        inputs: Vec::new(),
        send_all: false, // FIXME: now send_all is always false
    })
}
