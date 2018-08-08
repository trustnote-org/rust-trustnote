use db;
use error::Result;
use rusqlite::Connection;
use serde_json;

pub fn update_wallet_address(
    db: &Connection,
    device_address: &String,
    wallet_id: &String,
    address: &String,
    address_pubk: &String,
) -> Result<()> {
    let pubk_at_device = format!("$pubkey@{}", device_address);
    let definition_template = serde_json::to_string(&json!(["sig", { "pubkey": pubk_at_device }]))?;
    // insert or ignore into wallets ('wallet', 'account', 'definition_template')
    // values('3YgAjv3h6EhFmo4hBXkmwoszR51YXHjMgUS3EQXsuY8=', 0,
    // '["sig",{"pubkey":"$pubkey@0EQ2LEGHQ6UEUYRBWLADZ6IYDNTOKRER7"}]');
    let mut stmt = db.prepare_cached(
        "INSERT OR IGNORE INTO wallets ('wallet', 'account', 'definition_template') \
         VALUES (?, 0, ?)",
    )?;
    stmt.execute(&[wallet_id, &definition_template])?;

    // insert or ignore into wallet_signing_paths ('wallet', 'signing_path', 'device_address')
    // values ('3YgAjv3h6EhFmo4hBXkmwoszR51YXHjMgUS3EQXsuY8=', 'r', '0EQ2LEGHQ6UEUYRBWLADZ6IYDNTOKRER7');
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
    // insert or ignore into my_addresses ('address', 'wallet', 'is_change', 'address_index', 'definition')
    // values ('VSZLIBN4JCI2IMTKCVEM3B7AUQEERW46', '3YgAjv3h6EhFmo4hBXkmwoszR51YXHjMgUS3EQXsuY8=', 0, 0,
    // '["sig",{"pubkey":"A925AsiHhyP0FCT2uLOW5Ezn6lQzGMG+RUrCXeIqUAMr"}]');

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionHistory {
    pub id: usize,
    pub amount: i64,
    pub address_to: String,
    pub address_from: String,
    pub confirmations: bool,
    pub fee: u32,
    pub unit: String,
    pub time: String,
    pub level: u32,
    pub mci: Option<u32>,
}

pub fn read_transaction_history(address: &str) -> Result<Vec<TransactionHistory>> {
    let mut history_transactions = Vec::new();

    let db = db::DB_POOL.get_connection();

    let mut stmt = db.prepare_cached(
        "SELECT unit, level, is_stable, sequence, address, \
            units.creation_date AS ts, headers_commission+payload_commission AS fee, \
            SUM(amount) AS amount, address AS to_address, NULL AS from_address, main_chain_index AS mci \
        FROM units JOIN outputs USING(unit) \
        WHERE address=? AND asset is NULL \
        GROUP BY unit, address \
        UNION \
        SELECT unit, level, is_stable, sequence, address, \
            units.creation_date AS ts, headers_commission+payload_commission AS fee, \
            NULL AS amount, NULL AS to_address, address AS from_address, main_chain_index AS mci \
        FROM units JOIN inputs USING(unit) \
        WHERE address=? AND asset is NULL \
        ORDER BY ts DESC",
    )?;

    #[derive(Debug)]
    struct TempRow {
        unit: String,
        level: u32,
        is_stable: u32,
        sequence: String,
        address: String,
        timestamp: String,
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
            timestamp: row.get(5),
            fee: row.get(6),
            amount: row.get(7),
            to_address: row.get(8),
            from_address: row.get(9),
            mci: row.get(10),
        })?
        .collect::<::std::result::Result<Vec<_>, _>>()?;

    let mut id = 0;
    for row in rows {
        //info!("{:?}", row);
        if row.amount.is_some() {
            let mut amount = row.amount.unwrap();

            if row.from_address.is_some() {
                amount = -amount;
            }

            if amount > 0 {

            } else {

            }

            id = id + 1;
            let transaction = TransactionHistory {
                id: id,
                amount: amount,
                address_to: row.to_address.unwrap_or(String::new()),
                address_from: row.from_address.unwrap_or(String::new()),
                confirmations: row.is_stable > 0,
                fee: row.fee,
                unit: row.unit,
                time: row.timestamp,
                level: row.level,
                mci: row.mci,
            };

            history_transactions.push(transaction);
        }
    }

    //TODO: sort by level and time

    Ok(history_transactions)
}
