use db;
use error::Result;
use serde_json;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionHistory {
    pub id: usize,
    pub amount: Option<i64>,
    pub timestamp: String,
}

pub fn read_transaction_history(address: &str, index: usize) -> Result<Vec<String>> {
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
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    let mut id = 0;
    for row in rows {
        //info!("{:?}", row);
        if row.amount.is_some() {
            id = id + 1;
            let transaction = TransactionHistory {
                id: id,
                amount: row.amount,
                timestamp: row.timestamp,
            };

            history_transactions.push(serde_json::to_string_pretty(&transaction)?);
        }
    }

    if index > 0 && index <= history_transactions.len() {
        return Ok(vec![history_transactions[index - 1].clone()]);
    }

    Ok(history_transactions)
}
