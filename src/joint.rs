use std::collections::HashSet;

use db;
use definition;
use error::Result;
use may::sync::Mutex;
use object_hash::get_chash;
use rusqlite::Transaction;
use serde_json;
use spec::*;

lazy_static! {
    static ref WRITER_MUTEX: Mutex<()> = Mutex::new(());
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Joint {
    pub ball: Option<String>,
    pub skiplist_units: Option<Vec<String>>,
    pub unit: Unit,
    // TODO: can we move unit_hash to here from unit sub filed?
}

impl Joint {
    #[inline]
    fn get_unit_hash(&self) -> &String {
        self.unit.unit.as_ref().unwrap()
    }

    fn save_unit(&self, tx: &Transaction, sequence: &String) -> Result<()> {
        let unit = &self.unit;
        let unit_hash = self.get_unit_hash();

        let mut stmt = tx.prepare_cached(
            "INSERT INTO units \
             (unit, version, alt, witness_list_unit, last_ball_unit, \
             headers_commission, payload_commission, sequence, content_hash) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;

        stmt.insert(&[
            unit_hash,
            &unit.version,
            &unit.alt,
            &unit.witness_list_unit,
            &unit.last_ball_unit,
            &unit.headers_commission,
            &unit.payload_commission,
            sequence,
            &unit.content_hash,
        ])?;
        Ok(())
    }

    fn save_ball(&self, tx: &Transaction) -> Result<()> {
        if self.ball.is_none() {
            return Ok(());
        }

        let unit_hash = self.get_unit_hash();
        let ball_hash = self.ball.as_ref().unwrap();

        let mut stmt = tx.prepare_cached("INSERT INTO balls (ball, unit) VALUES (?, ?)")?;
        stmt.insert(&[ball_hash, unit_hash])?;

        let mut stmt = tx.prepare_cached("DELETE FROM hash_tree_balls WHERE ball=? AND unit=?")?;
        stmt.execute(&[ball_hash, unit_hash])?;

        if self.skiplist_units.is_some() {
            for unit in self.skiplist_units.as_ref().unwrap() {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO skiplist_units (unit, skiplist_unit) VALUES (?, ?)",
                )?;
                stmt.insert(&[unit_hash, unit])?;
            }
        }

        Ok(())
    }

    fn save_parents(&self, tx: &Transaction) -> Result<()> {
        let unit = &self.unit;
        let unit_hash = self.get_unit_hash();

        for parent in &unit.parent_units {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO parenthoods (child_unit, parent_unit) VALUES (?, ?)",
            )?;
            stmt.insert(&[unit_hash, parent])?;
        }

        if unit.is_genesis_unit() {
            #[cold]
            {
                let mut stmt = tx.prepare(
                    "UPDATE units SET \
                     is_on_main_chain=1, main_chain_index=0, \
                     is_stable=1, level=0, witnessed_level=0 \
                     WHERE unit=?",
                )?;
                stmt.execute(&[unit_hash])?;
            }
        } else {
            let parents_set = unit.parent_units
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");
            // TODO: how to pass a set parameter?
            let sql = format!("UPDATE units SET is_free=0 WHERE unit IN ({})", parents_set);
            let rows = tx.execute(&sql, &[])?;
            info!("{} free units consumed", rows);
        }
        Ok(())
    }

    // return a vec of author address
    fn save_authors(&self, tx: &Transaction) -> Result<()> {
        let unit_hash = self.get_unit_hash();
        for author in &self.unit.authors {
            let definition = &author.definition;
            let definition_chash = get_chash(definition)?;
            let mut stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO definitions \
                 (definition_chash, definition, has_references) \
                 VALUES (?, ?, ?)",
            )?;
            let definition_json = serde_json::to_string(definition)?;
            let has_references = definition::has_references(definition)? as u8;
            stmt.insert(&[&definition_chash, &definition_json, &has_references])?;

            // TODO: we ingore unit.content_hash here
            if definition_chash == author.address {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO addresses (address) \
                     VALUES (?)",
                )?;
                stmt.insert(&[&author.address])?;
            }

            let mut stmt = tx.prepare_cached(
                "INSERT INTO unit_authors \
                 (unit, address, definition_chash) \
                 VALUES(?, ?, ?)",
            )?;
            stmt.insert(&[unit_hash, &author.address, &definition_chash])?;

            for (path, authentifier) in &author.authentifiers {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO authentifiers \
                     (unit, address, path, authentifier) \
                     VALUES(?, ?, ?, ?)",
                )?;
                stmt.insert(&[unit_hash, &author.address, path, authentifier])?;
            }
        }
        Ok(())
    }

    fn save_messages(&self, tx: &Transaction) -> Result<()> {
        let unit_hash = self.get_unit_hash();
        for (i, message) in self.unit.messages.iter().enumerate() {
            let text_payload = match message.app.as_str() {
                "text" => unimplemented!(),
                "data" | "profile" | "attestation" | "definition_template" => {
                    let payload = serde_json::to_string(&message.payload)?;
                    Some(payload)
                }
                _ => None,
            };

            let mut stmt = tx.prepare_cached(
                "INSERT INTO messages \
                 (unit, message_index, app, payload_hash, payload_location, \
                 payload, payload_uri, payload_uri_hash) \
                 VALUES(?,?,?,?,?,?,?,?)",
            )?;
            stmt.insert(&[
                unit_hash,
                &(i as u32),
                &message.app,
                &message.payload_hash,
                &message.payload_location,
                &text_payload,
                &message.payload_uri,
                &message.payload_uri_hash,
            ])?;

            if message.payload_location.as_str() == "inline" {
                match message.app.as_str() {
                    "payment" => {}
                    _ => unimplemented!(),
                }
            }

            // TODO: add spend_proofs
            /*
            if ("spend_proofs" in message){
					for (var j=0; j<message.spend_proofs.length; j++){
						var objSpendProof = message.spend_proofs[j];
						conn.addQuery(arrQueries, 
							"INSERT INTO spend_proofs (unit, message_index, spend_proof_index, spend_proof, address) VALUES(?,?,?,?,?)", 
							[objUnit.unit, i, j, objSpendProof.spend_proof, objSpendProof.address || arrAuthorAddresses[0] ]);
					}
				}
            */
            // we dont't have spend_proofs now
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn save_header_earnings(&self, _tx: &Transaction) -> Result<()> {
        // TODO: unimplemented!()

        /*
        if ("earned_headers_commission_recipients" in objUnit){
			for (var i=0; i<objUnit.earned_headers_commission_recipients.length; i++){
				var recipient = objUnit.earned_headers_commission_recipients[i];
				conn.addQuery(arrQueries, 
					"INSERT INTO earned_headers_commission_recipients (unit, address, earned_headers_commission_share) VALUES(?,?,?)", 
					[objUnit.unit, recipient.address, recipient.earned_headers_commission_share]);
			}
		}
        */
        Ok(())
    }

    fn update_best_parent(&self, tx: &Transaction) -> Result<String> {
        let unit = &self.unit;
        let parents_set = unit.parent_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        // TODO: witness list is fixed
        let sql = format!(
            "SELECT unit \
             FROM units AS parent_units \
             WHERE unit IN({}) AND (witness_list_unit=?) \
             ORDER BY \
             witnessed_level DESC, \
             level-witnessed_level ASC, \
             unit ASC \
             LIMIT 1",
            parents_set
        );

        let best_parent_unit: String =
            tx.query_row(&sql, &[&unit.witness_list_unit], |row| row.get(0))?;

        let mut stmt = tx.prepare_cached("UPDATE units SET best_parent_unit=? WHERE unit=?")?;
        stmt.execute(&[&best_parent_unit, self.get_unit_hash()])?;
        Ok(best_parent_unit)
    }

    fn update_level(&self, tx: &Transaction) -> Result<()> {
        let parents_set = self.unit
            .parent_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        // TODO: witness list is fixed
        let sql = format!(
            "SELECT MAX(level) AS max_level FROM units WHERE unit IN({})",
            parents_set
        );

        let unit_level = tx.query_row(&sql, &[], |row| row.get::<_, u32>(0) + 1)?;

        let mut stmt = tx.prepare_cached("UPDATE units SET level=? WHERE unit=?")?;
        stmt.execute(&[&unit_level, self.get_unit_hash()])?;
        Ok(())
    }

    fn save_witness_level(&self, tx: &Transaction, witness_level: u32) -> Result<()> {
        let mut stmt = tx.prepare_cached("UPDATE units SET witnessed_level=? WHERE unit=?")?;
        stmt.execute(&[&witness_level, self.get_unit_hash()])?;
        Ok(())
    }

    fn update_witness_level_by_witness_list(
        &self,
        tx: &Transaction,
        witness_list: &[String],
        mut best_parent_unit: String,
    ) -> Result<()> {
        let mut collected_witnesses = HashSet::<String>::new();
        loop {
            let props = ::storage::get_static_unit_property(&best_parent_unit, tx)?;
            let authors = ::storage::get_unit_authors(&best_parent_unit, tx)?;
            let level = props.level;

            // genesis
            if level == 0 {
                return self.save_witness_level(tx, 0);
            }

            for address in authors {
                if !witness_list.contains(&address) && !collected_witnesses.contains(&address) {
                    collected_witnesses.insert(address);
                }
            }

            if collected_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES {
                return self.save_witness_level(tx, level);
            }

            // search next best parent
            best_parent_unit = props.best_parent_unit;
        }
    }

    fn update_witness_level(&self, tx: &Transaction, best_parent_unit: String) -> Result<()> {
        match self.unit.witnesses {
            Some(ref witness_list) => {
                self.update_witness_level_by_witness_list(tx, witness_list, best_parent_unit)
            }
            None => {
                let witness_list = ::storage::get_witness_list(self.get_unit_hash(), tx)?;
                self.update_witness_level_by_witness_list(tx, &witness_list, best_parent_unit)
            }
        }
    }

    fn save_inline_payment(&self, tx: &Transaction) -> Result<()> {
        let mut author_addresses = vec![];
        for author in &self.unit.authors {
            author_addresses.push(&author.address);
        }

        for (i, message) in self.unit.messages.iter().enumerate() {
            if message.payload_location.as_str() != "inline" || message.app.as_str() != "payment" {
                continue;
            }

            let payload = &message.payload;
            let denomination = payload.denomination.unwrap_or(1);

            for (j, input) in payload.inputs.iter().enumerate() {
                let default_kind = String::from("transfer");
                let kind = input.kind.as_ref().unwrap_or(&default_kind);
                let src_unit = some_if!(kind == "transfer", input.unit.clone());
                let src_message_index = some_if!(kind == "transfer", input.message_index);
                let src_output_index = some_if!(kind == "transfer", input.output_index);
                let from_main_chain_index = if kind == "witnessing" || kind == "headers_commission"
                {
                    input.from_main_chain_index
                } else {
                    None
                };
                let to_main_chain_index = if kind == "witnessing" || kind == "headers_commission" {
                    input.to_main_chain_index
                } else {
                    None
                };

                let address = if author_addresses.len() == 1 {
                    author_addresses[0].clone()
                } else {
                    match kind.as_str() {
                        "headers_commission" | "witnessing" | "issue" => unimplemented!(), //input.address.clone(),
                        _ => self.determine_input_address_from_output(
                            tx,
                            payload.asset.as_ref().unwrap(),
                            denomination,
                            &input,
                        )?,
                    }
                };

                // TODO: objValidationState.arrDoubleSpendInputs.some(...)
                // here we give it a unique as default
                let is_unique = 1;

                let mut stmt = tx.prepare_cached(
                    "INSERT INTO inputs \
                     (unit, message_index, input_index, type, \
                     src_unit, src_message_index, src_output_index, \
                     from_main_chain_index, to_main_chain_index, \
                     denomination, amount, serial_number, \
                     asset, is_unique, address) \
                     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                )?;
                stmt.insert(&[
                    self.get_unit_hash(),
                    &(i as u32),
                    &(j as u32),
                    kind,
                    &src_unit,
                    &src_message_index,
                    &src_output_index,
                    &from_main_chain_index,
                    &to_main_chain_index,
                    &payload.asset,
                    &is_unique,
                    &address,
                ])?;

                match kind.as_str() {
                    "headers_commission" | "witnessing" => {
                        let sql = format!(
                            "UPDATE {}_outputs SET is_spent=1 \
                             WHERE main_chain_index>=? AND main_chain_index<=? AND address=?",
                            kind
                        );
                        let mut stmt = tx.prepare_cached(&sql)?;
                        stmt.execute(&[&from_main_chain_index, &to_main_chain_index, &address])?;
                    }
                    "transfer" => {
                        let mut stmt = tx.prepare_cached(
                            "UPDATE outputs SET is_spent=1 \
                             WHERE unit=? AND message_index=? AND output_index=?",
                        )?;
                        stmt.execute(&[&src_unit, &src_message_index, &src_output_index])?;
                    }
                    _ => {}
                }
            }

            for (j, output) in payload.outputs.iter().enumerate() {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO outputs \
                     (unit, message_index, output_index, address, \
                     amount, asset, denomination, is_serial) \
                     VALUES(?,?,?,?,?,?,?,1)",
                )?;
                stmt.insert(&[
                    self.get_unit_hash(),
                    &(i as u32),
                    &(j as u32),
                    &output.address,
                    &output.amount,
                    &payload.asset,
                    &denomination,
                ])?;
            }
        }
        Ok(())
    }

    fn determine_input_address_from_output(
        &self,
        tx: &Transaction,
        asset: &String,
        denomination: u32,
        input: &Input,
    ) -> Result<String> {
        let mut stmt = tx.prepare_cached(
            "SELECT address, denomination, asset FROM outputs \
             WHERE unit=? AND message_index=? AND output_index=?",
        )?;
        let address = stmt.query_row(
            &[&input.unit, &input.message_index, &input.output_index],
            |row| {
                ensure!(asset == &row.get::<_, String>(2), "asset doesn't match");
                ensure!(
                    denomination == row.get::<_, u32>(1),
                    "denomination not match"
                );
                Ok(row.get(0))
            },
        )?;

        Ok(address?)
    }

    pub fn save(&self) -> Result<()> {
        // first construct all the sql within a mutex
        info!("saving unit = {:?}", self.unit);
        assert_eq!(self.unit.unit.is_some(), true);
        let _g = WRITER_MUTEX.lock()?;
        // and then execute the transaction
        let mut db = db::DB_POOL.get_connection();
        let tx = db.transaction()?;

        self.save_inline_payment(&tx)?;

        // TODO: add validation, default is good
        let sequence = String::from("good");

        self.save_unit(&tx, &sequence)?;
        self.save_ball(&tx)?;
        self.save_parents(&tx)?;
        self.save_authors(&tx)?;
        self.save_messages(&tx)?;
        self.save_header_earnings(&tx)?;
        let best_parent_unit = self.update_best_parent(&tx)?;
        self.update_level(&tx)?;
        self.update_witness_level(&tx, best_parent_unit)?;
        // TODO: add update mainchain()
        // main_chain::update_main_chain()?;

        // TODO: add precommit hook
        tx.commit()?;

        // TODO: add sqlite optimization
        Ok(())
    }
}

#[test]
fn test_write() {
    let unit = Unit {
        alt: String::from("1"),
        authors: Vec::new(),
        content_hash: None,
        earned_headers_commission_recipients: None,
        headers_commission: 0,
        last_ball: String::from("oiIA6Y+87fk6/QyrbOlwqsQ/LLr82Rcuzcr1G/GoHlA="),
        last_ball_unit: String::from("vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk="),
        messages: Vec::new(),
        parent_units: vec![
            "uPbobEuZL+FY1ujTNiYZnM9lgC3xysxuDIpSbvnmbac=".into(),
            "vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=".into(),
        ],
        payload_commission: 0,
        unit: Some(String::from("5CYeTTa4VQxgF4b1Tn33NBlKilJadddwBMLvtp1HIus=")),
        version: String::from("1.0"),
        witnesses: None,
        witness_list_unit: String::from("MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ="),
    };
    let joint = Joint {
        ball: None,
        skiplist_units: None,
        unit: unit,
    };
    let parents_set = joint
        .unit
        .parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    println!("{}", parents_set);
    // joint.save().unwrap();
}
