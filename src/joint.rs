use std::collections::HashSet;

use config::*;
use db;
use definition;
use error::Result;
use main_chain;
use may::sync::Mutex;
use object_hash::get_chash;
use rusqlite::Transaction;
use serde_json;
use spec::*;
use validation;

lazy_static! {
    pub static ref WRITER_MUTEX: Mutex<()> = Mutex::new(());
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Joint {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ball: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub skiplist_units: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unsigned: Option<bool>,
    pub unit: Unit,
}

impl Joint {
    #[inline]
    pub fn get_unit_hash(&self) -> &String {
        self.unit.unit.as_ref().unwrap()
    }

    fn save_unit(&self, tx: &Transaction, sequence: &String, is_light_wallet: bool) -> Result<()> {
        let unit = &self.unit;
        let unit_hash = self.get_unit_hash();

        if !is_light_wallet {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO units \
                 (unit, version, alt, witness_list_unit, last_ball_unit, \
                 headers_commission, payload_commission, sequence, content_hash) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            stmt.execute(&[
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
        } else {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO units \
                 (unit, version, alt, witness_list_unit, last_ball_unit, \
                 headers_commission, payload_commission, sequence, content_hash, main_chain_index, creation_date) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime(?, 'unixepoch'))",
            )?;
            stmt.execute(&[
                unit_hash,
                &unit.version,
                &unit.alt,
                &unit.witness_list_unit,
                &unit.last_ball_unit,
                &unit.headers_commission,
                &unit.payload_commission,
                sequence,
                &unit.content_hash,
                &unit.main_chain_index,
                &(unit.timestamp.unwrap_or(0) as i64),
            ])?;
        }

        Ok(())
    }

    fn save_ball(&self, tx: &Transaction) -> Result<()> {
        if self.ball.is_none() {
            return Ok(());
        }

        let unit_hash = self.get_unit_hash();
        let ball_hash = self.ball.as_ref().unwrap();

        let mut stmt = tx.prepare_cached("INSERT INTO balls (ball, unit) VALUES (?, ?)")?;
        stmt.execute(&[ball_hash, unit_hash])?;

        let mut stmt = tx.prepare_cached("DELETE FROM hash_tree_balls WHERE ball=? AND unit=?")?;
        stmt.execute(&[ball_hash, unit_hash])?;

        if !self.skiplist_units.is_empty() {
            for unit in &self.skiplist_units {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO skiplist_units (unit, skiplist_unit) VALUES (?, ?)",
                )?;
                stmt.execute(&[unit_hash, unit])?;
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
            stmt.execute(&[unit_hash, parent])?;
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
            let parents_set = unit
                .parent_units
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("UPDATE units SET is_free=0 WHERE unit IN ({})", parents_set);
            let rows = tx.execute(&sql, &[])?;
            info!("{} free units consumed", rows);
        }

        Ok(())
    }

    fn save_witnesses(&self, tx: &Transaction) -> Result<()> {
        let unit = &self.unit;
        if unit.witnesses.is_empty() {
            return Ok(());
        }

        let unit_hash = self.get_unit_hash();
        let mut stmt =
            tx.prepare_cached("INSERT INTO unit_witnesses (unit, address) VALUES(?,?)")?;
        for address in &unit.witnesses {
            stmt.execute(&[unit_hash, address])?;
        }
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO witness_list_hashes (witness_list_unit, witness_list_hash) VALUES (?,?)")?;
        let witnesses_hash = ::object_hash::get_base64_hash(&unit.witnesses)?;
        stmt.execute(&[unit_hash, &witnesses_hash])?;
        Ok(())
    }

    // return a vec of author address
    fn save_authors(&self, tx: &Transaction) -> Result<()> {
        let unit_hash = self.get_unit_hash();
        let mut definition_chash = None;
        for author in &self.unit.authors {
            let definition = &author.definition;
            if !definition.is_null() {
                definition_chash = Some(get_chash(definition)?);
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO definitions \
                     (definition_chash, definition, has_references) \
                     VALUES (?, ?, ?)",
                )?;
                let definition_json = serde_json::to_string(definition)?;
                let has_references = definition::has_references(definition)? as u8;
                stmt.execute(&[&definition_chash, &definition_json, &has_references])?;

                // TODO: we ingore unit.content_hash here
                if definition_chash.as_ref() == Some(&author.address) {
                    let mut stmt = tx.prepare_cached(
                        "INSERT OR IGNORE INTO addresses (address) \
                         VALUES (?)",
                    )?;
                    stmt.execute(&[&author.address])?;
                }
            } else if self.unit.content_hash.is_some() {
                let mut stmt = tx.prepare_cached(
                    "INSERT OR IGNORE INTO addresses (address) \
                     VALUES (?)",
                )?;
                stmt.execute(&[&author.address])?;
            }

            let mut stmt = tx.prepare_cached(
                "INSERT INTO unit_authors \
                 (unit, address, definition_chash) \
                 VALUES(?, ?, ?)",
            )?;
            stmt.execute(&[unit_hash, &author.address, &definition_chash])?;

            for (path, authentifier) in &author.authentifiers {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO authentifiers \
                     (unit, address, path, authentifier) \
                     VALUES(?, ?, ?, ?)",
                )?;
                stmt.execute(&[unit_hash, &author.address, path, authentifier])?;
            }
        }
        Ok(())
    }

    fn save_messages(&self, tx: &Transaction) -> Result<()> {
        let unit_hash = self.get_unit_hash();
        for (i, message) in self.unit.messages.iter().enumerate() {
            let text_payload = match message.app.as_str() {
                "text" => match &message.payload {
                    Some(Payload::Text(ref s)) => Some(s.to_owned()),
                    _ => {
                        error!("no text found in text payload!");
                        None
                    }
                },
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
            stmt.execute(&[
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
                use spec::Payload;
                match message.app.as_str() {
                    "payment" | "text" => {}
                    "data_feed" => match message.payload {
                        Some(Payload::Other(ref v)) => {
                            if let Some(map) = v.as_object() {
                                for (i, (k, v)) in map.iter().enumerate() {
                                    let field_name =
                                        if v.is_number() { "int_value" } else { "value" };
                                    let sql = format!(
                                        "INSERT INTO data_feeds \
                                         (unit, message_index, feed_name, {}) VALUES(?,?,?,?)",
                                        field_name
                                    );
                                    let mut stmt = tx.prepare_cached(&sql)?;
                                    if v.is_number() {
                                        stmt.execute(&[unit_hash, &(i as u32), k, &v.as_i64()])?;
                                    } else {
                                        stmt.execute(&[unit_hash, &(i as u32), k, &v.as_str()])?;
                                    }
                                }
                            }
                        }
                        _ => unreachable!("data_feed invalid message"),
                    },
                    app => unimplemented!("unknow message app: {}", app),
                }
            }

            for (j, spend_proof) in message.spend_proofs.iter().enumerate() {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO spend_proofs (unit, message_index, spend_proof_index, spend_proof, address) \
                    VALUES(?,?,?,?,?)")?;
                let address = spend_proof
                    .address
                    .as_ref()
                    .unwrap_or(&self.unit.authors[0].address);
                stmt.execute(&[
                    unit_hash,
                    &(i as u32),
                    &(j as u32),
                    &spend_proof.spend_proof,
                    address,
                ])?;
            }
        }
        Ok(())
    }

    fn save_header_earnings(&self, tx: &Transaction) -> Result<()> {
        let unit = &self.unit;
        for recipient in &unit.earned_headers_commission_recipients {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO earned_headers_commission_recipients \
                 (unit, address, earned_headers_commission_share) VALUES(?,?,?)",
            )?;
            stmt.execute(&[
                &unit.unit,
                &recipient.address,
                &recipient.earned_headers_commission_share,
            ])?;
        }

        Ok(())
    }

    fn update_best_parent(&self, tx: &Transaction) -> Result<String> {
        let unit = &self.unit;
        let parents_set = unit
            .parent_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        // TODO: witness list is fixed
        let sql = format!(
            "SELECT unit \
             FROM units AS parent_units \
             WHERE unit IN({}) \
             AND (witness_list_unit=? OR ( \
             SELECT COUNT(*) \
             FROM unit_witnesses \
             JOIN unit_witnesses AS parent_witnesses USING(address) \
             WHERE parent_witnesses.unit IN(parent_units.unit, parent_units.witness_list_unit) \
             AND unit_witnesses.unit IN(?, ?) \
             )>=?) \
             ORDER BY witnessed_level DESC, \
             level-witnessed_level ASC, \
             unit ASC \
             LIMIT 1",
            parents_set
        );

        let witness_diff: u32 = (COUNT_WITNESSES - MAX_WITNESS_LIST_MUTATIONS) as u32;

        let best_parent_unit: String = tx.query_row(
            &sql,
            &[
                &unit.witness_list_unit,
                &unit.unit,
                &unit.witness_list_unit,
                &witness_diff,
            ],
            |row| row.get(0),
        )?;

        let mut stmt = tx.prepare_cached("UPDATE units SET best_parent_unit=? WHERE unit=?")?;
        stmt.execute(&[&best_parent_unit, self.get_unit_hash()])?;
        Ok(best_parent_unit)
    }

    fn update_level(&self, tx: &Transaction) -> Result<()> {
        let parents_set = self
            .unit
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
            let props = ::storage::read_static_unit_property(tx, &best_parent_unit)?;
            let authors = ::storage::read_unit_authors(tx, &best_parent_unit)?;
            let level = props.level;

            // genesis
            if level == 0 {
                return self.save_witness_level(tx, 0);
            }

            for address in authors {
                if witness_list.contains(&address) && !collected_witnesses.contains(&address) {
                    collected_witnesses.insert(address);
                }
            }

            if collected_witnesses.len() >= MAJORITY_OF_WITNESSES {
                return self.save_witness_level(tx, level);
            }

            // search next best parent
            best_parent_unit = props.best_parent_unit.unwrap();
        }
    }

    fn update_witness_level(&self, tx: &Transaction, best_parent_unit: String) -> Result<()> {
        if self.unit.witnesses.is_empty() {
            let witnesses_list_unit = self
                .unit
                .witness_list_unit
                .as_ref()
                .expect("no witnesses list unit");
            let witness_list = ::storage::read_witness_list(tx, witnesses_list_unit)?;
            self.update_witness_level_by_witness_list(tx, &witness_list, best_parent_unit)
        } else {
            self.update_witness_level_by_witness_list(tx, &self.unit.witnesses, best_parent_unit)
        }
    }

    fn save_inline_payment(&self, tx: &Transaction, is_light_wallet: bool) -> Result<()> {
        let unit_hash = self.get_unit_hash();
        let mut author_addresses = vec![];
        for author in &self.unit.authors {
            author_addresses.push(&author.address);
        }

        for (i, message) in self.unit.messages.iter().enumerate() {
            if message.payload_location.as_str() != "inline" || message.app.as_str() != "payment" {
                continue;
            }

            // let payload = message.payload.as_ref().expect("no payload found");
            let payment = match message.payload.as_ref().expect("no payload found") {
                Payload::Payment(p) => p,
                _ => panic!("mismatch payload found"),
            };
            let denomination = payment.denomination.unwrap_or(1);

            for (j, input) in payment.inputs.iter().enumerate() {
                let default_kind = String::from("transfer");
                let kind = input.kind.as_ref().unwrap_or(&default_kind);
                let src_unit = some_if!(kind == "transfer", input.unit.clone());
                let src_message_index = some_if_option!(kind == "transfer", input.message_index);
                let src_output_index = some_if_option!(kind == "transfer", input.output_index);

                let from_main_chain_index = some_if_option!(
                    kind == "witnessing" || kind == "headers_commission",
                    input.from_main_chain_index
                );
                let to_main_chain_index = some_if_option!(
                    kind == "witnessing" || kind == "headers_commission",
                    input.to_main_chain_index
                );

                let address = if author_addresses.len() == 1 {
                    author_addresses[0].clone()
                } else {
                    match kind.as_str() {
                        "headers_commission" | "witnessing" | "issue" => {
                            input.address.as_ref().expect("no input address").clone()
                        }
                        x => {
                            info!("input type for multi authors: {}", x);
                            match self.determine_input_address_from_output(
                                tx,
                                &payment.asset,
                                denomination,
                                &input,
                            ) {
                                Ok(addr) => addr,
                                Err(e) => if is_light_wallet {
                                    "".to_string()
                                } else {
                                    bail!(
                                        "determine_input_address_from_output failed, err: {:?}",
                                        e
                                    );
                                },
                            }
                        }
                    }
                };

                // TODO: objValidationState.arrDoubleSpendInputs.some(...)
                // here we give it a unique as default, but we should check the ValidationState
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
                stmt.execute(&[
                    unit_hash,
                    &(i as u32),
                    &(j as u32),
                    kind,
                    &src_unit,
                    &src_message_index,
                    &src_output_index,
                    &from_main_chain_index,
                    &to_main_chain_index,
                    &denomination,
                    &input.amount,
                    &input.serial_number,
                    &payment.asset,
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

            for (j, output) in payment.outputs.iter().enumerate() {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO outputs \
                     (unit, message_index, output_index, address, \
                     amount, asset, denomination, is_serial) \
                     VALUES(?,?,?,?,?,?,?,1)",
                )?;
                stmt.execute(&[
                    self.get_unit_hash(),
                    &(i as u32),
                    &(j as u32),
                    &output.address,
                    &output.amount,
                    &payment.asset,
                    &denomination,
                ])?;
            }
        }
        Ok(())
    }

    fn determine_input_address_from_output(
        &self,
        tx: &Transaction,
        asset: &Option<String>,
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
                ensure!(
                    asset == &row.get::<_, Option<String>>(2),
                    "asset doesn't match"
                );
                ensure!(
                    denomination == row.get::<_, u32>(1),
                    "denomination not match"
                );
                Ok(row.get(0))
            },
        )?;

        Ok(address?)
    }

    pub fn save(
        &self,
        validation_state: validation::ValidationState,
        is_light_wallet: bool,
    ) -> Result<()> {
        // first construct all the sql within a mutex
        info!("saving unit = {:?}", self.unit);
        assert_eq!(self.unit.unit.is_some(), true);
        let _g = WRITER_MUTEX.lock()?;
        // and then execute the transaction
        let mut db = db::DB_POOL.get_connection();

        let tx = db.transaction()?;

        let sequence = validation_state.sequence;
        validation_state.additional_queries.execute(&*tx)?;

        self.save_unit(&tx, &sequence, is_light_wallet)?;
        if !is_light_wallet {
            self.save_ball(&tx)?;
        }
        self.save_parents(&tx)?;
        self.save_witnesses(&tx)?;
        self.save_authors(&tx)?;
        self.save_messages(&tx)?;
        self.save_header_earnings(&tx)?;
        self.save_inline_payment(&tx, is_light_wallet)?;
        if !is_light_wallet {
            if !self.unit.parent_units.is_empty() {
                let best_parent_unit = self.update_best_parent(&tx)?;
                self.update_level(&tx)?;
                self.update_witness_level(&tx, best_parent_unit)?;
                main_chain::update_main_chain(&tx, None)?;
            }
        }

        // TODO: add precommit hook
        tx.commit()?;

        // TODO: add sqlite optimization
        Ok(())
    }

    pub fn has_valid_hashes(&self) -> bool {
        let unit = &self.unit;
        if unit.unit.is_none() {
            return false;
        }

        self.get_unit_hash() == &unit.get_unit_hash()
    }

    pub fn get_joint_hash(&self) -> String {
        use base64;
        use sha2::{Digest, Sha256};
        base64::encode(&Sha256::digest(
            &serde_json::to_vec(self).expect("joint to json failed"),
        ))
    }
}

#[test]
fn test_write() {
    let unit = Unit {
        alt: String::from("1"),
        authors: Vec::new(),
        content_hash: None,
        earned_headers_commission_recipients: Vec::new(),
        headers_commission: None,
        last_ball: Some(String::from("oiIA6Y+87fk6/QyrbOlwqsQ/LLr82Rcuzcr1G/GoHlA=")),
        last_ball_unit: Some(String::from("vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=")),
        main_chain_index: None,
        messages: Vec::new(),
        parent_units: vec![
            "uPbobEuZL+FY1ujTNiYZnM9lgC3xysxuDIpSbvnmbac=".into(),
            "vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=".into(),
        ],
        payload_commission: None,
        timestamp: None,
        unit: Some(String::from("5CYeTTa4VQxgF4b1Tn33NBlKilJadddwBMLvtp1HIus=")),
        version: String::from("1.0"),
        witnesses: Vec::new(),
        witness_list_unit: Some(String::from("MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ=")),
    };
    let joint = Joint {
        ball: None,
        skiplist_units: Vec::new(),
        unit: unit,
        unsigned: None,
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
