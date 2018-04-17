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
}

impl Joint {
    #[inline]
    fn get_unit_hash(&self) -> &String {
        self.unit.unit.as_ref().unwrap()
    }

    fn save_unit(&self, tx: &Transaction, sequence: &String) -> Result<()> {
        let unit = &self.unit;
        let unit_hash = self.get_unit_hash();

        let mut stmt = tx.prepare_cached("INSERT INTO units \
            (unit, version, alt, witness_list_unit, last_ball_unit, headers_commission, payload_commission, sequence, content_hash) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")?;

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
                let mut stmt = tx.prepare("UPDATE units SET \
                                          is_on_main_chain=1, main_chain_index=0, is_stable=1, level=0, witnessed_level=0 \
                                          WHERE unit=?")?;
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
    fn save_authors(&self, tx: &Transaction) -> Result<Vec<&String>> {
        let unit_hash = self.get_unit_hash();
        let mut author_addresses = vec![];
        for author in &self.unit.authors {
            author_addresses.push(&author.address);
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
        Ok(author_addresses)
    }

    fn save_messages(&self, _tx: &Transaction) -> Result<()> {
        unimplemented!()
    }

    pub fn save(&self) -> Result<()> {
        // first construct all the sql within a mutex
        info!("saving unit = {:?}", self.unit);
        assert_eq!(self.unit.unit.is_some(), true);
        let _g = WRITER_MUTEX.lock()?;
        // and then execute the transaction
        let mut db = db::DB_POOL.get_connection();
        let tx = db.transaction()?;

        // TODO: add validation, default is good
        let sequence = String::from("good");

        self.save_unit(&tx, &sequence)?;
        self.save_ball(&tx)?;
        self.save_parents(&tx)?;
        self.save_authors(&tx)?;
        self.save_messages(&tx)?;

        tx.commit()?;
        Ok(())
    }
}

#[test]
fn test_write() {
    let unit = Unit {
        alt: String::from("1"),
        authors: Vec::new(),
        content_hash: None,
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
