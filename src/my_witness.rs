use config;
use db;

use error::Result;

pub struct MyWitness {
    witnesses: Vec<String>,
}

impl MyWitness {
    pub fn new() -> Result<Self> {
        // read from database
        let db = db::DB_POOL.get_connection();
        let witnesses = db.get_my_witnesses()?;

        // if the data base is empty we should wait until
        if witnesses.len() == 0 {
            // TODO: block until data available
        } else {
            assert_eq!(witnesses.len(), config::COUNT_WITNESSES);
        }

        Ok(MyWitness { witnesses })
    }

    pub fn get_witnesses(&self) -> &[String] {
        &self.witnesses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_witnesses() {
        let my_witness = MyWitness::new().unwrap();
        let witnesses = my_witness.get_witnesses();
        assert_eq!(witnesses.len(), config::COUNT_WITNESSES);
    }
}
