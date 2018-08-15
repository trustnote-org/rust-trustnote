use config;
use db;
use error::Result;

lazy_static! {
    pub static ref MY_WITNESSES: Vec<String> = read_my_witnesses().unwrap();
}

fn read_my_witnesses() -> Result<Vec<String>> {
    // read from database
    let db = db::DB_POOL.get_connection();
    let witnesses = db.get_my_witnesses()?;

    // if the data base is empty we should wait until
    if witnesses.is_empty() {
        let config_witnesses = config::get_witnesses();
        db.insert_witnesses(&config_witnesses)?;
        Ok(config_witnesses.to_vec())
    } else {
        assert_eq!(witnesses.len(), config::COUNT_WITNESSES);
        Ok(witnesses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_witnesses() {
        assert_eq!(MY_WITNESSES.len(), config::COUNT_WITNESSES);
    }
}
