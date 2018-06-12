use config;
use db;
use error::Result;

lazy_static! {
    pub static ref MY_WITNESSES: Vec<String> = read_my_witnesses().unwrap();
}

fn read_my_witnesses() -> Result<Vec<String>> {
    // read from database
    let db = db::DB_POOL.get_connection();
    let mut witnesses = db.get_my_witnesses()?;

    // if the data base is empty we should wait until
    if witnesses.len() == 0 {
        witnesses = config::CONFIG.read()?.get::<Vec<String>>("witnesses")?;
        ensure!(
            witnesses.len() == config::COUNT_WITNESSES,
            "attempting to insert wrong number of witnesses, check settings.json"
        );
        let witnesses_str = witnesses
            .iter()
            .map(|s| format!("('{}')", s))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO my_witnesses (address) VALUES {}",
            witnesses_str
        );

        println!("sql = {}", sql);
        let mut stmt = db.prepare_cached(&sql)?;
        stmt.execute(&[])?;
    } else {
        assert_eq!(witnesses.len(), config::COUNT_WITNESSES);
    }

    Ok(witnesses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_witnesses() {
        assert_eq!(MY_WITNESSES.len(), config::COUNT_WITNESSES);
    }
}
