use rusqlite::Connection;
use rusqlite::OpenFlags;
use rusqlite::Result;

pub struct Database {
    db: Connection,
}

impl Database {
    pub fn new() -> Result<Self> {
        Ok(Database {
            db: Connection::open_with_flags(
                "db/trustnote.sqlite",
                OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?,
        })
    }

    pub fn get_my_witnesses(&self) -> Result<Vec<String>> {
        let mut stmt = self.db.prepare("SELECT address FROM my_witnesses")?;
        let rows = stmt.query_map(&[], |row| row.get(0))?;

        let mut names = Vec::new();
        for name_result in rows {
            names.push(name_result?);
        }
        Ok(names)
    }

    // TODO:
    pub fn insert_witnesses(&self) {
        unimplemented!();
    }
}
