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
                "db/initial.trustnote.sqlite",
                OpenFlags::SQLITE_OPEN_READ_WRITE,
            )?,
        })
    }

    pub fn test(&self) -> Result<Vec<String>> {
        let mut stmt = self.db.prepare("SELECT unit FROM units")?;
        let rows = stmt.query_map(&[], |row| row.get(0))?;
        // let rows = stmt.query_map_named(&[(":id", &"one")], |row| row.get(0))?;

        let mut names = Vec::new();
        for name_result in rows {
            names.push(name_result?);
        }

        Ok(names)
    }
}
