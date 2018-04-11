use std::ops::Deref;

use num_cpus;
use rusqlite::{Connection, OpenFlags};

use may;
use may::sync::mpmc::{self, Receiver, Sender};

use error::Result;

lazy_static! {
    pub static ref DB_POOL: DatabasePool = DatabasePool::new();
}

pub struct DatabasePool {
    db_rx: Receiver<Connection>,
    db_tx: Sender<Connection>,
}

impl DatabasePool {
    pub fn new() -> Self {
        // create the connection pool
        let (db_tx, db_rx) = mpmc::channel();
        may::coroutine::scope(|s| {
            for _ in 0..(num_cpus::get() * 4) {
                go!(s, || {
                    let conn = match Connection::open_with_flags(
                        "db/trustnote.sqlite",
                        OpenFlags::SQLITE_OPEN_READ_WRITE,
                    ) {
                        Ok(conn) => conn,
                        Err(e) => {
                            error!("{}", e.to_string());
                            ::std::process::abort();
                        }
                    };

                    db_tx.send(conn).unwrap();
                });
            }
        });

        info!("open database connections done");
        DatabasePool { db_rx, db_tx }
    }

    pub fn get_connection(&self) -> Database {
        Database {
            db: Some(self.db_rx.recv().unwrap()),
            tx: self.db_tx.clone(),
        }
    }
}

pub struct Database {
    db: Option<Connection>,
    tx: Sender<Connection>,
}

impl Deref for Database {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        self.db.as_ref().unwrap()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let db = self.db.take().unwrap();
        self.tx.send(db).unwrap();
    }
}

impl Database {
    pub fn get_my_witnesses(&self) -> Result<Vec<String>> {
        let mut stmt = self.prepare("SELECT address FROM my_witnesses")?;
        let rows = stmt.query_map(&[], |row| row.get(0))?;

        let mut names = Vec::new();
        for name_result in rows {
            names.push(name_result?);
        }
        Ok(names)
    }

    // TODO:
    pub fn insert_witnesses(&self) -> Result<()> {
        unimplemented!();
    }
}
