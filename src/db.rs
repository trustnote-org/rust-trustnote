use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

use config;
use error::Result;
use may;
use may::sync::mpmc::{self, Receiver, Sender};
use num_cpus;
use rusqlite::{Connection, OpenFlags};

#[derive(RustEmbed)]
#[folder = "db/"]
struct InitDatabase;

lazy_static! {
    static ref IS_WALLET: AtomicBool = AtomicBool::new(false);
    pub static ref DB_POOL: DatabasePool = DatabasePool::new();
}

pub fn use_wallet_db() {
    IS_WALLET.store(true, Ordering::Relaxed);
}

fn create_database_if_necessary() -> Result<PathBuf> {
    let is_wallet = IS_WALLET.load(Ordering::Relaxed);
    let db_path = config::get_database_path(is_wallet);
    if !db_path.exists() {
        let init_db = if is_wallet {
            InitDatabase::get("initial.trustnote-light.sqlite")
        } else {
            InitDatabase::get("initial.trustnote.sqlite")
        }.expect("failed to find init db");
        info!("create_database_if_necessary done: db_path: {:?}", db_path);
        ::std::fs::write(&db_path, init_db)?;
    }

    Ok(db_path)
}

pub struct DatabasePool {
    db_rx: Receiver<Connection>,
    db_tx: Sender<Connection>,
}

impl Default for DatabasePool {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabasePool {
    pub fn new() -> Self {
        // database path
        let db_path = create_database_if_necessary().expect("create database error");
        // create the connection pool
        let (db_tx, db_rx) = mpmc::channel();

        may::coroutine::scope(|s| {
            for _ in 0..(num_cpus::get() * 4) {
                go!(s, || {
                    let conn = match Connection::open_with_flags(
                        &db_path,
                        OpenFlags::SQLITE_OPEN_READ_WRITE
                            // | OpenFlags::SQLITE_OPEN_SHARED_CACHE
                            // | OpenFlags::SQLITE_OPEN_NO_MUTEX,
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

impl DerefMut for Database {
    #[inline]
    fn deref_mut(&mut self) -> &mut Connection {
        self.db.as_mut().unwrap()
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
        let mut stmt = self.prepare_cached("SELECT address FROM my_witnesses")?;
        let rows = stmt.query_map(&[], |row| row.get(0))?;

        let mut names = Vec::new();
        for name_result in rows {
            names.push(name_result?);
        }
        Ok(names)
    }

    pub fn insert_witnesses(&self, witnesses: &[String]) -> Result<()> {
        let witnesses_str = witnesses
            .iter()
            .map(|s| format!("('{}')", s))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "INSERT INTO my_witnesses (address) VALUES {}",
            witnesses_str
        );

        let mut stmt = self.prepare_cached(&sql)?;
        stmt.execute(&[])?;
        Ok(())
    }
}

pub trait FnQuery {
    fn call_box(self: Box<Self>, &Connection) -> Result<()>;
}

impl<F: FnOnce(&Connection) -> Result<()>> FnQuery for F {
    fn call_box(self: Box<Self>, db: &Connection) -> Result<()> {
        (*self)(db)
    }
}

#[derive(Default)]
pub struct DbQueries {
    queries: Vec<Box<FnQuery>>,
}

impl fmt::Debug for DbQueries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        //TODO: to add some real debug info
        write!(f, "DbQueries")
    }
}

impl DbQueries {
    pub fn new() -> Self {
        DbQueries {
            queries: Vec::new(),
        }
    }

    #[inline]
    pub fn add_query<F>(&mut self, f: F)
    where
        F: FnOnce(&Connection) -> Result<()> + 'static,
    {
        self.queries.push(Box::new(f));
    }

    // execute all queries and ignore the error
    pub fn execute_all(self, db: &Connection) {
        for query in self.queries {
            t!(query.call_box(db));
        }
    }

    // execute queries and return earlier if any failed
    pub fn execute(self, db: &Connection) -> Result<()> {
        for query in self.queries {
            query.call_box(db)?;
        }
        Ok(())
    }
}

#[test]
fn test_db() -> Result<()> {
    let db = DB_POOL.get_connection();

    let names = db.get_my_witnesses()?;

    for name in names {
        println!("name = {}", name);
    }

    Ok(())
}
