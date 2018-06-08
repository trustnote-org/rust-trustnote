use std::fs;
use std::path::*;

use std::ops::{Deref, DerefMut};

use num_cpus;
use rusqlite::{Connection, OpenFlags};

use may;
use may::sync::mpmc::{self, Receiver, Sender};

use error::Result;

use app_dirs::*;

const APP_INFO: AppInfo = AppInfo {
    name: "rust-trustnote",
    author: "trustnote-hub",
};

const INITIAL_DB_NAME: &'static str = "db/initial.trustnote.sqlite";
const DB_NAME: &'static str = "trustnote.sqlite";

lazy_static! {
    pub static ref DB_POOL: DatabasePool = DatabasePool::new();
}

pub fn create_database_if_necessary() -> Result<()> {
    let app_path_buf: PathBuf = get_app_root(AppDataType::UserData, &APP_INFO)?;

    let mut initial_db_path_buf: PathBuf = PathBuf::new();
    initial_db_path_buf.push(INITIAL_DB_NAME);
    let initial_db_path: &Path = initial_db_path_buf.as_path();

    let mut db_path_buf: PathBuf = app_path_buf.clone();
    db_path_buf.push(DB_NAME);
    let db_path: &Path = db_path_buf.as_path();

    if !db_path.exists() {
        fs::create_dir_all(app_path_buf.as_path())?;
        fs::copy(initial_db_path, db_path)?;
        info!(
            "create_database_if_necessary done: db_path: {:?}, initial db path: {:?}",
            db_path.display(),
            initial_db_path.display()
        );
    }

    Ok(())
}

pub struct DatabasePool {
    db_rx: Receiver<Connection>,
    db_tx: Sender<Connection>,
}

impl DatabasePool {
    pub fn new() -> Self {
        create_database_if_necessary().expect("create database error");

        // database path
        let mut path_buf: PathBuf =
            get_app_root(AppDataType::UserData, &APP_INFO).expect("not found db");
        path_buf.push(DB_NAME);
        let db_path: &Path = path_buf.as_path();

        // create the connection pool
        let (db_tx, db_rx) = mpmc::channel();

        may::coroutine::scope(|s| {
            for _ in 0..(num_cpus::get() * 4) {
                go!(s, || {
                    let conn = match Connection::open_with_flags(
                        db_path,
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

    // TODO:
    pub fn insert_witnesses(&self) -> Result<()> {
        unimplemented!();
    }
}
