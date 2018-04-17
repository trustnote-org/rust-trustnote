use db;
use error::Result;
use spec;

use may::sync::Mutex;

lazy_static! {
    static ref WRITER_MUTEX: Mutex<()> = Mutex::new(());
}

/// save a unit
pub fn save_joint(joint: spec::Joint) -> Result<()> {
    // first construct all the sql within a mutex
    info!("saving unit = {:?}", joint.unit);
    let _g = WRITER_MUTEX.lock()?;
    // and then execute the transaction
    let mut db = db::DB_POOL.get_connection();
    let tx = db.transaction()?;
    tx.commit()?;
    unimplemented!();
}
