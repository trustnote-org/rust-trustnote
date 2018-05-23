use error::Result;
use joint::Joint;
use rusqlite::Connection;
use spec::Unit;

#[derive(Debug)]
pub enum CheckNewResult {
    Known,
    KnownUnverified,
    KnownBad,
    New,
}

pub fn check_new_unit(db: &Connection, unit: &Unit) -> Result<CheckNewResult> {
    let _ = (db, unit);
    unimplemented!()
}

pub fn check_new_joint(db: &Connection, joint: &Joint) -> Result<CheckNewResult> {
    let _ = (db, joint);
    unimplemented!()
}
