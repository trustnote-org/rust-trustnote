use error::Result;
use joint::Joint;
use rusqlite::Connection;
use spec::*;

const HASH_LENGTH: usize = 44;

#[derive(Debug)]
pub enum ValidationResult {
    UnitError(String),
    JointError(String),
    NeedHashTree,
    NeedParentUnits(Vec<String>),
    // false if unsinged
    // TODO: Ok takes two parameters: validation_state which contains extral sql and a lockguard
    Ok(bool),
    TransientError(String),
}

pub fn validate_author_signature_without_ref(
    _db: &Connection,
    _author: &Author,
    _unit: &Unit,
    _definition: &String,
) -> Result<()> {
    unimplemented!()
}

pub fn validate(_db: &Connection, joint: &Joint) -> Result<ValidationResult> {
    let unit = &joint.unit;
    // already checked in earlier network processing
    // ensure!(unit.unit.is_some(), "no unit");

    let unit_hash = unit.unit.as_ref().unwrap();
    info!("validating joint identified by unit {}", unit_hash);

    if unit_hash.len() != HASH_LENGTH {
        return Ok(ValidationResult::JointError("wrong unit length".to_owned()));
    }

    let calc_unit_hash = unit.get_unit_hash();
    if &calc_unit_hash != unit_hash {
        return Ok(ValidationResult::JointError(format!(
            "wrong unit hash: {} != {}",
            calc_unit_hash, unit_hash
        )));
    }

    // TODO: add more checks
    unimplemented!()
}
