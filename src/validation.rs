use config;
use error::Result;
use joint::Joint;
use rusqlite::Connection;
use spec::*;

const HASH_LENGTH: usize = 44;

#[derive(Debug)]
pub struct ValidationState {
    unsigned: bool,
}

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

    if joint.unsigned == Some(true) {
        if joint.ball.is_some() || joint.skiplist_units.is_some() {
            return Ok(ValidationResult::JointError(
                "unknown fields in unsigned unit-joint".to_owned(),
            ));
        }
    } else if joint.ball.is_some() {
        let ball = joint.ball.as_ref().unwrap();
        if ball.len() != HASH_LENGTH {
            return Ok(ValidationResult::JointError("wrong ball length".to_owned()));
        }
        if joint.skiplist_units.is_some() {
            if joint.skiplist_units.as_ref().unwrap().len() == 0 {
                return Ok(ValidationResult::JointError(
                    "missing or empty skiplist array".to_owned(),
                ));
            }
        }
    }

    if unit.content_hash.is_some() {
        let content_hash = unit.content_hash.as_ref().unwrap();
        if content_hash.len() != HASH_LENGTH {
            return Ok(ValidationResult::UnitError(
                "wrong content_hash length".to_owned(),
            ));
        }
        if unit.earned_headers_commission_recipients.is_some() || unit.headers_commission.is_some()
            || unit.payload_commission.is_some() || unit.main_chain_index.is_some()
            || !unit.messages.is_empty()
        {
            return Ok(ValidationResult::UnitError(
                "unknown fields in nonserial unit".to_owned(),
            ));
        }
        if joint.ball.is_none() {
            return Ok(ValidationResult::JointError(
                "content_hash allowed only in finished ball".to_owned(),
            ));
        }
    } else {
        // serial
        if unit.messages.is_empty() {
            return Ok(ValidationResult::UnitError(
                "missing or empty messages array".to_owned(),
            ));
        }

        if unit.messages.len() > config::MAX_MESSAGES_PER_UNIT {
            return Ok(ValidationResult::UnitError("too many messages".to_owned()));
        }

        let header_size = unit.get_header_size();
        if unit.headers_commission != Some(header_size) {
            let msg = format!("wrong headers commission, expected {}", header_size);
            return Ok(ValidationResult::UnitError(msg));
        }

        let payload_size = unit.get_payload_size();
        if unit.payload_commission != Some(payload_size) {
            let msg = format!("wrong payload commission, expected {}", payload_size);
            return Ok(ValidationResult::UnitError(msg));
        }
    }

    if unit.authors.is_empty() {
        return Ok(ValidationResult::UnitError(
            "missing or empty authors array".to_owned(),
        ));
    }

    if unit.version != config::VERSION {
        return Ok(ValidationResult::UnitError("wrong version".to_owned()));
    }

    if unit.alt != config::ALT {
        return Ok(ValidationResult::UnitError("wrong alt".to_owned()));
    }

    if !unit.is_genesis_unit() {
        if unit.parent_units.is_empty() {
            return Ok(ValidationResult::UnitError(
                "missing or empty parent units array".to_owned(),
            ));
        }

        if unit.last_ball.as_ref().map(|s| s.len()).unwrap_or(0) != HASH_LENGTH {
            return Ok(ValidationResult::UnitError(
                "wrong length of last ball".to_owned(),
            ));
        }

        if unit.last_ball_unit.as_ref().map(|s| s.len()).unwrap_or(0) != HASH_LENGTH {
            return Ok(ValidationResult::UnitError(
                "wrong length of last ball unit".to_owned(),
            ));
        }
    }

    if unit.witness_list_unit.is_some() && unit.witnesses.is_some() {
        return Ok(ValidationResult::UnitError(
            "ambiguous witnesses".to_owned(),
        ));
    }

    let _author_addresses: Vec<String> = unit.authors.iter().map(|a| a.address.clone()).collect();

    // TODO: add more checks
    unimplemented!()
}
