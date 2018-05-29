use config;
use error::Result;
use joint::Joint;
use map_lock::{self, MapLock};
use object_hash;
use rusqlite::{Connection, Transaction};
use spec::*;

const HASH_LENGTH: usize = 44;

// global address map lock
lazy_static! {
    // maybe this is too heavy, could use an optimized hashset<AtomicBool>
    static ref ADDRESS_LOCK: MapLock<String> = MapLock::new();
}

macro_rules! err {
    ($e:expr) => {
        return Err($e.into());
    };
}

#[derive(Debug)]
pub struct DoubleSpendInput {
    message_index: u32,
    input_index: u32,
}

#[derive(Debug)]
pub struct ValidationState {
    unsigned: bool,
    sequence: String,
    skiplist_balls: Vec<String>,
    pub additional_queries: Vec<String>,
    pub double_spend_inputs: Vec<DoubleSpendInput>,
    // input_keys: // what this?
}

impl ValidationState {
    pub fn new() -> Self {
        ValidationState {
            unsigned: false,
            sequence: "good".to_owned(),
            skiplist_balls: Vec::new(),
            additional_queries: Vec::new(),
            double_spend_inputs: Vec::new(),
        }
    }
}

#[derive(Debug, Fail)]
pub enum ValidationError {
    #[fail(display = "Unit Error: {}", err)]
    UnitError { err: String },
    #[fail(display = "Joint Error {}", err)]
    JointError { err: String },
    #[fail(display = "Need Hash Tree")]
    NeedHashTree,
    #[fail(display = "Need Parent Units")]
    NeedParentUnits(Vec<String>),
    #[fail(display = "TransientError: {}", err)]
    TransientError { err: String },
}

#[derive(Debug)]
pub enum ValidationOk {
    Unsigned(bool),
    Signed(ValidationState, map_lock::LockGuard<'static, String>),
}

pub fn is_valid_address(address: &String) -> Result<bool> {
    let address = address.to_uppercase();
    object_hash::is_chash_valid(&address)
}

pub fn validate_author_signature_without_ref(
    _db: &Connection,
    _author: &Author,
    _unit: &Unit,
    _definition: &String,
) -> Result<()> {
    unimplemented!()
}

pub fn validate(db: &mut Connection, joint: &Joint) -> Result<ValidationOk> {
    let unit = &joint.unit;
    // already checked in earlier network processing
    // ensure!(unit.unit.is_some(), "no unit");

    let unit_hash = unit.unit.as_ref().unwrap();
    info!("validating joint identified by unit {}", unit_hash);

    if unit_hash.len() != HASH_LENGTH {
        err!(ValidationError::JointError {
            err: "wrong unit length".to_owned()
        });
    }

    let calc_unit_hash = unit.get_unit_hash();
    if &calc_unit_hash != unit_hash {
        err!(ValidationError::JointError {
            err: format!("wrong unit hash: {} != {}", calc_unit_hash, unit_hash),
        });
    }

    if joint.unsigned == Some(true) {
        if joint.ball.is_some() || !joint.skiplist_units.is_empty() {
            err!(ValidationError::JointError {
                err: "unknown fields in unsigned unit-joint".to_owned(),
            });
        }
    } else if joint.ball.is_some() {
        let ball = joint.ball.as_ref().unwrap();
        if ball.len() != HASH_LENGTH {
            err!(ValidationError::JointError {
                err: "wrong ball length".to_owned()
            });
        }
        if !joint.skiplist_units.is_empty() {
            err!(ValidationError::JointError {
                err: "empty skiplist array".to_owned(),
            });
        }
    }

    if unit.content_hash.is_some() {
        let content_hash = unit.content_hash.as_ref().unwrap();
        if content_hash.len() != HASH_LENGTH {
            err!(ValidationError::UnitError {
                err: "wrong content_hash length".to_owned(),
            });
        }
        if unit.earned_headers_commission_recipients.len() > 0 || unit.headers_commission.is_some()
            || unit.payload_commission.is_some() || unit.main_chain_index.is_some()
            || !unit.messages.is_empty()
        {
            err!(ValidationError::UnitError {
                err: "unknown fields in nonserial unit".to_owned(),
            });
        }
        if joint.ball.is_none() {
            err!(ValidationError::JointError {
                err: "content_hash allowed only in finished ball".to_owned(),
            });
        }
    } else {
        // serial
        if unit.messages.is_empty() {
            err!(ValidationError::UnitError {
                err: "missing or empty messages array".to_owned(),
            });
        }

        if unit.messages.len() > config::MAX_MESSAGES_PER_UNIT {
            err!(ValidationError::UnitError {
                err: "too many messages".to_owned()
            });
        }

        let header_size = unit.get_header_size();
        if unit.headers_commission != Some(header_size) {
            err!(ValidationError::UnitError {
                err: format!("wrong headers commission, expected {}", header_size),
            });
        }

        let payload_size = unit.get_payload_size();
        if unit.payload_commission != Some(payload_size) {
            err!(ValidationError::UnitError {
                err: format!("wrong payload commission, expected {}", payload_size),
            });
        }
    }

    if unit.authors.is_empty() {
        err!(ValidationError::UnitError {
            err: "missing or empty authors array".to_owned(),
        });
    }

    if unit.version != config::VERSION {
        err!(ValidationError::UnitError {
            err: "wrong version".to_owned()
        });
    }

    if unit.alt != config::ALT {
        err!(ValidationError::UnitError {
            err: "wrong alt".to_owned()
        });
    }

    if !unit.is_genesis_unit() {
        if unit.parent_units.is_empty() {
            err!(ValidationError::UnitError {
                err: "missing or empty parent units array".to_owned(),
            });
        }

        if unit.last_ball.as_ref().map(|s| s.len()).unwrap_or(0) != HASH_LENGTH {
            err!(ValidationError::UnitError {
                err: "wrong length of last ball".to_owned(),
            });
        }

        if unit.last_ball_unit.as_ref().map(|s| s.len()).unwrap_or(0) != HASH_LENGTH {
            err!(ValidationError::UnitError {
                err: "wrong length of last ball unit".to_owned(),
            });
        }
    }

    if unit.witness_list_unit.is_some() && !unit.witnesses.is_empty() {
        err!(ValidationError::UnitError {
            err: "ambiguous witnesses".to_owned()
        });
    }

    let mut validate_state = ValidationState::new();
    if joint.unsigned == Some(true) {
        validate_state.unsigned = true;
    }

    let author_addresses: Vec<String> = unit.authors.iter().map(|a| a.address.clone()).collect();
    let lock = ADDRESS_LOCK.lock(author_addresses);

    let tx = db.transaction()?;
    check_duplicate(&tx, unit_hash)?;
    if unit.content_hash.is_none() {
        // this is not using db
        validate_headers_commission_recipients(unit)?;
    }

    if !unit.parent_units.is_empty() {
        validate_hash_tree(&tx, joint, &mut validate_state)?;
        validate_parents(&tx, joint, &mut validate_state)?;
    }

    if !joint.skiplist_units.is_empty() {
        validate_skip_list(&tx, &joint.skiplist_units)?;
    }

    validate_witnesses(&tx, unit, &mut validate_state)?;
    validate_authors(&tx, unit, &mut validate_state)?;

    if unit.content_hash.is_none() {
        // this is not using db
        validate_messages(&tx, unit, &mut validate_state)?;
    }

    // done the checks
    if joint.unsigned == Some(true) {
        return Ok(ValidationOk::Unsigned(validate_state.sequence == "good"));
    }

    // TODO: add more checks
    Ok(ValidationOk::Signed(validate_state, lock))
}

fn check_duplicate(tx: &Transaction, unit: &String) -> Result<()> {
    let mut stmt = tx.prepare_cached("SELECT 1 FROM units WHERE unit=?")?;
    if stmt.exists(&[unit])? {
        err!(ValidationError::JointError {
            err: format!("unit {} already exist", unit),
        });
    }
    Ok(())
}

fn validate_headers_commission_recipients(unit: &Unit) -> Result<()> {
    if unit.authors.len() > 1 && unit.earned_headers_commission_recipients.is_empty() {
        err!(ValidationError::UnitError {
            err: "must specify earned_headers_commission_recipients when more than 1 author"
                .to_owned(),
        });
    }

    if unit.earned_headers_commission_recipients.is_empty() {
        return Ok(());
    }

    let mut total_earned_headers_commission_share = 0;
    let mut prev_address = "".to_owned();
    for recipient in &unit.earned_headers_commission_recipients {
        if recipient.earned_headers_commission_share < 0 {
            err!(ValidationError::UnitError {
                err: "earned_headers_commission_share must be positive integer".to_owned(),
            });
        }
        if recipient.address <= prev_address {
            err!(ValidationError::UnitError {
                err: "recipient list must be sorted by address".to_owned(),
            });
        }
        if !is_valid_address(&recipient.address)? {
            err!(ValidationError::UnitError {
                err: "invalid recipient address checksum".to_owned(),
            });
        }
        total_earned_headers_commission_share += recipient.earned_headers_commission_share;
        prev_address = recipient.address.clone();
    }

    if total_earned_headers_commission_share != 100 {
        err!(ValidationError::UnitError {
            err: "sum of earned_headers_commission_share is not 100".to_owned(),
        });
    }

    Ok(())
}

fn validate_hash_tree(
    tx: &Transaction,
    joint: &Joint,
    validate_state: &mut ValidationState,
) -> Result<()> {
    if joint.ball.is_none() {
        return Ok(());
    }

    let ball = joint.ball.as_ref().unwrap();
    let unit = &joint.unit;
    let unit_hash = unit.unit.as_ref().unwrap();
    let mut stmt = tx.prepare_cached("SELECT unit FROM hash_tree_balls WHERE ball=?")?;
    let mut rows = stmt.query(&[ball])?;

    let row = rows.next();
    if row.is_none() {
        info!("ball {} is not known in hash tree", ball);
        err!(ValidationError::NeedHashTree);
    }
    let row = row.unwrap()?;
    if unit_hash != &row.get::<_, String>(0) {
        err!(ValidationError::JointError {
            err: format!("ball {} unit {} contradicts hash tree", ball, unit_hash),
        });
    }

    let parent_units = unit.parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT ball FROM hash_tree_balls WHERE unit IN({}) \
         UNION \
         SELECT ball FROM balls WHERE unit IN({}) \
         ORDER BY ball",
        parent_units, parent_units
    );
    let mut stmt = tx.prepare(&sql)?;
    let parent_balls = stmt.query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if parent_balls.len() != unit.parent_units.len() {
        err!(ValidationError::JointError {
            err: "some parents not found in balls nor in hash tree".to_owned(),
        });
    }

    let validate_ball_hash = || {
        let ball_hash = object_hash::get_ball_hash(
            unit_hash,
            &parent_balls,
            &validate_state.skiplist_balls,
            unit.content_hash.is_some(),
        );
        if &ball_hash != ball {
            err!(ValidationError::JointError {
                err: "ball hash is wrong".to_owned(),
            });
        }
        return Ok(());
    };

    if joint.skiplist_units.is_empty() {
        return validate_ball_hash();
    }

    let skiplist_units = joint
        .skiplist_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT ball FROM hash_tree_balls WHERE unit IN({}) \
         UNION \
         SELECT ball FROM balls WHERE unit IN({}) \
         ORDER BY ball",
        skiplist_units, skiplist_units
    );

    let mut stmt = tx.prepare(&sql)?;
    let skiplist_balls = stmt.query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if skiplist_balls.len() != joint.skiplist_units.len() {
        err!(ValidationError::JointError {
            err: "some skiplist balls not found".to_owned(),
        });
    }

    validate_ball_hash()
}

fn validate_parents(
    _tx: &Transaction,
    _joint: &Joint,
    _validate_state: &mut ValidationState,
) -> Result<()> {
    unimplemented!("validate_parents")
}

fn validate_skip_list(_tx: &Transaction, _skip_list: &Vec<String>) -> Result<()> {
    unimplemented!("validate_skip_list")
}

fn validate_witnesses(
    _tx: &Transaction,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
) -> Result<()> {
    unimplemented!()
}

fn validate_authors(
    _tx: &Transaction,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
) -> Result<()> {
    unimplemented!()
}

fn validate_messages(
    _tx: &Transaction,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
) -> Result<()> {
    unimplemented!()
}
