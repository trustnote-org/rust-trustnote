use config;
use definition;
use graph;
use joint::Joint;
use main_chain;
use map_lock::{self, MapLock};
use object_hash;
use rusqlite::{Connection, Transaction};
use serde_json::Value;
use spec::*;
use storage;
// global address map lock
lazy_static! {
    // maybe this is too heavy, could use an optimized hashset<AtomicBool>
    static ref ADDRESS_LOCK: MapLock<String> = MapLock::new();
}

macro_rules! bail_with_validation_err {
    (UnitError, $e:expr) => {
        return Err(ValidationError::UnitError {
            err: ($e).to_string(),
        });
    };
    (JointError, $e:expr) => {
        return Err(ValidationError::JointError {
            err: ($e).to_string(),
        });
    };
    (UnitError, $fmt:expr, $($arg:tt)+) => {
        return Err(ValidationError::UnitError {
            err: format!($fmt, $($arg)+)
        });
    };
    (JointError, $fmt:expr, $($arg:tt)+) => {
        return Err(ValidationError::JointError {
            err: format!($fmt, $($arg)+)
        });
    };
}

macro_rules! ensure_with_validation_err {
    ($cond:expr, UnitError, $e:expr) => {
        if !($cond) {
            bail_with_validation_err!(UnitError, $e);
        }
    };
    ($cond:expr, JointError, $e:expr) => {
        if !($cond) {
            bail_with_validation_err!(JointError, $e);
        }
    };
    ($cond:expr, UnitError, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            bail_with_validation_err!(UnitError, $fmt, $($arg)+);
        }
    };
    ($cond:expr, JointError, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            bail_with_validation_err!(JointError, $fmt, $($arg)+);
        }
    };
}

#[derive(Debug)]
pub struct DoubleSpendInput {
    message_index: u32,
    input_index: u32,
}

#[derive(Debug)]
pub struct ValidationState {
    pub unsigned: bool,
    pub sequence: String,
    pub last_ball_mci: u32,
    pub max_known_mci: u32,
    pub skiplist_balls: Vec<String>,
    pub max_parent_limci: u32,
    pub has_no_references: bool,
    pub unit_hash_to_sign: Option<Vec<u8>>,
    pub additional_queries: Vec<String>,
    pub double_spend_inputs: Vec<DoubleSpendInput>,
    pub addresses_with_forked_path: Vec<String>,
    // input_keys: // what this?
}

impl ValidationState {
    pub fn new() -> Self {
        ValidationState {
            unsigned: false,
            sequence: "good".to_owned(),
            max_known_mci: 0,
            last_ball_mci: 0,
            max_parent_limci: 0,
            has_no_references: true,
            unit_hash_to_sign: None,
            skiplist_balls: Vec::new(),
            additional_queries: Vec::new(),
            double_spend_inputs: Vec::new(),
            addresses_with_forked_path: Vec::new(),
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
    // convert other unkonw error to this one
    #[fail(display = "Other unknow error")]
    OtherError { err: String },
}

impl From<::failure::Error> for ValidationError {
    fn from(error: ::failure::Error) -> Self {
        ValidationError::OtherError {
            err: error.to_string(),
        }
    }
}

impl From<::rusqlite::Error> for ValidationError {
    fn from(error: ::rusqlite::Error) -> Self {
        ValidationError::OtherError {
            err: error.to_string(),
        }
    }
}

type Result<T> = ::std::result::Result<T, ValidationError>;

#[derive(Debug)]
pub enum ValidationOk {
    Unsigned(bool),
    Signed(ValidationState, map_lock::LockGuard<'static, String>),
}

pub fn is_valid_address(address: &String) -> bool {
    let address = address.to_uppercase();
    object_hash::is_chash_valid(&address)
}

pub fn validate_author_signature_without_ref(
    db: &Connection,
    author: &Author,
    unit: &Unit,
    definition: &Value,
) -> Result<()> {
    use definition;

    let mut validate_state = ValidationState::new();
    validate_state.unit_hash_to_sign = Some(unit.get_unit_hash_to_sign());
    validate_state.last_ball_mci = 0;
    validate_state.has_no_references = true;

    definition::validate_authentifiers(
        db,
        &author.address,
        &Value::Null,
        definition,
        unit,
        &mut validate_state,
        &author.authentifiers,
    )?;
    Ok(())
}

pub fn validate(db: &mut Connection, joint: &Joint) -> Result<ValidationOk> {
    let unit = &joint.unit;
    // already checked in earlier network processing
    // ensure!(unit.unit.is_some(), "no unit");

    let unit_hash = unit.unit.as_ref().unwrap();
    info!("validating joint identified by unit {}", unit_hash);

    if unit_hash.len() != config::HASH_LENGTH {
        bail_with_validation_err!(JointError, "wrong unit length");
    }

    let calc_unit_hash = unit.get_unit_hash();
    if &calc_unit_hash != unit_hash {
        bail_with_validation_err!(
            JointError,
            "wrong unit hash: {} != {}",
            calc_unit_hash,
            unit_hash
        );
    }

    if joint.unsigned == Some(true) {
        if joint.ball.is_some() || !joint.skiplist_units.is_empty() {
            bail_with_validation_err!(JointError, "unknown fields in unsigned unit-joint");
        }
    } else if joint.ball.is_some() {
        let ball = joint.ball.as_ref().unwrap();
        if ball.len() != config::HASH_LENGTH {
            bail_with_validation_err!(JointError, "wrong ball length");
        }
    }

    if unit.content_hash.is_some() {
        let content_hash = unit.content_hash.as_ref().unwrap();
        if content_hash.len() != config::HASH_LENGTH {
            bail_with_validation_err!(UnitError, "wrong content_hash length");
        }
        if unit.earned_headers_commission_recipients.len() > 0
            || unit.headers_commission.is_some()
            || unit.payload_commission.is_some()
            || unit.main_chain_index.is_some()
            || !unit.messages.is_empty()
        {
            bail_with_validation_err!(UnitError, "unknown fields in nonserial unit");
        }
        if joint.ball.is_none() {
            bail_with_validation_err!(JointError, "content_hash allowed only in finished ball");
        }
    } else {
        // serial
        if unit.messages.is_empty() {
            bail_with_validation_err!(UnitError, "missing or empty messages array");
        }

        if unit.messages.len() > config::MAX_MESSAGES_PER_UNIT {
            bail_with_validation_err!(UnitError, "too many messages");
        }

        let header_size = unit.get_header_size();
        if unit.headers_commission != Some(header_size) {
            bail_with_validation_err!(
                UnitError,
                "wrong headers commission, expected {}",
                header_size
            );
        }

        let payload_size = unit.get_payload_size();
        if unit.payload_commission != Some(payload_size) {
            bail_with_validation_err!(
                UnitError,
                "wrong payload commission, expected {}",
                payload_size
            );
        }
    }

    if unit.authors.is_empty() {
        bail_with_validation_err!(UnitError, "missing or empty authors array");
    }

    if unit.version != config::VERSION {
        bail_with_validation_err!(UnitError, "wrong version");
    }

    if unit.alt != config::ALT {
        bail_with_validation_err!(UnitError, "wrong alt");
    }

    if !unit.is_genesis_unit() {
        if unit.parent_units.is_empty() {
            bail_with_validation_err!(UnitError, "missing or empty parent units array");
        }

        if unit.last_ball.as_ref().map(|s| s.len()).unwrap_or(0) != config::HASH_LENGTH {
            bail_with_validation_err!(UnitError, "wrong length of last ball");
        }

        if unit.last_ball_unit.as_ref().map(|s| s.len()).unwrap_or(0) != config::HASH_LENGTH {
            bail_with_validation_err!(UnitError, "wrong length of last ball unit");
        }
    }

    if unit.witness_list_unit.is_some() && !unit.witnesses.is_empty() {
        bail_with_validation_err!(UnitError, "ambiguous witnesses");
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
        bail_with_validation_err!(JointError, "unit {} already exist", unit);
    }
    Ok(())
}

fn validate_headers_commission_recipients(unit: &Unit) -> Result<()> {
    if unit.authors.len() > 1 && unit.earned_headers_commission_recipients.is_empty() {
        bail_with_validation_err!(
            UnitError,
            "must specify earned_headers_commission_recipients when more than 1 author"
        );
    }

    if unit.earned_headers_commission_recipients.is_empty() {
        return Ok(());
    }

    let mut total_earned_headers_commission_share = 0;
    let mut prev_address = "".to_owned();
    for recipient in &unit.earned_headers_commission_recipients {
        if recipient.earned_headers_commission_share < 0 {
            bail_with_validation_err!(
                UnitError,
                "earned_headers_commission_share must be positive integer"
            );
        }
        if recipient.address <= prev_address {
            bail_with_validation_err!(UnitError, "recipient list must be sorted by address");
        }
        if !is_valid_address(&recipient.address) {
            bail_with_validation_err!(UnitError, "invalid recipient address checksum");
        }
        total_earned_headers_commission_share += recipient.earned_headers_commission_share;
        prev_address = recipient.address.clone();
    }

    if total_earned_headers_commission_share != 100 {
        bail_with_validation_err!(
            UnitError,
            "sum of earned_headers_commission_share is not 100"
        );
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
        return Err(ValidationError::NeedHashTree);
    }
    let row = row.unwrap()?;
    if unit_hash != &row.get::<_, String>(0) {
        bail_with_validation_err!(
            JointError,
            "ball {} unit {} contradicts hash tree",
            ball,
            unit_hash
        );
    }

    let parent_units = unit
        .parent_units
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
    let parent_balls = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if parent_balls.len() != unit.parent_units.len() {
        bail_with_validation_err!(
            JointError,
            "some parents not found in balls nor in hash tree"
        );
    }

    fn validate_ball_hash(
        unit_hash: &String,
        parent_balls: &Vec<String>,
        skiplist_balls: &Vec<String>,
        is_valide: bool,
        ball: &String,
    ) -> Result<()> {
        let ball_hash =
            object_hash::get_ball_hash(unit_hash, parent_balls, skiplist_balls, is_valide);
        if &ball_hash != ball {
            bail_with_validation_err!(JointError, "ball hash is wrong, expect {}", ball_hash);
        }
        return Ok(());
    }

    if joint.skiplist_units.is_empty() {
        return validate_ball_hash(
            unit_hash,
            &parent_balls,
            &validate_state.skiplist_balls,
            unit.content_hash.is_some(),
            ball,
        );
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
    let skiplist_balls = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if skiplist_balls.len() != joint.skiplist_units.len() {
        bail_with_validation_err!(JointError, "some skiplist balls not found");
    }
    validate_state.skiplist_balls = skiplist_balls;

    validate_ball_hash(
        unit_hash,
        &parent_balls,
        &validate_state.skiplist_balls,
        unit.content_hash.is_some(),
        ball,
    )
}

fn validate_parents(
    tx: &Transaction,
    joint: &Joint,
    validate_state: &mut ValidationState,
) -> Result<()> {
    let unit = &joint.unit;
    let unit_hash = unit.unit.as_ref().unwrap();
    let parent_units = unit
        .parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    if unit.parent_units.len() > config::MAX_PARENT_PER_UNIT {
        bail_with_validation_err!(UnitError, "too many parents: {}", unit.parent_units.len());
    }

    let mut prev = "".to_owned();

    struct UnitProps {
        unit_props: graph::UnitProps,
        ball: Option<String>,
    }

    let mut missing_parent_units = Vec::new();
    let mut prev_parent_unit_props = Vec::new();
    validate_state.max_parent_limci = 0;
    fn joint_err(e: String) -> Result<()> {
        Err(ValidationError::JointError { err: e })
    }
    fn unit_err(e: String) -> Result<()> {
        Err(ValidationError::JointError { err: e })
    }
    let (join, feild);
    let create_err: fn(String) -> Result<()>;
    if joint.ball.is_some() {
        join = "LEFT JOIN balls USING(unit) LEFT JOIN hash_tree_balls ON units.unit=hash_tree_balls.unit";
        feild = ", IFNULL(balls.ball, hash_tree_balls.ball) AS ball";
        create_err = joint_err;
    } else {
        join = "";
        feild = "";
        create_err = unit_err;
    }

    for parent_unit in &unit.parent_units {
        if parent_unit <= &prev {
            return create_err("parent units not ordered".to_owned());
        }
        prev = parent_unit.clone();
        let sql = format!(
            "SELECT units.* {} FROM units {} WHERE units.unit=?",
            feild, join
        );
        let mut stmt = tx.prepare_cached(&sql)?;
        let mut rows = stmt
            .query_map(&[parent_unit], |row| UnitProps {
                unit_props: graph::UnitProps {
                    unit: row.get("unit"),
                    level: row.get("level"),
                    latest_included_mc_index: row.get("latest_included_mc_index"),
                    main_chain_index: row.get("main_chain_index"),
                    is_on_main_chain: row.get("is_on_main_chain"),
                    is_free: row.get("is_free"),
                },
                ball: row.get_checked("ball").unwrap_or(None),
            })?
            .collect::<::std::result::Result<Vec<UnitProps>, _>>()?;
        if rows.is_empty() {
            missing_parent_units.push(parent_unit.clone());
            continue;
        }

        let parent_unit_props = rows.swap_remove(0);
        if joint.ball.is_some() && parent_unit_props.ball.is_none() {
            bail_with_validation_err!(
                JointError,
                "no ball corresponding to parent unit {}",
                parent_unit
            );
        }

        let parent_unit_props = parent_unit_props.unit_props;

        if parent_unit_props.latest_included_mc_index > Some(validate_state.max_parent_limci) {
            validate_state.max_parent_limci = parent_unit_props.latest_included_mc_index.unwrap();
        }

        for unit_prop in &prev_parent_unit_props {
            let ret = graph::compare_unit_props(tx, unit_prop, &parent_unit_props)?;
            if ret.is_none() {
                continue;
            }
            return create_err(format!(
                "parent unit {} is related to one of the other parent units",
                parent_unit
            ));
        }
        prev_parent_unit_props.push(parent_unit_props);
    }

    if !missing_parent_units.is_empty() {
        let units = missing_parent_units
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT error FROM known_bad_joints WHERE unit IN({})",
            units
        );
        let mut stmt = tx.prepare(&sql)?;
        if stmt.exists(&[])? {
            return create_err("some of the unit's parents are known bad".to_owned());
        }
        return Err(ValidationError::NeedParentUnits(missing_parent_units));
    }

    let mut stmt = tx.prepare_cached("SELECT is_stable, is_on_main_chain, main_chain_index, ball, (SELECT MAX(main_chain_index) FROM units) AS max_known_mci \n\
				FROM units LEFT JOIN balls USING(unit) WHERE unit=?")?;
    let last_ball = &unit.last_ball;
    let last_ball_unit = unit.last_ball_unit.as_ref().expect("no last ball unit");
    struct LastBallUnitProps {
        is_stable: u32,
        is_on_main_chain: u32,
        main_chain_index: u32,
        ball: Option<String>,
        max_known_mci: u32,
    }
    let mut rows = stmt
        .query_map(&[last_ball_unit], |row| LastBallUnitProps {
            is_stable: row.get(0),
            is_on_main_chain: row.get(1),
            main_chain_index: row.get(2),
            ball: row.get(3),
            max_known_mci: row.get(4),
        })?
        .collect::<::std::result::Result<Vec<LastBallUnitProps>, _>>()?;

    if rows.len() != 1 {
        return create_err(format!("last ball unit {} not found", last_ball_unit));
    }

    let last_ball_unit_props = rows.swap_remove(0);
    if last_ball_unit_props.ball.is_none() && last_ball_unit_props.is_stable == 1 {
        return create_err(format!(
            "last ball unit {} is stable but has no ball",
            last_ball_unit
        ));
    }

    if last_ball_unit_props.is_on_main_chain != 1 {
        return create_err(format!("last ball {:?} is not on MC", last_ball));
    }

    if last_ball_unit_props.ball.is_some() && &last_ball_unit_props.ball != last_ball {
        return create_err(format!(
            "last_ball {:?} and last_ball_unit {} do not match",
            last_ball, last_ball_unit
        ));
    }

    validate_state.last_ball_mci = last_ball_unit_props.main_chain_index;
    validate_state.max_known_mci = last_ball_unit_props.max_known_mci;

    if validate_state.max_parent_limci < validate_state.last_ball_mci {
        return create_err(format!(
            "last ball unit {} is not included in parents, unit {}",
            last_ball_unit, unit_hash
        ));
    }

    let check_last_ball_did_not_restart = || {
        let sql = format!(
            "SELECT MAX(lb_units.main_chain_index) AS max_parent_last_ball_mci \
             FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \
             WHERE units.unit IN({})",
            parent_units
        );
        let mut stmt = tx.prepare(&sql)?;
        let max_parent_last_ball_mci = stmt
            .query_map(&[], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<Option<u32>>, _>>()?;
        if max_parent_last_ball_mci[0] > Some(validate_state.last_ball_mci) {
            bail_with_validation_err!(
                JointError,
                "last ball mci must not retreat, parents: {:?}",
                parent_units
            );
        }
        Ok(())
    };

    let check_no_same_address_in_different_parents = || {
        if unit.parent_units.len() == 1 {
            return check_last_ball_did_not_restart();
        }
        let sql = format!(
            "SELECT address, COUNT(*) AS c FROM unit_authors WHERE unit IN({}) GROUP BY address HAVING c>1",
            parent_units
        );
        let mut stmt = tx.prepare(&sql)?;
        if stmt.exists(&[])? {
            bail_with_validation_err!(JointError, "some addresses found more than once in parents");
        }
        check_last_ball_did_not_restart()
    };

    if last_ball_unit_props.is_stable == 1 {
        // if it were not stable, we wouldn't have had the ball at all
        if &last_ball_unit_props.ball != last_ball {
            return create_err(format!(
                "stable: last_ball {:?} and last_ball_unit {} do not match",
                last_ball, last_ball_unit
            ));
        }

        // FIXME: what's this!!!
        if validate_state.last_ball_mci <= 800000 {
            return check_no_same_address_in_different_parents();
        }
    }

    // Last ball is not stable yet in our view. Check if it is stable in view of the parents
    // TODO: implment main_chain
    let is_stable = main_chain::determin_if_stable_in_laster_units_and_update_stable_mc_flag(
        tx,
        last_ball_unit,
        &unit.parent_units,
        last_ball_unit_props.is_stable == 1,
    )?;

    if !is_stable && last_ball_unit_props.is_stable == 1 {
        info!(
            "last ball is stable, but not stable in parents, unit {}",
            unit_hash
        );
        return check_no_same_address_in_different_parents();
    }
    if !is_stable {
        return create_err(format!(
            "{}: last ball unit {} is not stable in view of your parents {:?}",
            unit_hash, last_ball_unit, unit.parent_units
        ));
    }

    let mut stmt = tx.prepare_cached("SELECT ball FROM balls WHERE unit=?")?;
    let balls = stmt
        .query_map(&[unit_hash], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if balls.is_empty() {
        return create_err(format!(
            "last ball unit {} just became stable but ball not found",
            last_ball_unit
        ));
    }
    if last_ball.as_ref() != Some(&balls[0]) {
        return create_err(format!(
            "last_ball {:?} and last_ball_unit {} do not match after advancing stability point",
            last_ball, last_ball_unit
        ));
    }
    check_no_same_address_in_different_parents()
}

fn validate_skip_list(_tx: &Transaction, _skip_list: &Vec<String>) -> Result<()> {
    Ok(())
    // unimplemented!("validate_skip_list")
}

fn validate_witnesses(
    tx: &Transaction,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    let validate_witness_list_mutations = |temp_witnesses: &Vec<String>| -> Result<()> {
        if unit.parent_units.is_empty() {
            return Ok(());
        }
        let determine_result = storage::determine_if_has_witness_list_mutations_along_mc(
            tx,
            unit,
            &unit
                .last_ball_unit
                .as_ref()
                .expect("last_ball_unit is empty"),
            temp_witnesses,
        );
        if determine_result.is_err() && validate_state.last_ball_mci >= 512000 {
            bail_with_validation_err!(UnitError, "{}", determine_result.err().unwrap())
        }
        let str_witness: String = temp_witnesses
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
                "SELECT 1 \
			FROM address_definition_changes \
			JOIN definitions USING(definition_chash) \
			JOIN units AS change_units USING(unit) \
			JOIN unit_authors USING(definition_chash) \
			JOIN units AS definition_units ON unit_authors.unit=definition_units.unit \
			WHERE address_definition_changes.address IN({}) AND has_references=1 \
				AND change_units.is_stable=1 AND change_units.main_chain_index<=? AND +change_units.sequence='good' \
				AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \
			UNION \
			SELECT 1 \
			FROM definitions \
			CROSS JOIN unit_authors USING(definition_chash) \
			JOIN units AS definition_units ON unit_authors.unit=definition_units.unit \
			WHERE definition_chash IN({}) AND has_references=1 \
				AND definition_units.is_stable=1 AND definition_units.main_chain_index<=? AND +definition_units.sequence='good' \
			LIMIT 1",
                str_witness, str_witness
            );
        let mut stmt = tx.prepare_cached(&sql)?;
        if stmt.exists(&[
            &validate_state.last_ball_mci,
            &validate_state.last_ball_mci,
            &validate_state.last_ball_mci,
        ])? {
            bail_with_validation_err!(
                UnitError,
                "some witnesses have references in their addresses"
            )
        }
        Ok(())
    };
    if let Some(witness_list_unit) = unit.witness_list_unit.as_ref() {
        let mut stmt = tx.prepare_cached(
            "SELECT sequence, is_stable, main_chain_index FROM units WHERE unit=?",
        )?;
        struct TempUnits {
            sequence: String,
            is_stable: u32,
            main_chain_index: Option<u32>,
        }
        let units = stmt
            .query_map(&[witness_list_unit], |rows| TempUnits {
                sequence: rows.get(0),
                is_stable: rows.get(1),
                main_chain_index: rows.get(2),
            })?
            .collect::<::std::result::Result<Vec<_>, _>>()?;
        if units.is_empty() {
            bail_with_validation_err!(UnitError, "referenced witness list unit is empty")
        }
        let witness_list_unit_props = &units[0];
        if witness_list_unit_props.sequence != "good" {
            bail_with_validation_err!(UnitError, "witness list unit is not serialy")
        }
        if witness_list_unit_props.is_stable != 1 {
            bail_with_validation_err!(UnitError, "witness list unit is not stable")
        }
        if witness_list_unit_props.main_chain_index > Some(validate_state.last_ball_mci) {
            bail_with_validation_err!(UnitError, "witness list unit must come before last ball")
        }

        let mut stmt =
            tx.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
        let witnesses = stmt
            .query_map(&[witness_list_unit], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;
        if witnesses.is_empty() {
            bail_with_validation_err!(UnitError, "referenced witness list unit has no witnessesl")
        }
        if witnesses.len() != config::COUNT_WITNESSES {
            bail_with_validation_err!(UnitError, "wrong number of witnesses: {}", witnesses.len())
        }
        validate_witness_list_mutations(&witnesses)?;
    } else if unit.witnesses.len() == config::COUNT_WITNESSES {
        let mut witness_iter = unit.witnesses.iter();
        let mut prev_witness = witness_iter.next();
        for curr_witness in witness_iter {
            if !object_hash::is_chash_valid(curr_witness) {
                bail_with_validation_err!(UnitError, "witness address is invalid")
            }

            if Some(curr_witness) <= prev_witness {
                bail_with_validation_err!(UnitError, "wrong order of witnesses, or duplicates")
            }
            prev_witness = Some(curr_witness);
        }

        if is_genesis_unit(&unit.unit.as_ref().expect("unit hash missing")) {
            validate_witness_list_mutations(&unit.witnesses)?;
            return Ok(());
        }
        let unit_witnesses: String = unit
            .witnesses
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<String>>()
            .join(",");
        let sql = format!(
            "SELECT COUNT(DISTINCT address) AS \
             count_stable_good_witnesses FROM unit_authors JOIN units USING(unit) \
             WHERE address=definition_chash AND +sequence='good' AND \
             is_stable=1 AND main_chain_index<=? AND address IN({})",
            unit_witnesses
        );
        let mut stmt = tx.prepare_cached(&sql)?;
        let count_stable_good_witnesses =
            stmt.query_row(&[&validate_state.last_ball_mci], |row| row.get::<_, u32>(0))?;
        if count_stable_good_witnesses != config::COUNT_WITNESSES as u32 {
            bail_with_validation_err!(
                UnitError,
                "some witnesses are not stable, not serial, or don't come before last ball"
            )
        }
        validate_witness_list_mutations(&unit.witnesses)?;
    } else {
        bail_with_validation_err!(UnitError, "no witnesses or not enough witnesses")
    }

    Ok(())
}

fn validate_authors(
    tx: &Transaction,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    if unit.authors.len() > config::MAX_AUTHORS_PER_UNIT {
        bail!("too many authors");
    }
    let mut prev_address = String::from("");
    for author in unit.authors.iter() {
        if author.address <= prev_address {
            bail!("author addresses not sorted");
        }
        prev_address = author.address.clone();
    }
    validate_state.unit_hash_to_sign = Some(unit.get_unit_hash_to_sign());
    for author in unit.authors.iter() {
        validate_author(tx, author, unit, validate_state)?;
    }
    Ok(())
}

fn validate_author(
    tx: &Transaction,
    author: &Author,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    if author.address.len() != 32 {
        bail!("wrong address length");
    }
    if author.authentifiers.is_empty() && unit.content_hash.is_none() {
        bail!("no authentifiers");
    }
    for (_, value) in author.authentifiers.iter() {
        if value.is_empty() {
            bail!("authentifiers must be nonempty strings");
        }
        if value.len() > config::MAX_AUTHENTIFIER_LENGTH {
            bail!("authentifier too long");
        }
    }

    if author.definition == Value::Null {
        if !object_hash::is_chash_valid(&author.address).unwrap() {
            bail!("address checksum invalid");
        }
        if unit.content_hash.is_some() {
            validate_state.sequence = "final-bad".to_string();
            return Ok(());
        }

        let definition_chash = object_hash::get_chash(&author.definition)?;

        storage::read_definition_by_address(
            tx,
            &author.address,
            Some(validate_state.last_ball_mci),
        ).or_else(|e| {
            bail!(
                "definition {} bound to address {} is not defined, err = {}",
                definition_chash,
                author.address,
                e
            )
        })?;
    } else {
        definition::validate_authentifiers(
            tx,
            &author.address,
            &Value::Null,
            &author.definition,
            unit,
            validate_state,
            &author.authentifiers,
        )?;
        bail!("bad type of defination");
    }

    Ok(())
}

fn validate_messages(
    _tx: &Transaction,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
) -> Result<()> {
    Ok(())
    // unimplemented!()
}
