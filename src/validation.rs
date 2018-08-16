use config;
use definition;
use graph;
use headers_commission;
use joint::Joint;
use main_chain;
use mc_outputs;
use object_hash;
use paid_witnessing;
use rusqlite::{Connection, Transaction};
use serde_json::Value;
use spec::*;
use storage;
use utils::{MapLock, MapLockGuard};
// global address map lock
lazy_static! {
    // maybe this is too heavy, could use an optimized hashset<AtomicBool>
    static ref ADDRESS_LOCK: MapLock<String> = MapLock::new();
}

macro_rules! bail_with_validation_err {
    ($t:ident, $e:expr) => {
        return Err(ValidationError::$t {
            err: ($e).to_string(),
        });
    };
    ($t:ident, $fmt:expr, $($arg:tt)+) => {
        return Err(ValidationError::$t {
            err: format!($fmt, $($arg)+)
        });
    };
}
macro_rules! ensure_with_validation_err {
    ($cond:expr, $t:ident, $e:expr) => {
        if !($cond) {
            bail_with_validation_err!($t, $e);
        }
    };
    ($cond:expr, $t:ident, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            bail_with_validation_err!($t, $fmt, $($arg)+);
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
    pub additional_queries: ::db::DbQueries,
    pub double_spend_inputs: Vec<DoubleSpendInput>,
    pub addresses_with_forked_path: Vec<String>,
    pub conflicting_units: Vec<String>,
    pub input_keys: Vec<String>, //It could be spendproof in Spendproof or some input related customized string
    pub has_base_payment: bool,
    pub has_data_feed: bool,
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
            additional_queries: ::db::DbQueries::new(),
            double_spend_inputs: Vec::new(),
            addresses_with_forked_path: Vec::new(),
            conflicting_units: Vec::new(),
            input_keys: Vec::new(),
            has_base_payment: false,
            has_data_feed: false,
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
    Signed(ValidationState, MapLockGuard<'static, String>),
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
    // ensure_with_validation_err!(unit.unit.is_some(), "no unit");

    let unit_hash = joint.get_unit_hash();
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
    if unit.parent_units.len() > config::MAX_PARENT_PER_UNIT {
        bail_with_validation_err!(UnitError, "too many parents: {}", unit.parent_units.len());
    }

    let unit_hash = joint.get_unit_hash();
    let parent_units = unit
        .parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

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
            })?.collect::<::std::result::Result<Vec<UnitProps>, _>>()?;
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
        })?.collect::<::std::result::Result<Vec<LastBallUnitProps>, _>>()?;

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
        .query_map(&[last_ball_unit], |row| row.get(0))?
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

fn validate_skip_list(tx: &Transaction, skip_list: &Vec<String>) -> Result<()> {
    for skip_list_unit in skip_list {
        if skip_list_unit.is_empty() {
            bail_with_validation_err!(JointError, "skiplist units empty");
        }

        struct TempUnit {
            is_stable: u32,
            is_on_main_chain: Option<u32>,
            main_chain_index: Option<u32>,
        }
        let mut result = tx.prepare(
            "SELECT unit, is_stable, is_on_main_chain, main_chain_index FROM units WHERE unit=?",
        )?;
        let mut rows = result
            .query_map(&[skip_list_unit], |row| TempUnit {
                is_stable: row.get(1),
                is_on_main_chain: row.get(2),
                main_chain_index: row.get(3),
            })?.collect::<::std::result::Result<Vec<TempUnit>, _>>()?;

        if rows.is_empty() {
            bail_with_validation_err!(UnitError, "skiplist unit {} not found", skip_list_unit);
        }

        let row = rows.iter().nth(0).unwrap();
        if row.is_stable == 1 {
            if row.is_on_main_chain != Some(1) {
                bail_with_validation_err!(
                    UnitError,
                    "skiplist unit {} is not on MC",
                    skip_list_unit
                );
            }
            if row.main_chain_index.map(|v| v % 10) != Some(0) {
                bail_with_validation_err!(
                    JointError,
                    "skiplist unit {} MCI is not divisible by 10",
                    skip_list_unit
                );
            }
        }
    }

    Ok(())
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
            })?.collect::<::std::result::Result<Vec<_>, _>>()?;
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
        bail_with_validation_err!(UnitError, "too many authors");
    }
    let mut prev_address = String::from("");
    for author in &unit.authors {
        if author.address <= prev_address {
            bail_with_validation_err!(UnitError, "author addresses not sorted");
        }
        prev_address = author.address.clone();
    }
    validate_state.unit_hash_to_sign = Some(unit.get_unit_hash_to_sign());
    for author in &unit.authors {
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
        bail_with_validation_err!(UnitError, "wrong address length");
    }
    if author.authentifiers.is_empty() && unit.content_hash.is_none() {
        bail_with_validation_err!(UnitError, "no authentifiers");
    }
    for (_, value) in &author.authentifiers {
        if value.is_empty() {
            bail_with_validation_err!(UnitError, "authentifiers must be nonempty strings");
        }
        if value.len() > config::MAX_AUTHENTIFIER_LENGTH {
            bail_with_validation_err!(UnitError, "authentifier too long");
        }
    }

    let mut nonserial = false;
    let address_definition = author.definition.clone();

    let handle_duplicate_address_definition = |validate_state: &mut ValidationState,
                                               address_definition: Value,
                                               nonserial: bool|
     -> Result<()> {
        if !nonserial || !validate_state
            .addresses_with_forked_path
            .contains(&author.address)
        {
            bail_with_validation_err!(
                UnitError,
                "duplicate definition of address {}, bNonserial={}",
                author.address,
                nonserial
            );
        }

        if object_hash::get_chash(&address_definition).unwrap()
            != object_hash::get_chash(&author.definition).unwrap()
        {
            bail_with_validation_err!(
                UnitError,
                "unit definition doesn't match the stored definition"
            );
        }

        Ok(())
    };

    let validate_definition =
        |validate_state: &mut ValidationState, nonserial: bool| -> Result<()> {
            if author.definition.is_null() {
                return Ok(());
            }

            let ret_definition = storage::read_definition_by_address(
                tx,
                &author.address,
                Some(validate_state.last_ball_mci),
            )?;

            let definition = match ret_definition {
                Ok(v) => v,
                Err(chash) => {
                    let definition_chash = object_hash::get_chash(&author.definition)?;
                    if definition_chash != chash {
                        bail_with_validation_err!(
                            UnitError,
                            "wrong definition {}: chash {} != {}",
                            author.definition,
                            definition_chash,
                            chash,
                        );
                    }
                    return Ok(());
                }
            };

            handle_duplicate_address_definition(validate_state, definition, nonserial)?;

            Ok(())
        };

    let check_no_pending_definition = |validate_state: &mut ValidationState,
                                       nonserial: bool|
     -> Result<()> {
        let cross = if validate_state.max_known_mci - validate_state.last_ball_mci < 1000 {
            "CROSS"
        } else {
            ""
        };
        let sql = format!("SELECT unit FROM units {} JOIN unit_authors USING(unit) \
			WHERE address=? AND definition_chash IS NOT NULL AND ( main_chain_index>? OR main_chain_index IS NULL)",cross);
        let mut stmt = tx.prepare_cached(&sql)?;
        let rows = stmt
            .query_map(&[&author.address, &validate_state.last_ball_mci], |row| {
                row.get(0)
            })?.collect::<::std::result::Result<Vec<String>, _>>()?;

        if rows.is_empty() {
            validate_definition(validate_state, nonserial)?;
            return Ok(());
        }
        if !nonserial || !validate_state
            .addresses_with_forked_path
            .contains(&author.address)
        {
            bail_with_validation_err!(UnitError, "you can't send anything before your last definition is stable and before last ball");
        }
        for row in rows.into_iter() {
            let included = graph::determine_if_included_or_equal(tx, &row, &unit.parent_units)?;
            if included {
                info!("checkNoPendingDefinition: unit {} is included", row);
                bail_with_validation_err!(UnitError, "you can't send anything before your last included definition is stable and before last ball (self is nonserial)");
            }
        }
        validate_definition(validate_state, nonserial)?;
        Ok(())
    };

    let check_no_pending_change_of_definition_chash = |validate_state: &mut ValidationState,
                                                       nonserial: bool|
     -> Result<()> {
        let mut stmt = tx.prepare_cached(
            "SELECT unit FROM address_definition_changes JOIN units USING(unit) \
             WHERE address=? AND (is_stable=0 OR main_chain_index>? OR main_chain_index IS NULL)",
        )?;
        let rows = stmt
            .query_map(&[&author.address, &validate_state.last_ball_mci], |row| {
                row.get(0)
            })?.collect::<::std::result::Result<Vec<String>, _>>()?;

        if rows.is_empty() {
            check_no_pending_definition(validate_state, nonserial)?;
            return Ok(());
        }

        if !nonserial || !validate_state
            .addresses_with_forked_path
            .contains(&author.address)
        {
            bail_with_validation_err!(
                UnitError,
                "you can't send anything before your last keychange is stable and before last ball"
            );
        }

        for row in rows.into_iter() {
            let included = graph::determine_if_included_or_equal(tx, &row, &unit.parent_units)?;
            if included {
                info!(
                    "checkNoPendingChangeOfDefinitionChash: unit {} is included",
                    row
                );
                bail_with_validation_err!(UnitError, "you can't send anything before your last included keychange is stable and before last ball (self is nonserial)");
            }
        }
        check_no_pending_definition(validate_state, nonserial)?;

        Ok(())
    };

    let check_serial_address_use =
        |validate_state: &mut ValidationState, nonserial: &mut bool| -> Result<()> {
            let cross = if (validate_state.max_known_mci - validate_state.max_parent_limci) < 1000 {
                "CROSS"
            } else {
                ""
            };

            struct TempUnit {
                unit: String,
                is_stable: u32,
            }
            let sql = format!(
            "SELECT unit, is_stable \
             FROM units \
             {} JOIN unit_authors USING(unit) \
             WHERE address=? AND (main_chain_index>? OR main_chain_index IS NULL) AND unit != ?",
            cross
        );

            let mut stmt = tx.prepare_cached(&sql)?;
            let rows = stmt
                .query_map(
                    &[
                        &author.address,
                        &validate_state.max_parent_limci,
                        &unit.unit,
                    ],
                    |row| TempUnit {
                        unit: row.get(0),
                        is_stable: row.get(1),
                    },
                )?.collect::<::std::result::Result<Vec<_>, _>>()?;

            let mut conflicting_unit_props = Vec::new();
            for row in &rows {
                let included =
                    graph::determine_if_included_or_equal(tx, &row.unit, &unit.parent_units)?;
                if !included {
                    conflicting_unit_props.push(row);
                }
            }

            if conflicting_unit_props.is_empty() {
                if validate_state.sequence.is_empty() {
                    validate_state.sequence = "good".to_owned();
                }

                check_no_pending_change_of_definition_chash(validate_state, false)?;
                return Ok(());
            }

            let mut conflicting_units = conflicting_unit_props
                .iter()
                .map(|x| x.unit.clone())
                .collect::<Vec<_>>();

            info!(
                "========== found conflicting units {:?} =========",
                conflicting_units
            );
            info!(
                "========== will accept a conflicting unit {} =========",
                unit.unit.clone().unwrap()
            );

            validate_state
                .addresses_with_forked_path
                .push(author.address.clone());

            validate_state
                .conflicting_units
                .append(&mut conflicting_units);
            *nonserial = true;
            let unstable_conflicting_unit_props = conflicting_unit_props
                .iter()
                .filter(|x| x.is_stable == 0)
                .collect::<Vec<_>>();

            let is_conflicts_with_stable_units =
                conflicting_unit_props.iter().any(|x| x.is_stable == 1);

            if &validate_state.sequence != "final-bad" {
                validate_state.sequence = if is_conflicts_with_stable_units {
                    "final-bad".to_owned()
                } else {
                    "temp-bad".to_owned()
                };
            }
            let unstable_conflicting_units = unstable_conflicting_unit_props
                .iter()
                .map(|x| &x.unit)
                .collect::<Vec<_>>();

            if is_conflicts_with_stable_units || unstable_conflicting_units.is_empty() {
                check_no_pending_change_of_definition_chash(validate_state, *nonserial)?;
                return Ok(());
            }

            let units_list = unstable_conflicting_units
                .iter()
                .map(|s| format!("'{}'", s))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "UPDATE units SET sequence='temp-bad' WHERE unit IN ({}) AND +sequence='good'",
                units_list
            );

            validate_state.additional_queries.add_query(move |db| {
                info!("----- applying additional queries: {}", sql);
                let mut stmt = db.prepare(&sql)?;
                stmt.execute(&[])?;
                Ok(())
            });

            check_no_pending_change_of_definition_chash(validate_state, *nonserial)?;
            Ok(())
        };

    let validate_authentifiers = |address_definition: &Value,
                                  validate_state: &mut ValidationState,
                                  nonserial: &mut bool|
     -> Result<()> {
        definition::validate_authentifiers(
            tx,
            &author.address,
            &Value::Null,
            &address_definition,
            unit,
            validate_state,
            &author.authentifiers,
        )?;

        check_serial_address_use(validate_state, nonserial)?;
        Ok(())
    };
    if !address_definition.is_null() {
        validate_authentifiers(&address_definition, validate_state, &mut nonserial)?;
    } else {
        if !object_hash::is_chash_valid(&author.address) {
            bail_with_validation_err!(UnitError, "address checksum invalid");
        }
        if unit.content_hash.is_some() {
            validate_state.sequence = "final-bad".to_owned();
            return Ok(());
        }

        let tmp_address_definition = storage::read_definition_by_address(
            tx,
            &author.address,
            Some(validate_state.last_ball_mci),
        )?;
        let definition = match tmp_address_definition {
            Ok(v) => v,
            Err(chash) => {
                bail_with_validation_err!(
                    UnitError,
                    "definition {} bound to address {} is not defined",
                    chash,
                    author.address
                );
            }
        };

        validate_authentifiers(&definition, validate_state, &mut nonserial)?;
    }
    Ok(())
}

#[inline]
fn is_valid_base64(b64: &String, len: usize) -> bool {
    use base64;
    if b64.len() != len {
        return false;
    }
    match base64::decode(b64) {
        Ok(v) => b64 == &base64::encode(&v),
        _ => false,
    }
}

fn validate_messages(
    tx: &Transaction,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    info!("validateMessages {:?}", unit.unit);

    for (message_index, message) in unit.messages.iter().enumerate() {
        validate_message(tx, &message, message_index, unit, validate_state)?;
    }

    ensure_with_validation_err!(
        validate_state.has_base_payment,
        UnitError,
        "no base payment message"
    );

    Ok(())
}

fn validate_message(
    tx: &Transaction,
    message: &Message,
    message_index: usize,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    //Some quick checks includes spend proofs, payload, payment etc
    ensure_with_validation_err!(
        message.payload_hash.len() == config::HASH_LENGTH,
        UnitError,
        "wrong payload hash size"
    );

    if !message.spend_proofs.is_empty() {
        ensure_with_validation_err!(
            message.spend_proofs.len() <= config::MAX_SPEND_PROOFS_PER_MESSAGE,
            UnitError,
            "spend_proofs must be non-empty array max {} elements",
            config::MAX_SPEND_PROOFS_PER_MESSAGE
        );

        let author_addresses = unit
            .authors
            .iter()
            .map(|a| a.address.clone())
            .collect::<Vec<_>>();

        //Spend proofs are sorted in the same order as their corresponding inputs
        for spend_proof in message.spend_proofs.iter() {
            ensure_with_validation_err!(
                is_valid_base64(&spend_proof.spend_proof, config::HASH_LENGTH),
                UnitError,
                "spend proof {} is not a valid base64",
                spend_proof.spend_proof
            );

            if author_addresses.len() == 1 {
                ensure_with_validation_err!(
                    spend_proof.address.is_none(),
                    UnitError,
                    "when single-authored, must not put address in spend proof"
                );
            } else {
                ensure_with_validation_err!(
                    spend_proof.address.is_some(),
                    UnitError,
                    "when multi-authored, must put address in spend_proofs"
                );

                let spend_proof_address = spend_proof.address.as_ref().unwrap();
                ensure_with_validation_err!(
                    author_addresses.contains(spend_proof_address),
                    UnitError,
                    "spend proof address {} is not an author",
                    spend_proof_address
                );
            }

            if validate_state.input_keys.contains(&spend_proof.spend_proof) {
                bail_with_validation_err!(
                    UnitError,
                    "spend proof {} already used",
                    spend_proof.spend_proof
                );
            }
            validate_state
                .input_keys
                .push(spend_proof.spend_proof.clone());
        }

        ensure_with_validation_err!(
            message.payload_location != "inline",
            UnitError,
            "you don't need spend proofs when you have inline payload"
        );
    }

    if message.payload_location != "inline"
        && message.payload_location != "uri"
        && message.payload_location != "none"
    {
        bail_with_validation_err!(
            UnitError,
            "wrong payload location: {}",
            message.payload_location
        );
    }

    if message.payload_location == "uri" {
        ensure_with_validation_err!(
            message.payload.is_none(),
            UnitError,
            "must not contain payload"
        );
        ensure_with_validation_err!(message.payload_uri.is_some(), UnitError, "no payload uri");
        ensure_with_validation_err!(
            message.payload_uri_hash.is_some(),
            UnitError,
            "no payload uri hash"
        );

        let payload_uri = message.payload_uri.as_ref().unwrap();
        let payload_uri_hash = message.payload_uri_hash.as_ref().unwrap();
        ensure_with_validation_err!(
            payload_uri_hash.len() == config::HASH_LENGTH,
            UnitError,
            "wrong length of payload uri hash"
        );
        ensure_with_validation_err!(payload_uri.len() <= 500, UnitError, "payload_uri too long");
        ensure_with_validation_err!(
            object_hash::get_base64_hash(payload_uri)? == *payload_uri_hash,
            UnitError,
            "wrong payload_uri hash"
        );
    } else {
        ensure_with_validation_err!(
            message.payload_uri.is_none() && message.payload_uri_hash.is_none(),
            UnitError,
            "must not contain payload_uri and payload_uri_hash"
        );
    }

    if message.app == "payment" {
        ensure_with_validation_err!(
            message.payload_location == "inline" || message.payload_location == "none",
            UnitError,
            "payment location must be inline or none"
        );
        if message.payload_location == "none" && message.spend_proofs.len() == 0 {
            bail_with_validation_err!(UnitError, "private payment must come with spend proof(s)");
        }
    }

    let inline_only_apps = vec![
        "address_definition_change",
        "data_feed",
        "definition_template",
        "asset",
        "asset_attestors",
        "attestation",
        "poll",
        "vote",
    ];
    if inline_only_apps.contains(&message.app.as_str()) && message.payload_location != "inline" {
        bail_with_validation_err!(UnitError, "{} must be inline", message.app);
    }

    validate_spend_proofs(tx, message, unit, validate_state)?;

    validate_payload(tx, message, message_index, unit, validate_state)?;

    Ok(())
}

fn validate_spend_proofs(
    tx: &Transaction,
    message: &Message,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    if message.spend_proofs.is_empty() {
        return Ok(());
    }

    let eqs = message
        .spend_proofs
        .iter()
        .map(|s| {
            let address = if s.address.is_some() {
                s.address.as_ref().unwrap()
            } else {
                &unit.authors[0].address
            };
            format!("spend_proof='{}' AND address='{}'", s.spend_proof, address)
        }).collect::<Vec<_>>()
        .join(" OR ");

    let sql = format!(
        "SELECT address, unit, main_chain_index, sequence \
         FROM spend_proofs JOIN units USING(unit) WHERE unit != {} AND ({})",
        unit.unit.as_ref().unwrap(),
        eqs
    );

    check_for_double_spend(tx, "spend proof", &sql, unit, validate_state)?;

    Ok(())
}

fn check_for_double_spend(
    tx: &Transaction,
    kind: &str,
    sql: &String,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    let mut stmt = tx.prepare(sql)?;

    struct ConflictingRecord {
        address: String,
        unit: String,
        main_chain_index: Option<u32>,
        sequence: String,
    };

    let rows = stmt
        .query_map(&[], |row| ConflictingRecord {
            address: row.get("address"),
            unit: row.get("unit"),
            main_chain_index: row.get("main_chain_index"),
            sequence: row.get("sequence"),
        })?.collect::<::std::result::Result<Vec<ConflictingRecord>, _>>()?;

    if rows.is_empty() {
        return Ok(());
    }

    let author_addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();

    for conflicting_record in rows {
        if !author_addresses.contains(&&conflicting_record.address) {
            bail_with_validation_err!(
                UnitError,
                "conflicting {} spent from another address?",
                kind
            );
        }

        let included = graph::determine_if_included_or_equal(
            &*tx,
            &conflicting_record.unit,
            &unit.parent_units,
        )?;

        if included {
            let error = format!(
                "{:?}: conflicting {} in inner unit {}",
                unit.unit, kind, conflicting_record.unit
            );

            // too young (serial or nonserial)
            if conflicting_record.main_chain_index > Some(validate_state.last_ball_mci)
                || conflicting_record.main_chain_index == None
            {
                bail_with_validation_err!(UnitError, error);
            }

            match conflicting_record.sequence.as_str() {
                "good" => bail_with_validation_err!(UnitError, error), // in good sequence (final state)
                "final-bad" => continue, // to be voided: can reuse the output
                _ => bail_with_validation_err!(
                    UnitError,
                    "unreachable code, conflicting {} in unit {}",
                    kind,
                    conflicting_record.unit,
                ),
            }
        } else {
            ensure_with_validation_err!(
                validate_state
                    .addresses_with_forked_path
                    .contains(&conflicting_record.address),
                UnitError,
                "double spending {} without double spending address?",
                kind
            );
            continue;
        }
    }

    Ok(())
}

fn validate_payload(
    tx: &Transaction,
    message: &Message,
    message_index: usize,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    if message.payload_location == "inline" {
        validate_inline_payload(tx, message, message_index, unit, validate_state)?;
    } else {
        ensure_with_validation_err!(
            is_valid_base64(&message.payload_hash, config::HASH_LENGTH),
            UnitError,
            "wrong payload hash"
        );
    }

    Ok(())
}

fn validate_inline_payload(
    tx: &Transaction,
    message: &Message,
    message_index: usize,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    let ref payload = message.payload.as_ref();

    ensure_with_validation_err!(payload.is_some(), UnitError, "no inline payload");

    let payload_hash = object_hash::get_base64_hash(payload)?;
    ensure_with_validation_err!(
        payload_hash == message.payload_hash,
        UnitError,
        "wrong payload hash: expected {}, got {}",
        payload_hash,
        message.payload_hash
    );

    match message.app.as_str() {
        "text" => match payload {
            Some(Payload::Text(ref _s)) => {}
            _ => bail_with_validation_err!(UnitError, "payload must be string"),
        },
        "payment" => if let Some(Payload::Payment(ref payment)) = payload {
            validate_payment(tx, payment, message_index, unit, validate_state)?;
        },
        "data_feed" => {
            if validate_state.has_data_feed {
                bail_with_validation_err!(UnitError, "can be only one data feed");
            }
            validate_state.has_data_feed = true;
            match payload {
                Some(Payload::Other(ref v)) => {
                    if let Some(map) = v.as_object() {
                        if map.is_empty() {
                            bail_with_validation_err!(
                                UnitError,
                                "data feed payload is empty object"
                            )
                        }
                        for (k, v) in map {
                            if k.len() > config::MAX_DATA_FEED_NAME_LENGTH {
                                bail_with_validation_err!(UnitError, "feed name {} too long", k);
                            }
                            if let Some(s) = v.as_str() {
                                if s.len() > config::MAX_DATA_FEED_VALUE_LENGTH {
                                    bail_with_validation_err!(UnitError, "value {} too long", s);
                                }
                            } else if v.is_number() {
                                if v.is_f64() {
                                    bail_with_validation_err!(
                                        UnitError,
                                        "fractional numbers not allowed in data feeds"
                                    );
                                }
                            } else {
                                bail_with_validation_err!(
                                    UnitError,
                                    "data feed {} must be string or number",
                                    k
                                );
                            }
                        }
                    } else {
                        bail_with_validation_err!(UnitError, "data feed payload is not object")
                    }
                }
                _ => bail_with_validation_err!(UnitError, "data feed payload is not data_feed"),
            }
        }
        _ => unimplemented!(),
    }

    Ok(())
}

fn validate_payment(
    tx: &Transaction,
    payment: &Payment,
    message_index: usize,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    //Base currency
    if payment.asset.is_none() {
        ensure_with_validation_err!(
            payment.address.is_none()
                && payment.definition_chash.is_none()
                && payment.denomination.is_none(),
            UnitError,
            "unknown fields in payment message"
        );

        ensure_with_validation_err!(
            !validate_state.has_base_payment,
            UnitError,
            "can have only one base payment"
        );

        validate_state.has_base_payment = true;

        return validate_payment_inputs_and_outputs(
            tx,
            payment,
            None,
            message_index,
            unit,
            validate_state,
        );
    }

    //We do not handle assets for now
    unimplemented!();
}

fn validate_payment_inputs_and_outputs(
    tx: &Transaction,
    payment: &Payment,
    asset: Option<String>,
    message_index: usize,
    unit: &Unit,
    validate_state: &mut ValidationState,
) -> Result<()> {
    let denomination = payment.denomination.unwrap_or(1);

    let author_addresses = unit.authors.iter().map(|a| &a.address).collect::<Vec<_>>();

    ensure_with_validation_err!(
        payment.inputs.len() <= config::MAX_INPUTS_PER_PAYMENT_MESSAGE,
        UnitError,
        "too many inputs"
    );
    ensure_with_validation_err!(
        payment.outputs.len() <= config::MAX_OUTPUTS_PER_PAYMENT_MESSAGE,
        UnitError,
        "too many outputs"
    );

    let mut input_addresses = Vec::new();
    let mut output_addresses = Vec::new();
    let mut total_input = 0;
    let mut total_output = 0;
    let mut prev_address = String::new();
    let mut prev_amount = 0;

    for output in &payment.outputs {
        ensure_with_validation_err!(
            output.amount > 0,
            UnitError,
            "amount must be positive integer, found {:?}",
            output.amount
        );

        // TODO: add asset check, we don't support private asset payment
        let amount = output.amount;
        let address = &output.address;

        ensure_with_validation_err!(
            object_hash::is_chash_valid(address),
            UnitError,
            "output address {} invalid",
            address,
        );

        if prev_address > *address {
            bail_with_validation_err!(UnitError, "output addresses not sorted");
        } else if &prev_address == address && prev_amount > amount {
            bail_with_validation_err!(UnitError, "output amounts for same address not sorted");
        }

        prev_address = address.clone();
        prev_amount = amount;

        if !output_addresses.contains(address) {
            output_addresses.push(address.to_owned());
        }

        total_output += amount;
    }

    let mut b_issue = false;
    let mut b_have_headers_commissions = false;
    let mut b_have_witnessing = false;

    for (index, input) in payment.inputs.iter().enumerate() {
        //Non-asset case
        let transfer = String::from("transfer");
        let kind = input.kind.as_ref().unwrap_or(&transfer);
        ensure_with_validation_err!(!kind.is_empty(), UnitError, "bad input type");

        match kind.as_str() {
            "issue" => {
                ensure_with_validation_err!(index == 0, UnitError, "issue must come first");

                //Should we make Input as a enum and all types as its variants?
                ensure_with_validation_err!(
                    input.from_main_chain_index.is_none()
                        && input.message_index.is_none()
                        && input.output_index.is_none()
                        && input.to_main_chain_index.is_none()
                        && input.unit.is_none(),
                    UnitError,
                    "unknown fields in issue input"
                );

                ensure_with_validation_err!(
                    input.amount > Some(0),
                    UnitError,
                    "amount must be positive"
                );

                //serial_number is a u32!
                // ensure_with_validation_err!(
                //     input.serial_number > Some(0),
                //     UnitError,
                //     "serial_number must be positive"
                // );

                //if (!objAsset || objAsset.cap)
                ensure_with_validation_err!(
                    input.serial_number == Some(1),
                    UnitError,
                    "for capped asset serial_number must be 1"
                );

                ensure_with_validation_err!(
                    !b_issue,
                    UnitError,
                    "only one issue per message allowed"
                );
                b_issue = true;

                let address = if author_addresses.len() == 1 {
                    ensure_with_validation_err!(
                        input.address.is_none(),
                        UnitError,
                        "when single-authored, must not put address in issue input"
                    );

                    &author_addresses[0]
                } else {
                    ensure_with_validation_err!(
                        input.address.is_some(),
                        UnitError,
                        "when multi-authored, must put address in issue input"
                    );

                    let input_address = input.address.as_ref().unwrap();
                    ensure_with_validation_err!(
                        author_addresses.contains(&input_address),
                        UnitError,
                        "issue input address {} is not an author",
                        input_address
                    );

                    input_address
                };

                input_addresses.push(address.clone());

                //Why not checking this first?
                ensure_with_validation_err!(
                    unit.is_genesis_unit(),
                    UnitError,
                    "only genesis can issue base asset"
                );

                ensure_with_validation_err!(
                    input.amount == Some(config::TOTAL_WHITEBYTES),
                    UnitError,
                    "issue must be equal to cap"
                );

                total_input += input.amount.unwrap_or(0);

                let input_key = format!(
                    "base-{}-{}-{}",
                    denomination,
                    address,
                    input.serial_number.unwrap_or(0),
                );

                ensure_with_validation_err!(
                    !validate_state.input_keys.contains(&input_key),
                    UnitError,
                    "input {} already used",
                    input_key
                );
                validate_state.input_keys.push(input_key);

                let double_spend_where = "type='issue'".to_owned();
                check_input_double_spend(
                    tx,
                    &double_spend_where,
                    unit,
                    validate_state,
                    message_index,
                    index,
                )?;
            }
            "transfer" => {
                if b_have_headers_commissions || b_have_witnessing {
                    bail_with_validation_err!(
                        UnitError,
                        "all transfers must come before hc and witnessings"
                    );
                }

                ensure_with_validation_err!(
                    input.address.is_none()
                        && input.amount.is_none()
                        && input.from_main_chain_index.is_none()
                        && input.serial_number.is_none()
                        && input.to_main_chain_index.is_none(),
                    UnitError,
                    "unknown fields in payment input"
                );

                ensure_with_validation_err!(
                    input.unit.is_some()
                        && input.unit.as_ref().unwrap().len() == config::HASH_LENGTH,
                    UnitError,
                    "wrong unit length in payment input"
                );

                ensure_with_validation_err!(
                    input.message_index.is_some(),
                    UnitError,
                    "no message_index in payment input"
                );

                ensure_with_validation_err!(
                    input.output_index.is_some(),
                    UnitError,
                    "no output_index in payment input"
                );

                let input_unit = input.unit.as_ref().unwrap();
                let input_message_index = input.message_index.unwrap();
                let input_output_index = input.output_index.unwrap();

                let input_key = format!(
                    "base-{}-{}-{}",
                    input_unit, input_message_index, input_output_index,
                );

                ensure_with_validation_err!(
                    !validate_state.input_keys.contains(&input_key),
                    UnitError,
                    "input {} already used",
                    input_key
                );
                validate_state.input_keys.push(input_key);

                let mut stmt = tx.prepare_cached(
                    "SELECT amount, is_stable, sequence, address, main_chain_index, denomination, asset \
                        FROM outputs \
                        JOIN units USING(unit) \
                        WHERE outputs.unit=? AND message_index=? AND output_index=?",
                )?;

                struct OutputTemp {
                    amount: Option<i64>,
                    // is_stable: u32,
                    sequence: String,
                    address: String,
                    main_chain_index: Option<u32>,
                    denomination: u32,
                    asset: Option<String>,
                }

                let rows = stmt
                    .query_map(
                        &[input_unit, &input_message_index, &input_output_index],
                        |row| OutputTemp {
                            amount: row.get(0),
                            // is_stable: row.get(1),
                            sequence: row.get(2),
                            address: row.get(3),
                            main_chain_index: row.get(4),
                            denomination: row.get(5),
                            asset: row.get(6),
                        },
                    )?.collect::<::std::result::Result<Vec<_>, _>>()?;

                if rows.len() > 1 {
                    bail_with_validation_err!(UnitError, "more than 1 src output");
                }

                if rows.is_empty() {
                    bail_with_validation_err!(UnitError, "input unit {} not found", input_unit);
                }

                let src_output = &rows[0];

                ensure_with_validation_err!(
                    src_output.amount.is_some(),
                    UnitError,
                    "src output amount is not a number"
                );

                //Now the payment.asset is None
                ensure_with_validation_err!(
                    payment.asset == src_output.asset,
                    UnitError,
                    "asset mismatch"
                );

                if src_output.main_chain_index > Some(validate_state.last_ball_mci)
                    || src_output.main_chain_index.is_none()
                {
                    bail_with_validation_err!(UnitError, "src output must be before last ball");
                }

                ensure_with_validation_err!(
                    src_output.sequence == "good",
                    UnitError,
                    "input unit {} is not serial",
                    input_unit
                );

                let owner_address = &src_output.address;
                ensure_with_validation_err!(
                    author_addresses.contains(&owner_address),
                    UnitError,
                    "output owner is not among authors"
                );

                ensure_with_validation_err!(
                    denomination == src_output.denomination,
                    UnitError,
                    "denomination mismatch"
                );

                if !input_addresses.contains(owner_address) {
                    input_addresses.push(owner_address.clone());
                }

                total_input += src_output.amount.unwrap_or(0);

                let double_spend_where = format!(
                    "type='{}' AND src_unit='{}' AND src_message_index={} AND src_output_index={}",
                    kind, input_unit, input_message_index, input_output_index
                );
                check_input_double_spend(
                    tx,
                    &double_spend_where,
                    unit,
                    validate_state,
                    message_index,
                    index,
                )?;
            }
            "headers_commission" | "witnessing" => {
                if kind == "headers_commission" {
                    ensure_with_validation_err!(
                        !b_have_witnessing,
                        UnitError,
                        "all headers commissions must come before witnessings"
                    );
                    b_have_headers_commissions = true;
                } else {
                    b_have_witnessing = true;
                }
                ensure_with_validation_err!(
                    input.amount.is_none()
                        && input.serial_number.is_none()
                        && input.message_index.is_none()
                        && input.output_index.is_none()
                        && input.unit.is_none(),
                    UnitError,
                    "unknown fields in witnessing input"
                );

                ensure_with_validation_err!(
                    input.from_main_chain_index.is_some(),
                    UnitError,
                    "from_main_chain_index must be nonnegative int"
                );
                ensure_with_validation_err!(
                    input.to_main_chain_index.is_some(),
                    UnitError,
                    "to_main_chain_index must be nonnegative int"
                );
                ensure_with_validation_err!(
                    input.from_main_chain_index > input.to_main_chain_index,
                    UnitError,
                    "input.from_main_chain_index > input.to_main_chain_index"
                );
                ensure_with_validation_err!(
                    input.to_main_chain_index > Some(validate_state.last_ball_mci),
                    UnitError,
                    "input.to_main_chain_index > objValidationState.last_ball_mci"
                );
                ensure_with_validation_err!(
                    input.from_main_chain_index > Some(validate_state.last_ball_mci),
                    UnitError,
                    "input.from_main_chain_index > objValidationState.last_ball_mci"
                );
                let address = if author_addresses.len() == 1 {
                    ensure_with_validation_err!(
                        input.address.is_none(),
                        UnitError,
                        "when single-authored, must not put address in {} input",
                        kind
                    );
                    author_addresses[0].clone()
                } else {
                    let tmp_input_address = input.address.clone().unwrap();
                    ensure_with_validation_err!(
                        author_addresses.contains(&&tmp_input_address),
                        UnitError,
                        "{} input address {} is not an author",
                        kind,
                        tmp_input_address
                    );
                    input.address.clone().unwrap()
                };
                let input_key = format!(
                    "{}-{}-{}",
                    kind,
                    address,
                    input.from_main_chain_index.unwrap()
                );
                ensure_with_validation_err!(
                    !validate_state.input_keys.contains(&input_key),
                    UnitError,
                    "input {} already used",
                    input_key
                );
                validate_state.input_keys.push(input_key);

                let next_spendable_mc_index = mc_outputs::read_next_spendable_mc_index(
                    tx,
                    kind,
                    &address,
                    &validate_state.conflicting_units,
                )?;

                ensure_with_validation_err!(
                    input.from_main_chain_index >= Some(next_spendable_mc_index),
                    UnitError,
                    "{} ranges must not overlap",
                    kind
                );

                let max_mci = if kind == "headers_commission" {
                    Some(headers_commission::get_max_spendable_mci_for_last_ball_mci(
                        validate_state.last_ball_mci,
                    ))
                } else {
                    paid_witnessing::get_max_spendable_mci_for_last_ball_mci(
                        validate_state.last_ball_mci,
                    )
                };
                ensure_with_validation_err!(
                    input.to_main_chain_index <= max_mci,
                    UnitError,
                    "{} to_main_chain_index is too large",
                    kind
                );

                let commission = if kind == "headers_commission" {
                    mc_outputs::calc_earnings(
                        tx,
                        kind,
                        input.from_main_chain_index.unwrap(),
                        input.to_main_chain_index.unwrap(),
                        &address,
                    )?
                } else {
                    paid_witnessing::calc_witness_earnings(
                        tx,
                        kind,
                        input.from_main_chain_index.unwrap(),
                        input.to_main_chain_index.unwrap(),
                        &address,
                    )?
                };
                ensure_with_validation_err!(commission != 0, UnitError, "zero {} commission", kind);
                total_input += i64::from(commission);

                let double_spend_where = format!(
                    "type='{}' AND from_main_chain_index={} AND address={} AND asset IS NULL",
                    kind,
                    input.from_main_chain_index.unwrap(),
                    address
                );
                check_input_double_spend(
                    tx,
                    &double_spend_where,
                    unit,
                    validate_state,
                    message_index,
                    index,
                )?;
            }
            _ => bail_with_validation_err!(UnitError, "unrecognized input type: {}", kind),
        }
    }

    info!(
        "inputs done {:?} {:?} {:?}",
        asset, input_addresses, output_addresses
    );

    ensure_with_validation_err!(
        total_input
            == total_output
                + unit.headers_commission.unwrap_or(0) as i64
                + unit.payload_commission.unwrap_or(0) as i64,
        UnitError,
        "inputs and outputs do not balance: {} != {} + {} + {}",
        total_input,
        total_output,
        unit.headers_commission.unwrap_or(0),
        unit.payload_commission.unwrap_or(0)
    );

    info!("validatePaymentInputsAndOutputs done");

    Ok(())
}

fn check_input_double_spend(
    tx: &Transaction,
    double_spend_where: &String,
    unit: &Unit,
    validate_state: &mut ValidationState,
    message_index: usize,
    input_index: usize,
) -> Result<()> {
    let sql = format!(
        "SELECT unit, address, message_index, input_index, main_chain_index, sequence, is_stable \
         from inputs JOIN units USING(unit) WHERE {} AND unit !='{}' AND asset IS NULL",
        double_spend_where,
        unit.unit.as_ref().unwrap(),
    );

    check_for_double_spend(tx, "divisible input", &sql, unit, validate_state)?;

    //acceptDoublespends
    info!("--- accepting doublespend on unit {:?}", unit.unit);

    let sql = format!(
        "UPDATE inputs SET is_unique=NULL WHERE {} \
         AND (SELECT is_stable FROM units WHERE units.unit=inputs.unit)=0",
        double_spend_where
    );

    validate_state.additional_queries.add_query(move |db| {
        info!("----- applying additional queries: {}", sql);
        let mut stmt = db.prepare(&sql)?;
        stmt.execute(&[])?;
        Ok(())
    });

    validate_state.double_spend_inputs.push(DoubleSpendInput {
        message_index: message_index as u32,
        input_index: input_index as u32,
    });

    Ok(())
}
