use error::{Result, TrustnoteError};
use joint::Joint;
use my_witness::MY_WITNESSES;
use object_hash;
use rusqlite::Connection;
use serde_json::Value;
use spec::*;
use std::collections::HashMap;
use storage;
use validation;

pub struct PrepareWitnessProof {
    pub unstable_mc_joints: Vec<Joint>,
    pub witness_change_and_definition: Vec<Joint>,
    pub last_ball_unit: String,
    pub last_ball_mci: u32,
}

pub fn prepare_witness_proof(
    db: &Connection,
    witnesses: &[String],
    last_stable_mci: u32,
) -> Result<PrepareWitnessProof> {
    let mut witness_change_and_definition = Vec::new();
    let mut unstable_mc_joints = Vec::new();
    let mut last_ball_units = Vec::new();
    let last_ball_mci;
    let last_ball_unit;

    if storage::determine_if_witness_and_address_definition_have_refs(db, witnesses)? {
        return Err(TrustnoteError::WitnessChanged.into());
    }

    // collect all unstable MC units
    let mut found_witnesses = Vec::new();
    let mut stmt = db.prepare_cached(
        "SELECT unit FROM units \
         WHERE is_on_main_chain=1 AND is_stable=0 \
         ORDER BY main_chain_index DESC",
    )?;

    let units = stmt.query_map(&[], |row| row.get::<_, String>(0))?;

    for unit in units {
        if unit.is_err() {
            error!("failed to get unit, err={:?}", unit);
            continue;
        }
        let unit = unit.unwrap();
        // let unit = unit?;
        let mut joint = storage::read_joint_with_ball(db, &unit)?;
        // FIXME: WTF of this?!  the unit might get stabilized while we were reading other units
        joint.ball = None;
        for author in &joint.unit.authors {
            let address = &author.address;
            if witnesses.contains(address) && !found_witnesses.contains(address) {
                found_witnesses.push(address.clone());
            }

            if joint.unit.last_ball_unit.is_some()
                && found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES
            {
                last_ball_units.push(joint.unit.last_ball_unit.as_ref().unwrap().clone())
            }
        }
        unstable_mc_joints.push(joint);
    }

    // select the newest last ball unit
    if last_ball_units.is_empty() {
        bail!("your witness list might be too much off, too few witness authored units");
    }

    let last_ball_units_set = last_ball_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT unit, main_chain_index FROM units \
         WHERE unit IN({}) \
         ORDER BY main_chain_index DESC LIMIT 1",
        last_ball_units_set
    );
    let row = db.query_row(&sql, &[], |row| (row.get(0), row.get(1)))?;
    last_ball_unit = row.0;
    last_ball_mci = row.1;
    if last_stable_mci >= last_ball_mci {
        return Err(TrustnoteError::CatchupAlreadyCurrent.into());
    }

    // add definition changes and new definitions of witnesses
    let after_last_stable_mci_cond = if last_stable_mci > 0 {
        format!("latest_included_mc_index>={}", last_stable_mci)
    } else {
        "1".to_owned()
    };

    let witness_set = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT unit, `level` \
	    FROM unit_authors INDEXED BY unitAuthorsIndexByAddressDefinitionChash \
		CROSS JOIN units USING(unit) \
		WHERE address IN({}) AND definition_chash IS NOT NULL AND {} AND is_stable=1 AND sequence='good' \
	    UNION \
		SELECT unit, `level` \
		FROM address_definition_changes \
		CROSS JOIN units USING(unit) \
		WHERE address_definition_changes.address IN({}) AND {} AND is_stable=1 AND sequence='good' \
		ORDER BY `level`", witness_set, after_last_stable_mci_cond, witness_set, after_last_stable_mci_cond);

    let mut stmt = db.prepare(&sql)?;
    let units = stmt.query_map(&[], |row| row.get(0))?;
    for unit in units {
        let unit = unit?;
        let joint = storage::read_joint_directly(db, &unit)?;
        witness_change_and_definition.push(joint);
    }

    Ok(PrepareWitnessProof {
        unstable_mc_joints,
        witness_change_and_definition,
        last_ball_unit,
        last_ball_mci,
    })
}

#[derive(Debug)]
pub struct ProcessWitnessProof {
    pub last_ball_units: Vec<String>,
    pub assoc_last_ball_by_last_ball_unit: HashMap<String, String>,
}

pub fn process_witness_proof(
    db: &Connection,
    unstable_mc_joints: &[Joint],
    witness_change_and_definition: &[Joint],
    from_current: bool,
) -> Result<ProcessWitnessProof> {
    let mut parent_units = Vec::new();
    let mut found_witnesses = Vec::new();
    let mut last_ball_units = Vec::new();
    let mut assoc_last_ball_by_last_ball_unit = HashMap::<String, String>::new();
    let mut witness_joints = Vec::new();

    for joint in unstable_mc_joints {
        let unit = &joint.unit;
        let unit_hash = joint.get_unit_hash();
        ensure!(joint.ball.is_none(), "unstable mc but has ball");
        ensure!(joint.has_valid_hashes(), "invalid hash");
        if !parent_units.is_empty() {
            ensure!(parent_units.contains(unit_hash), "not in parents");
        }

        let mut added_joint = false;
        for author in &unit.authors {
            let address = &author.address;
            if MY_WITNESSES.contains(address) {
                if !found_witnesses.contains(address) {
                    found_witnesses.push(address.clone());
                }
                if !added_joint {
                    witness_joints.push(joint);
                }
                added_joint = true;
            }
        }
        parent_units = unit.parent_units.clone();
        if unit.last_ball_unit.is_some() && found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES
        {
            let last_ball_unit = unit.last_ball_unit.as_ref().unwrap().clone();
            let last_ball = unit.last_ball.as_ref().unwrap().clone();
            last_ball_units.push(last_ball_unit.clone());
            assoc_last_ball_by_last_ball_unit.insert(last_ball_unit, last_ball);
        }
    }

    ensure!(
        found_witnesses.len() >= ::config::MAJORITY_OF_WITNESSES,
        "not enough witnesses"
    );
    ensure!(
        !last_ball_units.is_empty(),
        "processWitnessProof: no last ball units"
    );

    // changes and definitions of witnesses
    for joint in witness_change_and_definition {
        ensure!(
            joint.ball.is_some(),
            "witness_change_and_definition_joints: joint without ball"
        );
        ensure!(
            joint.has_valid_hashes(),
            "witness_change_and_definition_joints: invalid hash"
        );
        let unit = &joint.unit;

        let mut author_by_witness = false;
        for author in &unit.authors {
            let address = &author.address;
            if MY_WITNESSES.contains(address) {
                author_by_witness = true;
                break;
            }
        }
        ensure!(author_by_witness, "not authored by my witness");
    }

    let mut assoc_definitions = HashMap::<String, Value>::new();
    let mut assoc_definition_chashes = HashMap::<String, String>::new();

    if !from_current {
        for address in MY_WITNESSES.iter() {
            assoc_definition_chashes.insert(address.clone(), address.clone());
        }
    }

    for address in MY_WITNESSES.iter() {
        match storage::read_definition_by_address(db, address, None)? {
            // if found
            Ok(definition) => {
                let definition_chash = object_hash::get_chash(&definition)?;
                assoc_definitions.insert(definition_chash.clone(), definition);
                assoc_definition_chashes.insert(address.clone(), definition_chash);
            }
            // if NotFound
            Err(chash) => {
                assoc_definition_chashes.insert(address.clone(), chash);
            }
        }
    }

    let mut validate_unit = |unit: &Unit, require_definition_or_change: bool| -> Result<()> {
        let mut b_found = false;
        for author in &unit.authors {
            let address = &author.address;
            if !MY_WITNESSES.contains(address) {
                // not a witness - skip it
                continue;
            }

            let definition_chash = {
                let chash = assoc_definition_chashes.get(address);
                ensure!(
                    chash.is_some(),
                    "definition chash not known for address {}",
                    address
                );
                chash.unwrap().clone()
            };

            if !author.definition.is_null() {
                let chash = object_hash::get_chash(&author.definition)?;
                ensure!(
                    chash == *definition_chash,
                    "definition doesn't hash to the expected value"
                );
                assoc_definitions.insert(definition_chash.clone(), author.definition.clone());
                b_found = true;
            }

            if assoc_definitions.get(&definition_chash).is_none() {
                let definition = storage::read_definition(db, &definition_chash)?;
                assoc_definitions.insert(definition_chash.clone(), definition);
            }

            // handle author
            validation::validate_author_signature_without_ref(
                db,
                author,
                unit,
                assoc_definitions.get(&definition_chash).unwrap_or_else(|| {
                    panic!(
                        "failed to find definition, definition_chash={}",
                        definition_chash
                    )
                }),
            )?;
            for message in &unit.messages {
                let payment = match message.payload.as_ref() {
                    Some(Payload::Payment(ref p)) => Some(p),
                    _ => None,
                };
                let payload_address = payment.and_then(|p| p.address.as_ref());
                if message.app == "address_definition_change"
                    && (payload_address == Some(address)
                        || (unit.authors.len() == 1 && &unit.authors[0].address == address))
                {
                    let payment = payment.unwrap();
                    let chash = payment
                        .definition_chash
                        .as_ref()
                        .expect("no chash in payload")
                        .clone();
                    assoc_definition_chashes.insert(address.clone(), chash);
                    b_found = true;
                }
            }
        }

        if require_definition_or_change && !b_found {
            bail!("neither definition nor change");
        }
        Ok(())
    };

    for joint in witness_change_and_definition {
        let unit_hash = joint.get_unit_hash();
        if from_current {
            let mut stmt = db.prepare_cached("SELECT 1 FROM units WHERE unit=? AND is_stable=1")?;
            let rows = stmt.query_map(&[unit_hash], |row| row.get::<_, u32>(0))?;
            if rows.count() > 0 {
                continue;
            }
        }
        validate_unit(&joint.unit, true)?;
    }

    for joint in witness_joints {
        validate_unit(&joint.unit, false)?;
    }

    Ok(ProcessWitnessProof {
        last_ball_units,
        assoc_last_ball_by_last_ball_unit,
    })
}
