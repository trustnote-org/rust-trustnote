use error::{Result, TrustnoteError};
use joint::Joint;
use rusqlite::Connection;
use storage;

pub struct WitnessProof {
    pub unstable_mc_joints: Vec<Joint>,
    pub witness_change_and_definition: Vec<Joint>,
    pub last_ball_unit: String,
    pub last_ball_mci: u32,
}

pub fn prepare_witness_proof(
    db: &Connection,
    witnesses: Vec<String>,
    last_stable_mci: u32,
) -> Result<WitnessProof> {
    let mut witness_change_and_definition = Vec::new();
    let mut unstable_mc_joints = Vec::new();
    let mut last_ball_units = Vec::new();
    let last_ball_mci;
    let last_ball_unit;

    if storage::determine_if_witness_and_address_definition_have_refs(db, &witnesses)? {
        bail!(TrustnoteError::WitnessChanged);
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
        for author in joint.unit.authors.iter() {
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
        // TODO: should we return a typed Error other than a string error?
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
    let row = db.query_row(&sql, &[], |row| (row.get::<_, String>(0), row.get(1)))?;
    last_ball_unit = row.0;
    last_ball_mci = row.1;
    if last_stable_mci >= last_ball_mci {
        bail!(TrustnoteError::CatchupAlreadyCurrent);
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

    Ok(WitnessProof {
        unstable_mc_joints,
        witness_change_and_definition,
        last_ball_unit,
        last_ball_mci,
    })
}
