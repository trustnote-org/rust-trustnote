use error::Result;
use failure::ResultExt;
use rusqlite::Connection;

use config;
use main_chain;
use spec::Unit;
use storage;

pub struct LastStableBallAndParentUnits {
    pub parent_units: Vec<String>,
    pub last_stable_mc_ball: String,
    pub last_stable_mc_ball_mci: u32,
    pub last_stable_mc_ball_unit: String,
}

pub fn pick_parent_units_and_last_ball(
    db: &Connection,
    witnesses: &Vec<String>,
) -> Result<LastStableBallAndParentUnits> {
    let parent_units = pick_parent_units(db, witnesses).context("parent units are not found")?;

    let last_stable_mc_ball_unit =
        find_last_stable_mc_ball(db, witnesses).context("failed to find_last_stable_mc_ball")?;

    let LastStableBallAndParentUnits {
        last_stable_mc_ball,
        last_stable_mc_ball_mci,
        last_stable_mc_ball_unit,
        parent_units,
    } = adjust_last_stable_mc_ball_and_parents(
        db,
        &last_stable_mc_ball_unit,
        &parent_units,
        witnesses,
    ).context("failed to adjust_last_stable_mc_ball_and_parents")?;

    let witness_list_unit =
        storage::find_witness_list_unit(db, witnesses, last_stable_mc_ball_mci)?;

    let mut tmp_unit = Unit::default();
    tmp_unit.parent_units = parent_units.clone();
    tmp_unit.witness_list_unit = witness_list_unit;

    storage::determine_if_has_witness_list_mutations_along_mc(
        db,
        &tmp_unit,
        &last_stable_mc_ball_unit,
        witnesses,
    ).context("failed to determine_if_has_witness_list_mutations_along_mc.")?;

    Ok(LastStableBallAndParentUnits {
        last_stable_mc_ball_unit,
        last_stable_mc_ball,
        last_stable_mc_ball_mci,
        parent_units,
    })
}
fn pick_parent_units(db: &Connection, witnesses: &[String]) -> Result<Vec<String>> {
    struct TempUnit {
        unit: String,
        version: String,
        alt: String,
        count_matching_witnesses: u32,
    }
    let ss = "INDEXED BY byFree";
    let witnesses_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!("SELECT unit, version, alt, (\
            SELECT COUNT(*) \
			FROM unit_witnesses \
			WHERE unit_witnesses.unit IN(units.unit, units.witness_list_unit) AND address IN({}) \
		) AS count_matching_witnesses \
		FROM units '{}'\
		LEFT JOIN archived_joints USING(unit) \
		WHERE + sequence = 'good' AND is_free=1 AND archived_joints.unit IS NULL ORDER BY unit LIMIT {}",
        witnesses_list, ss, config::MAX_PARENT_PER_UNIT);

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| TempUnit {
            unit: row.get(0),
            version: row.get(1),
            alt: row.get(2),
            count_matching_witnesses: row.get(3),
        })?
        .collect::<::std::result::Result<Vec<_>, _>>()?;

    if rows
        .iter()
        .any(|row| row.version != config::VERSION || row.alt != config::ALT)
    {
        bail!("wrong network");
    }
    let count_required_matches = config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS;
    let tmp_units = rows
        .into_iter()
        .filter(|row| row.count_matching_witnesses >= count_required_matches as u32)
        .collect::<Vec<_>>();

    if tmp_units.is_empty() {
        let parent_units =
            pick_deep_parent_units(db, witnesses).context("failed to pick deep parent units")?;
        Ok(parent_units)
    } else {
        Ok(tmp_units.into_iter().map(|x| x.unit).collect::<Vec<_>>())
    }
}

fn pick_deep_parent_units(db: &Connection, witnesses: &[String]) -> Result<Vec<String>> {
    let witnesses_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT unit \
         FROM units \
         WHERE +sequence='good' \
         AND ( \
         SELECT COUNT(*) \
         FROM unit_witnesses \
         WHERE unit_witnesses.unit IN(units.unit, units.witness_list_unit) AND address IN({}) \
         )>={} \
         ORDER BY main_chain_index DESC LIMIT 1",
        witnesses_list,
        config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if rows.is_empty() {
        bail!("failed to find compatible parents: no deep units");
    }

    Ok(rows)
}

fn find_last_stable_mc_ball(db: &Connection, witnesses: &[String]) -> Result<String> {
    let witnesses_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT ball, unit, main_chain_index FROM units JOIN balls USING(unit) \
         WHERE is_on_main_chain=1 AND is_stable=1 AND +sequence='good' AND ( \
         SELECT COUNT(*) \
         FROM unit_witnesses \
         WHERE unit_witnesses.unit IN(units.unit, units.witness_list_unit) AND address IN({}) \
         )>={} \
         ORDER BY main_chain_index DESC LIMIT 1",
        witnesses_list,
        config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS,
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| row.get(1))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if rows.is_empty() {
        bail!("failed to find last stable ball");
    }
    Ok(rows.into_iter().nth(0).unwrap())
}

fn adjust_last_stable_mc_ball_and_parents(
    db: &Connection,
    last_stable_mc_ball_unit: &String,
    parent_units: &[String],
    witnesses: &[String],
) -> Result<LastStableBallAndParentUnits> {
    let is_stable = main_chain::determin_if_stable_in_laster_units(
        db,
        &last_stable_mc_ball_unit,
        parent_units,
    )?;
    if is_stable {
        struct TempBallMci {
            ball: String,
            main_chain_index: u32,
        }

        let sql = format!(
            "SELECT ball, main_chain_index FROM units JOIN balls USING(unit) WHERE unit='{}'",
            last_stable_mc_ball_unit
        );
        let mut stmt = db.prepare(&sql)?;
        let rows = stmt
            .query_map(&[], |row| TempBallMci {
                ball: row.get(0),
                main_chain_index: row.get(1),
            })?
            .collect::<::std::result::Result<Vec<_>, _>>()?;
        if rows.len() != 1 {
            bail!("not 1 ball by unit {}", last_stable_mc_ball_unit);
        }
        let row = rows.into_iter().nth(0).unwrap();
        return Ok(LastStableBallAndParentUnits {
            last_stable_mc_ball: row.ball,
            last_stable_mc_ball_unit: last_stable_mc_ball_unit.to_owned(),
            last_stable_mc_ball_mci: row.main_chain_index,
            parent_units: parent_units.to_owned(),
        });
    }
    let parent_units_list = parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    info!(
        "will adjust last stable ball because {} is not stable in view of parents {}",
        &last_stable_mc_ball_unit, parent_units_list
    );
    if parent_units.len() > 1 {
        let deep_parent_units = pick_deep_parent_units(db, witnesses)
            .context("pick_deep_parent_units in adjust failed: ")?;
        adjust_last_stable_mc_ball_and_parents(
            db,
            last_stable_mc_ball_unit,
            &deep_parent_units,
            witnesses,
        );
        //TODO: return what?
    }
    let obj_unit_props = storage::read_static_unit_property(db, &last_stable_mc_ball_unit)?;
    if obj_unit_props.best_parent_unit.is_none() {
        bail!("no best parent of {}", last_stable_mc_ball_unit);
    }
    adjust_last_stable_mc_ball_and_parents(
        db,
        &obj_unit_props.best_parent_unit.unwrap(),
        parent_units,
        witnesses,
    );
    //TODO: why call fn (adjust_last_stable_mc_ball_and_parent) again?
    unimplemented!()
}
