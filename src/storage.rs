use std::collections::{HashMap, HashSet};

use db;
use error::Result;
use joint::Joint;
use may::sync::RwLock;
use rusqlite::Connection;
use serde_json::{self, Value};
use spec::*;

// global data that store unit info
lazy_static! {
    static ref CACHED_UNIT: RwLock<HashMap<String, StaticUnitProperty>> =
        RwLock::new(HashMap::new());
    static ref KNOWN_UNIT: RwLock<HashSet<String>> = RwLock::new(HashSet::new());
    static ref MIN_RETRIEVABLE_MCI: RwLock<u32> = RwLock::new({
        let db = db::DB_POOL.get_connection();
        let mut stmt =
            db.prepare_cached(
                "SELECT MAX(lb_units.main_chain_index) AS min_retrievable_mci \
                 FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \
                 WHERE units.is_on_main_chain=1 AND units.is_stable=1",
            ).expect("Initialzing MIN_RETRIEVABLE_MCI failed");

        stmt.query_row(&[], |row| row.get::<_, u32>(0)).unwrap_or(0)
    });
}

#[inline]
pub fn is_genesis_unit(unit: &String) -> bool {
    unit == ::config::GENESIS_UNIT
}

pub fn is_genesis_ball(ball: &String) -> bool {
    let _ = ball;
    unimplemented!()
}

pub fn is_known_unit(unit: &String) -> bool {
    {
        let g = CACHED_UNIT.read().unwrap();
        if g.contains_key(unit) {
            return true;
        }
    }
    let g = KNOWN_UNIT.read().unwrap();
    g.contains(unit)
}

pub fn set_unit_is_known(unit: &String) {
    let mut g = KNOWN_UNIT.write().unwrap();
    g.insert(unit.to_owned());
}

pub fn forget_unit(unit: &String) {
    {
        let mut g = KNOWN_UNIT.write().unwrap();
        g.remove(unit);
    }

    {
        let mut g = CACHED_UNIT.write().unwrap();
        g.remove(unit);
    }

    unimplemented!()
}

// TODO: need to cache in memory
pub fn read_witness_list(db: &Connection, unit_hash: &String) -> Result<Vec<String>> {
    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let rows = stmt.query_map(&[unit_hash], |row| row.get(0))?;
    let mut names = Vec::new();
    for name_result in rows {
        names.push(name_result?);
    }

    if names.len() != ::config::COUNT_WITNESSES {
        return Err(format_err!(
            "wrong number of witnesses in unit {}",
            unit_hash
        ));
    }
    Ok(names)
}

pub fn read_last_main_chain_index(db: &Connection) -> Result<u32> {
    let mut stmt = db.prepare_cached("SELECT MAX(main_chain_index) AS last_mc_index FROM units")?;
    let ret = stmt.query_row(&[], |row| row.get_checked(0))?;
    let mci = ret.unwrap_or(0);
    Ok(mci)
}

pub fn read_unit_props(db: &Connection, unit_hash: &String) -> Result<UnitProps> {
    let mut stmt = db.prepare_cached(
        "SELECT unit, level, latest_included_mc_index, main_chain_index, \
         is_on_main_chain, is_free, is_stable \
         FROM units WHERE unit=?",
    )?;
    let ret = stmt.query_row(&[unit_hash], |row| UnitProps {
        unit: row.get(0),
        level: row.get(1),
        latest_included_mc_index: row.get(2),
        main_chain_index: row.get(3),
        is_on_main_chain: row.get(4),
        is_free: row.get(5),
        is_stable: row.get(6),
    })?;

    Ok(ret)
}

pub fn read_props_of_units(
    db: &Connection,
    unit_hash: &String,
    later_unit_hashes: &[String],
) -> Result<(UnitProps, Vec<UnitProps>)> {
    let b_earlier_in_later_units = later_unit_hashes.contains(unit_hash);

    let mut hash_list = later_unit_hashes
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>();

    hash_list.push(unit_hash.clone());

    let hash_list = hash_list.join(", ");

    let sql = format!(
        "SELECT unit, level, latest_included_mc_index, main_chain_index, is_on_main_chain, is_free FROM units WHERE unit IN ({})",
        hash_list
    );
    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(&[], |row| UnitProps {
        unit: row.get(0),
        level: row.get(1),
        latest_included_mc_index: row.get(2),
        main_chain_index: row.get(3),
        is_on_main_chain: row.get(4),
        is_free: row.get(5),
        is_stable: 0,
    })?;

    let mut props = Vec::new();
    for row in rows {
        let row = row?;
        props.push(row);
    }

    if props.len() != later_unit_hashes.len() + if b_earlier_in_later_units { 0 } else { 1 } {
        bail!(
            "wrong number of rows for earlier {:?}, later {:?}",
            unit_hash,
            later_unit_hashes
        );
    }

    let mut prop = None;
    for p in &props {
        if &p.unit == unit_hash {
            prop = Some(p.clone());
            break;
        }
    }

    ensure!(prop.is_some(), "unit prop not found");

    Ok((prop.unwrap(), props))
}

// TODO: need to cache in memory
pub fn read_static_unit_property(
    db: &Connection,
    unit_hash: &String,
) -> Result<StaticUnitProperty> {
    let mut stmt = db.prepare_cached(
        "SELECT level, witnessed_level, best_parent_unit, witness_list_unit \
         FROM units WHERE unit=?",
    )?;
    let ret = stmt.query_row(&[unit_hash], |row| StaticUnitProperty {
        level: row.get(0),
        witnessed_level: row.get(1),
        best_parent_unit: row.get(2),
        witness_list_unit: row.get(3),
    })?;

    Ok(ret)
}

// TODO: need to cache in memory
pub fn read_unit_authors(db: &Connection, unit_hash: &String) -> Result<Vec<String>> {
    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let rows = stmt.query_map(&[unit_hash], |row| row.get(0))?;
    let mut names = Vec::new();
    for name_result in rows {
        names.push(name_result?);
    }

    if names.len() != ::config::COUNT_WITNESSES {
        return Err(format_err!(
            "wrong number of witnesses in unit {}",
            unit_hash
        ));
    }
    Ok(names)
}

// only need part of it.
pub struct LastStableMcUnitProps {
    pub unit: String,
    pub ball: String,
    pub main_chain_index: u32,
}

pub fn read_last_stable_mc_unit_props(db: &Connection) -> Result<Option<LastStableMcUnitProps>> {
    let mut stmt = db.prepare_cached(
        "SELECT units.*, ball FROM units LEFT JOIN balls USING(unit) \
         WHERE is_on_main_chain=1 AND is_stable=1 ORDER BY main_chain_index DESC LIMIT 1",
    )?;
    let mut props = stmt
        .query_map(&[], |row| LastStableMcUnitProps {
            unit: row.get::<_, String>("unit"),
            // FIXME: here ball may be empty
            ball: row.get::<_, String>("ball"),
            main_chain_index: row.get::<_, u32>("main_chain_index"),
        })?
        .collect::<::std::result::Result<Vec<_>, _>>()?;

    if props.is_empty() {
        return Ok(None);
    }
    Ok(Some(props.swap_remove(0)))
}

pub fn read_last_stable_mc_index(db: &Connection) -> Result<u32> {
    let ret = read_last_stable_mc_unit_props(db)?;
    match ret {
        Some(prop) => Ok(prop.main_chain_index),
        _ => Ok(0),
    }
}

pub fn determine_if_witness_and_address_definition_have_refs(
    db: &Connection,
    witnesses: &[String],
) -> Result<bool> {
    let witness_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "SELECT 1 FROM address_definition_changes JOIN definitions USING(definition_chash) \
         WHERE address IN({}) AND has_references=1 \
         UNION \
         SELECT 1 FROM definitions WHERE definition_chash IN({}) AND has_references=1 \
         LIMIT 1",
        witness_list, witness_list
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt.query_map(&[], |row| row.get::<_, u32>(0))?;
    Ok(rows.count() > 0)
}

pub fn read_joint_with_ball(db: &Connection, unit: &String) -> Result<Joint> {
    let mut joint = read_joint_directly(db, unit)?;
    if joint.ball.is_none() {
        let mut stmt = db.prepare_cached("SELECT ball FROM balls WHERE unit=?")?;
        if let Ok(ball) = stmt.query_row(&[unit], |row| row.get(0)) {
            joint.ball = Some(ball);
        }
    }

    Ok(joint)
}

#[inline]
pub fn read_joint(db: &Connection, unit: &String) -> Result<Joint> {
    read_joint_directly(db, unit)
}

pub fn read_joint_directly(db: &Connection, unit_hash: &String) -> Result<Joint> {
    let min_retrievable_mci = {
        let g = MIN_RETRIEVABLE_MCI.read().unwrap();
        *g
    };

    let mut stmt = db.prepare_cached(
        "SELECT units.unit, version, alt, witness_list_unit, last_ball_unit, \
         balls.ball AS last_ball, is_stable, content_hash, headers_commission, \
         payload_commission, main_chain_index, \
         strftime('%s', units.creation_date) AS timestamp \
         FROM units \
         LEFT JOIN balls ON last_ball_unit=balls.unit WHERE units.unit=?",
    )?;

    struct UnitTemp {
        unit: Option<String>,
        version: String,
        alt: String,
        //witness_list_unit: Option<String>, //Not used by now
        last_ball_unit: Option<String>,
        last_ball: Option<String>,
        //is_stable: u32, //Not used by now
        content_hash: Option<String>,
        headers_commission: Option<u32>,
        payload_commission: Option<u32>,
        main_chain_index: Option<u32>,
        timestamp: Option<u32>,
    }

    let mut unit = stmt.query_row(&[unit_hash], |row| UnitTemp {
        unit: row.get(0),
        version: row.get(1),
        alt: row.get(2),
        //witness_list_unit: row.get(3),
        last_ball_unit: row.get(4),
        last_ball: row.get(5),
        //is_stable: row.get(6),
        content_hash: row.get(7),
        headers_commission: row.get(8),
        payload_commission: row.get(9),
        main_chain_index: row.get(10),
        timestamp: row.get(11),
    })?;

    let main_chain_index = unit.main_chain_index;

    //let b_final_bad = unit.content_hash.is_some();
    //let b_stable = unit.is_stable;
    let b_voided = unit.content_hash.is_some() && main_chain_index < Some(min_retrievable_mci);
    let b_retrievable = main_chain_index >= Some(min_retrievable_mci) || main_chain_index.is_none();

    // unit hash verification below will fail if:
    // 1. the unit was received already voided, i.e. its messages are stripped and content_hash is set
    // 2. the unit is still retrievable (e.g. we are syncing)
    // In this case, bVoided=false hence content_hash will be deleted but the messages are missing
    if b_voided {
        unit.headers_commission = None;
        unit.payload_commission = None;
    } else {
        unit.content_hash = None;
    }

    //Parents
    let mut stmt = db.prepare_cached(
        "SELECT parent_unit FROM parenthoods \
         WHERE child_unit=? ORDER BY parent_unit",
    )?;
    let parent_units = stmt
        .query_map(&[unit_hash], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    //Ball
    let ball = if b_retrievable && !is_genesis_unit(unit_hash) {
        None
    } else {
        let mut stmt = db.prepare_cached("SELECT ball FROM balls WHERE unit=?")?;
        let row = stmt.query_row(&[unit_hash], |row| row.get::<_, String>(0))?;
        Some(row)
    };

    //Skiplist
    let mut skiplist_units = Vec::new();
    if !b_retrievable {
        let mut stmt = db.prepare_cached(
            "SELECT skiplist_unit FROM skiplist_units \
             WHERE unit=? ORDER BY skiplist_unit",
        )?;
        skiplist_units = stmt
            .query_map(&[unit_hash], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<String>, _>>()?;
    }

    //Witness
    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let witnesses = stmt
        .query_map(&[unit_hash], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    //Earned_headers_commission_recipients
    let mut earned_headers_commission_recipients = Vec::new();
    if !b_voided {
        let mut stmt = db.prepare_cached(
            "SELECT address, earned_headers_commission_share \
             FROM earned_headers_commission_recipients",
        )?;
        earned_headers_commission_recipients = stmt
            .query_map(&[unit_hash], |row| HeaderCommissionShare {
                address: row.get(0),
                earned_headers_commission_share: row.get(1),
            })?
            .collect::<::std::result::Result<Vec<HeaderCommissionShare>, _>>()?;
    }

    //Authors
    let mut authors = Vec::new();
    let mut stmt = db.prepare_cached(
        "SELECT address, definition_chash FROM unit_authors \
         WHERE unit=? ORDER BY address",
    )?;

    struct UnitAuthor {
        address: String,
        definition_chash: Option<String>,
    };

    let rows = stmt.query_map(&[unit_hash], |row| UnitAuthor {
        address: row.get(0),
        definition_chash: row.get(1),
    })?;

    for row in rows {
        let row = row?;
        let address = row.address;
        let mut authentifiers = HashMap::new();
        let mut definition = Value::Null;

        if !b_voided {
            let mut stmt = db.prepare_cached(
                "SELECT path, authentifier FROM authentifiers \
                 WHERE unit=? AND address=?",
            )?;

            struct Authentifier {
                path: String,
                authentifier: String,
            };

            let sig_rows = stmt.query_map(&[unit_hash, &address], |row| Authentifier {
                path: row.get(0),
                authentifier: row.get(1),
            })?;

            for sig_row in sig_rows {
                let sig_row = sig_row?;
                authentifiers.insert(sig_row.path, sig_row.authentifier);
            }

            if row.definition_chash.is_some() {
                let definition_chash = &row.definition_chash.unwrap();
                definition = serde_json::from_str(read_definition(db, definition_chash)?.as_str())?;
            }
        }

        authors.push(Author {
            address: address,
            authentifiers: authentifiers,
            definition: definition,
        });
    }

    //Messages
    let mut messages = Vec::new();
    if !b_voided {
        let mut stmt = db.prepare_cached(
            "SELECT app, payload_hash, payload_location, payload, payload_uri, \
             payload_uri_hash, message_index FROM messages \
             WHERE unit=? ORDER BY message_index",
        )?;

        struct MessageTemp {
            pub app: String,
            pub message_index: Option<u32>,
            pub payload: Option<String>,
            pub payload_hash: String,
            pub payload_location: String,
            pub payload_uri: Option<String>,
            pub payload_uri_hash: Option<String>,
        }

        let rows = stmt.query_map(&[unit_hash], |row| MessageTemp {
            app: row.get(0),
            payload_hash: row.get(1),
            payload_location: row.get(2),
            payload: row.get(3),
            payload_uri: row.get(4),
            payload_uri_hash: row.get(5),
            message_index: row.get(6),
        })?;

        for row in rows {
            let msg = row?;
            let message_index = msg.message_index;
            let mut payload_asset = None;
            let mut payload_denomination = None;
            let mut inputs = Vec::new();
            let mut outputs = Vec::new();

            let mut prev_asset = None;
            let mut prev_denomination = None;

            if msg.payload_location == "inline" {
                match msg.app.as_str() {
                    "address_definition_change" => unimplemented!(),
                    "poll" => unimplemented!(),
                    "vote" => unimplemented!(),
                    "asset" => unimplemented!(),
                    "asset_attestors" => unimplemented!(),
                    "data_feed" => unimplemented!(),
                    "profile" | "attestation" | "data" | "definition_template" => unimplemented!(),
                    "payment" => {
                        //Read Inputs
                        let mut stmt = db.prepare_cached(
                            "SELECT type, denomination, assets.fixed_denominations, \
                             src_unit AS unit, src_message_index AS message_index, \
                             src_output_index AS output_index, \
                             from_main_chain_index, to_main_chain_index, serial_number, \
                             amount, address, asset \
                             FROM inputs \
                             LEFT JOIN assets ON asset=assets.unit \
                             WHERE inputs.unit=? AND inputs.message_index=? \
                             ORDER BY input_index",
                        )?;

                        struct InputTemp {
                            kind: Option<String>,
                            denomination: Option<u32>,
                            fixed_denominations: Option<u32>,
                            unit: String,
                            message_index: u32,
                            output_index: u32,
                            from_main_chain_index: Option<u32>,
                            to_main_chain_index: Option<u32>,
                            //serial_number: Option<i64>,
                            amount: Option<i64>,
                            address: Option<String>,
                            asset: Option<String>,
                        }

                        let mut rows = stmt
                            .query_map(&[unit_hash, &message_index], |row| InputTemp {
                                kind: row.get(0),
                                denomination: row.get(1),
                                fixed_denominations: row.get(2),
                                unit: row.get(3),
                                message_index: row.get(4),
                                output_index: row.get(5),
                                from_main_chain_index: row.get(6),
                                to_main_chain_index: row.get(7),
                                //serial_number: row.get(8),
                                amount: row.get(9),
                                address: row.get(10),
                                asset: row.get(11),
                            })?
                            .collect::<::std::result::Result<Vec<InputTemp>, _>>()?;

                        if rows.len() > 0 {
                            //Record the first one for later ones to check against
                            prev_asset = rows[0].asset.clone();
                            prev_denomination = rows[0].denomination;

                            if rows[0].asset.is_some() {
                                payload_asset = rows[0].asset.clone();

                                if rows[0].fixed_denominations.is_some() {
                                    payload_denomination = rows[0].denomination;
                                }
                            }

                            for row in rows.iter_mut() {
                                let mut input = row;

                                ensure!(
                                    !input.address.is_none(),
                                    "readJoint: input address is NULL"
                                );

                                ensure!(prev_asset == input.asset, "different assets in inputs?");
                                ensure!(
                                    prev_denomination == input.denomination,
                                    "different denomination in inputs?"
                                );

                                if input.kind == Some("transfer".to_string()) || authors.len() == 1
                                {
                                    input.address = None;
                                }

                                if input.kind == Some("transfer".to_string()) {
                                    input.kind = None;
                                }

                                inputs.push(Input {
                                    kind: input.kind.clone(),
                                    unit: input.unit.clone(),
                                    message_index: input.message_index,
                                    output_index: input.output_index,
                                    from_main_chain_index: input.from_main_chain_index,
                                    to_main_chain_index: input.to_main_chain_index,
                                    amount: input.amount,
                                    address: input.address.clone(),
                                });
                            }
                        }

                        //Read Outputs
                        let mut stmt = db.prepare_cached(
                            "SELECT address, amount, asset, denomination \
                             FROM outputs WHERE unit=? AND message_index=? ORDER BY output_index",
                        )?;

                        struct OutputTemp {
                            address: Option<String>,
                            amount: Option<i64>,
                            asset: Option<String>,
                            denomination: Option<u32>,
                        }

                        let mut rows =
                            stmt.query_map(&[unit_hash, &message_index], |row| OutputTemp {
                                address: row.get(0),
                                amount: row.get(1),
                                asset: row.get(2),
                                denomination: row.get(3),
                            })?;

                        for row in rows {
                            let output = row?;

                            ensure!(prev_asset == output.asset, "different assets in outputs?");
                            ensure!(
                                prev_denomination == output.denomination,
                                "different denomination in outputs?"
                            );

                            outputs.push(Output {
                                amount: output.amount,
                                address: output.address.clone(),
                            });
                        }
                    }
                    _ => unimplemented!(),
                }
            }

            //Add spend proofs
            let mut stmt = db.prepare_cached(
                "SELECT spend_proof, address FROM spend_proofs \
                 WHERE unit=? AND message_index=? ORDER BY spend_proof_index",
            )?;

            let rows = stmt.query_map(&[unit_hash, &message_index], |row| SpendProof {
                spend_proof: row.get(0),
                address: row.get(1),
            })?;

            let mut spend_proofs = Vec::new();
            for row in rows {
                let mut row = row?;

                if authors.len() == 1 {
                    row.address = None;
                }

                spend_proofs.push(row);
            }

            messages.push(Message {
                app: msg.app,
                payload: Some(Payload::Payment(Payment {
                    address: None,
                    asset: payload_asset,
                    definition_chash: None,
                    denomination: payload_denomination,
                    inputs: inputs,
                    outputs: outputs,
                })),
                payload_hash: msg.payload_hash,
                payload_location: msg.payload_location,
                payload_uri: msg.payload_uri,
                payload_uri_hash: msg.payload_uri_hash,
                spend_proofs: spend_proofs,
            });
        }
    }

    let unit = Unit {
        alt: unit.alt,
        authors: authors,
        content_hash: unit.content_hash,
        earned_headers_commission_recipients: earned_headers_commission_recipients,
        headers_commission: unit.headers_commission,
        last_ball: unit.last_ball,
        last_ball_unit: unit.last_ball_unit,
        main_chain_index: unit.main_chain_index,
        messages: messages,
        parent_units: parent_units,
        payload_commission: unit.payload_commission,
        timestamp: unit.timestamp,
        unit: unit.unit,
        version: unit.version,
        witnesses: witnesses,
        witness_list_unit: None,
    };

    let joint = Joint {
        unit: unit,
        ball: ball,
        skiplist_units: skiplist_units,
        unsigned: None,
    };

    //TODO: Retry if the hash verification fails

    Ok(joint)
}

pub fn read_definition(db: &Connection, definition_chash: &String) -> Result<String> {
    let mut stmt =
        db.prepare_cached("SELECT definition FROM definitions WHERE definition_chash=?")?;
    let definition = stmt.query_row(&[definition_chash], |row| row.get(0))?;
    Ok(definition)
}

pub fn read_definition_by_address(
    db: &Connection,
    address: &String,
    max_mci: Option<u32>,
) -> Result<String> {
    let max_mci = max_mci.unwrap_or(::std::u32::MAX);
    let mut stmt = db.prepare_cached(
        "SELECT definition_chash FROM address_definition_changes CROSS JOIN units USING(unit) \
         WHERE address=? AND is_stable=1 AND sequence='good' AND main_chain_index<=? \
         ORDER BY level DESC LIMIT 1",
    )?;
    let rows = stmt
        .query_map(&[address, &max_mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    let definition_chash = if rows.is_empty() { address } else { &rows[0] };
    read_definition_at_mci(db, definition_chash, max_mci)
}

fn read_definition_at_mci(
    db: &Connection,
    definition_chash: &String,
    max_mci: u32,
) -> Result<String> {
    let mut stmt = db.prepare_cached(
        "SELECT definition FROM definitions \
         CROSS JOIN unit_authors USING(definition_chash) CROSS JOIN units USING(unit) \
         WHERE definition_chash=? AND is_stable=1 AND sequence='good' AND main_chain_index<=?",
    )?;
    let definition = stmt.query_row(&[definition_chash, &max_mci], |row| row.get(0))?;
    Ok(definition)
}
