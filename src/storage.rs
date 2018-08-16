use config;
use db;
use error::Result;
use failure::ResultExt;
use joint::Joint;
use may::sync::RwLock;
use rusqlite::{self, Connection};
use serde_json::{self, Value};
use spec::*;
use std::collections::HashMap;
use std::rc::Rc;
use utils::FifoCache;

// global data that store unit info
lazy_static! {
    static ref MIN_RETRIEVABLE_MCI: RwLock<u32> = RwLock::new({
        let db = db::DB_POOL.get_connection();
        let mut stmt = db
            .prepare_cached(
                "SELECT MAX(lb_units.main_chain_index) AS min_retrievable_mci \
                 FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \
                 WHERE units.is_on_main_chain=1 AND units.is_stable=1",
            ).expect("Initialzing MIN_RETRIEVABLE_MCI failed");

        stmt.query_row(&[], |row| row.get::<_, Option<u32>>(0))
            .unwrap_or(None)
            .unwrap_or(0)
    });
    static ref CACHED_UNIT: FifoCache<String, StaticUnitProperty> =
        FifoCache::with_capacity(config::MAX_ITEMS_IN_CACHE);
    static ref KNOWN_UNIT: FifoCache<String, ()> =
        FifoCache::with_capacity(config::MAX_ITEMS_IN_CACHE);
    static ref CACHED_UNIT_AUTHORS: FifoCache<String, Vec<String>> =
        FifoCache::with_capacity(config::MAX_ITEMS_IN_CACHE);
    static ref CACHED_UNIT_WITNESSES: FifoCache<String, Vec<String>> =
        FifoCache::with_capacity(config::MAX_ITEMS_IN_CACHE);
    static ref _CACHED_ASSET_INFOS: FifoCache<String, Option<String>> =
        FifoCache::with_capacity(config::MAX_ITEMS_IN_CACHE);
}

pub fn is_known_unit(unit: &String) -> bool {
    CACHED_UNIT.get(unit).is_some() || KNOWN_UNIT.get(unit).is_some()
}

pub fn set_unit_is_known(unit: &String) {
    KNOWN_UNIT.insert(unit.to_owned(), ());
}

pub fn forget_unit(unit: &String) {
    KNOWN_UNIT.remove(unit);
    CACHED_UNIT.remove(unit);
    CACHED_UNIT_AUTHORS.remove(unit);
    CACHED_UNIT_WITNESSES.remove(unit);
}

pub fn read_witnesses(db: &Connection, unit_hash: &String) -> Result<Vec<String>> {
    let mut stmt = db.prepare_cached("SELECT witness_list_unit FROM units WHERE unit=?")?;
    let witness_hash: Option<String> = stmt.query_row(&[unit_hash], |row| row.get(0))?;

    if witness_hash.is_some() {
        read_witness_list(db, &witness_hash.unwrap())
    } else {
        read_witness_list(db, unit_hash)
    }
}

pub fn read_witness_list(db: &Connection, unit_hash: &String) -> Result<Vec<String>> {
    if let Some(g) = CACHED_UNIT_WITNESSES.get(unit_hash) {
        return Ok(g.to_vec());
    }

    let mut stmt =
        db.prepare_cached("SELECT address FROM unit_witnesses WHERE unit=? ORDER BY address")?;
    let rows = stmt.query_map(&[unit_hash], |row| row.get(0))?;
    let mut names = Vec::new();
    for name_result in rows {
        names.push(name_result?);
    }

    if names.len() != config::COUNT_WITNESSES {
        return Err(format_err!(
            "wrong number of witnesses in unit {}",
            unit_hash
        ));
    }
    CACHED_UNIT_WITNESSES.insert(unit_hash.to_string(), names.clone());
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

    let hash_list = later_unit_hashes
        .iter()
        .chain([unit_hash].iter().map(|s| *s))
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");

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

pub fn read_static_unit_property(
    db: &Connection,
    unit_hash: &String,
) -> Result<StaticUnitProperty> {
    if let Some(g) = CACHED_UNIT.get(unit_hash) {
        return Ok(g);
    }
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

    CACHED_UNIT.insert(unit_hash.to_string(), ret.clone());
    Ok(ret)
}

pub fn read_unit_authors(db: &Connection, unit_hash: &String) -> Result<Vec<String>> {
    if let Some(g) = CACHED_UNIT_AUTHORS.get(unit_hash) {
        return Ok(g);
    }
    let mut stmt = db.prepare_cached("SELECT address FROM unit_authors WHERE unit=?")?;
    let mut names = stmt
        .query_map(&[unit_hash], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    ensure!(!names.is_empty(), "no authors");
    names.sort();
    CACHED_UNIT_AUTHORS.insert(unit_hash.to_string(), names.clone());
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
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

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
    let min_retrievable_mci = *MIN_RETRIEVABLE_MCI.read().unwrap();

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
        witness_list_unit: Option<String>,
        last_ball_unit: Option<String>,
        last_ball: Option<String>,
        //is_stable: u32, //Not used by now
        content_hash: Option<String>,
        headers_commission: Option<u32>,
        payload_commission: Option<u32>,
        main_chain_index: Option<u32>,
        timestamp: u64,
    }

    let mut unit = stmt.query_row(&[unit_hash], |row| UnitTemp {
        unit: row.get(0),
        version: row.get(1),
        alt: row.get(2),
        witness_list_unit: row.get(3),
        last_ball_unit: row.get(4),
        last_ball: row.get(5),
        //is_stable: row.get(6),
        content_hash: row.get(7),
        headers_commission: row.get(8),
        payload_commission: row.get(9),
        main_chain_index: row.get(10),
        timestamp: row.get::<_, String>(11).parse::<u64>().unwrap() * 1000,
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
             FROM earned_headers_commission_recipients \
             WHERE unit=? ORDER BY address",
        )?;
        earned_headers_commission_recipients = stmt
            .query_map(&[unit_hash], |row| HeaderCommissionShare {
                address: row.get(0),
                earned_headers_commission_share: row.get(1),
            })?.collect::<::std::result::Result<Vec<HeaderCommissionShare>, _>>()?;
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
                definition = read_definition(db, definition_chash)?;
            }
        }

        authors.push(Author {
            address,
            authentifiers,
            definition,
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
            pub message_index: u32,
            pub payload_hash: String,
            pub payload_location: String,
            pub payload: Option<String>,
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

            let mut payload = Payload::Other(Value::Null);
            if msg.payload_location == "inline" {
                match msg.app.as_str() {
                    "text" => {
                        if let Some(s) = msg.payload {
                            payload = Payload::Text(s);
                        }
                    }
                    "data_feed" => {
                        struct DataFeed {
                            feed_name: String,
                            value: Option<String>,
                            int_value: Option<i64>,
                        }

                        let mut stmt = db.prepare_cached(
                            "SELECT feed_name, `value`, int_value FROM data_feeds \
                             WHERE unit=? AND message_index=?",
                        )?;
                        let df_rows = stmt
                            .query_map(&[unit_hash, &message_index], |row| DataFeed {
                                feed_name: row.get(0),
                                value: row.get(1),
                                int_value: row.get(2),
                            })?.collect::<::std::result::Result<Vec<_>, _>>()?;

                        ensure!(!df_rows.is_empty(), "no data feed");
                        use serde_json::Map;

                        let mut map = Map::new();
                        for df in df_rows {
                            if let Some(s) = df.value {
                                map.insert(df.feed_name, Value::from(s));
                            } else if let Some(i) = df.int_value {
                                map.insert(df.feed_name, Value::from(i));
                            }
                        }

                        payload = Payload::Other(Value::from(map));
                    }
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
                            unit: Option<String>,
                            message_index: Option<u32>,
                            output_index: Option<u32>,
                            from_main_chain_index: Option<u32>,
                            to_main_chain_index: Option<u32>,
                            serial_number: Option<u32>,
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
                                serial_number: row.get(8),
                                amount: row.get(9),
                                address: row.get(10),
                                asset: row.get(11),
                            })?.collect::<::std::result::Result<Vec<InputTemp>, _>>()?;

                        if !rows.is_empty() {
                            //Record the first one for later ones to check against
                            prev_asset = rows[0].asset.clone();
                            prev_denomination = rows[0].denomination;

                            if rows[0].asset.is_some() {
                                payload_asset = rows[0].asset.clone();

                                if rows[0].fixed_denominations.is_some() {
                                    payload_denomination = rows[0].denomination;
                                }
                            }

                            for row in &mut rows {
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
                                    serial_number: input.serial_number,
                                    blinding: None,
                                });
                            }
                        }

                        //Read Outputs
                        let mut stmt = db.prepare_cached(
                            "SELECT address, amount, asset, denomination \
                             FROM outputs WHERE unit=? AND message_index=? ORDER BY output_index",
                        )?;

                        struct OutputTemp {
                            address: String,
                            amount: i64,
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

                        payload = Payload::Payment(Payment {
                            address: None,
                            asset: payload_asset,
                            definition_chash: None,
                            denomination: payload_denomination,
                            inputs,
                            outputs,
                        });
                    }
                    app => unimplemented!("app = {}", app),
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
                payload: Some(payload),
                payload_hash: msg.payload_hash,
                payload_location: msg.payload_location,
                payload_uri: msg.payload_uri,
                payload_uri_hash: msg.payload_uri_hash,
                spend_proofs,
            });
        }
    }

    let unit = Unit {
        alt: unit.alt,
        authors,
        content_hash: unit.content_hash,
        earned_headers_commission_recipients,
        headers_commission: unit.headers_commission,
        last_ball: unit.last_ball,
        last_ball_unit: unit.last_ball_unit,
        main_chain_index: unit.main_chain_index,
        messages,
        parent_units,
        payload_commission: unit.payload_commission,
        timestamp: Some(unit.timestamp),
        unit: unit.unit,
        version: unit.version,
        witnesses,
        witness_list_unit: unit.witness_list_unit,
    };

    //TODO: Retry if the hash verification fails
    ensure!(
        &unit.get_unit_hash() == unit_hash,
        "unit hash verification failed, unit: {:?} unit hash {}",
        unit,
        unit_hash,
    );

    let joint = Joint {
        unit,
        ball,
        skiplist_units,
        unsigned: None,
    };

    Ok(joint)
}

pub fn update_min_retrievable_mci_after_stabilizing_mci(
    db: &Connection,
    last_stable_mci: u32,
) -> Result<u32> {
    info!(
        "updateMinRetrievableMciAfterStabilizingMci {}",
        last_stable_mci
    );

    let last_ball_mci = find_last_ball_mci_of_mci(db, last_stable_mci)?;
    let min_retrievable_mci = *MIN_RETRIEVABLE_MCI.read().unwrap();
    if last_ball_mci <= min_retrievable_mci {
        return Ok(min_retrievable_mci);
    }
    let prev_min_retrievable_mci = min_retrievable_mci;
    let mut g = MIN_RETRIEVABLE_MCI.write().unwrap();
    *g = last_ball_mci;

    // strip content off units older than min_retrievable_mci
    // 'JOIN messages' filters units that are not stripped yet
    let mut stmt = db.prepare_cached(
        "SELECT DISTINCT unit, content_hash FROM units JOIN messages USING(unit) \
         WHERE main_chain_index<=? AND main_chain_index>=? AND sequence='final-bad'",
    )?;

    struct TempUnitProp {
        unit: String,
        content_hash: Option<String>,
    }

    let unit_rows = stmt
        .query_map(&[&min_retrievable_mci, &prev_min_retrievable_mci], |row| {
            TempUnitProp {
                unit: row.get(0),
                content_hash: row.get(1),
            }
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    let mut queries = db::DbQueries::new();

    for unit_row in &unit_rows {
        let unit = &unit_row.unit;
        ensure!(
            unit_row.content_hash.is_some(),
            "no content hash in bad unit {}",
            unit
        );
        let joint = read_joint(db, unit)
            .map_err(|e| format_err!("bad unit not found: {}, err={}", unit, e))?;

        generate_queries_to_archive_joint(db, &joint, ArchiveJointReason::Voided, &mut queries)?;
    }

    queries.execute_all(db);
    for unit_row in unit_rows {
        forget_unit(&unit_row.unit);
    }

    Ok(min_retrievable_mci)
}

pub enum ArchiveJointReason {
    Uncovered,
    Voided,
}

impl ToString for ArchiveJointReason {
    fn to_string(&self) -> String {
        match self {
            ArchiveJointReason::Uncovered => String::from("uncovered"),
            ArchiveJointReason::Voided => String::from("voided"),
        }
    }
}

pub fn generate_queries_to_archive_joint(
    db: &Connection,
    joint: &Joint,
    reason: ArchiveJointReason,
    queries: &mut db::DbQueries,
) -> Result<()> {
    let unit = Rc::new(joint.get_unit_hash().clone());
    match reason {
        ArchiveJointReason::Uncovered => {
            generate_queries_to_remove_joint(db, unit.clone(), queries)?
        }
        ArchiveJointReason::Voided => generate_queries_to_void_joint(db, unit.clone(), queries)?,
    }

    let reason_str = reason.to_string();
    let json = serde_json::to_string(joint)?;
    queries.add_query(move |db| {
        let mut stmt = db.prepare_cached(
            "INSERT OR IGNORE INTO archived_joints (unit, reason, json) VALUES (?,?,?)",
        )?;
        stmt.execute(&[&*unit, &reason_str, &json])?;
        Ok(())
    });

    Ok(())
}

fn generate_queries_to_remove_joint(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    generate_queries_to_unspend_outputs_spent_in_archived_unit(db, unit.clone(), queries)?;
    queries.add_query(move |db| {
        let mut stmt =
            db.prepare_cached("DELETE FROM witness_list_hashes WHERE witness_list_unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt =
            db.prepare_cached("DELETE FROM earned_headers_commission_recipients WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM unit_witnesses WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM authentifiers WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM unit_authors WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM parenthoods WHERE child_unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM address_definition_changes WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM inputs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM outputs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM spend_proofs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM data_feeds WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM poll_choices WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM data_feeds WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM poll_choices WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM polls WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM votes WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM attestations WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM asset_denominations WHERE asset=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM asset_attestors WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM assets WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM messages WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM units WHERE unit=?")?;
        stmt.execute(&[&*unit])?;

        Ok(())
    });
    Ok(())
}

fn generate_queries_to_void_joint(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    generate_queries_to_unspend_outputs_spent_in_archived_unit(db, unit.clone(), queries)?;
    queries.add_query(move |db| {
        let mut stmt =
            db.prepare_cached("DELETE FROM witness_list_hashes WHERE witness_list_unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt =
            db.prepare_cached("DELETE FROM earned_headers_commission_recipients WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM authentifiers WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt =
            db.prepare_cached("UPDATE unit_authors SET definition_chash=NULL WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM address_definition_changes WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM inputs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM outputs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM spend_proofs WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM data_feeds WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM poll_choices WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM polls WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM votes WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM attestations WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM asset_denominations WHERE asset=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM asset_attestors WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM assets WHERE unit=?")?;
        stmt.execute(&[&*unit])?;
        let mut stmt = db.prepare_cached("DELETE FROM messages WHERE unit=?")?;
        stmt.execute(&[&*unit])?;

        Ok(())
    });
    Ok(())
}

fn generate_queries_to_unspend_outputs_spent_in_archived_unit(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    generate_queries_to_unspend_transfer_outputs_spent_in_archived_unit(db, unit.clone(), queries)?;
    generate_queries_to_unspend_headers_commission_outputs_spent_in_archived_unit(
        db,
        unit.clone(),
        queries,
    )?;
    generate_queries_to_unspend_witnessing_outputs_spent_in_archived_unit(db, unit, queries)?;
    Ok(())
}

fn generate_queries_to_unspend_transfer_outputs_spent_in_archived_unit(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT src_unit, src_message_index, src_output_index \
         FROM inputs \
         WHERE inputs.unit=? \
         AND inputs.type='transfer' \
         AND NOT EXISTS ( \
         SELECT 1 FROM inputs AS alt_inputs \
         WHERE inputs.src_unit=alt_inputs.src_unit \
         AND inputs.src_message_index=alt_inputs.src_message_index \
         AND inputs.src_output_index=alt_inputs.src_output_index \
         AND alt_inputs.type='transfer' \
         AND inputs.unit!=alt_inputs.unit \
         )",
    )?;
    struct TempUnitProp {
        src_unit: String,
        src_message_index: u32,
        src_output_index: u32,
    }
    let unit_rows = stmt
        .query_map(&[&*unit], |row| TempUnitProp {
            src_unit: row.get(0),
            src_message_index: row.get(1),
            src_output_index: row.get(2),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    queries.add_query(move |db| {
        for unit_row in unit_rows {
            let mut stmt = db.prepare_cached(
                "UPDATE outputs SET is_spent=0 WHERE unit=? AND message_index=? AND output_index=?",
            )?;
            stmt.execute(&[
                &unit_row.src_unit,
                &unit_row.src_message_index,
                &unit_row.src_output_index,
            ])?;
        }
        Ok(())
    });

    Ok(())
}

fn generate_queries_to_unspend_headers_commission_outputs_spent_in_archived_unit(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT headers_commission_outputs.address, headers_commission_outputs.main_chain_index \
         FROM inputs \
         CROSS JOIN headers_commission_outputs \
         ON inputs.from_main_chain_index <= +headers_commission_outputs.main_chain_index \
         AND inputs.to_main_chain_index >= +headers_commission_outputs.main_chain_index \
         AND inputs.address = headers_commission_outputs.address \
         WHERE inputs.unit=? \
         AND inputs.type='headers_commission' \
         AND NOT EXISTS ( \
         SELECT 1 FROM inputs AS alt_inputs \
         WHERE headers_commission_outputs.main_chain_index >= alt_inputs.from_main_chain_index \
         AND headers_commission_outputs.main_chain_index <= alt_inputs.to_main_chain_index \
         AND inputs.address=alt_inputs.address \
         AND alt_inputs.type='headers_commission' \
         AND inputs.unit!=alt_inputs.unit \
         )",
    )?;
    struct TempUnitProp {
        address: String,
        main_chain_index: u32,
    }
    let unit_rows = stmt
        .query_map(&[&*unit], |row| TempUnitProp {
            address: row.get(0),
            main_chain_index: row.get(1),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    queries.add_query(move |db| {
        for unit_row in unit_rows {
            let mut stmt = db.prepare_cached(
                "UPDATE headers_commission_outputs SET is_spent=0 WHERE address=? AND main_chain_index=?",
            )?;
            stmt.execute(&[&unit_row.address, &unit_row.main_chain_index])?;
        }
        Ok(())
    });

    Ok(())
}

fn generate_queries_to_unspend_witnessing_outputs_spent_in_archived_unit(
    db: &Connection,
    unit: Rc<String>,
    queries: &mut db::DbQueries,
) -> Result<()> {
    let mut stmt = db.prepare_cached(
        "SELECT witnessing_outputs.address, witnessing_outputs.main_chain_index \
         FROM inputs \
         CROSS JOIN witnessing_outputs \
         ON inputs.from_main_chain_index <= +witnessing_outputs.main_chain_index \
         AND inputs.to_main_chain_index >= +witnessing_outputs.main_chain_index \
         AND inputs.address = witnessing_outputs.address \
         WHERE inputs.unit=? \
         AND inputs.type='witnessing' \
         AND NOT EXISTS ( \
         SELECT 1 FROM inputs AS alt_inputs \
         WHERE witnessing_outputs.main_chain_index >= alt_inputs.from_main_chain_index \
         AND witnessing_outputs.main_chain_index <= alt_inputs.to_main_chain_index \
         AND inputs.address=alt_inputs.address \
         AND alt_inputs.type='witnessing' \
         AND inputs.unit!=alt_inputs.unit \
         )",
    )?;
    struct TempUnitProp {
        address: String,
        main_chain_index: u32,
    }
    let unit_rows = stmt
        .query_map(&[&*unit], |row| TempUnitProp {
            address: row.get(0),
            main_chain_index: row.get(1),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    queries.add_query(move |db| {
        for unit_row in unit_rows {
            let mut stmt = db.prepare_cached(
                "UPDATE witnessing_outputs SET is_spent=0 WHERE address=? AND main_chain_index=?",
            )?;
            stmt.execute(&[&unit_row.address, &unit_row.main_chain_index])?;
        }
        Ok(())
    });

    Ok(())
}

pub fn find_last_ball_mci_of_mci(db: &Connection, mci: u32) -> Result<u32> {
    ensure!(mci != 0, "find_last_ball_mci_of_mci called with mci=0");
    let mut stmt = db.prepare_cached(
        "SELECT lb_units.main_chain_index, lb_units.is_on_main_chain \
         FROM units JOIN units AS lb_units ON units.last_ball_unit=lb_units.unit \
         WHERE units.is_on_main_chain=1 AND units.main_chain_index=?",
    )?;

    struct LbUnitProp {
        main_chain_index: u32,
        is_on_main_chain: u32,
    }

    let rows = stmt
        .query_map(&[&mci], |row| LbUnitProp {
            main_chain_index: row.get(0),
            is_on_main_chain: row.get(1),
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    ensure!(
        rows.len() == 1,
        "last ball's mci count {} != 1, mci = {}",
        rows.len(),
        mci
    );

    ensure!(rows[0].is_on_main_chain == 1, "lb is not on mc?");

    Ok(rows[0].main_chain_index)
}

pub fn read_free_joints(db: &Connection) -> Result<Vec<Joint>> {
    let mut stmt = db.prepare_cached(
        "SELECT units.unit FROM units LEFT JOIN archived_joints USING(unit) WHERE is_free=1 AND archived_joints.unit IS NULL",
    )?;

    let units = stmt
        .query_map(&[], |row| row.get::<_, String>(0))?
        .collect::<::std::result::Result<Vec<_>, _>>()?;
    let mut joints = Vec::new();
    for unit in units {
        let joint = read_joint(&db, &unit).or_else(|e| bail!("free ball lost, error = {}", e))?;
        joints.push(joint);
    }

    Ok(joints)
}
pub fn read_definition(db: &Connection, definition_chash: &String) -> Result<Value> {
    let mut stmt =
        db.prepare_cached("SELECT definition FROM definitions WHERE definition_chash=?")?;
    let definition: String = stmt.query_row(&[definition_chash], |row| row.get(0))?;
    Ok(serde_json::from_str(&definition)
        .context(format!("failed to read definition: {}", definition_chash))?)
}

pub fn read_definition_by_address(
    db: &Connection,
    address: &String,
    max_mci: Option<u32>,
) -> Result<::std::result::Result<Value, String>> {
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
    let ret = read_definition_at_mci(db, definition_chash, max_mci)?;
    Ok(ret.ok_or_else(|| definition_chash.clone()))
}

fn read_definition_at_mci(
    db: &Connection,
    definition_chash: &String,
    max_mci: u32,
) -> Result<Option<Value>> {
    let mut stmt = db.prepare_cached(
        "SELECT definition FROM definitions \
         CROSS JOIN unit_authors USING(definition_chash) CROSS JOIN units USING(unit) \
         WHERE definition_chash=? AND is_stable=1 AND sequence='good' AND main_chain_index<=?",
    )?;
    let definition = stmt
        .query_map(&[definition_chash, &max_mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()
        .context(format!(
            "failed to read definition at mci: definition_chash={}, max_mci={}",
            definition_chash, max_mci
        ))?;

    // not found
    if definition.is_empty() {
        return Ok(None);
    }

    Ok(Some(serde_json::from_str(&definition[0])?))
}

pub fn determine_best_parents(
    db: &Connection,
    unit: &Unit,
    witnesses: &[String],
) -> Result<Option<String>> {
    let parent_units = unit
        .parent_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let witness_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(", ");
    let witness_list_unit = unit.witness_list_unit.clone().unwrap();
    let sql = format!(
        "SELECT unit \
    FROM units AS parent_units \
    WHERE unit IN({}) \
      AND (witness_list_unit='{}' OR ( \
        SELECT COUNT(*) \
        FROM unit_witnesses AS parent_witnesses \
        WHERE parent_witnesses.unit IN(parent_units.unit, parent_units.witness_list_unit) AND address IN({}) \
      )>={}) \
    ORDER BY witnessed_level DESC, \
      level-witnessed_level ASC, \
      unit ASC \
    LIMIT 1",
        parent_units, witness_list_unit, witness_list, config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS
    );

    let mut stmt = db.prepare(&sql)?;
    let rows = stmt
        .query_map(&[], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    if rows.is_empty() {
        return Ok(None);
    }
    Ok(rows.into_iter().nth(0))
}

pub fn determine_if_has_witness_list_mutations_along_mc(
    db: &Connection,
    unit: &Unit,
    last_ball_unit: &String,
    witnesses: &[String],
) -> Result<()> {
    //Genesis
    if unit.parent_units.is_empty() {
        return Ok(());
    }

    let witness_list = witnesses
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let mc_units = build_list_of_mc_units_with_potentially_different_witness_lists(
        db,
        unit,
        last_ball_unit,
        witnesses,
    ).context("failed to build list of mc units with potentially different witness lists")?;

    info!("###### MC units {:?}", mc_units);

    if mc_units.is_empty() {
        return Ok(());
    }

    let mc_unit_list = mc_units
        .iter()
        .map(|s| format!("'{}'", s))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "SELECT units.unit, COUNT(*) AS count_matching_witnesses \
        FROM units CROSS JOIN unit_witnesses \
        ON (units.unit=unit_witnesses.unit OR units.witness_list_unit=unit_witnesses.unit) AND address IN({}) \
        WHERE units.unit IN({}) \
        GROUP BY units.unit \
        HAVING count_matching_witnesses<{}",
        witness_list, mc_unit_list, config::COUNT_WITNESSES - config::MAX_WITNESS_LIST_MUTATIONS);
    let mut stmt = db.prepare(&sql)?;

    let row = stmt.query_row(&[], |row| (row.get::<_, String>(0), row.get::<_, u32>(1)));

    if let Ok((unit, count_matching_witnesses)) = row {
        bail!(
            "too many ({}) witness list mutations relative to MC unit {}",
            config::COUNT_WITNESSES as u32 - count_matching_witnesses,
            unit
        );
    }

    Ok(())
}

// the MC return from this function is the MC built from this unit, not our current MC
fn build_list_of_mc_units_with_potentially_different_witness_lists(
    db: &Connection,
    unit: &Unit,
    last_ball_unit: &String,
    witnesses: &[String],
) -> Result<(Vec<String>)> {
    let mut mc_units = Vec::new();

    let best_parent_unit =
        determine_best_parents(db, unit, witnesses).context("failed to determine best parent")?;
    ensure!(best_parent_unit.is_some(), "no compatible best parent");

    //Add and go up
    let mut parent_hash = best_parent_unit.unwrap();
    loop {
        let parent_props = read_static_unit_property(db, &parent_hash)
            .context("failed to read static unit property")?;

        // the parent has the same witness list and the parent has already passed the MC compatibility test
        if unit.witness_list_unit.is_some()
            && unit.witness_list_unit == parent_props.witness_list_unit
        {
            break;
        } else {
            mc_units.push(parent_hash.clone());
        }

        if &parent_hash == last_ball_unit {
            break;
        }

        ensure!(
            parent_props.best_parent_unit.is_some(),
            "no best parent of unit {}?",
            parent_hash
        );

        parent_hash = parent_props.best_parent_unit.unwrap();
    }

    Ok(mc_units)
}

#[inline]
pub fn get_min_retrievable_mci() -> u32 {
    *MIN_RETRIEVABLE_MCI.read().unwrap()
}

pub fn slice_and_execute_query<S, F, T>(
    db: &Connection,
    sql_str: &str,
    param: &[&rusqlite::types::ToSql],
    array_para: &[S],
    mut f: F,
) -> Result<Vec<T>>
where
    S: ::std::fmt::Display,
    F: FnMut(&rusqlite::Row) -> T,
{
    let mut ret = Vec::new();
    for chunk in array_para.chunks(100) {
        let array_str = chunk
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join(",");

        let sql = sql_str.replace("{}", &array_str);
        let mut stmt = db.prepare(&sql)?;
        let rows = stmt.query_map(param, |row| f(row))?;
        for row in rows {
            ret.push(row?);
        }
    }

    Ok(ret)
}

pub fn find_witness_list_unit(
    db: &Connection,
    witnesses: &Vec<String>,
    last_ball_mci: u32,
) -> Result<Option<String>> {
    let mut stmt = db.prepare_cached(
        "SELECT witness_list_hashes.witness_list_unit \
         FROM witness_list_hashes CROSS JOIN units ON witness_list_hashes.witness_list_unit=unit \
         WHERE witness_list_hash=? AND sequence='good' AND is_stable=1 AND main_chain_index<=?",
    )?;
    let witness_list_hash = ::object_hash::get_base64_hash(witnesses)?;
    let rows = stmt
        .query_map(&[&witness_list_hash, &last_ball_mci], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;

    Ok(rows.into_iter().nth(0))
}
