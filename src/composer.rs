use std::collections::HashMap;

use config;
use error::Result;
use joint::Joint;
use light::LastStableBallAndParentUnitsAndWitnessListUnit;
use mc_outputs;
use object_hash;
use paid_witnessing;
use rusqlite::Connection;
use serde_json::{self, Value};
use signature::Signer;
use spec;
use spec::*;

#[derive(Debug)]
struct InputWithProof {
    spend_proof: Option<spec::SpendProof>,
    input: Option<spec::Input>,
}

#[derive(Debug)]
struct InputsAndAmount {
    input_with_proofs: Vec<InputWithProof>,
    amount: u64,
}

#[derive(Debug)]
struct Asset {
    asset: Option<String>,
    issued_by_definer_only: Option<u32>,
    definer_address: String,
    cap: bool,
    auto_destroy: u32,
    is_private: bool,
}

#[derive(Debug)]
struct InputInfo {
    multi_authored: bool,
    inputs_and_amount: InputsAndAmount,
    paying_addresses: Vec<String>,
    required_amount: u64,
}

fn issue_asset(
    db: &Connection,
    mut input_info: InputInfo,
    asset: Option<Asset>,
    is_base: bool,
    send_all: bool,
) -> Result<InputsAndAmount> {
    if asset.is_none() || asset.as_ref().unwrap().asset.is_none() {
        return finish(send_all, input_info.inputs_and_amount);
    } else {
        if send_all && !asset.as_ref().unwrap().cap {
            bail!(
                "error_code: NOT_ENOUGH_FUNDS 
                 error: not enough spendable funds from {:?} for {}",
                input_info.inputs_and_amount.input_with_proofs,
                input_info.inputs_and_amount.amount
            )
        }
    }

    let asset = asset.as_ref().unwrap();

    if asset.issued_by_definer_only.is_some()
        && !input_info.paying_addresses.contains(&asset.definer_address)
    {
        return finish(send_all, input_info.inputs_and_amount);
    }

    let issuer_address = if asset.issued_by_definer_only.is_some() {
        asset.definer_address.clone()
    } else {
        input_info.paying_addresses[0].clone()
    };

    let add_issue_input = |serial_number: u32, closer_input_info: &mut InputInfo| -> Result<bool> {
        #[derive(Serialize)]
        struct TmpSpendProof<'a> {
            asset: &'a Option<String>,
            amount: u64,
            address: &'a String,
            c: u32,
            serial_number: u32,
        }

        closer_input_info.inputs_and_amount.amount += 1;

        let mut input = spec::Input {
            amount: Some(1),
            kind: Some(String::from("issue")),
            serial_number: Some(serial_number),
            ..Default::default()
        };

        if closer_input_info.multi_authored {
            input.address = Some(issuer_address.clone());
        }

        let mut input_with_proof = InputWithProof {
            spend_proof: None,
            input: None,
        };

        if asset.is_private {
            let spend_proof = object_hash::get_base64_hash(&TmpSpendProof {
                asset: &asset.asset,
                amount: 1,
                c: 1,
                address: &issuer_address,
                serial_number: serial_number,
            })?;
            let mut spend_proof = spec::SpendProof {
                spend_proof,
                address: None,
            };
            if closer_input_info.multi_authored {
                spend_proof.address = input.address.clone();
            }

            input_with_proof.spend_proof = Some(spend_proof);
        }
        input_with_proof.input = Some(input);

        closer_input_info
            .inputs_and_amount
            .input_with_proofs
            .push(input_with_proof);

        Ok(if is_base {
            closer_input_info.inputs_and_amount.amount > closer_input_info.required_amount
        } else {
            closer_input_info.inputs_and_amount.amount >= closer_input_info.required_amount
        })
    };

    if asset.cap {
        let mut stmt = db.prepare_cached("SELECT 1 FROM inputs WHERE type='issue' AND asset=?")?;

        let input_rows = stmt
            .query_map(&[asset.asset.as_ref().unwrap()], |row| row.get(0))?
            .collect::<::std::result::Result<Vec<Option<String>>, _>>()?;
        if !input_rows.is_empty() {
            return finish(send_all, input_info.inputs_and_amount);
        }

        if add_issue_input(1, &mut input_info)? {
            return Ok(input_info.inputs_and_amount);
        }
    } else {
        let mut stmt = db.prepare_cached(
            "SELECT MAX(serial_number) AS max_serial_number \
             FROM inputs WHERE type='issue' AND asset=? AND address=?",
        )?;

        let max_serial_numbers = stmt
            .query_map(&[asset.asset.as_ref().unwrap(), &issuer_address], |row| {
                row.get(0)
            })?.collect::<::std::result::Result<Vec<Option<u32>>, _>>()?;
        let max_serial_number = if max_serial_numbers.is_empty() {
            0
        } else {
            max_serial_numbers[0].unwrap()
        };
        if add_issue_input(max_serial_number + 1, &mut input_info)? {
            return Ok(input_info.inputs_and_amount);
        }
    }
    finish(send_all, input_info.inputs_and_amount)
}

fn add_input(
    mut inputs_and_amount: InputsAndAmount,
    mut input: spec::Input,
    asset: &Option<Asset>,
    multi_authored: bool,
) -> Result<InputsAndAmount> {
    #[derive(Serialize)]
    struct TmpSpendProof<'a> {
        asset: &'a Option<String>,
        amount: &'a Option<i64>,
        address: &'a Option<String>,
        unit: &'a Option<String>,
        message_index: &'a Option<u32>,
        output_index: &'a Option<u32>,
        blinding: &'a Option<String>,
    }

    if let Some(amount) = input.amount {
        assert!(amount >= 0, "negative input");
        inputs_and_amount.amount += amount as u64;
    }

    let mut input_with_proof = InputWithProof {
        spend_proof: None,
        input: None,
    };

    if asset.is_some() && asset.as_ref().unwrap().is_private {
        let spend_proof = object_hash::get_base64_hash(&TmpSpendProof {
            asset: &asset.as_ref().unwrap().asset,
            amount: &input.amount,
            address: &input.address,
            unit: &input.unit,
            message_index: &input.message_index,
            output_index: &input.output_index,
            blinding: &input.blinding,
        })?;
        let mut spend_proof = spec::SpendProof {
            spend_proof,
            address: None,
        };
        if multi_authored {
            spend_proof.address = input.address.clone();
        }

        input_with_proof.spend_proof = Some(spend_proof);
    }

    if !multi_authored || !input.kind.is_none() {
        input.address = None;
    }

    input.amount = None;
    input.blinding = None;
    input_with_proof.input = Some(input);

    inputs_and_amount.input_with_proofs.push(input_with_proof);
    Ok(inputs_and_amount)
}

fn finish(send_all: bool, inputs_and_amount: InputsAndAmount) -> Result<InputsAndAmount> {
    if !send_all || inputs_and_amount.input_with_proofs.is_empty() {
        debug!(
            "NOT_ENOUGH_FUNDS, inputs_and_amount:{:?}",
            inputs_and_amount
        );
        bail!("NOT_ENOUGH_FUNDS, maybe try later");
    }
    Ok(inputs_and_amount)
}

fn add_mc_inputs(
    db: &Connection,
    input_info: &mut InputInfo,
    input_type: &str,
    input_size: u64,
    max_mci: u32,
) -> Result<()> {
    for addr in &input_info.paying_addresses {
        let adjust = if input_info.multi_authored {
            config::ADDRESS_SIZE
        } else {
            0
        };
        let target_amount = input_info.required_amount + input_size + adjust as u64
            - input_info.inputs_and_amount.amount;
        let mc_result = mc_outputs::find_mc_index_interval_to_target_amount(
            db,
            input_type,
            addr,
            max_mci,
            target_amount,
        );

        if let Ok(Some(mc_index_interval)) = mc_result {
            if mc_index_interval.accumulated == 0 {
                bail!("earnings is 0")
            }

            input_info.inputs_and_amount.amount += mc_index_interval.accumulated as u64;

            let mut input = spec::Input {
                kind: Some(input_type.to_string()),
                from_main_chain_index: Some(mc_index_interval.from_mci),
                to_main_chain_index: Some(mc_index_interval.to_mci),
                ..Default::default()
            };

            if input_info.multi_authored {
                input_info.required_amount += config::ADDRESS_SIZE as u64;
                input.address = Some(addr.to_owned());
            }

            input_info.required_amount += input_size;

            input_info
                .inputs_and_amount
                .input_with_proofs
                .push(InputWithProof {
                    input: Some(input),
                    spend_proof: None,
                });

            if input_info.inputs_and_amount.amount > input_info.required_amount {
                return Ok(());
            }
        }
    }
    bail!("not found")
}

fn add_headers_commission_inputs(
    db: &Connection,
    asset: Option<Asset>,
    mut input_info: InputInfo,
    is_base: bool,
    last_ball_mci: u32,
    send_all: bool,
) -> Result<InputsAndAmount> {
    if let Some(max_mci) = paid_witnessing::get_max_spendable_mci_for_last_ball_mci(last_ball_mci) {
        if add_mc_inputs(
            db,
            &mut input_info,
            "headers_commission",
            config::HEADERS_COMMISSION_INPUT_SIZE as u64,
            max_mci,
        ).is_err()
        {
            if add_mc_inputs(
                db,
                &mut input_info,
                "witnessing",
                config::WITNESSING_INPUT_SIZE as u64,
                max_mci,
            ).is_err()
            {
                return issue_asset(db, input_info, asset, is_base, send_all);
            }
        }
    }
    Ok(input_info.inputs_and_amount)
}

fn pick_multiple_coins_and_continue(
    db: &Connection,
    asset: Option<Asset>,
    spendable_addresses: Vec<String>,
    mut input_info: InputInfo,
    is_base: bool,
    last_ball_mci: u32,
    send_all: bool,
) -> Result<InputsAndAmount> {
    let tmp_sql = if asset.is_none() || asset.as_ref().unwrap().asset.is_none() {
        " IS NULL".to_string()
    } else {
        "=".to_string() + asset.as_ref().unwrap().asset.as_ref().unwrap()
    };

    let addresses = spendable_addresses
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "SELECT unit, message_index, output_index, amount, address, blinding \
         FROM outputs \
         CROSS JOIN units USING(unit) \
         WHERE address IN({}) AND asset {} AND is_spent=0 \
         AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \
         ORDER BY amount DESC",
        addresses, tmp_sql,
    );
    let mut stmt = db.prepare_cached(&sql)?;

    let input_rows = stmt
        .query_map(&[&last_ball_mci], |row| spec::Input {
            unit: row.get(0),
            message_index: row.get(1),
            output_index: row.get(2),
            amount: row.get(3),
            address: row.get(4),
            blinding: row.get(5),
            ..Default::default()
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;

    for mut input in input_rows {
        input_info.required_amount += is_base as u64 * config::TRANSFER_INPUT_SIZE as u64;
        input_info.inputs_and_amount = add_input(
            input_info.inputs_and_amount,
            input,
            &asset,
            input_info.multi_authored,
        )?;
        let is_found = if is_base {
            input_info.inputs_and_amount.amount > input_info.required_amount
        } else {
            input_info.inputs_and_amount.amount >= input_info.required_amount
        };
        if is_found {
            return Ok(input_info.inputs_and_amount);
        }
    }

    if asset.is_some() {
        return issue_asset(db, input_info, asset, is_base, send_all);
    } else {
        return add_headers_commission_inputs(
            db,
            asset,
            input_info,
            is_base,
            last_ball_mci,
            send_all,
        );
    }
}

fn pick_one_coin_just_bigger_and_continue(
    db: &Connection,
    spendable_addresses: &Vec<String>,
    asset: &Option<Asset>,
    input_info: &InputInfo,
    is_base: bool,
    last_ball_mci: u32,
) -> Result<spec::Input> {
    let asset_tmp = if asset.is_none() {
        " IS NULL".to_string()
    } else {
        "=".to_string() + asset.as_ref().unwrap().asset.as_ref().unwrap()
    };

    let more = if is_base { ">" } else { ">=" };

    let addresses = spendable_addresses
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(",");

    let sql = format!(
        "SELECT unit, message_index, output_index, amount, blinding, address \
         FROM outputs \
         CROSS JOIN units USING(unit) \
         WHERE address IN({}) AND asset{} AND is_spent=0 AND amount {} ? \
         AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \
         ORDER BY amount LIMIT 1",
        addresses, asset_tmp, more
    );
    let mut stmt = db.prepare_cached(&sql)?;

    let input_rows = stmt
        .query_map(
            &[
                &(input_info.required_amount as i64
                    + is_base as i64 * config::TRANSFER_INPUT_SIZE as i64),
                &last_ball_mci,
            ],
            |row| spec::Input {
                unit: row.get(0),
                message_index: row.get(1),
                output_index: row.get(2),
                amount: row.get(3),
                address: row.get(4),
                blinding: row.get(5),
                ..Default::default()
            },
        )?.collect::<::std::result::Result<Vec<_>, _>>()?;
    if input_rows.len() == 1 {
        return Ok(input_rows.into_iter().nth(0).unwrap());
    } else {
        bail!("no needed input")
    }
}

fn pick_divisible_coins_for_amount(
    db: &Connection,
    asset: Option<Asset>,
    paying_addresses: Vec<String>,
    last_ball_mci: u32,
    amount: u64,
    multi_authored: bool,
    send_all: bool,
) -> Result<InputsAndAmount> {
    let is_base = if asset.is_none() { true } else { false };

    let mut spendable_addresses = paying_addresses.clone();

    debug!("spendable_addresses = {:?}", spendable_addresses);

    //now asset is None
    if let Some(tmp) = &asset {
        spendable_addresses = spendable_addresses
            .into_iter()
            .filter(|v| v != &tmp.definer_address)
            .collect::<Vec<_>>()
    }

    let input_info = InputInfo {
        multi_authored,
        inputs_and_amount: InputsAndAmount {
            input_with_proofs: Vec::new(),
            amount: 0,
        },
        paying_addresses,
        required_amount: amount,
    };

    if !spendable_addresses.is_empty() {
        if send_all {
            return pick_multiple_coins_and_continue(
                db,
                asset,
                spendable_addresses,
                input_info,
                is_base,
                last_ball_mci,
                send_all,
            );
        }

        let input = pick_one_coin_just_bigger_and_continue(
            db,
            &spendable_addresses,
            &asset,
            &input_info,
            is_base,
            last_ball_mci,
        );

        if let Ok(input) = input {
            return add_input(
                input_info.inputs_and_amount,
                input,
                &asset,
                input_info.multi_authored,
            );
        } else {
            return pick_multiple_coins_and_continue(
                db,
                asset,
                spendable_addresses,
                input_info,
                is_base,
                last_ball_mci,
                send_all,
            );
        }
    }

    issue_asset(db, input_info, asset, is_base, send_all)
}

pub struct ComposeInfo {
    pub signing_addresses: Vec<String>,
    pub paying_addresses: Vec<String>,
    pub outputs: Vec<Output>,
    pub messages: Vec<Message>,
    pub light_props: LastStableBallAndParentUnitsAndWitnessListUnit,
    pub earned_headers_commission_recipients: Vec<spec::HeaderCommissionShare>,
    pub witnesses: Vec<String>,
    pub inputs: Vec<Input>,
    pub input_amount: u64,
    pub send_all: bool,
}

pub fn compose_joint<T: Signer>(db: &Connection, params: ComposeInfo, signer: &T) -> Result<Joint> {
    let ComposeInfo {
        mut signing_addresses,
        mut paying_addresses,
        outputs,
        messages,
        light_props,
        mut earned_headers_commission_recipients,
        witnesses,
        inputs,
        input_amount,
        send_all,
    } = params;

    let change_outputs = outputs
        .iter()
        .filter(|output| output.amount == 0)
        .cloned()
        .collect::<Vec<_>>();
    let external_outputs = outputs
        .into_iter()
        .filter(|output| output.amount > 0)
        .collect::<Vec<_>>();

    if change_outputs.len() > 1 {
        bail!("more than one change output");
    }
    if change_outputs.is_empty() {
        bail!("no change outputs");
    }
    if paying_addresses.is_empty() {
        bail!("no payers?");
    }

    let from_addresses = {
        signing_addresses.append(&mut paying_addresses);
        signing_addresses.sort();
        signing_addresses
    };

    let mut payment_message = Message {
        app: "payment".to_string(),
        payload_location: "inline".to_string(),
        payload_hash: String::new(),
        payload: Some(Payload::Payment(Payment {
            address: None,
            asset: None,
            definition_chash: None,
            denomination: None,
            inputs: Vec::new(),
            outputs: change_outputs.clone(),
        })),
        payload_uri: None,
        payload_uri_hash: None,
        spend_proofs: Vec::new(),
    };

    for output in external_outputs.into_iter() {
        match payment_message.payload {
            Some(Payload::Payment(ref mut x)) => x.outputs.push(output),
            _ => {}
        }
    }

    let is_multi_authored = from_addresses.len() > 1;
    let mut unit = Unit {
        messages,
        ..Default::default()
    };

    if !earned_headers_commission_recipients.is_empty() {
        earned_headers_commission_recipients.sort_by(|a, b| a.address.cmp(&b.address));
        unit.earned_headers_commission_recipients = earned_headers_commission_recipients;
    } else if is_multi_authored {
        unit.earned_headers_commission_recipients
            .push(HeaderCommissionShare {
                address: change_outputs.into_iter().nth(0).unwrap().address,
                earned_headers_commission_share: 100,
            });
    }

    // TODO: lock

    let LastStableBallAndParentUnitsAndWitnessListUnit {
        last_stable_mc_ball,
        last_stable_mc_ball_mci,
        last_stable_mc_ball_unit,
        parent_units,
        witness_list_unit,
    } = light_props;

    unit.parent_units = parent_units;
    unit.last_ball = last_stable_mc_ball;
    unit.last_ball_unit = last_stable_mc_ball_unit;

    check_for_unstable_predecessors(db, last_stable_mc_ball_mci, &from_addresses)?;

    //authors
    for from_address in &from_addresses {
        let mut author = Author {
            address: from_address.clone(),
            authentifiers: HashMap::new(),
            definition: Value::Null,
        };

        let mut stmt = db.prepare_cached(
            "SELECT 1 FROM unit_authors CROSS JOIN units USING(unit) \
             WHERE address=? AND is_stable=1 AND sequence='good' AND main_chain_index<=? \
             LIMIT 1",
        )?;
        if !stmt.exists(&[from_address, &last_stable_mc_ball_mci])? {
            author.definition = read_definition(&db, &from_address)?;
        }

        unit.authors.push(author);
    }

    // witnesses
    if witness_list_unit.is_some() {
        unit.witness_list_unit = witness_list_unit;
    } else {
        unit.witnesses = witnesses;
    }

    // input coins
    let total_input;
    unit.headers_commission = Some(unit.get_header_size() + config::SIG_LENGTH as u32);
    let naked_payload_commission = unit.get_payload_size();
    if !inputs.is_empty() {
        total_input = input_amount;
        match payment_message.payload {
            Some(Payload::Payment(ref mut x)) => x.inputs = inputs,
            _ => {}
        }
    } else {
        let target_amount = if params.send_all {
            ::std::u64::MAX
        } else {
            input_amount + unit.headers_commission.unwrap() as u64 + naked_payload_commission as u64
        };
        let input_and_amount = pick_divisible_coins_for_amount(
            &db,
            None,
            from_addresses,
            last_stable_mc_ball_mci,
            target_amount as u64,
            is_multi_authored,
            send_all,
        )?;
        debug!("input_and_amount = {:?}", input_and_amount);
        if input_and_amount.input_with_proofs.is_empty() {
            bail!(
                "NOT_ENOUGH_FUNDS, not enough spendable funds from {:?} for {}",
                paying_addresses,
                target_amount
            );
        }
        total_input = input_and_amount.amount as u64;

        match payment_message.payload {
            Some(Payload::Payment(ref mut x)) => {
                for input in input_and_amount.input_with_proofs.into_iter() {
                    x.inputs.push(input.input.unwrap());
                }
            }
            _ => {}
        }
    }
    unit.messages.push(payment_message);
    unit.payload_commission = Some(unit.get_payload_size() + config::HASH_LENGTH as u32);
    info!(
        "inputs increased payload by {}",
        unit.payload_commission.unwrap() - naked_payload_commission
    );
    {
        let payment_message = unit.messages.last_mut().unwrap();

        let change = total_input as i64
            - input_amount as i64
            - unit.headers_commission.unwrap() as i64
            - unit.payload_commission.unwrap() as i64;
        if change <= 0 {
            if !send_all {
                bail!("change = {}", change);
            }
            bail!(
                "NOT_ENOUGH_FUNDS: not enough spendable funds from {:?} for fees",
                paying_addresses
            );
        }
        match payment_message.payload {
            Some(Payload::Payment(ref mut x)) => {
                x.outputs[0].amount = change;
                x.outputs.sort_by(|a, b| {
                    if a.address == b.address {
                        a.amount.cmp(&b.amount)
                    } else {
                        a.address.cmp(&b.address)
                    }
                });

                payment_message.payload_hash = object_hash::get_base64_hash(&x)?;
            }
            _ => {}
        }
    }

    let unit_hash = unit.get_unit_hash_to_sign();
    for mut author in &mut unit.authors {
        let signature = signer.sign(&unit_hash, &author.address)?;
        author.authentifiers.insert("r".to_string(), signature);
    }

    unit.timestamp = Some(::time::now() / 1000);
    unit.unit = Some(unit.get_unit_hash());

    debug!("-----unit---------{}", serde_json::to_string_pretty(&unit)?);
    Ok(Joint {
        ball: None,
        skiplist_units: Vec::new(),
        unsigned: None,
        unit,
    })
}

fn check_for_unstable_predecessors(
    db: &Connection,
    last_ball_mci: u32,
    from_addresses: &Vec<String>,
) -> Result<()> {
    let addresses = from_addresses
        .iter()
        .map(|v| format!("'{}'", v))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT 1 FROM units CROSS JOIN unit_authors USING(unit) \
		 WHERE  (main_chain_index>? OR main_chain_index IS NULL) AND address IN({}) AND definition_chash IS NOT NULL \
		 UNION \
		 SELECT 1 FROM units JOIN address_definition_changes USING(unit) \
		 WHERE (main_chain_index>? OR main_chain_index IS NULL) AND address IN({}) \
		 UNION \
		 SELECT 1 FROM units CROSS JOIN unit_authors USING(unit) \
		 WHERE (main_chain_index>? OR main_chain_index IS NULL) AND address IN({}) AND sequence!='good'",
        addresses, addresses, addresses);
    let mut stmt = db.prepare(&sql)?;
    if stmt.exists(&[&last_ball_mci, &last_ball_mci, &last_ball_mci])? {
        bail!("some definition changes or definitions or nonserials are not stable yet");
    }
    Ok(())
}

fn read_definition(db: &Connection, address: &String) -> Result<Value> {
    let mut stmt = db.prepare_cached(
        "SELECT definition FROM my_addresses WHERE address=? \
         UNION \
         SELECT definition FROM shared_addresses WHERE shared_address=?",
    )?;
    let rows = stmt
        .query_map(&[address, address], |row| row.get(0))?
        .collect::<::std::result::Result<Vec<String>, _>>()?;
    if rows.len() != 1 {
        bail!("definition not found");
    }
    Ok(serde_json::from_str(&rows.into_iter().nth(0).unwrap())?)
}

pub fn create_text_message(text: &String) -> Result<spec::Message> {
    Ok(spec::Message {
        app: String::from("text"),
        payload_location: String::from("inline"),
        payload_hash: object_hash::get_base64_hash(text)?,
        payload: Some(spec::Payload::Text(text.to_string())),
        ..Default::default()
    })
}
