use std::collections::HashMap;

use db;
use error::Result;
use my_witness;
use parent_composer::LastStableBallAndParentUnits;
use rusqlite::Connection;
use serde_json::Value;
use spec::*;
use storage;

struct Param {
    signing_addresses: Vec<String>,
    paying_addresses: Vec<String>,
    outputs: Vec<Output>,
    messages: Vec<Message>,
    signer: String,
    light_props: Option<LastStableBallAndParentUnits>,
    witnesses: Vec<String>,
    inputs: Vec<Input>,
    input_amount: Option<u32>,
    send_all: bool,
}

// TODO: params name
#[allow(dead_code)]
fn compose_joint(mut params: Param) -> Result<()> {
    let witnesses = params.witnesses.clone();
    if witnesses.is_empty() {
        params.witnesses = my_witness::read_my_witnesses()?;
        return compose_joint(params);
    }

    if params.light_props.is_none() {
        match request_from_light_vendor(
            "light/get_parents_and_last_ball_and_witness_list_unit",
            witnesses,
        ) {
            Ok(res) => {
                if res.parent_units.is_empty()
                    || res.last_stable_mc_ball.is_none()
                    || res.last_stable_mc_ball_unit.is_none()
                {
                    bail!("invalid parents from light vendor");
                }
                params.light_props = Some(res);
                return compose_joint(params);
            }
            Err(e) => bail!("request from light vendor, err = {}", e),
        }
    }
    //TODO: trySubset()

    let mut signing_addresses = params.signing_addresses;
    let mut paying_addresses = params.paying_addresses;
    let outputs = params.outputs;
    let mut messages = params.messages.clone();

    let light_props = params.light_props;
    let signer = params.signer;

    if light_props.is_none() {
        bail!("no parent props for light");
    }

    let change_outputs = outputs
        .iter()
        .filter(|output| output.amount == Some(0))
        .cloned()
        .collect::<Vec<_>>();
    let external_outputs = outputs
        .iter()
        .filter(|output| output.amount > Some(0))
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

    let payment_message = Message {
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

    let mut total_amount = 0;

    for output in external_outputs.iter() {
        match payment_message.clone().payload.unwrap() {
            Payload::Payment(mut x) => x.outputs.push(output.clone().clone()),
            _ => {}
        }
        total_amount += output.amount.unwrap();
    }

    messages.push(payment_message.clone());

    let is_multi_authored = from_addresses.len() > 1;
    let mut unit = Unit::default();
    unit.messages = messages.clone(); //part of unit
    unit.earned_headers_commission_recipients = Vec::new(); //part of unit
    if is_multi_authored {
        unit.earned_headers_commission_recipients
            .push(HeaderCommissionShare {
                address: change_outputs.into_iter().nth(0).unwrap().address.unwrap(),
                earned_headers_commission_share: 100,
            });
    }

    // TODO: lock

    unit.parent_units = light_props.clone().unwrap().parent_units; //part of unit
    unit.last_ball = light_props.clone().unwrap().last_stable_mc_ball;
    unit.last_ball_unit = light_props.clone().unwrap().last_stable_mc_ball_unit;
    let last_ball_mci = light_props.unwrap().last_stable_mc_ball_mci;

    check_for_unstable_predecessors()?;

    //authors
    let db = db::DB_POOL.get_connection();
    for from_address in from_addresses {
        let _author = Author {
            address: from_address.clone(),
            authentifiers: HashMap::new(),
            definition: Value::Null,
        };
        read_signing_paths(&db, from_address, &signer)?;
        //TODO: something undone
    }

    // witnesses

    if storage::determine_if_witness_and_address_definition_have_refs(&db, &witnesses)? {
        bail!("some witnesses have references in their addresses");
    }
    unit.witness_list_unit = Some(String::new()); //part of unit
    unit.witnesses = Vec::new(); //part of unit
    match storage::find_witness_list_unit(&db, &witnesses, last_ball_mci)? {
        Some(witness_list_unit) => unit.witness_list_unit = Some(witness_list_unit),
        None => unit.witnesses = witnesses,
    }

    // messages retrieved via callback

    // input coins
    let mut total_input;
    unit.headers_commission = Some(unit.get_header_size());
    let naked_payload_commission = unit.get_payload_size();

    if params.inputs.is_empty() {
        if params.input_amount.is_none() {
            bail!("inputs but no input_amount");
        }
        total_input = params.input_amount.unwrap();
        match payment_message.payload.unwrap() {
            Payload::Payment(mut x) => x.inputs = params.inputs,
            _ => {}
        }
        unit.payload_commission = Some(unit.get_payload_size());
    } else {
        let target_amount = if params.send_all {
            ::std::i64::MAX
        } else {
            total_amount + unit.headers_commission.unwrap() as i64 + naked_payload_commission as i64
        };
        let input_and_amount = pick_divisible_coins_for_amount(
            &db,
            None,
            &mut paying_addresses,
            last_ball_mci,
            target_amount,
            is_multi_authored,
        )?;
        if input_and_amount.input_with_proofs.is_empty() {
            bail!(
                "NOT_ENOUGH_FUNDS, not enough spendable funds from {:?} for {}",
                paying_addresses,
                target_amount
            );
        }
        total_input = input_and_amount.amount;
        match payment_message.payload.unwrap() {
            Payload::Payment(mut x) => {
                x.inputs = input_and_amount
                    .input_with_proofs
                    .iter()
                    .map(|s| s.input.clone().unwrap())
                    .collect::<Vec<_>>()
            }
            _ => {}
        }
        unit.payload_commission = Some(unit.get_payload_size());
        info!(
            "inputs increased payload by {}",
            unit.payload_commission.unwrap() - naked_payload_commission
        );
    }

    // TODO: handle err and return true value

    Ok(())
}

#[allow(dead_code)]
// move this to network
pub fn request_from_light_vendor(
    _request: &str,
    _witnesses: Vec<String>,
) -> Result<LastStableBallAndParentUnits> {
    unimplemented!()
}

#[allow(dead_code)]
fn check_for_unstable_predecessors() -> Result<()> {
    unimplemented!()
}

//signer.
#[allow(dead_code)]
fn read_definition(_db: Connection, _from_address: String) -> Result<()> {
    unimplemented!()
}

#[allow(dead_code)]
fn read_signing_paths(_db: &Connection, _from_address: String, _signer: &String) -> Result<()> {
    unimplemented!()
}

fn pick_divisible_coins_for_amount(
    _db: &Connection,
    _asset: Option<String>,
    _paying_addresses: &mut Vec<String>,
    _last_ball_mci: u32,
    _amount: i64,
    _multi_authored: bool,
) -> Result<InputsAndAmount> {
    unimplemented!()
}
struct InputsAndAmount {
    input_with_proofs: Vec<InputWithProof>,
    amount: u32,
}
struct InputWithProof {
    spend_proof: Option<SpendProof>,
    input: Option<Input>,
}
