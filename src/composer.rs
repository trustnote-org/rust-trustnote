use config;
use error::Result;
use mc_outputs;
use object_hash;
use paid_witnessing;
use rusqlite::Connection;

#[derive(Debug, Clone)]
struct SpendProof {
    spend_proof: String,
    address: Option<String>,
}

#[derive(Debug, Clone)]
// #![feature(type_ascription)]
struct Input {
    amount: Option<u32>,
    address: Option<String>,
    unit: Option<String>,
    message_index: Option<u32>,
    output_index: Option<u32>,
    blinding: Option<String>,

    input_type: Option<String>,
    denomination: Option<u32>,
    serial_number: Option<u32>,

    from_main_chain_index: Option<u32>,
    to_main_chain_index: Option<u32>,
}

impl Default for Input {
    fn default() -> Input {
        Input {
            unit: None,
            message_index: None,
            output_index: None,
            amount: None,
            address: None,
            blinding: None,
            input_type: None,
            denomination: None,
            serial_number: None,
            from_main_chain_index: None,
            to_main_chain_index: None,
        }
    }
}

#[derive(Debug, Clone)]
struct InputWithProof {
    spend_proof: Option<SpendProof>,
    input: Option<Input>,
}

#[derive(Debug, Clone)]
struct InputsAndAmount {
    input_with_proofs: Vec<InputWithProof>,
    amount: u32,
}

impl InputsAndAmount {
    fn new() -> InputsAndAmount {
        InputsAndAmount {
            input_with_proofs: Vec::new(),
            amount: 0,
        }
    }
}

#[derive(Debug, Clone)]
//TODO: when Asset is null
struct Asset {
    asset: Option<String>,
    issued_by_definer_only: Option<u32>,
    definer_address: String,
    cap: bool,
    auto_destroy: Option<u32>,
    is_private: bool,
}

#[derive(Debug, Clone)]
struct InputInfo {
    multi_authored: bool,

    inputs_and_amount: InputsAndAmount,
    paying_addresses: Vec<String>,
    required_amount: u32,
}

fn issue_asset(
    db: &Connection,
    mut input_info: InputInfo,
    asset: Option<Asset>,

    is_base: bool,
) -> Result<InputsAndAmount> {
    //TODO: mount === Infinity && !objAsset.cap
    if asset.is_none() || asset.as_ref().unwrap().asset.is_none() {
        return finish(input_info.inputs_and_amount);
    }
    let asset = asset.as_ref().unwrap();
    if asset.issued_by_definer_only.is_some()
        && !input_info.paying_addresses.contains(&asset.definer_address)
    {
        return finish(input_info.inputs_and_amount);
    }

    let issuer_address = if asset.issued_by_definer_only.is_some() {
        asset.definer_address.clone()
    } else {
        input_info.paying_addresses[0].clone()
    };

    let add_issue_input = |serial_number: u32, closer_input_info: &mut InputInfo| -> Result<bool> {
        closer_input_info.inputs_and_amount.amount += 1;

        #[derive(Serialize)]
        struct TmpSpendProof {
            asset: Option<String>,
            amount: u32,
            address: String,
            c: u32,
            serial_number: u32,
        }

        let mut input = Input {
            amount: Some(1),

            input_type: Some(String::from("issue")),
            denomination: None,
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
                asset: asset.asset.clone(),
                amount: 1,
                c: 1,
                address: issuer_address.clone(),
                serial_number: serial_number,
            })?;
            let mut spend_proof = SpendProof {
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
            return finish(input_info.inputs_and_amount);
        }

        if add_issue_input(1, &mut input_info)? {
            return Ok(input_info.inputs_and_amount);
        }
        return finish(input_info.inputs_and_amount);
    } else {
        let mut stmt =
                db.prepare_cached("SELECT MAX(serial_number) AS max_serial_number FROM inputs WHERE type='issue' AND asset=? AND address=?")?;

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
        finish(input_info.inputs_and_amount)
    }
}

fn add_input(
    inputs_and_amount: &mut InputsAndAmount,
    input: Input,
    asset: &Option<Asset>,
    multi_authored: bool,
) -> Result<InputsAndAmount> {
    inputs_and_amount.amount += input.amount.map_or(0, |v| v);

    #[derive(Serialize)]
    struct TmpSpendProof {
        asset: Option<String>,
        amount: Option<u32>,
        address: Option<String>,
        unit: Option<String>,
        message_index: Option<u32>,
        output_index: Option<u32>,
        blinding: Option<String>,
    }

    let mut input_with_proof = InputWithProof {
        spend_proof: None,
        input: Some(input.clone()),
    };

    if asset.is_some() && asset.as_ref().unwrap().is_private {
        let tmp_input = input.clone();
        let spend_proof = object_hash::get_base64_hash(&TmpSpendProof {
            asset: asset.clone().unwrap().asset,
            amount: tmp_input.amount,
            address: tmp_input.address,
            unit: tmp_input.unit,
            message_index: tmp_input.message_index,
            output_index: tmp_input.output_index,
            blinding: tmp_input.blinding,
        })?;
        let mut spend_proof = SpendProof {
            spend_proof,
            address: None,
        };
        if multi_authored {
            spend_proof.address = input.address.clone();
        }

        input_with_proof.spend_proof = Some(spend_proof);
    }
    inputs_and_amount.input_with_proofs.push(input_with_proof);
    Ok(inputs_and_amount.clone())
}

fn finish(inputs_and_amount: InputsAndAmount) -> Result<InputsAndAmount> {
    if inputs_and_amount.input_with_proofs.is_empty() {
        bail!(
            "error_code: NOT_ENOUGH_FUNDS\nerror: not enough spendable funds from {:?} for {}",
            inputs_and_amount.input_with_proofs,
            inputs_and_amount.amount
        )
    }
    Ok(inputs_and_amount)
}

//FIXME:
fn add_mc_inputs(
    db: &Connection,
    input_info: &mut InputInfo,
    input_type: &str,
    input_size: u32,
    max_mci: u32,
) -> Result<()> {
    for addr in &input_info.paying_addresses {
        let target_amount =
            input_info.required_amount + input_size + if input_info.multi_authored {
                config::ADDRESS_SIZE
            } else {
                0
            } - input_info.inputs_and_amount.amount;
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

            input_info.inputs_and_amount.amount += mc_index_interval.accumulated;

            let mut input = Input {
                input_type: Some(input_type.to_string()),
                from_main_chain_index: Some(mc_index_interval.from_mci),
                to_main_chain_index: Some(mc_index_interval.to_mci),
                ..Default::default()
            };

            if input_info.multi_authored {
                input_info.required_amount += config::ADDRESS_SIZE;
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
    bail!("found")
}

fn add_headers_commission_inputs(
    db: &Connection,
    asset: Option<Asset>,
    mut input_info: InputInfo,
    is_base: bool,
    last_ball_mci: u32,
) -> Result<InputsAndAmount> {
    if let Some(max_mci) = paid_witnessing::get_max_spendable_mci_for_last_ball_mci(last_ball_mci) {
        if add_mc_inputs(
            db,
            &mut input_info,
            "headers_commission",
            config::HEADERS_COMMISSION_INPUT_SIZE,
            max_mci,
        ).is_ok()
        {
            if add_mc_inputs(
                db,
                &mut input_info,
                "witnessing",
                config::WITNESSING_INPUT_SIZE,
                max_mci,
            ).is_ok()
            {
                return issue_asset(db, input_info, asset, is_base);
            }
        }
    }
    Ok(input_info.inputs_and_amount)
}

fn pick_multiple_coins_and_continue(
    db: &Connection,
    asset: Option<Asset>,
    spendable_addresses: String,
    mut input_info: InputInfo,
    is_base: bool,
    last_ball_mci: u32,
) -> Result<InputsAndAmount> {
    let tmp_sql = if asset.is_none() {
        " IS NULL".to_string()
    } else {
        "=".to_string() + asset.as_ref().unwrap().asset.as_ref().unwrap()
    };
    let sql = format!(
        "SELECT unit, message_index, output_index, amount, address, blinding \
         FROM outputs \
         CROSS JOIN units USING(unit) \
         WHERE address IN({}) AND asset {} AND is_spent=0 \
         AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \
         ORDER BY amount DESC",
        spendable_addresses, tmp_sql,
    );
    let mut stmt = db.prepare_cached(&sql)?;

    let input_rows = stmt
        .query_map(&[&last_ball_mci], |row| Input {
            unit: row.get(0),
            message_index: row.get(1),
            output_index: row.get(2),
            amount: row.get(3),
            address: row.get(4),
            blinding: row.get(5),
            ..Default::default()
        })?.collect::<::std::result::Result<Vec<_>, _>>()?;
    for mut input in input_rows {
        input_info.required_amount += is_base as u32 * config::TRANSFER_INPUT_SIZE;
        add_input(
            &mut input_info.inputs_and_amount,
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
        return issue_asset(db, input_info, asset, is_base);
    } else {
        return add_headers_commission_inputs(db, asset, input_info, is_base, last_ball_mci);
    }
}

fn pick_one_coin_just_bigger_and_continue(
    db: &Connection,
    spendable_addresses: String,
    asset: Option<Asset>,
    mut input_info: InputInfo,
    is_base: bool,
    last_ball_mci: u32,
) -> Result<InputsAndAmount> {
    //TODO: infinity
    let tmp_sql = if asset.is_none() {
        " IS NULL".to_string()
    } else {
        "=".to_string() + asset.as_ref().unwrap().asset.as_ref().unwrap()
    };
    let more = if is_base { ">" } else { ">=" };

    let sql = format!(
        "SELECT unit, message_index, output_index, amount, blinding, address \
         FROM outputs \
         CROSS JOIN units USING(unit) \
         WHERE address IN({}) AND asset{} AND is_spent=0 AND amount {} ? \
         AND is_stable=1 AND sequence='good' AND main_chain_index<=?  \
         ORDER BY amount LIMIT 1",
        spendable_addresses, tmp_sql, more
    );
    let mut stmt = db.prepare_cached(&sql)?;

    let input_rows = stmt
        .query_map(
            &[
                &(input_info.required_amount + is_base as u32 * config::TRANSFER_INPUT_SIZE),
                &last_ball_mci,
            ],
            |row| Input {
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
        return add_input(
            &mut input_info.inputs_and_amount,
            input_rows[0].clone(),
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
        );
    }
}

#[allow(dead_code)]
fn pick_divisible_coins_for_amount(
    db: &Connection,
    asset: Option<Asset>,
    paying_addresses: Vec<String>,
    last_ball_mci: u32,
    amount: u32,
    multi_authored: bool,
) -> Result<InputsAndAmount> {
    let is_base = if asset.is_none() { false } else { true };

    let mut spendable = String::new();

    let input_info = InputInfo {
        multi_authored: multi_authored,
        inputs_and_amount: InputsAndAmount {
            input_with_proofs: Vec::new(),
            amount: 0,
        },
        paying_addresses: paying_addresses,
        required_amount: amount,
    };

    if let Some(asset) = &asset {
        if asset.auto_destroy.is_some() {
            let mut spendable_addresses = input_info
                .paying_addresses
                .iter()
                .filter(|v| v != &&asset.definer_address)
                .collect::<Vec<_>>();
            spendable = spendable_addresses
                .iter()
                .map(|v| format!("'{}'", v))
                .collect::<Vec<_>>()
                .join(",");
        };
    }

    if spendable.len() > 0 {
        return pick_one_coin_just_bigger_and_continue(
            db,
            spendable,
            asset,
            input_info,
            is_base,
            last_ball_mci,
        );
    }
    issue_asset(db, input_info, asset, is_base)
}
