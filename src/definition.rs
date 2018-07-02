use std::collections::HashMap;

use config;
use error::Result;
use rusqlite::Connection;
use serde_json::Value;
use spec::*;
use validation::ValidationState;

pub fn validate_definition(
    _db: &Connection,
    definition: &Value,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
    is_asset: bool,
) -> Result<()> {
    fn evaluate(
        definition: &Value,
        is_in_negation: bool,
        is_asset: bool,
        complexity: &mut usize,
    ) -> Result<bool> {
        *complexity += 1;
        if *complexity > config::MAX_COMPLEXITY {
            bail!("complexity exceeded");
        }

        if !definition.is_array() {
            bail!("definition must be array");
        }

        let arr = definition.as_array().unwrap();
        if arr.len() != 2 {
            bail!("expression must be 2-element array");
        }
        let op = arr[0]
            .as_str()
            .ok_or_else(|| format_err!("op is not a string"))?;
        let args = &arr[1];

        match op {
            "sig" => {
                if is_in_negation {
                    bail!("sig cannot be negated");
                }
                if is_asset {
                    bail!("asset condition cannot have sig");
                }
                if !args.is_object() {
                    bail!("sig args is not object");
                }
                let args = args.as_object().unwrap();
                for (k, v) in args {
                    // let key = k
                    //     .as_str()
                    //     .ok_or_else(|| format_err!("sig key is not string"))?;
                    let value = v
                        .as_str()
                        .ok_or_else(|| format_err!("sig value is not string"))?;

                    match k.as_str() {
                        // "algo" => unimplemented!(),
                        "pubkey" => {
                            if value.len() != config::HASH_LENGTH {
                                bail!("wrong pubkey length")
                            }
                        }
                        _ => bail!("unknown fields in sig"),
                    }
                    return Ok(true);
                }
            }
            op => unimplemented!("unsupported op: {}", op),
        }

        Ok(false)
    }

    let mut complexity = 0;
    let has_sig = evaluate(definition, false, is_asset, &mut complexity)?;

    if !has_sig && !is_asset {
        bail!("each branch must have a signature");
    }

    Ok(())
}

pub fn validate_authentifiers(
    db: &Connection,
    _address: &String,
    asset: &Value,
    definition: &Value,
    unit: &Unit,
    validate_state: &mut ValidationState,
    authentifiers: &HashMap<String, String>,
) -> Result<()> {
    let is_asset = authentifiers.is_empty();
    if asset.is_null() && !is_asset {
        bail!("incompatible params");
    }
    validate_definition(db, definition, unit, validate_state, is_asset)?;
    // let mut used_path = Vec::new();
    // let res = evaluate(definition, "r", &mut used_path)?;
    // if !is_asset && used_path.len() != authentifiers.len() {
    //     bail!(
    //         "some authentifiers are not used, res= {:?}, used={:?}, passed={:?}",
    //         res,
    //         used_path,
    //         authentifiers
    //     );
    // }
    Ok(())
}

pub fn has_references(definition: &Value) -> Result<bool> {
    let op = definition
        .as_str()
        .ok_or_else(|| format_err!("failed to get op from definition"))?;

    match op {
        "sig" | " hash" | "cosigned by" => Ok(false),
        op => unimplemented!("unkonw op: {}", op),
    }
}
