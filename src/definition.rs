use std::collections::HashMap;

use config;
use error::Result;
use failure::ResultExt;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::Value;
use signature;
use spec::*;
use validation::ValidationState;

struct Definition<'a> {
    op: &'a str,
    args: &'a Value,
}

impl<'a> Definition<'a> {
    fn from_value(value: &'a Value) -> Result<Self> {
        if !value.is_array() {
            println!("definition={:?}", value);
            bail!("definition must be array");
        }

        let arr = value.as_array().unwrap();
        if arr.len() != 2 {
            bail!("expression must be 2-element array");
        }
        let op = arr[0]
            .as_str()
            .ok_or_else(|| format_err!("op is not a string"))?;
        let args = &arr[1];

        Ok(Definition { op, args })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SigValue<'a> {
    algo: Option<&'a str>,
    pubkey: &'a str,
}

pub fn validate_definition(definition: &Value, is_asset: bool) -> Result<()> {
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

        let definition = Definition::from_value(definition)?;

        match definition.op {
            "sig" => {
                if is_in_negation {
                    bail!("sig cannot be negated");
                }
                if is_asset {
                    bail!("asset condition cannot have sig");
                }

                let sig_value =
                    SigValue::deserialize(definition.args).context("can't convert to SigValue")?;

                if let Some(algo) = sig_value.algo {
                    ensure!(algo == "secp256k1", "unsupported sig algo");
                }

                ensure!(
                    sig_value.pubkey.len() == config::HASH_LENGTH,
                    "wrong pubkey length"
                );
            }
            op => unimplemented!("unsupported op: {}", op),
        }
        Ok(true)
    }

    let mut complexity = 0;
    let has_sig = evaluate(definition, false, is_asset, &mut complexity)?;

    if !has_sig && !is_asset {
        bail!("each branch must have a signature");
    }

    Ok(())
}

pub fn validate_authentifiers(
    _db: &Connection,
    _address: &str,
    asset: &Value,
    definition: &Value,
    _unit: &Unit,
    validate_state: &mut ValidationState,
    authentifiers: &HashMap<String, String>,
) -> Result<()> {
    let evaluate = |definition: &Value, path: &str, used_path: &mut Vec<String>| -> Result<()> {
        let definition = Definition::from_value(definition)?;
        match definition.op {
            "sig" => {
                let sig = authentifiers
                    .get(path)
                    .ok_or_else(|| format_err!("authentifier path not found: {}", path))?;
                used_path.push(path.to_owned());

                if validate_state.unsigned && sig.starts_with('-') {
                    return Ok(());
                }

                let sig_value =
                    SigValue::deserialize(definition.args).context("can't conver to SigValue")?;
                let unit_hash = validate_state
                    .unit_hash_to_sign
                    .as_ref()
                    .expect("no unit hash to sign found");

                signature::verify(unit_hash, sig, sig_value.pubkey)
                    .context(format!("bad signature at path: {:?}", path))?;
            }
            op => unimplemented!("unsupported op: {}", op),
        }
        Ok(())
    };

    let is_asset = authentifiers.is_empty();
    if is_asset && !asset.is_null() {
        bail!("incompatible params");
    }
    validate_definition(definition, is_asset)?;
    let mut used_path = Vec::new();
    evaluate(definition, "r", &mut used_path)?;
    if !is_asset && used_path.len() != authentifiers.len() {
        bail!(
            "some authentifiers are not used, used={:?}, passed={:?}",
            used_path,
            authentifiers
        );
    }
    Ok(())
}

pub fn has_references(definition: &Value) -> Result<bool> {
    let definition = Definition::from_value(definition).context("has_references")?;

    match definition.op {
        "sig" | " hash" | "cosigned by" => Ok(false),
        op => unimplemented!("unkonw op: {}", op),
    }
}
