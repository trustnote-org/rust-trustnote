use std::collections::HashMap;

use error::Result;
use rusqlite::Connection;
use serde_json::Value;
use spec::*;
use validation::ValidationState;

pub fn validate_definition(
    _db: &Connection,
    _definition: &Value,
    _unit: &Unit,
    _validate_state: &mut ValidationState,
    _is_asset: bool,
) -> Result<()> {
    unimplemented!()
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
