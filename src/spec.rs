//! TODO: how to sort struct fields with serde?
//! within this mod all the struct fields should be "sorted" statically to generate the correct
//! object hash, this is annoying but we have no way to find out how to do that with serde

use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Authors {
    pub address: String,
    pub authentifiers: BTreeMap<String, String>,
    pub definition: Vec<Value>,
}

// TODO: Input struct is from type
#[derive(Debug, Serialize, Deserialize)]
pub struct Input {
    pub from_main_chain_index: Option<u32>,
    pub message_index: u32,
    pub kind: Option<String>,
    pub output_index: u32,
    pub to_main_chain_index: Option<u32>,
    pub unit: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub app: String,
    pub payload: Payload,
    pub payload_hash: String,
    pub payload_location: String,
    pub payload_uri: Option<String>,
    pub payload_uri_hash: Option<String>,
    pub spend_proofs: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    pub address: String,
    pub amount: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub asset: Option<String>,
    pub denomination: Option<u32>,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HeaderCommissionShare {
    address: String,
    earned_headers_commission_share: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ball {
    // TODO: need a real definition
    pub unit: String,
}

// TODO: use specific struct for address and hash
#[derive(Debug, Serialize, Deserialize)]
pub struct Unit {
    pub alt: String,
    pub authors: Vec<Authors>,
    pub content_hash: Option<String>, // this may not exist
    pub earned_headers_commission_recipients: Option<Vec<HeaderCommissionShare>>,
    pub headers_commission: u32, // default 0
    pub last_ball: String,
    pub last_ball_unit: String,
    pub messages: Vec<Message>,
    pub parent_units: Vec<String>,
    pub payload_commission: u32, // default 0
    pub unit: Option<String>,    // this may not exist TODO: remove the option
    pub version: String,
    pub witnesses: Option<Vec<String>>,
    pub witness_list_unit: String,
}

#[derive(Debug)]
/// internally used struct
pub struct StaticUnitProperty {
    pub level: u32,
    pub witnessed_level: u32,
    pub best_parent_unit: String,
    pub witness_list_unit: String,
}

impl Unit {
    pub fn is_genesis_unit(&self) -> bool {
        match self.unit {
            Some(ref hash) if hash == ::config::GENESIS_UNIT => true,
            _ => false,
        }
    }
}
