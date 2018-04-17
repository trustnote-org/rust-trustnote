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

#[derive(Debug, Serialize, Deserialize)]
pub struct Inputs {
    pub message_index: u64,
    pub output_index: u64,
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
pub struct Outputs {
    pub address: String,
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub inputs: Vec<Inputs>,
    pub outputs: Vec<Outputs>,
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
    pub unit: Option<String>,    // this may not exist
    pub version: String,
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
