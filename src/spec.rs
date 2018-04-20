//! TODO: how to sort struct fields with serde?
//! within this mod all the struct fields should be "sorted" statically to generate the correct
//! object hash, this is annoying but we have no way to find out how to do that with serde

use std::collections::BTreeMap;

use object_hash::get_base64_hash;
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Authors {
    pub address: String,
    pub authentifiers: BTreeMap<String, String>,
    pub definition: Vec<Value>,
}

// TODO: Input struct is from type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Input {
    pub from_main_chain_index: Option<u32>,
    pub message_index: u32,
    pub kind: Option<String>,
    pub output_index: u32,
    pub to_main_chain_index: Option<u32>,
    pub unit: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    pub app: String,
    pub payload: Option<Payload>,
    pub payload_hash: String,
    pub payload_location: String,
    pub payload_uri: Option<String>,
    pub payload_uri_hash: Option<String>,
    pub spend_proofs: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Output {
    pub address: String,
    pub amount: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payload {
    pub asset: Option<String>,
    pub denomination: Option<u32>,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeaderCommissionShare {
    address: String,
    earned_headers_commission_share: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ball {
    // TODO: need a real definition
    pub unit: String,
}

// TODO: use specific struct for address and hash
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Unit {
    pub alt: String,
    pub authors: Vec<Authors>,
    pub content_hash: Option<String>, // this may not exist
    pub earned_headers_commission_recipients: Option<Vec<HeaderCommissionShare>>,
    pub headers_commission: Option<u32>, // default 0
    pub last_ball: Option<String>,
    pub last_ball_unit: Option<String>,
    pub main_chain_index: Option<u32>,
    pub messages: Vec<Message>,
    pub parent_units: Vec<String>,
    pub payload_commission: Option<u32>, // default 0
    pub timestamp: Option<u32>,
    pub unit: Option<String>, // this may not exist
    pub version: String,
    pub witnesses: Option<Vec<String>>,
    pub witness_list_unit: Option<String>,
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

    fn get_naked_unit(&self) -> Unit {
        let mut naked_unit: Unit = self.clone();
        naked_unit.unit = None;
        naked_unit.headers_commission = None;
        naked_unit.payload_commission = None;
        naked_unit.main_chain_index = None;
        naked_unit.timestamp = None;

        for message in naked_unit.messages.iter_mut() {
            message.payload = None;
            message.payload_uri = None;
        }

        naked_unit
    }

    pub fn get_unit_content_hash(&self) -> String {
        get_base64_hash(&self.get_naked_unit()).expect("get_unit_content_hash failed")
    }

    pub fn get_unit_hash(&self) -> String {
        if self.content_hash.is_some() {
            return get_base64_hash(&self.get_naked_unit()).expect("get_unit_hash naked failed");
        }

        #[derive(Debug, Serialize, Deserialize)]
        pub struct StrippedUnit {
            alt: String,
            authors: Vec<String>,
            content_hash: String,
            last_ball: Option<String>,
            last_ball_unit: Option<String>,
            parent_units: Vec<String>,
            version: String,
            witnesses: Option<Vec<String>>,
            witness_list_unit: Option<String>,
        }

        let mut stripped_unit = StrippedUnit {
            alt: self.alt.clone(),
            authors: self.authors
                .iter()
                .map(|a| a.address.clone())
                .collect::<Vec<_>>(),
            content_hash: self.get_unit_content_hash(),
            last_ball: None,
            last_ball_unit: None,
            parent_units: self.parent_units.clone(),
            version: self.version.clone(),
            witnesses: None,
            witness_list_unit: None,
        };

        if self.witness_list_unit.is_some() {
            stripped_unit.witness_list_unit = self.witness_list_unit.clone();
        } else {
            stripped_unit.witnesses = self.witnesses.clone();
        }

        if self.parent_units.len() > 0 {
            stripped_unit.last_ball = self.last_ball.clone();
            stripped_unit.last_ball_unit = self.last_ball_unit.clone();
        }

        get_base64_hash(&stripped_unit).expect("get_unit_hash failed")
    }

    pub fn get_unit_hash_to_sign(&self) -> Vec<u8> {
        use obj_ser;
        use sha2::{Digest, Sha256};

        let mut naked_unit = self.get_naked_unit();
        for author in naked_unit.authors.iter_mut() {
            author.authentifiers.clear();
        }

        let obj_str = obj_ser::to_string(&naked_unit).expect("naked_unit to string failed");

        Sha256::digest(obj_str.as_bytes()).to_vec()
    }
}
