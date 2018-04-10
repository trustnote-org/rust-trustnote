use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Authentifiers {
    pub r: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Authors {
    pub address: String,
    pub authentifiers: Authentifiers,
    pub definition: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Inputs {
    pub unit: String,
    pub message_index: u64,
    pub output_index: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Messages {
    pub app: String,
    pub payload_location: String,
    pub payload_hash: String,
    pub payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Outputs {
    pub address: String,
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub outputs: Vec<Outputs>,
    pub inputs: Vec<Inputs>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Unit {
    pub version: String,
    pub alt: String,
    pub messages: Vec<Messages>,
    pub authors: Vec<Authors>,
    pub parent_units: Vec<String>,
    pub last_ball: String,
    pub last_ball_unit: String,
    pub witness_list_unit: String,
    pub headers_commission: u64,
    pub payload_commission: u64,
}
