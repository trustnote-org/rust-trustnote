use std::collections::HashMap;

use config;
use obj_ser;
use object_hash::get_base64_hash;
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Author {
    pub address: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub authentifiers: HashMap<String, String>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Value::is_null")]
    pub definition: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpendProof {
    pub spend_proof: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
}

// TODO: Input struct is from type
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Input {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_main_chain_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial_number: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "type")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_main_chain_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blinding: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Message {
    pub app: String,
    pub payload: Option<Payload>,
    pub payload_hash: String,
    pub payload_location: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_uri_hash: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub spend_proofs: Vec<SpendProof>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Output {
    pub address: String,
    pub amount: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Payload {
    Text(String),
    Payment(Payment),
    Other(Value),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Payment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition_chash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub denomination: Option<u32>,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeaderCommissionShare {
    pub address: String,
    pub earned_headers_commission_share: i64,
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
    pub authors: Vec<Author>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>, // this may not exist
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub earned_headers_commission_recipients: Vec<HeaderCommissionShare>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers_commission: Option<u32>, // default 0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_ball: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_ball_unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub main_chain_index: Option<u32>,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub parent_units: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_commission: Option<u32>, // default 0
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>, // this may not exist
    pub version: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub witnesses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub witness_list_unit: Option<String>,
}

#[derive(Debug, Clone)]
/// internally used struct
pub struct StaticUnitProperty {
    pub level: u32,
    pub witnessed_level: u32,
    pub best_parent_unit: Option<String>,
    pub witness_list_unit: Option<String>,
}

#[derive(Debug, Clone)]
/// internally used struct
pub struct UnitProps {
    pub unit: String,
    pub level: u32,
    pub latest_included_mc_index: Option<u32>,
    pub main_chain_index: Option<u32>,
    pub is_on_main_chain: u32,
    pub is_free: u32,
    pub is_stable: u32,
}

#[inline]
lazy_static! {
    static ref GENESIS_UNIT: String = ::config::get_genesis_unit();
}

pub fn is_genesis_unit(unit: &str) -> bool {
    unit == *GENESIS_UNIT
}

pub fn is_genesis_ball(ball: &str) -> bool {
    lazy_static! {
        //GENESIS_UNIT's parent and skiplist is null
        static ref GENESIS_BALL: String =
            ::object_hash::get_ball_hash(&GENESIS_UNIT, &Vec::new(), &Vec::new(), false);
    }
    ball == *GENESIS_BALL
}

impl Unit {
    pub fn is_genesis_unit(&self) -> bool {
        match self.unit {
            Some(ref hash) => is_genesis_unit(hash),
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

        for message in &mut naked_unit.messages {
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

        #[derive(Debug, Serialize)]
        struct Address {
            address: String,
        }

        #[derive(Debug, Serialize)]
        struct StrippedUnit {
            alt: String,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            authors: Vec<Address>,
            content_hash: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            last_ball: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            last_ball_unit: Option<String>,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            parent_units: Vec<String>,
            version: String,
            #[serde(skip_serializing_if = "Vec::is_empty")]
            witnesses: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            witness_list_unit: Option<String>,
        }

        let mut stripped_unit = StrippedUnit {
            alt: self.alt.clone(),
            authors: self
                .authors
                .iter()
                .map(|a| Address {
                    address: a.address.clone(),
                }).collect::<Vec<_>>(),
            content_hash: self.get_unit_content_hash(),
            last_ball: None,
            last_ball_unit: None,
            parent_units: self.parent_units.clone(),
            version: self.version.clone(),
            witnesses: Vec::new(),
            witness_list_unit: None,
        };

        if self.witness_list_unit.is_some() {
            stripped_unit.witness_list_unit = self.witness_list_unit.clone();
        } else {
            stripped_unit.witnesses = self.witnesses.clone();
        }

        if !self.parent_units.is_empty() {
            stripped_unit.last_ball = self.last_ball.clone();
            stripped_unit.last_ball_unit = self.last_ball_unit.clone();
        }

        get_base64_hash(&stripped_unit).expect("get_unit_hash failed")
    }

    pub fn get_unit_hash_to_sign(&self) -> Vec<u8> {
        use sha2::{Digest, Sha256};

        let mut naked_unit = self.get_naked_unit();
        for author in &mut naked_unit.authors {
            author.authentifiers.clear();
        }

        let obj_str = obj_ser::to_string(&naked_unit).expect("naked_unit to string failed");

        Sha256::digest(obj_str.as_bytes()).to_vec()
    }

    pub fn get_header_size(&self) -> u32 {
        if self.content_hash.is_some() {
            error!("trying to get headers size of stripped unit");
            return 0;
        }

        let mut header = self.clone();
        header.unit = None;
        header.headers_commission = None;
        header.payload_commission = None;
        header.main_chain_index = None;
        header.timestamp = None;
        header.messages.clear();
        header.parent_units.clear();

        const PARENT_UNITS_SIZE: u32 = 2 * 44;

        let size = match obj_ser::obj_size(&header) {
            Ok(s) => s as u32,
            Err(e) => {
                error!("failed to get header size, err={}", e);
                0
            }
        };

        size + PARENT_UNITS_SIZE
    }

    pub fn get_payload_size(&self) -> u32 {
        if self.content_hash.is_some() {
            error!("trying to get payload size of stripped unit");
            return 0;
        }

        match obj_ser::obj_size(&self.messages) {
            Ok(s) => s as u32,
            Err(e) => {
                error!("failed to get payload size, err={}", e);
                0
            }
        }
    }
}

impl Default for Unit {
    fn default() -> Self {
        Unit {
            alt: config::ALT.to_string(),
            authors: Vec::new(),
            content_hash: None,
            earned_headers_commission_recipients: Vec::new(),
            headers_commission: None,
            last_ball: None,
            last_ball_unit: None,
            main_chain_index: None,
            messages: Vec::new(),
            parent_units: Vec::new(),
            payload_commission: None,
            timestamp: None,
            unit: None,
            version: config::VERSION.to_string(),
            witnesses: Vec::new(),
            witness_list_unit: None,
        }
    }
}

#[test]
fn test_unit_hash() {
    use serde_json;
    let unit = r#"{
      "unit":"nIcYRvz1AiAwoMWhOz/h5tRL3fZvI2CdEg4tNo7hhLk=",
      "version":"1.0",
      "alt":"1",
      "witness_list_unit":"MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ=",
      "last_ball_unit":"dimZTmLvmjNfo7I6Go9juCIokk5I+tgyxAfNPlg16G4=",
      "last_ball":"SVnrEYhIOKmku91eWlwnPMV2gf/lMYpg36AL/zfakag=",
      "headers_commission":344,
      "payload_commission":157,
      "main_chain_index":65936,
      "timestamp":1527218469,
      "parent_units":[  
         "Y+A+trJA30+P6PsC0hX5CwhNDj80w4OmJMcnq5Ou1FU="
      ],
      "authors":[  
         {  
            "address":"D27P6DGHLPO5A7MSOZABHOOWQ3BJ56ZI",
            "authentifiers":{  
               "r":"+/d2BCSgLE30z8M1XUHQc6slv9w+Srf8yOQZf7IZQP4i1Xzmyj2ycce5yKnQOj3ZBupX28cQ+FWB1DRbkTrn2g=="
            }
         }
      ],
      "messages":[  
         {  
            "app":"payment",
            "payload_hash":"15LThwlDEC1nRe48EGg5giJsMkQ9Bhe3Z/kRyZ0RmNY=",
            "payload_location":"inline",
            "payload":{  
               "inputs":[  
                  {  
                     "unit":"rHwZyXWZRFeU/LA3Kga+xGvjijNXYQwTbufMjqdxmPg=",
                     "message_index":0,
                     "output_index":0
                  }
               ],
               "outputs":[  
                  {  
                     "address":"D27P6DGHLPO5A7MSOZABHOOWQ3BJ56ZI",
                     "amount":82375
                  }
               ]
            }
         }
      ]
   }"#;

    let unit: Unit = serde_json::from_str(unit).unwrap();
    assert_eq!(
        unit.get_unit_hash(),
        "nIcYRvz1AiAwoMWhOz/h5tRL3fZvI2CdEg4tNo7hhLk="
    );
    assert_eq!(unit.get_header_size(), 344);
    assert_eq!(unit.get_payload_size(), 157);
}

#[test]
fn test_unit_json() {
    use serde_json;
    let data = r#"
    {
    "version": "1.0",
    "alt": "1",
    "messages": [
        {
            "app": "payment",
            "payload_location": "inline",
            "payload_hash": "5CYeTTa4VQxgF4b1Tn33NBlKilJadddwBMLvtp1HIus=",
            "payload": {
                "outputs": [
                    {
                        "address": "7JXBJQPQC3466UPK7C6ABA6VVU6YFYAI",
                        "amount": 10000
                    },
                    {
                        "address": "JERTY5XNENMHYQW7NVBXUB5CU3IDODA3",
                        "amount": 99989412
                    }
                ],
                "inputs": [
                    {
                        "unit": "lQCxxsMslXLzQKybX2KArOGho8XuNf1Lpds2abdf8O4=",
                        "message_index": 0,
                        "output_index": 1
                    }
                ]
            }
        }
    ],
    "authors": [
        {
            "address": "JERTY5XNENMHYQW7NVBXUB5CU3IDODA3",
            "authentifiers": {
                "r": "tHLxvXNYVwDnQg3N4iNHtHZ4mXvqRW+ZMPkQadev6MpAWbEPVcIpme1Vz1nyskWYgueREZoEbQeEWtC/oCQbxQ=="
            },
            "definition": [
                "sig",
                {
                    "pubkey": "A0gKwkLedQgzm32JtEo6KmuRcyZa3beikS3xfrwdXAMU"
                }
            ]
        }
    ],
    "parent_units": [
        "uPbobEuZL+FY1ujTNiYZnM9lgC3xysxuDIpSbvnmbac="
    ],
    "last_ball": "oiIA6Y+87fk6/QyrbOlwqsQ/LLr82Rcuzcr1G/GoHlA=",
    "last_ball_unit": "vxrlKyY517Z+BGMNG35ExiQsYv3ncp/KU414SqXKXTk=",
    "witness_list_unit": "MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ=",
    "headers_commission": 391,
    "payload_commission": 197
    }"#;

    let u: Unit = serde_json::from_str(data).unwrap();
    assert_eq!(u.authors[0].definition[0], json!("sig"));
    assert_eq!(
        u.authors[0].definition[1],
        json!({"pubkey": "A0gKwkLedQgzm32JtEo6KmuRcyZa3beikS3xfrwdXAMU"})
    );
}
