use base32;
use base64;
use bit_vec::BitVec;
use error::Result;
use obj_ser::to_string;
use rand::{self, Rng};
use ripemd160::Ripemd160;
use serde::ser::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub fn get_base64_hash<T>(object: &T) -> Result<String>
where
    T: Serialize,
{
    Ok(base64::encode(&Sha256::digest(
        to_string(object)?.as_bytes(),
    )))
}

pub fn get_chash<T>(object: &T) -> Result<String>
where
    T: Serialize,
{
    let hash = Ripemd160::digest(&to_string(object)?.as_bytes());
    let truncate_hash = &hash[4..];

    let mut chash = BitVec::from_elem(160, false);
    let clean_data = BitVec::from_bytes(&truncate_hash);
    let checksum = get_checksum(&truncate_hash);

    let mut clean_data_index = 0;
    let mut checksum_index = 0;
    let mut chash_index = 0;

    while chash_index < chash.len() {
        if CHECKSUM_OFFSETS.contains(&chash_index) {
            chash.set(chash_index, checksum[checksum_index]);
            checksum_index += 1;
        } else {
            chash.set(chash_index, clean_data[clean_data_index]);
            clean_data_index += 1;
        }
        chash_index += 1;
    }

    Ok(base32::encode(
        base32::Alphabet::RFC4648 { padding: true },
        &chash.to_bytes(),
    ))
}

//A constant HashSet to store the offsets to insert the checksum into clean data
//When mix or separate data, it can be used to check whether the bit should be a checksum
//The original array pi is the fractional part from PI as a array.
//See the original chash.js for more details.
lazy_static! {
    static ref CHECKSUM_OFFSETS: HashSet<usize> = {
        let pi = [
            1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3, 8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7,
            9, 5, 0, 2, 8, 8, 4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1, 0,
        ];

        let mut offset = 0;
        let mut set = HashSet::new();
        for i in pi.iter() {
            if *i > 0 {
                offset += i;
                set.insert(offset);
            }
        }

        set
    };
}

fn get_checksum(data: &[u8]) -> BitVec {
    let sha256 = Sha256::digest(data);
    let checksum = [sha256[5], sha256[13], sha256[21], sha256[29]];
    BitVec::from_bytes(&checksum)
}

pub fn is_chash_valid(encoded: &str) -> bool {
    let chash = base32::decode(base32::Alphabet::RFC4648 { padding: true }, &encoded)
        .expect("base32 decode return None");

    let chash = BitVec::from_bytes(&chash);
    let mut checksum = BitVec::new();
    let mut clean_data = BitVec::new();

    //let mut chash_index = 0;
    for (chash_index, bit) in chash.iter().enumerate() {
        if CHECKSUM_OFFSETS.contains(&chash_index) {
            checksum.push(bit);
        } else {
            clean_data.push(bit);
        }
    }

    get_checksum(&clean_data.to_bytes()) == checksum
}

pub fn get_ball_hash(
    unit: &str,
    parent_balls: &[String],
    skiplist_balls: &[String],
    is_nonserial: bool,
) -> String {
    #[inline]
    fn is_empty<T>(arr: &[T]) -> bool {
        arr.is_empty()
    }

    #[derive(Serialize)]
    struct BallHashObj<'a> {
        unit: &'a str,
        #[serde(skip_serializing_if = "is_empty")]
        parent_balls: &'a [String],
        #[serde(skip_serializing_if = "is_empty")]
        skiplist_balls: &'a [String],
        #[serde(skip_serializing_if = "Option::is_none")]
        is_nonserial: Option<bool>,
    }

    let is_nonserial = if is_nonserial { Some(true) } else { None };
    let ball = BallHashObj {
        unit,
        parent_balls,
        skiplist_balls,
        is_nonserial,
    };

    get_base64_hash(&ball).expect("failed to calc ball hash")
}

#[inline]
pub fn gen_random_string(len: usize) -> String {
    use rand::distributions::Standard;

    let bytes: Vec<u8> = rand::thread_rng()
        .sample_iter(&Standard)
        .take(len)
        .collect();
    base64::encode(&bytes)
}

////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_payload() {
    use serde_json;
    use spec;

    //Copied from the Unit json string
    let json = r#"{
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
            }"#;
    let payload: spec::Payload = serde_json::from_str(json).unwrap();
    let expected = "5CYeTTa4VQxgF4b1Tn33NBlKilJadddwBMLvtp1HIus=";

    //println!("{:?}", to_base64_hash(&payload));
    assert_eq!(get_base64_hash(&payload).unwrap(), expected);
}

#[test]
fn test_chash160() {
    let data = "A0mQdZvy+bGpIu/yBSNt7eB4mTZUQiM173bIQTOQRz3U";
    let expected = "YFAR4AK2RSRTAWZ3ILRFZOMN7M7QJTJ2";

    assert_eq!(get_chash(&data).unwrap(), expected);
}

#[test]
fn test_chash160_validation() {
    let valid = "YFAR4AK2RSRTAWZ3ILRFZOMN7M7QJTJ2";
    let invalid = "NFAR4AK2RSRTAWZ3ILRFZOMN7M7QJTJ2";

    assert_eq!(is_chash_valid(valid), true);
    assert_eq!(is_chash_valid(invalid), false);
}
