use base32;
use base64;
use bit_vec::BitVec;
use error::Result;
use obj_ser::to_string;
use ripemd160::Ripemd160;
use serde::ser::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub fn get_base64_hash<T>(object: &T) -> Result<String>
where
    T: Serialize,
{
    Ok(base64::encode(
        &Sha256::digest(&to_string(object)?.as_bytes()),
    ))
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

    let checksum_offsets = get_checksum_offsets();
    let mut clean_data_index = 0;
    let mut checksum_index = 0;
    let mut chash_index = 0;

    while chash_index < chash.len() {
        if checksum_offsets.contains(&chash_index) {
            chash.set(chash_index, checksum[checksum_index]);
            checksum_index = checksum_index + 1;
        } else {
            chash.set(chash_index, clean_data[clean_data_index]);
            clean_data_index = clean_data_index + 1;
        }
        chash_index = chash_index + 1;
    }

    Ok(base32::encode(
        base32::Alphabet::RFC4648 { padding: true },
        &chash.to_bytes(),
    ))
}

fn get_checksum_offsets() -> HashSet<usize> {
    let index_for_mix = [
        1, 4, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 7, 9, 3, 2, 3, 8, 4, 6, 2, 6, 4, 3, 3, 8, 3, 2, 7, 9,
        5, /*0,*/ 2, 8, 8, 4, 1, 9, 7, 1, 6, 9, 3, 9, 9, 3, 7, 5, 1 /*0,*/,
    ];

    let mut offset = 0;
    let mut checksum_offsets = HashSet::new();
    for i in index_for_mix.iter() {
        offset = offset + i;
        checksum_offsets.insert(offset);
    }

    checksum_offsets
}

fn get_checksum(data: &[u8]) -> BitVec {
    let sha256 = Sha256::digest(data);
    let checksum = [sha256[5], sha256[13], sha256[21], sha256[29]];
    BitVec::from_bytes(&checksum)
}

pub fn is_chash_valid(encoded: String) -> Result<bool> {
    let chash = base32::decode(base32::Alphabet::RFC4648 { padding: true }, &encoded).unwrap();

    let chash = BitVec::from_bytes(&chash);
    let mut checksum = BitVec::new();
    let mut clean_data = BitVec::new();

    let checksum_offsets = get_checksum_offsets();
    let mut chash_index = 0;
    for bit in chash.iter() {
        if checksum_offsets.contains(&chash_index) {
            checksum.push(bit);
        } else {
            clean_data.push(bit);
        }
        chash_index = chash_index + 1;
    }

    Ok(get_checksum(&clean_data.to_bytes()) == checksum)
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

    assert_eq!(is_chash_valid(valid.to_string()).unwrap(), true);
    assert_eq!(is_chash_valid(invalid.to_string()).unwrap(), false);
}
