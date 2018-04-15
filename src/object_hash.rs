use error::Result;
use obj_ser::to_string;
use serde::ser::Serialize;
use sha2::{Digest, Sha256};

pub fn to_base64_hash<T>(object: &T) -> Result<String>
where
    T: Serialize,
{
    use base64;

    // create a Sha256 object
    let mut hasher = Sha256::default();

    // write input message
    hasher.input(&to_string(object)?.as_bytes());

    Ok(base64::encode(&hasher.result()))
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
    assert_eq!(to_base64_hash(&payload).unwrap(), expected);
}
