#[macro_use]
extern crate log;
// #[macro_use]
// extern crate failure;
#[macro_use]
extern crate serde_json;
extern crate base64;
extern crate fern;
extern crate trustnote;

#[macro_use]
extern crate may;

// use serde_json::Value;
use trustnote::*;

fn test_json() -> Result<()> {
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

    let u: spec::Unit = serde_json::from_str(data)?;
    // println!("unit = {:?}", u);
    assert_eq!(u.authors[0].definition[0], json!("sig"));
    assert_eq!(
        u.authors[0].definition[1],
        json!({"pubkey": "A0gKwkLedQgzm32JtEo6KmuRcyZa3beikS3xfrwdXAMU"})
    );
    // assert_eq!(
    //     u.authors[0].definition[1]["pubkey"].as_str().unwrap(),
    //     "A0gKwkLedQgzm32JtEo6KmuRcyZa3beikS3xfrwdXAMU"
    // );
    Ok(())
}

fn test_db() -> Result<()> {
    let db = db::DB_POOL.get_connection();

    let names = db.get_my_witnesses()?;

    for name in names {
        println!("name = {}", name);
    }

    Ok(())
}

#[allow(dead_code)]
fn test_ws() -> Result<()> {
    use network::hub::{self, WSS};
    use network::WsServer;

    let _server = WsServer::start(("0.0.0.0", config::WS_PORT), |c| {
        WSS.add_inbound(c);
    });
    println!(
        "Websocket server running on ws://0.0.0.0:{}",
        config::WS_PORT
    );

    let client = hub::create_outbound_conn(("127.0.0.1", config::WS_PORT))?;
    client.send_version()?;

    let server = WSS.get_next_inbound();
    server.send_version()?;

    Ok(())
}

fn test_signature() -> Result<()> {
    let hash = "KLop9582tzXZJbytWjiWLcnpEdvJI7mUymbnUPXweOM=";
    let priv_key = "jQGnkLnZlX2DjBUd8JKgHgw23zSdRL/Azx3foi/WqvE=";
    let sig =
        "YCdh5Q6jOiKQy2R9mQwKJ6tBnq31VFZX2dkb7Ypr+/5z6jj4GLEFT9RtryC4+mSILtKKLeN9YnBmYI4Xa+4tDw==";

    assert_eq!(
        signature::sign(&base64::decode(hash)?, &base64::decode(priv_key)?)?,
        sig
    );

    let hash = "uPQs4TwLtDGRAdH8sbIJ1ZyWpEmwHWRAhXpamODZ7Kk=";
    let pub_key = "A0qTjB3ZjHf2yT1EIvLrkVAWY8MPSueNcB4GTlKGo/o6";
    let sig =
        "up+2Fjhnu4OjJeesBPCgoZE+6ReqQDdnqcjhbq2iaulHjlwKYLcwRrD3udSWdHS57ceQeZ+LVPWYBMWBloAgpA==";

    assert_eq!(signature::verify(&base64::decode(hash)?, sig, pub_key)?, ());

    Ok(())
}

fn show_config() -> Result<()> {
    println!("debug = {}", config::CONFIG.read()?.get::<bool>("debug")?);
    Ok(())
}

fn log_init() {
    // Configure logger at runtime
    fern::Dispatch::new()
    // Perform allocation-free log formatting
    // .format(|out, message, record| {
    //     out.finish(format_args!(
    //         "{}[{}][{}] {}",
    //         chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
    //         record.target(),
    //         record.level(),
    //         message
    //     ))
    // })
    // Add blanket level filter -
    .level(log::LevelFilter::Debug)
    // - and per-module overrides
    // .level_for("hyper", log::LevelFilter::Info)
    // Output to stdout, files, and other Dispatch configurations
    .chain(std::io::stdout())
    // .chain(fern::log_file("output.log")?)
    // Apply globally
    .apply().unwrap();

    // and log using log crate macros!
    info!("log init done!");
}

#[allow(dead_code)]
fn test_ws_client() -> Result<()> {
    use network::hub;
    // let mut client = network::WssClient::new("shawtest.trustnote.org")?;
    let client = hub::create_outbound_conn(("127.0.0.1", 6655))?;
    client.send_version()?;
    client.send_subscribe()?;
    Ok(())
}

fn network_clean() {
    // remove all the actors
    network::hub::WSS.close_all();
}

// the main test logic that run in coroutine context
fn main_run() -> Result<()> {
    test_json()?;
    test_db()?;
    test_signature()?;
    // test_ws()?;
    test_ws_client()?;
    Ok(())
}

fn pause() {
    use std::io::{self, Read};
    io::stdin().read(&mut [0]).unwrap();
}

fn main() -> Result<()> {
    may::config().set_stack_size(0x2000);
    signature::init_secp256k1()?;
    log_init();
    show_config()?;
    // run the network stuff in coroutine context
    go!(|| main_run().unwrap()).join().unwrap();
    pause();
    network_clean();
    info!("bye from main!\n\n");
    Ok(())
}
