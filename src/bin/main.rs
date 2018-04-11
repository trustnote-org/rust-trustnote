#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;
extern crate trustnote;

extern crate fern;

use trustnote::*;

fn test_json() {
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

    let u: spec::Unit = serde_json::from_str(data).unwrap();
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
}

fn test_db() {
    let db = db::DB_POOL.get_connection();

    let names = db.get_my_witnesses().expect("failed to query database");

    for name in names {
        println!("name = {}", name);
    }
}

fn test_ws() {
    let _server = network::run_websocket_server(("0.0.0.0", config::WS_PORT));
    println!(
        "Websocket server running on ws://0.0.0.0:{}",
        config::WS_PORT
    );

    let mut client = network::WsClient::new(("127.0.0.1", config::WS_PORT)).unwrap();
    client.send_message("hello world".into()).unwrap();
    loop {
        let msg = client.recv_message().unwrap();
        println!("recv {}", msg);
    }
    // server.join().unwrap();
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

fn main() {
    log_init();
    test_json();
    test_db();
    test_ws();
}
