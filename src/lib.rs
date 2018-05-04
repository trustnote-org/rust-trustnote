#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
extern crate num_cpus;
extern crate rusqlite;
extern crate serde;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate tungstenite;
extern crate url;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate base32;
extern crate base64;
extern crate bit_vec;
extern crate native_tls;
extern crate ripemd160;
extern crate secp256k1;
extern crate sha2;

macro_rules! some_if {
    ($condition:expr, $some:expr) => {{
        match $condition {
            true => Some($some),
            _ => None,
        }
    }};
}

pub mod config;
pub mod db;
pub mod error;
pub mod graph;
pub mod my_witness;
pub mod network;
pub mod spec;

pub mod catchup;
mod definition;
pub mod joint;
mod obj_ser;
pub mod object_hash;
pub mod signature;
mod storage;
pub mod validation;
pub mod witness_proof;

pub use error::{Result, TrustnoteError};
