#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
extern crate num_cpus;
extern crate rusqlite;
extern crate serde;
#[macro_use]
extern crate serde_json;
extern crate tungstenite;
extern crate url;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
extern crate app_dirs;
extern crate base32;
extern crate base64;
extern crate bit_vec;
extern crate may_waiter;
extern crate rand;
extern crate ripemd160;
extern crate secp256k1;
extern crate sha1;
extern crate sha2;

macro_rules! some_if {
    ($condition:expr, $some:expr) => {{
        match $condition {
            true => Some($some),
            _ => None,
        }
    }};
}

macro_rules! some_if_option {
    ($condition:expr, $some:expr) => {{
        match $condition {
            true => $some,
            _ => None,
        }
    }};
}

macro_rules! t {
    ($e:expr) => {
        match $e {
            Ok(val) => val,
            Err(err) => {
                error!("call = {:?}\nerr = {:?}", stringify!($e), err);
            }
        }
    };
}

// this is a special go macro that can return Result and print the error and backtrace
macro_rules! try_go {
    ($func:expr) => {{
        fn _go_check<F, E>(f: F) -> F
        where
            F: FnOnce() -> ::std::result::Result<(), E> + Send + 'static,
            E: Send + 'static,
        {
            f
        }
        let f = _go_check($func);
        go!(move || if let Err(e) = f() {
            error!("coroutine error: {}", e);
            error!("back_trace={}", e.backtrace());
        })
    }};
}

pub mod config;
pub mod db;
#[macro_use]
pub mod error;
pub mod atomic_lock;
pub mod graph;
pub mod headers_commission;
pub mod map_lock;
pub mod mc_outputs;
pub mod my_witness;
pub mod network;
pub mod paid_witnessing;
pub mod spec;

pub mod catchup;
mod definition;
pub mod joint;
pub mod joint_storage;
pub mod main_chain;
mod obj_ser;
pub mod object_hash;
pub mod signature;
pub mod storage;
pub mod time;
pub mod validation;
pub mod witness_proof;

pub use error::{Result, TrustnoteError};
