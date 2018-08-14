#[macro_use]
extern crate log;
#[macro_use]
extern crate may;
extern crate may_waiter;
extern crate num_cpus;
extern crate rusqlite;
extern crate serde;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate tungstenite;
extern crate url;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate rust_embed;
#[macro_use]
extern crate lazy_static;
extern crate app_dirs;
extern crate base32;
extern crate base64;
extern crate bit_vec;
extern crate crossbeam;
extern crate rand;
extern crate ripemd160;
extern crate secp256k1;
extern crate sha1;
extern crate sha2;

macro_rules! some_if {
    ($condition:expr, $some:expr) => {{
        if $condition {
            Some($some)
        } else {
            None
        }
    }};
}

macro_rules! some_if_option {
    ($condition:expr, $some:expr) => {{
        if $condition {
            $some
        } else {
            None
        }
    }};
}

#[macro_export]
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
#[macro_export]
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

#[macro_use]
pub mod utils;

pub mod config;
pub mod db;
pub mod error;
pub mod graph;
pub mod headers_commission;
pub mod mc_outputs;
pub mod my_witness;
pub mod network;
pub mod paid_witnessing;
pub mod spec;

pub mod catchup;
pub mod composer;
mod definition;
pub mod joint;
pub mod joint_storage;
pub mod light;
pub mod light_wallet;
pub mod main_chain;
mod obj_ser;
pub mod object_hash;
pub mod parent_composer;
pub mod signature;
pub mod storage;
pub mod time;
pub mod validation;
pub mod wallet;
pub mod witness_proof;
pub use error::{Result, TrustnoteError};
