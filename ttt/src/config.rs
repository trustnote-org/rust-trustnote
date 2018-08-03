use std::fs::File;
use std::path::Path;

use may::sync::RwLock;
use trustnote::error::Result;
use trustnote_wallet_base::Mnemonic;

const SETTINGS_FILE: &str = "settings.json";

lazy_static! {
    static ref PREFERENCES: RwLock<Settings> = RwLock::new({
        match open_settings() {
            Ok(p) => p,
            Err(e) => {
                error!("open preference err={}", e);
                default_settings().unwrap()
            }
        }
    });
}

pub struct Settings {
    hub_url: Vec<String>,
    mnemonic: String,
    // TODO: create db with code
    initial_db_path: String,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            hub_url: vec![String::from("119.28.86.54:6616")],
            mnemonic: 
        }
    }
}
