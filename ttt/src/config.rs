use std::fs::File;

use serde_json;
use trustnote::Result;
use trustnote_wallet_base::*;

pub const DB_PATH: &str = "trustnote_light.sqlite";
const SETTINGS_FILE: &str = "settings.json";

lazy_static! {
    static ref SETTINGS: Settings = {
        match open_settings() {
            Ok(s) => s,
            Err(_) => {
                warn!("can't open settings.json, will use default settings");
                let settings = Settings::default();
                settings.show_config();
                save_settings(&settings).expect("failed to save settings");
                settings
            }
        }
    };
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub hub_url: Vec<String>,
    pub mnemonic: String,
    pub initial_db_path: String,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            hub_url: vec![String::from("119.28.86.54:6616")],
            mnemonic: mnemonic("")
                .expect("failed to generate mnemonic")
                .to_string(),
            initial_db_path: String::from("../db/initial.trustnote.sqlite"),
        }
    }
}

impl Settings {
    pub fn show_config(&self) {
        use std::io::stdout;
        println!("settings:");
        serde_json::to_writer_pretty(stdout(), self).unwrap();
        println!("\n");
    }
}

fn open_settings() -> Result<Settings> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);
    let file = File::open(settings_path)?;
    let settings = serde_json::from_reader(file)?;
    Ok(settings)
}

fn save_settings(settings: &Settings) -> Result<()> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);

    let file = File::create(settings_path)?;

    serde_json::to_writer_pretty(file, settings)?;
    Ok(())
}

pub fn get_settings() -> &'static Settings {
    &SETTINGS
}
