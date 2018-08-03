use std::fs::File;
// use std::path::Path;

use may::sync::RwLock;
use serde_json;
use trustnote::Result;
use trustnote_wallet_base::*;

const SETTINGS_FILE: &str = "settings.json";

lazy_static! {
    static ref SETTINGS: RwLock<Settings> = RwLock::new({
        match open_settings() {
            Ok(s) => s,
            Err(_) => {
                warn!("can't open settings.json, will use default settings");
                let settings = Settings::default();
                save_settings(&settings).expect("failed to save settings");
                settings
            }
        }
    });
}

#[derive(Debug, Serialize, Deserialize)]
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
            mnemonic: mnemonic("")
                .expect("failed to generate mnemonic")
                .to_string(),
            initial_db_path: String::from("db/initial.trustnote.sqlite"),
        }
    }
}

fn open_settings() -> Result<Settings> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);
    let file = File::open(settings_path)?;
    let settings = serde_json::from_reader(file)?;
    Ok(settings)
}

pub fn show_config() {
    use std::io::stdout;
    println!("config:");
    serde_json::to_writer_pretty(stdout(), &*SETTINGS.read().unwrap()).unwrap();
    println!("\n");
}

fn save_settings(settings: &Settings) -> Result<()> {
    let mut settings_path = ::std::env::current_dir()?;
    settings_path.push(SETTINGS_FILE);

    let file = File::create(settings_path)?;

    serde_json::to_writer_pretty(file, settings)?;
    Ok(())
}
