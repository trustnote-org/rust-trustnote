use std::fs::File;

use serde_json;
use trustnote::Result;
use trustnote_wallet_base::*;

const SETTINGS_FILE: &str = "settings.json";

#[derive(Debug, Serialize, Deserialize)]
pub struct Settings {
    pub hub_url: Vec<String>,
    pub mnemonic: String,
    pub genesis_unit: String,
    pub witnesses: Vec<String>,
}

impl Default for Settings {
    fn default() -> Self {
        let hub_url;
        let genesis_unit;
        let witnesses;
        if cfg!(debug_assertions) {
            hub_url = vec![String::from("119.28.86.54:6616")];
            genesis_unit = String::from("V/NuDxzT7VFa/AqfBsAZ8suG4uj3u+l0kXOLE+nP+dU=");
            witnesses = vec![
                String::from("6LDM27ELDDAJBTNTVVQQYW7MWOK3F6WD"),
                String::from("BP2NYKORMOB5SEUTFSVPF2CMSQSVEZOS"),
                String::from("C6D4XKXDO4JAUT3BR27RM3UHKYGILR3X"),
                String::from("CGCU5BBDWY2ZU3XKUXNGDTXDY7VXXJNJ"),
                String::from("E45DPZHBPI7YX3CDG7HWTWBWRNGBV6C3"),
                String::from("EPG47NW4DDKIBUFZBDVQU3KHYCCMXTDN"),
                String::from("FF6X4KX3OOAAZUYWXDAHQJIJ5HDZLSXL"),
                String::from("JVFHPXAA7FJEJU3TSTR5ETYVOXHOBR4H"),
                String::from("MWJTSFCRBCV2CVT3SCDYZW2F2N3JKPIP"),
                String::from("NJSDFSIRZT5I5YQONDNEMKXSFNJPSO6A"),
                String::from("OALYXCMDI6ODRWMY6YO6WUPL6Q5ZBAO5"),
                String::from("UABSDF77S6SU4FDAXWTYIODVODCAA22A"),
            ];
        } else {
            hub_url = vec![String::from("raytest.trustnote.org:6616")];
            genesis_unit = String::from("MtzrZeOHHjqVZheuLylf0DX7zhp10nBsQX5e/+cA3PQ=");
            witnesses = vec![
                String::from("34NRY6HRBMWYMJQUKBF22R7JEKXYUHHW"),
                String::from("3C3OHD7WEFKV6RDF2U4M74RVK7YMDP7I"),
                String::from("4QBVMWX7DRAIVV4CZEVKS3IAQAFDPFBB"),
                String::from("4VCBX74SQMW46OKDTHXDVIFVIP2V6NFX"),
                String::from("4VYYR2YO6NV4NTF572AUBEKJLSTM4J4E"),
                String::from("AKB7DYDKTIMSOUNHUFB5PHKXOOYCM3YF"),
                String::from("B4Z366GZMCWJGPCQI5ROPK3L5OEBT7QD"),
                String::from("D27P6DGHLPO5A7MSOZABHOOWQ3BJ56ZI"),
                String::from("I6IK6MIYY34C4LV3JU6MNMGCJJN6VSKC"),
                String::from("KPQ3CRPBG5FSKVEH6Y76ETGD5D2N7QZ7"),
                String::from("NKLP6XURGMNT3ZUCJBCUVHB6BRNZTZL5"),
                String::from("QSOMNL7YPFQCYDKFUO63Y7RBLXDRDVJX"),
            ];
        }

        Settings {
            hub_url,
            mnemonic: mnemonic("")
                .expect("failed to generate mnemonic")
                .to_string(),
            genesis_unit,
            witnesses,
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

pub fn update_mnemonic(mnemonic: &str) -> Result<()> {
    let mnemonic = Mnemonic::from(mnemonic)?.to_string();
    let mut settings = get_settings();
    if settings.mnemonic != mnemonic {
        println!("will update mnemonic to: {}", mnemonic);
        settings.mnemonic = mnemonic;
    }
    save_settings(&settings)
}

pub fn get_settings() -> Settings {
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
}
