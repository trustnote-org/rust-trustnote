use std::fs::File;
use std::path::Path;

use may::sync::RwLock;
use preferences::{Preferences, PreferencesMap};
use trustnote::*;

const PREF_FILE: &str = "preferences.json";

lazy_static! {
    static ref PREFERENCES: RwLock<PreferencesMap> = RwLock::new({
        let pref_path = Path::new(PREF_FILE);

        let preferences = if pref_path.is_file() {
            let mut file = File::open(pref_path).expect("open preference file failed");
            PreferencesMap::<String>::load_from(&mut file).expect("load preference file failed")
        } else {
            PreferencesMap::<String>::new()
        };

        preferences
    });
}

fn update(key: &str, value: &str) -> Result<()> {
    let mut preferences = PREFERENCES.write().unwrap();

    // Edit the preferences (std::collections::HashMap)
    preferences.insert(key.into(), value.into());

    // Store the user's preferences
    let mut file = File::create(PREF_FILE)?;
    preferences.save_to(&mut file)?;

    Ok(())
}

fn get(key: &str) -> Result<Option<String>> {
    let preferences = PREFERENCES.read().unwrap();
    Ok(preferences.get(key).cloned())
}

#[inline]
pub fn update_mnemonic(mnemonic: &str) -> Result<()> {
    update("mnemonic", mnemonic)
}

#[inline]
pub fn get_mnemonic() -> Result<Option<String>> {
    get("mnemonic")
}

#[test]
fn test_preferences() -> Result<()> {
    let key = "name";
    let value = "TTT cli";
    let mnemonic = "machine leader snap nut spare hill wild enough twenty cupboard flock canyon";

    //Update
    update(key, value)?;
    update_mnemonic(mnemonic)?;

    //Read from global variable
    assert_eq!(Some(value.to_string()), get(&key)?);
    assert_eq!(Some(mnemonic.to_string()), get_mnemonic()?);

    let mut pref = PreferencesMap::<String>::new();
    pref.insert(key.into(), value.into());
    pref.insert("mnemonic".into(), mnemonic.into());

    //Reload from the file
    let mut file = File::open(PREF_FILE).expect("open preference file failed");
    let load_result = PreferencesMap::<String>::load_from(&mut file)?;
    assert_eq!(load_result, pref);

    Ok(())
}
