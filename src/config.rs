extern crate config;

use self::config::*;
use may::sync::RwLock;

pub const WS_PORT: u16 = 8080;
pub const COUNT_WITNESSES: usize = 12;

lazy_static! {
    pub static ref CONFIG: RwLock<Config> = RwLock::new({
        let mut settings = Config::default();
        settings
            .merge(File::with_name("settings.json"))
            .expect("failed to load config");
        settings
    });
}
