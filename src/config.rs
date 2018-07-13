extern crate config;

use self::config::*;
use may::sync::RwLock;

pub const HASH_LENGTH: usize = 44;
pub const WS_PORT: u16 = 6616;
pub const MAX_COMPLEXITY: usize = 100;
pub const COUNT_WITNESSES: usize = 12;
pub const TOTAL_WHITEBYTES: i64 = 500_000_000_000_000;
pub const MAX_WITNESS_LIST_MUTATIONS: usize = 1;
pub const MAJORITY_OF_WITNESSES: usize = 7;
pub const VERSION: &str = "1.0";
pub const ALT: &str = "1";
pub const STALLED_TIMEOUT: usize = 10;
pub const MAX_MESSAGES_PER_UNIT: usize = 128;
pub const MAX_PARENT_PER_UNIT: usize = 16;
pub const MAX_AUTHORS_PER_UNIT: usize = 16;
pub const MAX_SPEND_PROOFS_PER_MESSAGE: usize = 128;
pub const MAX_INPUTS_PER_PAYMENT_MESSAGE: usize = 128;
pub const MAX_OUTPUTS_PER_PAYMENT_MESSAGE: usize = 128;
pub const MAX_AUTHENTIFIER_LENGTH: usize = 4096;
pub const COUNT_MC_BALLS_FOR_PAID_WITNESSING: u32 = 100;
pub const MAX_DATA_FEED_NAME_LENGTH: usize = 64;
pub const MAX_DATA_FEED_VALUE_LENGTH: usize = 64;
pub const MAX_ITEMS_IN_CACHE: usize = 1000;

lazy_static! {
    pub static ref CONFIG: RwLock<Config> = RwLock::new({
        let mut settings = Config::default();
        settings
            .merge(File::with_name("settings.json"))
            .expect("failed to load config");
        settings
    });
}
