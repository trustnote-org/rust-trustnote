extern crate config;

use self::config::*;
use may::sync::RwLock;
use time;

pub const WS_PORT: u16 = 8080;
pub const COUNT_WITNESSES: usize = 12;
pub const MAX_WITNESS_LIST_MUTATIONS: usize = 1;
pub const MAJORITY_OF_WITNESSES: usize = 7;
pub const VERSION: &str = "1.0";
pub const ALT: &str = "1";
pub const STALLED_TIMEOUT: usize = 10;
pub const MAX_MESSAGES_PER_UNIT: usize = 128;
pub const MAX_PARENT_PER_UNIT: usize = 16;

pub const STORAGE: &str = "sqlite";
pub const MAX_LENGTH: usize = 200;
//pub arrBreadcrumbs:Vec<String> = vec![];
//pub const MAJORITY_OF_WITNESSES: usize = (COUNT_WITNESSES%2===0) ? (COUNT_WITNESSES/2+1) : (COUNT_WITNESSES/2);
#[derive(Debug)]
struct bread_crumbs {
    breadcrumbs:Vec<String>,
}

impl bread_crumbs{
    fn get(&self)->usize{
        self.breadcrumbs.len()
    }

    fn add(&mut self, breadcrumb:&str) {
        if self.breadcrumbs.len() > MAX_LENGTH {
             self.breadcrumbs.remove(0);
        }
        self.breadcrumbs.push(time::now().to_string() + ":" + breadcrumb);
    }
}

pub const COUNT_MC_BALLS_FOR_PAID_WITNESSING: u32 = 100;

lazy_static! {
    pub static ref CONFIG: RwLock<Config> = RwLock::new({
        let mut settings = Config::default();
        settings
            .merge(File::with_name("settings.json"))
            .expect("failed to load config");
        settings
    });
}
