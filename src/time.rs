use std::time::{Duration, SystemTime, UNIX_EPOCH};

use db;
use may::coroutine;
use network::hub;

/// return milliseconds since unix epoch
pub fn now() -> usize {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    let ret = dur.as_secs() * 1000 + u64::from(dur.subsec_nanos()) / 1_000_000;
    ret as usize
}

pub fn start_global_timers() {
    // find and handle ready joints
    go!(move || loop {
        coroutine::sleep(Duration::from_secs(5));
        info!("find_and_handle_joints_that_are_ready");
        let mut db = db::DB_POOL.get_connection();
        t!(hub::find_and_handle_joints_that_are_ready(&mut db, None));
    });

    // request needed joints that were not received during the previous session
    go!(move || loop {
        let db = db::DB_POOL.get_connection();
        t!(hub::re_requeset_lost_joints(&db));
        coroutine::sleep(Duration::from_secs(8));
    });

    // this should be run in a single thread to remove those junk joints
    go!(move || loop {
        let db = db::DB_POOL.get_connection();
        t!(hub::purge_junk_unhandled_joints(&db));
        coroutine::sleep(Duration::from_secs(30 * 60));
    });
}
