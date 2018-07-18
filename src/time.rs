use std::time::{Duration, SystemTime, UNIX_EPOCH};

use db;
use joint_storage;
use may::coroutine;
use network::hub;

/// return milliseconds since unix epoch
pub fn now() -> u64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    dur.as_secs() * 1000 + u64::from(dur.subsec_nanos()) / 1_000_000
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
        info!("re_requeset_lost_joints");
        t!(hub::re_requeset_lost_joints(&db));
        coroutine::sleep(Duration::from_secs(8));
    });

    // remove those junk joints
    go!(move || loop {
        coroutine::sleep(Duration::from_secs(30 * 60));
        let db = db::DB_POOL.get_connection();
        info!("purge_junk_unhandled_joints");
        t!(hub::purge_junk_unhandled_joints(&db));
    });

    // purge uncovered nonserial joints
    go!(move || loop {
        coroutine::sleep(Duration::from_secs(60));
        info!("purge_uncovered_nonserial_joints_under_lock");
        t!(joint_storage::purge_uncovered_nonserial_joints_under_lock());
    });
}
