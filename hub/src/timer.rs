use std::time::Duration;

use may::coroutine;
use trustnote::db;
use trustnote::joint_storage;
use trustnote::network::hub;

pub fn start_global_timers() {
    // find and handle ready joints
    go!(move || loop {
        info!("find_and_handle_joints_that_are_ready");
        let mut db = db::DB_POOL.get_connection();
        t!(hub::find_and_handle_joints_that_are_ready(&mut db, None));
        coroutine::sleep(Duration::from_secs(5));
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

    // auto connection if peers count is under threshold
    go!(move || loop {
        coroutine::sleep(Duration::from_secs(30));
        info!("auto conntect to other peers");
        t!(hub::auto_connection());
    });
}
