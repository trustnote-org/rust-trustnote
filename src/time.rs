use std::time::{SystemTime, UNIX_EPOCH};

/// return milliseconds since unix epoch
pub fn now() -> usize {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    let ret = dur.as_secs() * 1000 + dur.subsec_nanos() as u64 / 1_000_000;
    ret as usize
}
