use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> u64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    dur.as_secs() * 1000 + dur.subsec_nanos() as u64 / 1_000_000
}
