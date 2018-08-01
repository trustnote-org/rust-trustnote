use std::time::{SystemTime, UNIX_EPOCH};

/// return milliseconds since unix epoch
pub fn now() -> u64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    dur.as_secs() * 1000 + u64::from(dur.subsec_nanos()) / 1_000_000
}
