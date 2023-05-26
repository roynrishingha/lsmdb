use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

pub fn generate_timestamp() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("failed to generate timestamp")
        .as_micros()
}
