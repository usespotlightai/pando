use std::time::SystemTime;

use pando_engine::ports::{Clock, Rng};

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Debug, Default)]
pub struct SystemRng;

impl Rng for SystemRng {
    fn next_u64(&self) -> u64 {
        rand::random()
    }
}
