use std::sync::atomic::{AtomicU64, Ordering};

use crate::ports::Rng;

/// SplitMix64: small, seeded, and good enough for ids and jitter in tests.
#[derive(Debug)]
pub struct SimRng {
    state: AtomicU64,
}

impl SimRng {
    pub fn new(seed: u64) -> SimRng {
        SimRng {
            state: AtomicU64::new(seed),
        }
    }
}

impl Rng for SimRng {
    fn next_u64(&self) -> u64 {
        let mut z = self
            .state
            .fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed)
            .wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}
