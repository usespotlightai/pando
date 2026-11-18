use std::time::SystemTime;

use pando_engine::ports::{Clock, Rng};

#[derive(Debug, Default)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Reads the kernel's random source on every call and keeps no state, so processes restored
/// from one memory snapshot draw different numbers.
#[derive(Debug, Default)]
pub(crate) struct SystemRng;

impl Rng for SystemRng {
    #[expect(
        clippy::expect_used,
        reason = "identifiers drawn without the kernel's randomness could repeat across restored snapshots"
    )]
    fn next_u64(&self) -> u64 {
        getrandom::u64().expect("the kernel's random source failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draws_differ() {
        let r = SystemRng;
        let a: Vec<u64> = (0..8).map(|_| r.next_u64()).collect();
        let mut b = a.clone();
        b.sort_unstable();
        b.dedup();
        assert_eq!(a.len(), b.len());
    }
}
