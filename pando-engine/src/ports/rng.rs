use std::fmt;

/// Randomness, seeded in the simulator.
pub trait Rng: Send + Sync + fmt::Debug {
    fn next_u64(&self) -> u64;

    fn next_u32(&self) -> u32 {
        (self.next_u64() >> 32) as u32
    }
}
