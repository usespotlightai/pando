use std::fmt;

/// Randomness, seeded in the simulator.
pub trait Rng: Send + Sync + fmt::Debug {
    /// The next 64 random bits. Not for secrets: the simulator's are predictable.
    fn next_u64(&self) -> u64;

    /// The next 32 random bits: the high half of [`Rng::next_u64`].
    fn next_u32(&self) -> u32 {
        (self.next_u64() >> 32) as u32
    }
}
