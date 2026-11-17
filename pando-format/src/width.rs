const _: () = assert!(usize::BITS == u64::BITS, "Pando runs only where usize is 64 bits");

/// `n` as a `usize`. Lossless, since the workspace builds only where `usize` is 64 bits.
#[must_use]
#[expect(
    clippy::cast_possible_truncation,
    reason = "usize is 64 bits, asserted at compile time"
)]
pub const fn usize_from(n: u64) -> usize {
    n as usize
}

/// `n` as a `u64`. Lossless, since the workspace builds only where `usize` is 64 bits.
#[must_use]
pub const fn u64_from(n: usize) -> u64 {
    n as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widths_round_trip_at_the_extremes() {
        assert_eq!(usize_from(u64::MAX), usize::MAX);
        assert_eq!(u64_from(usize::MAX), u64::MAX);
    }
}
