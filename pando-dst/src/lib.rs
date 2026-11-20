//! Tests that run the engine on simulated ports: conformance suites every adapter of a port
//! must pass, and seeded crash and fault scenarios as the engine grows.

#![forbid(unsafe_code)]
#![expect(
    clippy::unwrap_used,
    clippy::print_stderr,
    reason = "a conformance check fails by panicking, and a traced scenario reports to the test's stderr"
)]

pub mod gc_race;
pub mod race;
pub mod scenario;
pub mod store_conformance;
pub mod trace;
