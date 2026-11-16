//! Tests that run the engine on simulated ports: conformance suites every adapter of a port
//! must pass, and seeded crash and fault scenarios as the engine grows.

#![forbid(unsafe_code)]

pub mod scenario;
pub mod store_conformance;
