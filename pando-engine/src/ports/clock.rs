use std::fmt;
use std::time::SystemTime;

/// Wall time, for what is compared across hosts: segment ages in GC, learned-chunk ages. Code
/// that measures durations uses `tokio::time`, which the simulator pauses.
pub trait Clock: Send + Sync + fmt::Debug {
    /// The current wall-clock time. It may step back or jump, so it never times a wait.
    fn wall(&self) -> SystemTime;
}
