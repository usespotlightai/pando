use std::time::{Duration, SystemTime};

use tokio::time::Instant;

use crate::ports::Clock;

/// Wall time that starts at a fixed date and moves with `tokio::time`, so a paused runtime
/// moves it too.
#[derive(Debug)]
pub struct SimClock {
    start: Instant,
}

impl SimClock {
    pub const EPOCH: Duration = Duration::from_secs(1_800_000_000);

    pub fn new() -> SimClock {
        SimClock {
            start: Instant::now(),
        }
    }
}

impl Default for SimClock {
    fn default() -> Self {
        SimClock::new()
    }
}

impl Clock for SimClock {
    fn wall(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Self::EPOCH + self.start.elapsed()
    }
}
