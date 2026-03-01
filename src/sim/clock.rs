use crate::traits::io::Clock;

/// Deterministic simulation clock.
///
/// Time only advances when the bus explicitly ticks it — never from
/// wall-clock time. This guarantees deterministic execution.
#[derive(Debug)]
pub struct SimClock {
    now: u64,
}

impl SimClock {
    pub fn new() -> Self {
        Self { now: 0 }
    }

    /// Advance the clock to the given tick.
    pub fn advance_to(&mut self, tick: u64) {
        debug_assert!(tick >= self.now, "SimClock cannot go backwards");
        self.now = tick;
    }

    /// Advance the clock by one tick.
    pub fn tick(&mut self) {
        self.now += 1;
    }
}

impl Clock for SimClock {
    fn now(&self) -> u64 {
        self.now
    }
}

impl Default for SimClock {
    fn default() -> Self {
        Self::new()
    }
}
