/// Deterministic PRNG seeded once at simulation start.
///
/// All randomness in the simulation flows through this single source.
/// Same seed = same sequence = deterministic replay.
///
/// Uses xorshift64 — simple, fast, no external dependency.
#[derive(Debug)]
pub struct SeededRng {
    state: u64,
}

impl SeededRng {
    pub fn new(seed: u64) -> Self {
        // Avoid zero state (xorshift64 fixpoint).
        let state = if seed == 0 { 1 } else { seed };
        Self { state }
    }

    /// Return the next pseudorandom u64.
    pub fn next_u64(&mut self) -> u64 {
        // xorshift64
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Return a pseudorandom f64 in [0.0, 1.0).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Return a pseudorandom u64 in [0, upper_exclusive).
    pub fn next_range(&mut self, upper_exclusive: u64) -> u64 {
        self.next_u64() % upper_exclusive
    }

    /// Return true with the given probability.
    pub fn chance(&mut self, probability: f64) -> bool {
        self.next_f64() < probability
    }
}
