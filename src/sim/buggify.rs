// BUGGIFY — cooperative fault injection for production code paths.
// Inspired by FoundationDB's BUGGIFY macro.
// In simulation (when enabled), returns true ~5% of the time.
// In production (disabled), always returns false.
// Thread-local so tests can toggle independently.

use std::cell::Cell;

thread_local! {
    /// Whether BUGGIFY is active in the current thread.
    static BUGGIFY_ENABLED: Cell<bool> = const { Cell::new(false) };
    /// Invocation counter used as a deterministic seed supplement.
    static BUGGIFY_COUNTER: Cell<u64> = const { Cell::new(0) };
    /// Seed set externally (by the simulation harness).
    static BUGGIFY_SEED: Cell<u64> = const { Cell::new(0) };
}

/// Enable or disable BUGGIFY for the current thread.
pub fn set_buggify_enabled(enabled: bool) {
    BUGGIFY_ENABLED.with(|b| b.set(enabled));
    if enabled {
        BUGGIFY_COUNTER.with(|c| c.set(0));
    }
}

/// Set the seed used by BUGGIFY for this thread.
pub fn set_buggify_seed(seed: u64) {
    BUGGIFY_SEED.with(|s| s.set(seed));
}

/// Check whether BUGGIFY is currently enabled for this thread.
pub fn buggify_enabled() -> bool {
    BUGGIFY_ENABLED.with(|b| b.get())
}

/// Returns true with ~5% probability (deterministic, driven by seed + counter).
/// Always returns false when BUGGIFY is disabled.
pub fn buggify() -> bool {
    if !buggify_enabled() {
        return false;
    }

    let counter = BUGGIFY_COUNTER.with(|c| {
        let v = c.get();
        c.set(v.wrapping_add(1));
        v
    });

    let seed = BUGGIFY_SEED.with(|s| s.get());

    // xorshift64 mix of (seed XOR counter).
    let mut x = seed ^ (counter.wrapping_mul(0x9e3779b97f4a7c15));
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;

    // ~5% probability.
    x % 20 == 0
}

/// Run a closure with BUGGIFY enabled for this thread, then restore state.
pub fn with_buggify<F, R>(seed: u64, f: F) -> R
where
    F: FnOnce() -> R,
{
    let was_enabled = buggify_enabled();
    set_buggify_seed(seed);
    set_buggify_enabled(true);
    let result = f();
    set_buggify_enabled(was_enabled);
    result
}
