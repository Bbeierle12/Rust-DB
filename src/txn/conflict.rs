//! OCC (Optimistic Concurrency Control) write-write conflict detection.
//!
//! At commit time, validates that no key in the transaction's write set
//! has been written by another committed transaction since our snapshot.
//! This implements snapshot isolation: first committer wins.

use std::collections::BTreeMap;

use crate::txn::mvcc::MvccStore;

/// Result of conflict validation.
#[derive(Debug, PartialEq, Eq)]
pub enum ValidationResult {
    /// No conflicts — safe to commit.
    Ok,
    /// Write-write conflict detected on the given key.
    Conflict { key: Vec<u8> },
}

/// Validate a transaction's write set against the MVCC store.
///
/// For each key in `write_set`, checks if the store has a committed
/// version with `commit_ts > start_ts`. If so, another transaction
/// wrote to the same key after our snapshot — conflict.
pub fn validate_write_set(
    store: &MvccStore,
    write_set: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    start_ts: u64,
) -> ValidationResult {
    for key in write_set.keys() {
        if store.has_write_after(key, start_ts) {
            return ValidationResult::Conflict { key: key.clone() };
        }
    }
    ValidationResult::Ok
}
