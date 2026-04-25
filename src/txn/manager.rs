use std::collections::BTreeMap;

use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;
use crate::txn::conflict::{self, ValidationResult};
use crate::txn::mvcc::MvccStore;

/// WAL record types for transaction recovery.
const WAL_TXN_COMMIT: u8 = 10;

/// Decoded commit-record writes: list of (key, optional value) pairs.
type CommitWrites = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// State of an active transaction.
#[derive(Debug)]
struct ActiveTxn {
    start_ts: u64,
    /// Buffered writes: key → Some(value) for puts, None for deletes.
    write_set: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

/// Transaction Manager state machine.
///
/// Manages transaction lifecycle with MVCC snapshot isolation and OCC
/// (optimistic concurrency control). Each transaction:
///
/// 1. **Begin**: Assigns a transaction ID and snapshot timestamp.
/// 2. **Read**: Returns the version visible at the snapshot (from MVCC store
///    or the transaction's own write set).
/// 3. **Write**: Buffers puts/deletes in the transaction's write set.
/// 4. **Commit**: Validates no write-write conflicts (OCC), applies the
///    write set to the MVCC store, logs to WAL.
/// 5. **Abort**: Discards the write set.
///
/// The bus controls interleaving of concurrent transactions, making this
/// the highest-value DST stage — subtle concurrency bugs surface through
/// deterministic replay with different seeds.
pub struct TransactionManager {
    id: ActorId,
    wal_actor: ActorId,
    /// MVCC versioned key-value store.
    store: MvccStore,
    /// Active transactions: txn_id → state.
    active: BTreeMap<u64, ActiveTxn>,
    /// Monotonically increasing timestamp for both txn IDs and commit timestamps.
    next_ts: u64,
}

impl TransactionManager {
    pub fn new(id: ActorId, wal_actor: ActorId) -> Self {
        Self {
            id,
            wal_actor,
            store: MvccStore::new(),
            active: BTreeMap::new(),
            next_ts: 1,
        }
    }

    fn alloc_ts(&mut self) -> u64 {
        let ts = self.next_ts;
        self.next_ts += 1;
        ts
    }

    /// Encode a WAL commit record.
    /// Format: [WAL_TXN_COMMIT:u8][txn_id:u64][commit_ts:u64][write_count:u32]
    ///         [for each write: is_delete:u8, key_len:u32, key, (value_len:u32, value)?]
    fn encode_commit_record(
        txn_id: u64,
        commit_ts: u64,
        write_set: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(WAL_TXN_COMMIT);
        data.extend_from_slice(&txn_id.to_le_bytes());
        data.extend_from_slice(&commit_ts.to_le_bytes());
        data.extend_from_slice(&(write_set.len() as u32).to_le_bytes());

        for (key, value) in write_set {
            match value {
                Some(v) => {
                    data.push(0); // put
                    data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    data.extend_from_slice(key);
                    data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    data.extend_from_slice(v);
                }
                None => {
                    data.push(1); // delete
                    data.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    data.extend_from_slice(key);
                }
            }
        }

        data
    }

    /// Decode a WAL commit record.
    /// Returns (txn_id, commit_ts, writes: Vec<(key, Option<value>)>).
    pub fn decode_commit_record(data: &[u8]) -> Option<(u64, u64, CommitWrites)> {
        if data.is_empty() || data[0] != WAL_TXN_COMMIT {
            return None;
        }
        let mut pos = 1;

        let txn_id = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let commit_ts = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let write_count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;

        let mut writes = Vec::with_capacity(write_count);
        for _ in 0..write_count {
            let is_delete = *data.get(pos)?;
            pos += 1;

            let key_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
            pos += 4;
            let key = data.get(pos..pos + key_len)?.to_vec();
            pos += key_len;

            if is_delete == 0 {
                let val_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let value = data.get(pos..pos + val_len)?.to_vec();
                pos += val_len;
                writes.push((key, Some(value)));
            } else {
                writes.push((key, None));
            }
        }

        Some((txn_id, commit_ts, writes))
    }

    /// Replay WAL records to rebuild MVCC state after crash.
    pub fn replay_wal(&mut self, records: &[(u64, Vec<u8>)]) {
        for (_lsn, data) in records {
            if let Some((_, commit_ts, writes)) = Self::decode_commit_record(data) {
                for (key, value) in writes {
                    self.store.write(key, commit_ts, value);
                }
                // Advance our timestamp past any replayed commit.
                if commit_ts >= self.next_ts {
                    self.next_ts = commit_ts + 1;
                }
            }
        }
    }

    /// Update GC watermark to the minimum start_ts of all active transactions.
    fn update_gc_watermark(&mut self) {
        let min_ts = self
            .active
            .values()
            .map(|txn| txn.start_ts)
            .min()
            .unwrap_or(self.next_ts);
        self.store.update_gc_watermark(min_ts.saturating_sub(1));
    }

    pub fn store(&self) -> &MvccStore {
        &self.store
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

impl StateMachine for TransactionManager {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::TxnBegin => {
                let txn_id = self.alloc_ts();
                let start_ts = txn_id; // snapshot at begin time
                self.active.insert(
                    txn_id,
                    ActiveTxn {
                        start_ts,
                        write_set: BTreeMap::new(),
                    },
                );
                Some(vec![(
                    Message::TxnBeginOk { txn_id },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::TxnGet { txn_id, key } => {
                let value = if let Some(txn) = self.active.get(&txn_id) {
                    // Read-your-writes: check write set first.
                    if let Some(buffered) = txn.write_set.get(&key) {
                        buffered.clone()
                    } else {
                        // Read from MVCC store at snapshot.
                        self.store.read(&key, txn.start_ts)
                    }
                } else {
                    None
                };
                Some(vec![(
                    Message::TxnGetResult { txn_id, key, value },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::TxnPut { txn_id, key, value } => {
                if let Some(txn) = self.active.get_mut(&txn_id) {
                    txn.write_set.insert(key, Some(value));
                }
                Some(vec![(
                    Message::TxnPutOk { txn_id },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::TxnDelete { txn_id, key } => {
                if let Some(txn) = self.active.get_mut(&txn_id) {
                    txn.write_set.insert(key, None);
                }
                Some(vec![(
                    Message::TxnDeleteOk { txn_id },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::TxnCommit { txn_id } => {
                let Some(txn) = self.active.remove(&txn_id) else {
                    return Some(vec![(
                        Message::TxnCommitErr {
                            txn_id,
                            reason: "transaction not found".into(),
                        },
                        Destination {
                            actor: from,
                            delay: 0,
                        },
                    )]);
                };

                // OCC validation: check for write-write conflicts.
                let validation =
                    conflict::validate_write_set(&self.store, &txn.write_set, txn.start_ts);

                match validation {
                    ValidationResult::Conflict { key } => {
                        // Conflict — abort.
                        Some(vec![(
                            Message::TxnCommitErr {
                                txn_id,
                                reason: format!("write-write conflict on key {:?}", key),
                            },
                            Destination {
                                actor: from,
                                delay: 0,
                            },
                        )])
                    }
                    ValidationResult::Ok => {
                        if txn.write_set.is_empty() {
                            // Read-only transaction — commit immediately.
                            self.update_gc_watermark();
                            return Some(vec![(
                                Message::TxnCommitOk { txn_id },
                                Destination {
                                    actor: from,
                                    delay: 0,
                                },
                            )]);
                        }

                        // Allocate a commit timestamp.
                        let commit_ts = self.alloc_ts();

                        // Log to WAL.
                        let wal_data =
                            Self::encode_commit_record(txn_id, commit_ts, &txn.write_set);

                        // Apply writes to MVCC store.
                        for (key, value) in &txn.write_set {
                            self.store.write(key.clone(), commit_ts, value.clone());
                        }

                        self.update_gc_watermark();

                        let mut outgoing = vec![
                            (
                                Message::WalAppend { data: wal_data },
                                Destination {
                                    actor: self.wal_actor,
                                    delay: 0,
                                },
                            ),
                            (
                                Message::TxnCommitOk { txn_id },
                                Destination {
                                    actor: from,
                                    delay: 0,
                                },
                            ),
                        ];

                        let _ = &mut outgoing; // suppress unused warning
                        Some(outgoing)
                    }
                }
            }

            Message::TxnAbort { txn_id } => {
                self.active.remove(&txn_id);
                self.update_gc_watermark();
                Some(vec![(
                    Message::TxnAbortOk { txn_id },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::TxnScan { txn_id, start, end } => {
                let entries = if let Some(txn) = self.active.get(&txn_id) {
                    // Get committed entries visible at snapshot.
                    let mut entries =
                        self.store
                            .scan(start.as_deref(), end.as_deref(), txn.start_ts);

                    // Merge in write set entries (read-your-writes).
                    // Write set overrides committed versions.
                    let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
                    for (k, v) in &entries {
                        merged.insert(k.clone(), Some(v.clone()));
                    }
                    for (k, v) in &txn.write_set {
                        // Check range bounds.
                        if let Some(ref s) = start
                            && k.as_slice() < s.as_slice() {
                                continue;
                            }
                        if let Some(ref e) = end
                            && k.as_slice() >= e.as_slice() {
                                continue;
                            }
                        merged.insert(k.clone(), v.clone());
                    }

                    entries = merged
                        .into_iter()
                        .filter_map(|(k, v)| v.map(|val| (k, val)))
                        .collect();

                    entries
                } else {
                    Vec::new()
                };

                Some(vec![(
                    Message::TxnScanResult { txn_id, entries },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            // Ignore WAL acknowledgments.
            Message::WalAppendOk { .. } => None,

            _ => None,
        }
    }

    fn tick(&mut self, _now: u64) -> Option<Vec<(Message, Destination)>> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
