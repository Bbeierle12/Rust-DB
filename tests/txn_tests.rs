//! Stage 3 tests: ACID transactions with MVCC snapshot isolation and OCC.
//!
//! - Snapshot isolation reads see consistent state
//! - Write-write conflicts detected and aborted
//! - Commit ordering under concurrent transactions
//! - Abort rollback restores prior state
//! - Crash during commit → WAL ensures atomicity
//! - Randomized interleaving of N concurrent transactions (bus-controlled)

mod helpers;

use helpers::Collector;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::txn::manager::TransactionManager;
use rust_dst_db::wal::reader::WalReader;
use rust_dst_db::wal::writer::WalWriter;

const DISK_ID: ActorId = ActorId(1);
const WAL_WRITER_ID: ActorId = ActorId(2);
const WAL_READER_ID: ActorId = ActorId(3);
const TXN_MGR_ID: ActorId = ActorId(4);
const CLIENT_ID: ActorId = ActorId(5);
/// A second client for concurrent transaction tests.
const CLIENT2_ID: ActorId = ActorId(6);

const WAL_FILE_ID: u64 = 0;

fn setup_txn(seed: u64, fault_config: FaultConfig) -> MessageBus {
    let injector = FaultInjector::new(fault_config);
    let mut bus = MessageBus::new(seed, injector);

    bus.register(Box::new(SimDisk::new(DISK_ID, false)));
    bus.register(Box::new(WalWriter::new(
        WAL_WRITER_ID,
        DISK_ID,
        TXN_MGR_ID,
        WAL_FILE_ID,
    )));
    bus.register(Box::new(WalReader::new(
        WAL_READER_ID,
        DISK_ID,
        CLIENT_ID,
        WAL_FILE_ID,
    )));
    bus.register(Box::new(TransactionManager::new(TXN_MGR_ID, WAL_WRITER_ID)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));
    bus.register(Box::new(Collector::new(CLIENT2_ID)));

    bus
}

fn setup_default(seed: u64) -> MessageBus {
    setup_txn(seed, FaultConfig::none())
}

/// Helper: extract a specific message type from a collector.
fn get_txn_id(client: &Collector) -> u64 {
    for msg in &client.received {
        if let Message::TxnBeginOk { txn_id } = msg {
            return *txn_id;
        }
    }
    panic!("No TxnBeginOk found");
}

fn get_last_txn_id(client: &Collector) -> u64 {
    for msg in client.received.iter().rev() {
        if let Message::TxnBeginOk { txn_id } = msg {
            return *txn_id;
        }
    }
    panic!("No TxnBeginOk found");
}

// ═══════════════════════════════════════════════════════════════════════
// Basic transaction operations
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn txn_begin_and_commit_empty() {
    let mut bus = setup_default(42);

    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let txn_id = get_txn_id(client);

    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id }, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    assert!(client.received.iter().any(|m| matches!(m, Message::TxnCommitOk { .. })));
}

#[test]
fn txn_put_get_commit() {
    let mut bus = setup_default(42);

    // Begin.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let txn_id = get_txn_id(client);

    // Put.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id,
            key: b"key1".to_vec(),
            value: b"val1".to_vec(),
        },
        0,
    );
    bus.run(100);

    // Read-your-writes: should see the buffered value.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id,
            key: b"key1".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let get_result = client
        .received
        .iter()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match get_result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(value, &Some(b"val1".to_vec()));
        }
        other => panic!("Expected TxnGetResult, got {:?}", other),
    }

    // Commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id }, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    assert!(client.received.iter().any(|m| matches!(m, Message::TxnCommitOk { .. })));

    // New transaction should see the committed value.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let txn2_id = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id: txn2_id,
            key: b"key1".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let get_result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match get_result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(value, &Some(b"val1".to_vec()));
        }
        other => panic!("Expected committed value, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Snapshot isolation — reads see consistent state
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn snapshot_isolation_reads_consistent_state() {
    let mut bus = setup_default(42);

    // T1: write key1=v1 and commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"key1".to_vec(),
            value: b"v1".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(200);

    // T2: begin (snapshot AFTER t1 commit).
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t2 = get_last_txn_id(client);

    // T3: write key1=v2 and commit AFTER t2 begins.
    bus.send(CLIENT2_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    let t3 = get_txn_id(client2);

    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t3,
            key: b"key1".to_vec(),
            value: b"v2".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnCommit { txn_id: t3 },
        0,
    );
    bus.run(200);

    // T2 should still see v1 (its snapshot is before T3's commit).
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id: t2,
            key: b"key1".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let get_result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match get_result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(
                value,
                &Some(b"v1".to_vec()),
                "T2 should see v1 (snapshot isolation), not v2"
            );
        }
        other => panic!("Expected v1, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Write-write conflicts detected and aborted
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn write_write_conflict_detected() {
    let mut bus = setup_default(42);

    // Pre-populate: T0 writes key1=v0.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t0 = get_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t0,
            key: b"key1".to_vec(),
            value: b"v0".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t0 }, 0);
    bus.run(200);

    // T1 and T2 both begin.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.send(CLIENT2_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_last_txn_id(client);
    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    let t2 = get_last_txn_id(client2);

    // Both write to the same key.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"key1".to_vec(),
            value: b"v1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: b"key1".to_vec(),
            value: b"v2".to_vec(),
        },
        0,
    );
    bus.run(100);

    // T1 commits first — should succeed.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    assert!(
        client
            .received
            .iter()
            .any(|m| matches!(m, Message::TxnCommitOk { txn_id } if *txn_id == t1)),
        "T1 should commit successfully"
    );

    // T2 commits second — should fail (write-write conflict).
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnCommit { txn_id: t2 },
        0,
    );
    bus.run(200);

    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    let conflict = client2
        .received
        .iter()
        .find(|m| matches!(m, Message::TxnCommitErr { .. }));
    match conflict {
        Some(Message::TxnCommitErr { txn_id, reason }) => {
            assert_eq!(*txn_id, t2);
            assert!(
                reason.contains("conflict"),
                "Reason should mention conflict: {}",
                reason
            );
        }
        other => panic!("Expected TxnCommitErr for T2, got {:?}", other),
    }
}

#[test]
fn no_conflict_on_different_keys() {
    let mut bus = setup_default(42);

    // T1 and T2 both begin.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.send(CLIENT2_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);
    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    let t2 = get_txn_id(client2);

    // T1 writes key_a, T2 writes key_b — no conflict.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"key_a".to_vec(),
            value: b"v1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: b"key_b".to_vec(),
            value: b"v2".to_vec(),
        },
        0,
    );
    bus.run(100);

    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnCommit { txn_id: t2 },
        0,
    );
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    assert!(client
        .received
        .iter()
        .any(|m| matches!(m, Message::TxnCommitOk { txn_id } if *txn_id == t1)));

    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    assert!(client2
        .received
        .iter()
        .any(|m| matches!(m, Message::TxnCommitOk { txn_id } if *txn_id == t2)));
}

// ═══════════════════════════════════════════════════════════════════════
// Commit ordering under concurrent transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn commit_ordering_first_committer_wins() {
    let mut bus = setup_default(42);

    // Both begin at the same time, write same key.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.send(CLIENT2_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);
    let client2 = bus.actor::<Collector>(CLIENT2_ID).unwrap();
    let t2 = get_txn_id(client2);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"x".to_vec(),
            value: b"from_t1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: b"x".to_vec(),
            value: b"from_t2".to_vec(),
        },
        0,
    );
    bus.run(100);

    // T1 commits first.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(200);

    // T2 tries to commit — conflict.
    bus.send(
        CLIENT2_ID,
        TXN_MGR_ID,
        Message::TxnCommit { txn_id: t2 },
        0,
    );
    bus.run(200);

    // Verify T1's value won.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t3 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id: t3,
            key: b"x".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let get_result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match get_result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(value, &Some(b"from_t1".to_vec()), "First committer wins");
        }
        other => panic!("Expected from_t1, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Abort rollback restores prior state
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn abort_rollback_restores_state() {
    let mut bus = setup_default(42);

    // Commit initial value.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"key1".to_vec(),
            value: b"original".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(200);

    // T2: write and then abort.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t2 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: b"key1".to_vec(),
            value: b"should_be_discarded".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnAbort { txn_id: t2 }, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    assert!(client
        .received
        .iter()
        .any(|m| matches!(m, Message::TxnAbortOk { .. })));

    // T3: read should see original value.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t3 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id: t3,
            key: b"key1".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(
                value,
                &Some(b"original".to_vec()),
                "Aborted write should not be visible"
            );
        }
        other => panic!("Expected original, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction scan
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn txn_scan_sees_committed_and_buffered() {
    let mut bus = setup_default(42);

    // T1: commit some data.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);

    for i in 0u8..5 {
        bus.send(
            CLIENT_ID,
            TXN_MGR_ID,
            Message::TxnPut {
                txn_id: t1,
                key: vec![i],
                value: vec![i * 10],
            },
            0,
        );
    }
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(300);

    // T2: add a new key + overwrite one + delete one, then scan.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t2 = get_last_txn_id(client);

    // Add key 10.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: vec![10],
            value: vec![100],
        },
        0,
    );
    // Overwrite key 2.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: vec![2],
            value: vec![222],
        },
        0,
    );
    // Delete key 4.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnDelete {
            txn_id: t2,
            key: vec![4],
        },
        0,
    );
    bus.run(100);

    // Scan all.
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnScan {
            txn_id: t2,
            start: None,
            end: None,
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let scan = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnScanResult { .. }));

    if let Some(Message::TxnScanResult { entries, .. }) = scan {
        // Should see keys: 0, 1, 2(overwritten), 3, 10(new). Key 4 deleted.
        let keys: Vec<u8> = entries.iter().map(|(k, _)| k[0]).collect();
        assert_eq!(keys, vec![0, 1, 2, 3, 10]);

        // Key 2 should have overwritten value.
        let val_2 = entries.iter().find(|(k, _)| k[0] == 2).unwrap();
        assert_eq!(val_2.1, vec![222]);
    } else {
        panic!("Expected TxnScanResult");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Crash during commit → WAL ensures atomicity
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn crash_recovery_via_wal_replay() {
    let mut bus = setup_default(42);

    // T1: write and commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"a".to_vec(),
            value: b"1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"b".to_vec(),
            value: b"2".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(500);

    // T2: write and commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t2 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t2,
            key: b"c".to_vec(),
            value: b"3".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t2 }, 0);
    bus.run(500);

    // Read WAL.
    bus.send(CLIENT_ID, WAL_READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records = match client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::WalRecords { .. }))
    {
        Some(Message::WalRecords { records }) => records.clone(),
        _ => panic!("Expected WAL records"),
    };

    assert!(records.len() >= 2, "Should have at least 2 WAL records (2 commits)");

    // "Crash" — create fresh TransactionManager and replay WAL.
    let mut fresh_mgr = TransactionManager::new(TXN_MGR_ID, WAL_WRITER_ID);
    fresh_mgr.replay_wal(&records);

    // Set up bus for verification.
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus2 = MessageBus::new(42, injector);
    bus2.register(Box::new(fresh_mgr));
    bus2.register(Box::new(Collector::new(CLIENT_ID)));

    // Begin a new txn and read all keys.
    bus2.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus2.run(100);
    let client = bus2.actor::<Collector>(CLIENT_ID).unwrap();
    let t3 = get_txn_id(client);

    for key in [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()] {
        bus2.send(
            CLIENT_ID,
            TXN_MGR_ID,
            Message::TxnGet {
                txn_id: t3,
                key,
            },
            0,
        );
    }
    bus2.run(100);

    let client = bus2.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client
        .received
        .iter()
        .filter_map(|m| {
            if let Message::TxnGetResult { key, value, .. } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), 3);
    for (key, value) in &gets {
        let expected = match key.as_slice() {
            b"a" => b"1".to_vec(),
            b"b" => b"2".to_vec(),
            b"c" => b"3".to_vec(),
            _ => panic!("Unexpected key"),
        };
        assert_eq!(
            value,
            &Some(expected),
            "Key {:?} not recovered correctly",
            std::str::from_utf8(key).unwrap()
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Randomized interleaving of N concurrent transactions
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn randomized_concurrent_transactions() {
    // Run many concurrent transactions with deterministic interleaving
    // controlled by the bus. Verify consistency after all commit/abort.
    fn run_scenario(seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let injector = FaultInjector::new(FaultConfig::none());
        let mut bus = MessageBus::new(seed, injector);

        bus.register(Box::new(SimDisk::new(DISK_ID, false)));
        bus.register(Box::new(WalWriter::new(
            WAL_WRITER_ID,
            DISK_ID,
            TXN_MGR_ID,
            WAL_FILE_ID,
        )));
        bus.register(Box::new(TransactionManager::new(TXN_MGR_ID, WAL_WRITER_ID)));

        // Create 5 client collectors.
        let client_ids: Vec<ActorId> = (10..15).map(ActorId).collect();
        for &cid in &client_ids {
            bus.register(Box::new(Collector::new(cid)));
        }

        // Each client begins a transaction.
        for &cid in &client_ids {
            bus.send(cid, TXN_MGR_ID, Message::TxnBegin, 0);
        }
        bus.run(200);

        // Collect txn IDs.
        let txn_ids: Vec<u64> = client_ids
            .iter()
            .map(|&cid| {
                let c = bus.actor::<Collector>(cid).unwrap();
                get_txn_id(c)
            })
            .collect();

        // Each writes to its own key and to a shared key "shared".
        for (i, (&cid, &tid)) in client_ids.iter().zip(txn_ids.iter()).enumerate() {
            // Own key.
            bus.send(
                cid,
                TXN_MGR_ID,
                Message::TxnPut {
                    txn_id: tid,
                    key: format!("key_{}", i).into_bytes(),
                    value: format!("val_{}", i).into_bytes(),
                },
                0,
            );
            // Shared key — only one will win.
            bus.send(
                cid,
                TXN_MGR_ID,
                Message::TxnPut {
                    txn_id: tid,
                    key: b"shared".to_vec(),
                    value: format!("from_{}", i).into_bytes(),
                },
                0,
            );
        }
        bus.run(300);

        // All try to commit.
        for (&cid, &tid) in client_ids.iter().zip(txn_ids.iter()) {
            bus.send(cid, TXN_MGR_ID, Message::TxnCommit { txn_id: tid }, 0);
        }
        bus.run(500);

        // Count successes and failures.
        let mut commits = 0;
        let mut aborts = 0;
        for &cid in &client_ids {
            let c = bus.actor::<Collector>(cid).unwrap();
            if c.received.iter().any(|m| matches!(m, Message::TxnCommitOk { .. })) {
                commits += 1;
            }
            if c.received.iter().any(|m| matches!(m, Message::TxnCommitErr { .. })) {
                aborts += 1;
            }
        }

        // Exactly one should win the shared key, rest conflict.
        // Own keys don't conflict, so at least 1 commit.
        assert!(commits >= 1, "At least one transaction should commit");
        assert_eq!(commits + aborts, 5, "All txns should have resolved");

        // Read final state.
        let reader_id = ActorId(99);
        bus.register(Box::new(Collector::new(reader_id)));

        bus.send(reader_id, TXN_MGR_ID, Message::TxnBegin, 0);
        bus.run(100);
        let reader = bus.actor::<Collector>(reader_id).unwrap();
        let read_txn = get_txn_id(reader);

        bus.send(
            reader_id,
            TXN_MGR_ID,
            Message::TxnScan {
                txn_id: read_txn,
                start: None,
                end: None,
            },
            0,
        );
        bus.run(100);

        let reader = bus.actor::<Collector>(reader_id).unwrap();
        if let Some(Message::TxnScanResult { entries, .. }) = reader
            .received
            .iter()
            .rev()
            .find(|m| matches!(m, Message::TxnScanResult { .. }))
        {
            entries.clone()
        } else {
            panic!("Expected scan result");
        }
    }

    // Run twice with same seed — must produce identical results.
    let result_a = run_scenario(42);
    let result_b = run_scenario(42);
    assert_eq!(
        result_a, result_b,
        "Same seed must produce identical transaction outcomes"
    );

    // The "shared" key should exist exactly once.
    let shared_entries: Vec<_> = result_a
        .iter()
        .filter(|(k, _)| k == b"shared")
        .collect();
    assert_eq!(shared_entries.len(), 1, "Exactly one writer should win 'shared'");

    // Run with different seed — may produce different outcomes.
    let result_c = run_scenario(999);
    // Both are valid, but the shared key winner may differ.
    let shared_c: Vec<_> = result_c
        .iter()
        .filter(|(k, _)| k == b"shared")
        .collect();
    assert_eq!(shared_c.len(), 1, "Exactly one winner for shared key");
}

// ═══════════════════════════════════════════════════════════════════════
// Transaction delete with MVCC
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn txn_delete_makes_key_invisible() {
    let mut bus = setup_default(42);

    // T1: write and commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t1 = get_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnPut {
            txn_id: t1,
            key: b"doomed".to_vec(),
            value: b"exists".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
    bus.run(300);

    // T2: delete and commit.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t2 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnDelete {
            txn_id: t2,
            key: b"doomed".to_vec(),
        },
        0,
    );
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t2 }, 0);
    bus.run(300);

    // T3: should not see the deleted key.
    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
    bus.run(100);
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let t3 = get_last_txn_id(client);

    bus.send(
        CLIENT_ID,
        TXN_MGR_ID,
        Message::TxnGet {
            txn_id: t3,
            key: b"doomed".to_vec(),
        },
        0,
    );
    bus.run(100);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::TxnGetResult { .. }));
    match result {
        Some(Message::TxnGetResult { value, .. }) => {
            assert_eq!(value, &None, "Deleted key should be invisible");
        }
        other => panic!("Expected None, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Deterministic replay
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn txn_deterministic_replay() {
    fn run_txn_scenario(seed: u64) -> Vec<String> {
        let mut bus = setup_default(seed);

        bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, 0);
        bus.run(100);
        let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
        let t1 = get_txn_id(client);

        for i in 0u8..5 {
            bus.send(
                CLIENT_ID,
                TXN_MGR_ID,
                Message::TxnPut {
                    txn_id: t1,
                    key: vec![i],
                    value: vec![i],
                },
                0,
            );
        }
        bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id: t1 }, 0);
        bus.run(500);

        bus.trace()
            .iter()
            .map(|(tick, from, to, msg)| format!("{}:{}->{}:{}", tick, from, to, msg))
            .collect()
    }

    let trace_a = run_txn_scenario(777);
    let trace_b = run_txn_scenario(777);
    assert_eq!(
        trace_a, trace_b,
        "Transaction operations must replay deterministically"
    );
}
