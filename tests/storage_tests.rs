//! Stage 2 tests: B-tree CRUD, splitting, scan, buffer pool eviction,
//! crash recovery via WAL, and fault injection.

mod helpers;

use helpers::Collector;
use rust_dst_db::config;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::storage::btree::BTreeEngine;
use rust_dst_db::storage::buffer_pool::BufferPool;
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::wal::reader::WalReader;
use rust_dst_db::wal::writer::WalWriter;

// Actor IDs.
const DISK_ID: ActorId = ActorId(1);
const WAL_WRITER_ID: ActorId = ActorId(2);
const WAL_READER_ID: ActorId = ActorId(3);
const BUF_POOL_ID: ActorId = ActorId(4);
const BTREE_ID: ActorId = ActorId(5);
const CLIENT_ID: ActorId = ActorId(6);

const WAL_FILE_ID: u64 = 0;

/// Set up a full storage stack: disk, WAL, buffer pool, B-tree, client.
fn setup_storage(
    seed: u64,
    fault_config: FaultConfig,
    max_leaf: usize,
    max_internal: usize,
    buf_pool_capacity: usize,
) -> MessageBus {
    let injector = FaultInjector::new(fault_config);
    let mut bus = MessageBus::new(seed, injector);

    bus.register(Box::new(SimDisk::new(DISK_ID, false)));
    bus.register(Box::new(WalWriter::new(
        WAL_WRITER_ID,
        DISK_ID,
        BTREE_ID,
        WAL_FILE_ID,
    )));
    bus.register(Box::new(WalReader::new(
        WAL_READER_ID,
        DISK_ID,
        CLIENT_ID,
        WAL_FILE_ID,
    )));
    bus.register(Box::new(BufferPool::new(
        BUF_POOL_ID,
        DISK_ID,
        config::BTREE_DATA_FILE_ID,
        buf_pool_capacity,
    )));
    bus.register(Box::new(BTreeEngine::with_limits(
        BTREE_ID,
        BUF_POOL_ID,
        WAL_WRITER_ID,
        max_leaf,
        max_internal,
    )));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    bus
}

/// Default setup with small node limits for easy split testing.
fn setup_default(seed: u64) -> MessageBus {
    setup_storage(seed, FaultConfig::none(), 4, 4, 64)
}

// ═══════════════════════════════════════════════════════════════════════
// Basic CRUD
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_put_and_get() {
    let mut bus = setup_default(42);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreePut {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        },
        0,
    );
    bus.run(500);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeGet {
            key: b"hello".to_vec(),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .find(|m| matches!(m, Message::BTreeGetResult { .. }));

    match result {
        Some(Message::BTreeGetResult { key, value }) => {
            assert_eq!(key, b"hello");
            assert_eq!(value, &Some(b"world".to_vec()));
        }
        other => panic!("Expected BTreeGetResult, got {:?}", other),
    }
}

#[test]
fn btree_get_missing_key() {
    let mut bus = setup_default(42);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeGet {
            key: b"nonexistent".to_vec(),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .find(|m| matches!(m, Message::BTreeGetResult { .. }));

    match result {
        Some(Message::BTreeGetResult { value, .. }) => {
            assert_eq!(value, &None);
        }
        other => panic!("Expected BTreeGetResult with None, got {:?}", other),
    }
}

#[test]
fn btree_put_overwrite() {
    let mut bus = setup_default(42);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreePut {
            key: b"key".to_vec(),
            value: b"v1".to_vec(),
        },
        0,
    );
    bus.run(500);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreePut {
            key: b"key".to_vec(),
            value: b"v2".to_vec(),
        },
        0,
    );
    bus.run(500);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeGet {
            key: b"key".to_vec(),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::BTreeGetResult { .. }));

    match result {
        Some(Message::BTreeGetResult { value, .. }) => {
            assert_eq!(value, &Some(b"v2".to_vec()));
        }
        other => panic!("Expected v2, got {:?}", other),
    }
}

#[test]
fn btree_delete() {
    let mut bus = setup_default(42);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreePut {
            key: b"key".to_vec(),
            value: b"val".to_vec(),
        },
        0,
    );
    bus.run(500);

    // Delete it.
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeDelete {
            key: b"key".to_vec(),
        },
        0,
    );
    bus.run(500);

    // Check it's gone.
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let del_result = client
        .received
        .iter()
        .find(|m| matches!(m, Message::BTreeDeleteOk { .. }));
    match del_result {
        Some(Message::BTreeDeleteOk { found }) => assert!(found),
        other => panic!("Expected BTreeDeleteOk, got {:?}", other),
    }

    // Get should return None.
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeGet {
            key: b"key".to_vec(),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let get_result = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::BTreeGetResult { .. }));
    match get_result {
        Some(Message::BTreeGetResult { value, .. }) => {
            assert_eq!(value, &None, "Deleted key should return None");
        }
        other => panic!("Expected None, got {:?}", other),
    }
}

#[test]
fn btree_delete_missing() {
    let mut bus = setup_default(42);

    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeDelete {
            key: b"nope".to_vec(),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let result = client
        .received
        .iter()
        .find(|m| matches!(m, Message::BTreeDeleteOk { .. }));
    match result {
        Some(Message::BTreeDeleteOk { found }) => assert!(!found),
        other => panic!("Expected BTreeDeleteOk {{ found: false }}, got {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Leaf and internal node splitting
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_leaf_split() {
    // max_leaf=4: inserting 5 entries triggers a split.
    let mut bus = setup_default(42);

    for i in 0u8..5 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i * 10],
            },
            0,
        );
    }
    bus.run(1000);

    // Verify all 5 entries are retrievable.
    for i in 0u8..5 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: vec![i] },
            0,
        );
    }
    bus.run(1000);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), 5);
    for (i, (key, value)) in gets.iter().enumerate() {
        assert_eq!(key, &vec![i as u8]);
        assert_eq!(value, &Some(vec![i as u8 * 10]));
    }

    // The tree should have more than 1 node (split occurred).
    let btree = bus.actor::<BTreeEngine>(BTREE_ID).unwrap();
    assert!(btree.node_count() > 1, "Split should have created new nodes");
}

#[test]
fn btree_multi_level_split() {
    // max_leaf=4, max_internal=4: insert enough keys to trigger internal splits.
    let mut bus = setup_default(42);

    // Insert 20 keys — should create multiple levels.
    for i in 0u8..20 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i],
            },
            0,
        );
    }
    bus.run(2000);

    // Verify all 20 entries.
    for i in 0u8..20 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: vec![i] },
            0,
        );
    }
    bus.run(2000);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), 20, "Should have 20 get results");
    for (key, value) in &gets {
        assert!(value.is_some(), "Key {:?} should have a value", key);
        assert_eq!(key, value.as_ref().unwrap());
    }

    let btree = bus.actor::<BTreeEngine>(BTREE_ID).unwrap();
    assert!(
        btree.node_count() > 5,
        "20 keys with max_leaf=4 should create many nodes, got {}",
        btree.node_count()
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Scan ranges
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_scan_all() {
    let mut bus = setup_default(42);

    for i in 0u8..10 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i],
            },
            0,
        );
    }
    bus.run(2000);

    // Unbounded scan.
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeScan {
            start: None,
            end: None,
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let scan = client
        .received
        .iter()
        .find(|m| matches!(m, Message::BTreeScanResult { .. }));

    if let Some(Message::BTreeScanResult { entries }) = scan {
        assert_eq!(entries.len(), 10);
        // Should be in sorted order.
        for (i, (key, val)) in entries.iter().enumerate() {
            assert_eq!(key, &vec![i as u8]);
            assert_eq!(val, &vec![i as u8]);
        }
    } else {
        panic!("Expected BTreeScanResult");
    }
}

#[test]
fn btree_scan_range() {
    let mut bus = setup_default(42);

    for i in 0u8..10 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i],
            },
            0,
        );
    }
    bus.run(2000);

    // Scan [3, 7).
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeScan {
            start: Some(vec![3]),
            end: Some(vec![7]),
        },
        0,
    );
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let scan = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::BTreeScanResult { .. }));

    if let Some(Message::BTreeScanResult { entries }) = scan {
        let keys: Vec<u8> = entries.iter().map(|(k, _)| k[0]).collect();
        assert_eq!(keys, vec![3, 4, 5, 6]);
    } else {
        panic!("Expected BTreeScanResult");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Buffer pool eviction under memory pressure
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn buffer_pool_eviction() {
    // buf_pool_capacity=4: inserting more pages than capacity triggers eviction.
    let mut bus = setup_storage(42, FaultConfig::none(), 4, 4, 4);

    // Insert enough keys to create more than 4 pages.
    for i in 0u8..20 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i; 10],
            },
            0,
        );
    }
    bus.run(3000);

    // All keys should still be retrievable (they're in the B-tree's in-memory state).
    for i in 0u8..20 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: vec![i] },
            0,
        );
    }
    bus.run(2000);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), 20);
    for (key, value) in &gets {
        assert!(value.is_some(), "Key {:?} should be found despite eviction", key);
    }

    // Buffer pool should have at most 4 pages cached.
    let pool = bus.actor::<BufferPool>(BUF_POOL_ID).unwrap();
    assert!(
        pool.cached_count() <= 4,
        "Pool should respect capacity: {} cached",
        pool.cached_count()
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Crash recovery via WAL
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_crash_recovery_via_wal() {
    // 1. Insert keys into the B-tree (WAL records are written).
    // 2. "Crash" — create a fresh B-tree.
    // 3. Read WAL records and replay them.
    // 4. Verify all keys are recovered.

    // Phase 1: Write data.
    let mut bus = setup_default(42);

    let test_data: Vec<(Vec<u8>, Vec<u8>)> = (0u8..8)
        .map(|i| (vec![i], vec![i * 10]))
        .collect();

    for (key, value) in &test_data {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: key.clone(),
                value: value.clone(),
            },
            0,
        );
    }
    bus.run(2000);

    // Phase 2: Read WAL records (simulates reading the WAL after crash).
    bus.send(CLIENT_ID, WAL_READER_ID, Message::WalReadAll, 0);
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let wal_records = client
        .received
        .iter()
        .find(|m| matches!(m, Message::WalRecords { .. }));

    let records = match wal_records {
        Some(Message::WalRecords { records }) => records.clone(),
        _ => panic!("Should have received WAL records"),
    };

    assert_eq!(records.len(), 8, "Should have 8 WAL records");

    // Phase 3: Create fresh B-tree and replay WAL.
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus2 = MessageBus::new(42, injector);

    bus2.register(Box::new(SimDisk::new(DISK_ID, false)));
    bus2.register(Box::new(BufferPool::new(
        BUF_POOL_ID,
        DISK_ID,
        config::BTREE_DATA_FILE_ID,
        64,
    )));
    let mut fresh_btree = BTreeEngine::with_limits(BTREE_ID, BUF_POOL_ID, WAL_WRITER_ID, 4, 4);
    fresh_btree.replay_wal(&records);
    bus2.register(Box::new(fresh_btree));
    bus2.register(Box::new(Collector::new(CLIENT_ID)));

    // Phase 4: Verify all keys recovered.
    for (key, _) in &test_data {
        bus2.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: key.clone() },
            0,
        );
    }
    bus2.run(1000);

    let client2 = bus2.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client2
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), test_data.len());
    for (key, value) in &gets {
        let expected_val = test_data
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone());
        assert_eq!(value, &expected_val, "Recovered key {:?} mismatch", key);
    }
}

#[test]
fn btree_crash_recovery_with_deletes() {
    // Insert some keys, delete some, verify recovery replays both.
    let mut bus = setup_default(42);

    // Insert keys 0-5.
    for i in 0u8..6 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i],
            },
            0,
        );
    }
    bus.run(2000);

    // Delete keys 2 and 4.
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeDelete { key: vec![2] },
        0,
    );
    bus.send(
        CLIENT_ID,
        BTREE_ID,
        Message::BTreeDelete { key: vec![4] },
        0,
    );
    bus.run(1000);

    // Read WAL.
    bus.send(CLIENT_ID, WAL_READER_ID, Message::WalReadAll, 0);
    bus.run(500);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records = match client
        .received
        .iter()
        .find(|m| matches!(m, Message::WalRecords { .. }))
    {
        Some(Message::WalRecords { records }) => records.clone(),
        _ => panic!("Expected WAL records"),
    };

    // Replay on fresh tree.
    let mut fresh = BTreeEngine::with_limits(BTREE_ID, BUF_POOL_ID, WAL_WRITER_ID, 4, 4);
    fresh.replay_wal(&records);

    // Set up bus for verification.
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus2 = MessageBus::new(42, injector);
    bus2.register(Box::new(fresh));
    bus2.register(Box::new(Collector::new(CLIENT_ID)));

    // Check all keys.
    for i in 0u8..6 {
        bus2.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: vec![i] },
            0,
        );
    }
    bus2.run(500);

    let client2 = bus2.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client2
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key[0], value.clone()))
            } else {
                None
            }
        })
        .collect();

    // Keys 0,1,3,5 should exist; 2,4 should be deleted.
    for (k, v) in &gets {
        if *k == 2 || *k == 4 {
            assert_eq!(v, &None, "Key {} should be deleted", k);
        } else {
            assert_eq!(v, &Some(vec![*k]), "Key {} should exist", k);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Random IO failures during operations (fault injection)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_survives_io_faults() {
    // The B-tree operates in-memory, so disk failures affect persistence
    // but not correctness of the in-memory state.
    let config = FaultConfig {
        disk_write_error_probability: 0.1,
        disk_read_error_probability: 0.1,
        ..FaultConfig::none()
    };

    let mut bus = setup_storage(42, config, 4, 4, 64);

    // Insert keys — some disk writes will fail, but in-memory tree is fine.
    for i in 0u8..15 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreePut {
                key: vec![i],
                value: vec![i],
            },
            0,
        );
    }
    bus.run(3000);

    // All keys should be in the in-memory tree.
    for i in 0u8..15 {
        bus.send(
            CLIENT_ID,
            BTREE_ID,
            Message::BTreeGet { key: vec![i] },
            0,
        );
    }
    bus.run(2000);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let gets: Vec<_> = client
        .received
        .iter()
        .filter_map(|m| {
            if let Message::BTreeGetResult { key, value } = m {
                Some((key.clone(), value.clone()))
            } else {
                None
            }
        })
        .collect();

    assert_eq!(gets.len(), 15);
    for (key, value) in &gets {
        assert!(value.is_some(), "Key {:?} should exist despite IO faults", key);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Deterministic replay
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn btree_deterministic_replay() {
    fn run_scenario(seed: u64) -> Vec<String> {
        let mut bus = setup_default(seed);

        for i in 0u8..10 {
            bus.send(
                CLIENT_ID,
                BTREE_ID,
                Message::BTreePut {
                    key: vec![i],
                    value: vec![i],
                },
                0,
            );
        }
        bus.run(2000);

        bus.trace()
            .iter()
            .map(|(tick, from, to, msg)| format!("{}:{}->{}:{}", tick, from, to, msg))
            .collect()
    }

    let trace_a = run_scenario(999);
    let trace_b = run_scenario(999);
    assert_eq!(
        trace_a, trace_b,
        "B-tree operations must replay deterministically"
    );
}
