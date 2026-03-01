//! Stage 6: Determinism Validation — Meta-Tests + BUGGIFY
//!
//! S2's meta-test pattern: rerun the same seed, compare TRACE-level logs
//! byte-for-byte. Any divergence means a determinism bug was introduced.
//! S2 found 17 bugs using this pattern.
//!
//! BUGGIFY tests confirm cooperative fault injection works correctly:
//! always false when disabled, probabilistic when enabled, deterministic
//! given the same seed.

mod helpers;

use helpers::Collector;
use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::buggify::{buggify, buggify_enabled, set_buggify_enabled, set_buggify_seed, with_buggify};
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::txn::manager::TransactionManager;
use rust_dst_db::wal::writer::WalWriter;

const DISK_ID: ActorId = ActorId(1);
const WAL_WRITER_ID: ActorId = ActorId(2);
const TXN_MGR_ID: ActorId = ActorId(3);
const CLIENT_ID: ActorId = ActorId(4);
const WAL_FILE_ID: u64 = 0;

// ═══════════════════════════════════════════════════════════════════════
// BUGGIFY tests
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn buggify_disabled_always_returns_false() {
    set_buggify_enabled(false);

    for _ in 0..1000 {
        assert!(!buggify(), "buggify() must return false when disabled");
    }
}

#[test]
fn buggify_enabled_returns_some_trues() {
    with_buggify(42, || {
        let hits = (0..1000).filter(|_| buggify()).count();
        // With 5% probability, expected ~50 hits. Must have at least some.
        assert!(
            hits > 0,
            "buggify should return true some of the time when enabled"
        );
        // But not too many.
        assert!(
            hits < 200,
            "buggify returning true too often: {} / 1000",
            hits
        );
    });
}

#[test]
fn buggify_same_seed_same_sequence() {
    let seq_a: Vec<bool> = with_buggify(12345, || {
        (0..100).map(|_| buggify()).collect()
    });
    let seq_b: Vec<bool> = with_buggify(12345, || {
        (0..100).map(|_| buggify()).collect()
    });

    assert_eq!(seq_a, seq_b, "Same seed must produce identical BUGGIFY sequence");
}

#[test]
fn buggify_different_seeds_different_sequences() {
    let seq_a: Vec<bool> = with_buggify(111, || (0..100).map(|_| buggify()).collect());
    let seq_b: Vec<bool> = with_buggify(222, || (0..100).map(|_| buggify()).collect());

    // Different seeds must produce different sequences (probabilistically certain).
    assert_ne!(seq_a, seq_b, "Different seeds should yield different sequences");
}

#[test]
fn buggify_disabled_after_with_buggify() {
    assert!(!buggify_enabled(), "Should start disabled");
    with_buggify(42, || {
        assert!(buggify_enabled(), "Should be enabled inside with_buggify");
    });
    assert!(!buggify_enabled(), "Should be disabled after with_buggify");
}

#[test]
fn buggify_manual_enable_disable() {
    set_buggify_enabled(true);
    set_buggify_seed(999);
    assert!(buggify_enabled());

    set_buggify_enabled(false);
    assert!(!buggify_enabled());
    assert!(!buggify(), "Must be false when disabled");
}

// ═══════════════════════════════════════════════════════════════════════
// Meta-tests: rerun same seed, compare TRACE-level logs byte-for-byte
// ═══════════════════════════════════════════════════════════════════════

/// Run a complete workload on the bus and return the full trace.
fn run_workload(seed: u64, fault_prob: f64) -> Vec<String> {
    let config = FaultConfig {
        drop_probability: fault_prob,
        disk_write_error_probability: fault_prob * 0.5,
        ..FaultConfig::none()
    };

    let injector = FaultInjector::new(config);
    let cfg = DatabaseConfig::default();
    let mut bus = MessageBus::new(seed, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(WalWriter::new(WAL_WRITER_ID, DISK_ID, TXN_MGR_ID, &cfg)));
    bus.register(Box::new(TransactionManager::new(TXN_MGR_ID, WAL_WRITER_ID)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Run a multi-transaction workload.
    // Retry TxnBegin until acknowledged — fault injection may drop messages.
    let mut txn_id = None;
    for attempt in 0..20 {
        bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnBegin, attempt);
        bus.run(50);
        txn_id = bus.actor::<Collector>(CLIENT_ID).unwrap()
            .received
            .iter()
            .rev()
            .find_map(|m| if let Message::TxnBeginOk { txn_id } = m { Some(*txn_id) } else { None });
        if txn_id.is_some() {
            break;
        }
    }
    let txn_id = txn_id.unwrap_or(0);

    for i in 0u8..10 {
        bus.send(
            CLIENT_ID,
            TXN_MGR_ID,
            Message::TxnPut {
                txn_id,
                key: vec![i],
                value: vec![i * 2],
            },
            0,
        );
    }
    bus.run(500);

    bus.send(CLIENT_ID, TXN_MGR_ID, Message::TxnCommit { txn_id }, 0);
    bus.run(500);

    // Collect the complete trace — this is the "log" we compare.
    bus.trace()
        .iter()
        .map(|(tick, from, to, msg)| format!("{:05}:{}->{}:{}", tick, from, to, msg))
        .collect()
}

#[test]
fn meta_test_clean_run_deterministic() {
    // No faults — clean execution must be byte-for-byte identical.
    let trace_a = run_workload(42, 0.0);
    let trace_b = run_workload(42, 0.0);

    assert_eq!(
        trace_a, trace_b,
        "DETERMINISM REGRESSION: clean run with same seed produced different traces"
    );
    assert!(!trace_a.is_empty());
}

#[test]
fn meta_test_with_faults_deterministic() {
    // With fault injection — same seed must still replay identically.
    let trace_a = run_workload(12345, 0.1);
    let trace_b = run_workload(12345, 0.1);

    assert_eq!(
        trace_a, trace_b,
        "DETERMINISM REGRESSION: fault-injected run with same seed produced different traces"
    );
}

#[test]
fn meta_test_different_seeds_produce_different_traces() {
    // Different seeds with faults must diverge.
    let trace_a = run_workload(100, 0.15);
    let trace_b = run_workload(200, 0.15);

    assert_ne!(
        trace_a, trace_b,
        "Different seeds should produce different fault patterns"
    );
}

#[test]
fn meta_test_multiple_seeds_all_deterministic() {
    // Validate determinism across a range of seeds — each must replay.
    for seed in [0u64, 1, 42, 100, 999, 0xDEADBEEF, u64::MAX / 2] {
        let trace_a = run_workload(seed, 0.05);
        let trace_b = run_workload(seed, 0.05);
        assert_eq!(
            trace_a,
            trace_b,
            "DETERMINISM REGRESSION at seed {}",
            seed
        );
    }
}

#[test]
fn meta_test_high_fault_rate_deterministic() {
    // Even with aggressive faults (30% message drop), must replay.
    let trace_a = run_workload(777, 0.3);
    let trace_b = run_workload(777, 0.3);
    assert_eq!(
        trace_a, trace_b,
        "DETERMINISM REGRESSION under high fault rate"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Trace content validation
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn trace_contains_expected_message_types() {
    let trace = run_workload(42, 0.0);

    let has_wal_append = trace.iter().any(|t| t.contains("WalAppend"));
    let has_commit = trace.iter().any(|t| t.contains("TxnCommit"));
    let has_disk_write = trace.iter().any(|t| t.contains("DiskWrite"));

    assert!(has_wal_append, "Trace should contain WalAppend messages");
    assert!(has_commit, "Trace should contain TxnCommit messages");
    assert!(has_disk_write, "Trace should contain DiskWrite messages");
}

#[test]
fn trace_timestamps_monotonically_increasing() {
    let trace = run_workload(42, 0.0);

    let mut last_tick = 0u64;
    for entry in &trace {
        // Entries are formatted as "TTTTT:..."
        let tick: u64 = entry[..5].trim().parse().unwrap_or(0);
        assert!(
            tick >= last_tick,
            "Trace timestamps must be non-decreasing: {} < {}",
            tick,
            last_tick
        );
        last_tick = tick;
    }
}
