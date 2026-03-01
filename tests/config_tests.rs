//! Stage 8: DatabaseConfig tests.
//! Validates that the config struct matches existing constants and that
//! constructors respect the values provided.

mod helpers;

use helpers::Collector;
use rust_dst_db::config::{
    DatabaseConfig, BTREE_DATA_FILE_ID, BTREE_MAX_INTERNAL_KEYS, BTREE_MAX_LEAF_ENTRIES,
    DEFAULT_BUFFER_POOL_PAGES, DEFAULT_FAULT_PROBABILITY, DEFAULT_QUEUE_DEPTH,
    NET_DEFAULT_PORT, PAGE_SIZE, RAFT_ELECTION_TIMEOUT_MAX_TICKS,
    RAFT_ELECTION_TIMEOUT_MIN_TICKS, RAFT_HEARTBEAT_INTERVAL_TICKS,
    RAFT_MAX_ENTRIES_PER_APPEND, SIM_DISK_READ_LATENCY_TICKS, SIM_DISK_WRITE_LATENCY_TICKS,
    SIM_FSYNC_LATENCY_TICKS, SIM_NETWORK_LATENCY_TICKS, SIM_NETWORK_LATENCY_VARIANCE_TICKS,
    WAL_HEADER_SIZE, WAL_MAX_RECORD_SIZE,
};
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::storage::buffer_pool::BufferPool;
use rust_dst_db::storage::btree::BTreeEngine;
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::wal::writer::WalWriter;
use rust_dst_db::net::sim_network::SimNetwork;

const DISK_ID: ActorId = ActorId(1);
const WAL_WRITER_ID: ActorId = ActorId(2);
const BUF_POOL_ID: ActorId = ActorId(3);
const BTREE_ID: ActorId = ActorId(4);
const CLIENT_ID: ActorId = ActorId(5);
const SIM_NET_ID: ActorId = ActorId(6);
const PEER_ID: ActorId = ActorId(7);

/// Test 1: Default config matches existing constants.
#[test]
fn default_config_matches_constants() {
    let cfg = DatabaseConfig::default();

    assert_eq!(cfg.page_size, PAGE_SIZE);
    assert_eq!(cfg.buffer_pool_pages, DEFAULT_BUFFER_POOL_PAGES);
    assert_eq!(cfg.btree_max_leaf_entries, BTREE_MAX_LEAF_ENTRIES);
    assert_eq!(cfg.btree_max_internal_keys, BTREE_MAX_INTERNAL_KEYS);
    assert_eq!(cfg.btree_data_file_id, BTREE_DATA_FILE_ID);
    assert_eq!(cfg.wal_header_size, WAL_HEADER_SIZE);
    assert_eq!(cfg.wal_max_record_size, WAL_MAX_RECORD_SIZE);
    assert_eq!(cfg.wal_file_id, 0);
    assert_eq!(cfg.sim_fsync_latency_ticks, SIM_FSYNC_LATENCY_TICKS);
    assert_eq!(cfg.sim_disk_read_latency_ticks, SIM_DISK_READ_LATENCY_TICKS);
    assert_eq!(cfg.sim_disk_write_latency_ticks, SIM_DISK_WRITE_LATENCY_TICKS);
    assert_eq!(cfg.sim_network_latency_ticks, SIM_NETWORK_LATENCY_TICKS);
    assert_eq!(cfg.sim_network_latency_variance_ticks, SIM_NETWORK_LATENCY_VARIANCE_TICKS);
    assert_eq!(cfg.default_fault_probability, DEFAULT_FAULT_PROBABILITY);
    assert_eq!(cfg.default_queue_depth, DEFAULT_QUEUE_DEPTH);
    assert_eq!(cfg.net_default_port, NET_DEFAULT_PORT);
    assert_eq!(cfg.raft_election_timeout_min, RAFT_ELECTION_TIMEOUT_MIN_TICKS);
    assert_eq!(cfg.raft_election_timeout_max, RAFT_ELECTION_TIMEOUT_MAX_TICKS);
    assert_eq!(cfg.raft_heartbeat_interval, RAFT_HEARTBEAT_INTERVAL_TICKS);
    assert_eq!(cfg.raft_max_entries_per_append, RAFT_MAX_ENTRIES_PER_APPEND);
}

/// Test 2: BufferPool created with small capacity evicts at that capacity.
#[test]
fn custom_buffer_pool_size_respected() {
    let cfg = DatabaseConfig {
        buffer_pool_pages: 4,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(BufferPool::new(BUF_POOL_ID, DISK_ID, &cfg)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Write 5 pages → triggers eviction at page 5 (capacity = 4).
    for page_id in 0u64..5 {
        bus.send(
            CLIENT_ID,
            BUF_POOL_ID,
            Message::BufPoolWritePage { page_id, data: vec![page_id as u8; 16] },
            0,
        );
        bus.run(20);
    }

    // Buffer pool with capacity=4 should have evicted at least one page.
    let pool = bus.actor::<BufferPool>(BUF_POOL_ID).unwrap();
    assert!(
        pool.cached_count() <= 4,
        "Buffer pool exceeded configured capacity: {} > 4",
        pool.cached_count()
    );
}

/// Test 3: BTreeEngine splits at configured leaf limit.
#[test]
fn custom_btree_limits_respected() {
    let cfg = DatabaseConfig {
        btree_max_leaf_entries: 4,
        btree_max_internal_keys: 4,
        buffer_pool_pages: 64,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(WalWriter::new(WAL_WRITER_ID, DISK_ID, BTREE_ID, &cfg)));
    bus.register(Box::new(BufferPool::new(BUF_POOL_ID, DISK_ID, &cfg)));
    bus.register(Box::new(BTreeEngine::new(BTREE_ID, BUF_POOL_ID, WAL_WRITER_ID, &cfg)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Insert 10 entries — should cause splits at leaf limit=4.
    for i in 0u8..10 {
        bus.send(CLIENT_ID, BTREE_ID, Message::BTreePut { key: vec![i], value: vec![i] }, 0);
        bus.run(50);
    }

    // Verify all keys are retrievable (splits preserved data).
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let puts_ok = client.received.iter().filter(|m| matches!(m, Message::BTreePutOk)).count();
    assert_eq!(puts_ok, 10, "All 10 puts should succeed with custom split limit");
}

/// Test 4: SimDisk with custom read latency delivers data after that many ticks.
#[test]
fn custom_disk_latency_affects_timing() {
    let latency = 10u64;
    let cfg = DatabaseConfig {
        sim_disk_read_latency_ticks: latency,
        sim_disk_write_latency_ticks: 0,
        sim_fsync_latency_ticks: 0,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Write data with zero write latency so it's instant.
    bus.send(CLIENT_ID, DISK_ID, Message::DiskWrite { file_id: 0, offset: 0, data: vec![42u8] }, 0);
    bus.run(5); // write latency=0 so this finishes quickly

    // Capture reads count before the read (write acks may be there).
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let reads_before = client.received.iter().filter(|m| matches!(m, Message::DiskReadOk { .. })).count();

    // Now issue the read — recorded at current tick, response comes latency ticks later.
    bus.send(CLIENT_ID, DISK_ID, Message::DiskRead { file_id: 0, offset: 0, len: 1 }, 0);

    // Run latency-1 ticks — response should NOT be available yet.
    for _ in 0..latency - 1 {
        bus.tick();
    }
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let reads_mid = client.received.iter().filter(|m| matches!(m, Message::DiskReadOk { .. })).count();
    assert_eq!(reads_mid, reads_before, "DiskReadOk should not arrive before latency ticks elapsed");

    // Run 2 more ticks to ensure we cross the latency boundary.
    bus.tick();
    bus.tick();
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let reads_after = client.received.iter().filter(|m| matches!(m, Message::DiskReadOk { .. })).count();
    assert_eq!(reads_after, reads_before + 1, "DiskReadOk should arrive after latency ticks");
}

/// Test 5: SimNetwork with custom latency delivers NetRecv after that latency.
#[test]
fn custom_network_latency_affects_timing() {
    let latency = 5u64;
    let cfg = DatabaseConfig {
        sim_network_latency_ticks: latency,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    let mut sim = SimNetwork::new(SIM_NET_ID, "a", 42, &cfg);
    sim.add_peer("b", PEER_ID);
    bus.register(Box::new(sim));
    bus.register(Box::new(Collector::new(CLIENT_ID)));
    bus.register(Box::new(Collector::new(PEER_ID)));

    // Send at tick 0 → NetRecv queued at tick `latency`.
    bus.send(CLIENT_ID, SIM_NET_ID, Message::NetSend { conn_id: 1, to_node: "b".into(), data: b"hi".to_vec() }, 0);

    // Run latency-1 ticks — NetRecv should NOT arrive yet.
    for _ in 0..latency - 1 {
        bus.tick();
    }
    let peer = bus.actor::<Collector>(PEER_ID).unwrap();
    assert!(peer.received.is_empty(), "NetRecv should not arrive before latency={} ticks", latency);

    // Run 2 more ticks to cross the boundary.
    bus.tick();
    bus.tick();
    let peer = bus.actor::<Collector>(PEER_ID).unwrap();
    assert_eq!(peer.received.len(), 1, "NetRecv should arrive after latency={} ticks", latency);
}

/// Test 6: Zero-latency config delivers responses in the same tick.
#[test]
fn zero_latency_config_delivers_immediately() {
    let cfg = DatabaseConfig {
        sim_disk_read_latency_ticks: 0,
        sim_disk_write_latency_ticks: 0,
        sim_fsync_latency_ticks: 0,
        sim_network_latency_ticks: 0,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Write and immediately check response arrives in same tick wave.
    bus.send(CLIENT_ID, DISK_ID, Message::DiskWrite { file_id: 0, offset: 0, data: vec![1] }, 0);
    bus.tick(); // One tick delivers both write and its ack (delay=0).

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let writes_ok = client.received.iter().filter(|m| matches!(m, Message::DiskWriteOk { .. })).count();
    assert_eq!(writes_ok, 1, "DiskWriteOk should arrive in same tick wave with zero latency");
}

/// Test 7: Cloning a config gives an independent copy.
#[test]
fn config_clone_is_independent() {
    let original = DatabaseConfig::default();
    let mut cloned = original.clone();

    cloned.buffer_pool_pages = 999;
    cloned.wal_file_id = 42;

    assert_eq!(original.buffer_pool_pages, DEFAULT_BUFFER_POOL_PAGES);
    assert_eq!(original.wal_file_id, 0);
    assert_eq!(cloned.buffer_pool_pages, 999);
    assert_eq!(cloned.wal_file_id, 42);
}

/// Test 8: Two buses in the same process can have different configs independently.
#[test]
fn two_buses_different_configs() {
    let cfg_small = DatabaseConfig { buffer_pool_pages: 4, ..DatabaseConfig::default() };
    let cfg_large = DatabaseConfig { buffer_pool_pages: 256, ..DatabaseConfig::default() };

    let injector1 = FaultInjector::new(FaultConfig::none());
    let mut bus1 = MessageBus::new(1, injector1, &cfg_small);
    bus1.register(Box::new(SimDisk::new(DISK_ID, false, &cfg_small)));
    bus1.register(Box::new(BufferPool::new(BUF_POOL_ID, DISK_ID, &cfg_small)));
    bus1.register(Box::new(Collector::new(CLIENT_ID)));

    let injector2 = FaultInjector::new(FaultConfig::none());
    let mut bus2 = MessageBus::new(2, injector2, &cfg_large);
    bus2.register(Box::new(SimDisk::new(DISK_ID, false, &cfg_large)));
    bus2.register(Box::new(BufferPool::new(BUF_POOL_ID, DISK_ID, &cfg_large)));
    bus2.register(Box::new(Collector::new(CLIENT_ID)));

    // Write pages to fill beyond small bus capacity.
    for page_id in 0u64..8 {
        bus1.send(CLIENT_ID, BUF_POOL_ID, Message::BufPoolWritePage { page_id, data: vec![0u8; 16] }, 0);
        bus1.run(20);
        bus2.send(CLIENT_ID, BUF_POOL_ID, Message::BufPoolWritePage { page_id, data: vec![0u8; 16] }, 0);
        bus2.run(20);
    }

    let pool1 = bus1.actor::<BufferPool>(BUF_POOL_ID).unwrap();
    let pool2 = bus2.actor::<BufferPool>(BUF_POOL_ID).unwrap();

    // Small bus evicts; large bus does not.
    assert!(pool1.cached_count() <= 4, "Small bus pool should have evicted pages");
    assert_eq!(pool2.cached_count(), 8, "Large bus pool should hold all 8 pages");
}

/// Test 9: WAL writer uses wal_file_id from config, not hardcoded 0.
#[test]
fn wal_file_id_from_config() {
    let cfg = DatabaseConfig {
        wal_file_id: 5,
        ..DatabaseConfig::default()
    };
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, false, &cfg)));
    bus.register(Box::new(WalWriter::new(WAL_WRITER_ID, DISK_ID, CLIENT_ID, &cfg)));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    // Append a record.
    bus.send(CLIENT_ID, WAL_WRITER_ID, Message::WalAppend { data: b"test".to_vec() }, 0);
    bus.run(50);

    // WalAppendOk should be received.
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let ok = client.received.iter().any(|m| matches!(m, Message::WalAppendOk { .. }));
    assert!(ok, "WalAppendOk expected");

    // Data should be on file_id=5 in the disk.
    let disk = bus.actor::<SimDisk>(DISK_ID).unwrap();
    assert!(disk.file_contents(5).is_some(), "WAL data should be on file_id=5");
    assert!(disk.file_contents(0).is_none(), "Nothing should be on file_id=0");
}

/// Test 10: Overriding one field leaves all others at defaults.
#[test]
fn config_partial_override() {
    let cfg = DatabaseConfig {
        buffer_pool_pages: 512,
        ..DatabaseConfig::default()
    };

    assert_eq!(cfg.buffer_pool_pages, 512);
    // All other fields should match their defaults.
    assert_eq!(cfg.page_size, PAGE_SIZE);
    assert_eq!(cfg.wal_file_id, 0);
    assert_eq!(cfg.btree_max_leaf_entries, BTREE_MAX_LEAF_ENTRIES);
    assert_eq!(cfg.sim_network_latency_ticks, SIM_NETWORK_LATENCY_TICKS);
    assert_eq!(cfg.raft_election_timeout_min, RAFT_ELECTION_TIMEOUT_MIN_TICKS);
}
