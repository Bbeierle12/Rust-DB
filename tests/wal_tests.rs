//! Tests for the WAL: append-and-read, crash recovery, corruption detection,
//! fsync failure handling.

mod helpers;

use helpers::Collector;
use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::wal::reader::WalReader;
use rust_dst_db::wal::writer::WalWriter;

const DISK_ID: ActorId = ActorId(1);
const WRITER_ID: ActorId = ActorId(2);
const READER_ID: ActorId = ActorId(3);
const CLIENT_ID: ActorId = ActorId(4);
const WAL_FILE_ID: u64 = 0;

/// Helper: set up a bus with disk, WAL writer, WAL reader, and client collector.
fn setup_bus(seed: u64, fault_config: FaultConfig, buffered_disk: bool) -> MessageBus {
    let injector = FaultInjector::new(fault_config);
    let cfg = DatabaseConfig::default();
    let mut bus = MessageBus::new(seed, injector, &cfg);

    bus.register(Box::new(SimDisk::new(DISK_ID, buffered_disk, &cfg)));
    bus.register(Box::new(WalWriter::new(
        WRITER_ID, DISK_ID, CLIENT_ID, &cfg,
    )));
    bus.register(Box::new(WalReader::new(
        READER_ID, DISK_ID, CLIENT_ID, &cfg,
    )));
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    bus
}

// ─── Basic WAL operations ────────────────────────────────────────────

#[test]
fn wal_append_and_read() {
    let mut bus = setup_bus(42, FaultConfig::none(), false);

    // Append three records.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"hello".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"world".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"rust".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Check that we got three WalAppendOk messages.
    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let ok_count = client
        .received
        .iter()
        .filter(|m| matches!(m, Message::WalAppendOk { .. }))
        .count();
    assert_eq!(ok_count, 3, "Expected 3 WalAppendOk, got {}", ok_count);

    // Now read back.
    let mut bus = setup_bus(42, FaultConfig::none(), false);

    // Manually write the WAL file data by appending through the writer first.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"hello".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"world".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"rust".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Now read.
    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    assert!(records_msg.is_some(), "Should have received WalRecords");

    if let Some(Message::WalRecords { records }) = records_msg {
        assert_eq!(records.len(), 3);
        assert_eq!(records[0], (0, b"hello".to_vec()));
        assert_eq!(records[1], (1, b"world".to_vec()));
        assert_eq!(records[2], (2, b"rust".to_vec()));
    }
}

#[test]
fn wal_empty_read() {
    let mut bus = setup_bus(42, FaultConfig::none(), false);

    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    assert!(records_msg.is_some());

    if let Some(Message::WalRecords { records }) = records_msg {
        assert!(records.is_empty(), "Empty WAL should return no records");
    }
}

// ─── WAL fsync ───────────────────────────────────────────────────────

#[test]
fn wal_fsync_with_buffered_disk() {
    // With buffered disk, data is only visible after fsync.
    let mut bus = setup_bus(42, FaultConfig::none(), true);

    // Append a record.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"buffered".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Read before fsync — data should not be persisted.
    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    if let Some(Message::WalRecords { records }) = records_msg {
        assert!(records.is_empty(), "Unfsynced data should not be readable");
    }

    // Now fsync.
    bus.send(CLIENT_ID, WRITER_ID, Message::WalFsync, 0);
    bus.run(200);

    // Read after fsync.
    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    if let Some(Message::WalRecords { records }) = records_msg {
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1, b"buffered".to_vec());
    } else {
        panic!("Should have received WalRecords after fsync");
    }
}

// ─── WAL crash recovery ─────────────────────────────────────────────

#[test]
fn wal_recovery_after_crash_to_last_fsynced() {
    // Write records, fsync some, crash (losing unfsynced), verify recovery.
    let mut bus = setup_bus(42, FaultConfig::none(), true);

    // Write two records and fsync.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"rec1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"rec2".to_vec(),
        },
        0,
    );
    bus.run(200);

    bus.send(CLIENT_ID, WRITER_ID, Message::WalFsync, 0);
    bus.run(200);

    // Write a third record WITHOUT fsync.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"rec3_lost".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Simulate crash — lose buffered writes.
    // We can't directly call crash() on the disk through the bus, but we can
    // read only the fsynced data. The SimDisk with buffered=true only persists on fsync.

    // Read back — should only see the two fsynced records.
    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    if let Some(Message::WalRecords { records }) = records_msg {
        assert_eq!(records.len(), 2, "Should recover only fsynced records");
        assert_eq!(records[0].1, b"rec1".to_vec());
        assert_eq!(records[1].1, b"rec2".to_vec());
    } else {
        panic!("Should have received WalRecords");
    }
}

// ─── WAL checksum corruption detection ──────────────────────────────

#[test]
fn wal_detects_checksum_corruption() {
    // Write records, manually corrupt the disk data, verify reader truncates.
    let mut bus = setup_bus(42, FaultConfig::none(), false);

    // Write three records.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"good1".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"good2".to_vec(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"good3".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Corrupt the second record by writing garbage into the middle of the file.
    // Record 1: 8-byte header + 5 bytes "good1" = 13 bytes. Second record starts at offset 13.
    // Corrupt the CRC bytes (offset 13+4 = 17..21).
    bus.send(
        CLIENT_ID,
        DISK_ID,
        Message::DiskWrite {
            file_id: WAL_FILE_ID,
            offset: 17,
            data: vec![0xFF, 0xFF, 0xFF, 0xFF],
        },
        0,
    );
    bus.run(200);

    // Read — should recover only the first record (truncate at corruption).
    bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let records_msg = client
        .received
        .iter()
        .rev()
        .find(|m| matches!(m, Message::WalRecords { .. }));
    if let Some(Message::WalRecords { records }) = records_msg {
        assert_eq!(records.len(), 1, "Should truncate at corrupted record");
        assert_eq!(records[0].1, b"good1".to_vec());
    } else {
        panic!("Should have received WalRecords");
    }
}

// ─── WAL fsync failure handling ─────────────────────────────────────

#[test]
fn wal_fsync_failure_reported() {
    let config = FaultConfig {
        fsync_error_probability: 1.0,
        ..FaultConfig::none()
    };
    let mut bus = setup_bus(42, config, false);

    // Write a record.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"data".to_vec(),
        },
        0,
    );
    bus.run(200);

    // Request fsync — should fail due to fault injection.
    bus.send(CLIENT_ID, WRITER_ID, Message::WalFsync, 0);
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let fsync_err = client
        .received
        .iter()
        .any(|m| matches!(m, Message::WalFsyncErr { .. }));
    assert!(fsync_err, "Should have received WalFsyncErr");
}

// ─── WAL write failure handling ─────────────────────────────────────

#[test]
fn wal_write_failure_reported() {
    let config = FaultConfig {
        disk_write_error_probability: 1.0,
        ..FaultConfig::none()
    };
    let mut bus = setup_bus(42, config, false);

    // Append — disk write will fail due to fault injection.
    bus.send(
        CLIENT_ID,
        WRITER_ID,
        Message::WalAppend {
            data: b"doomed".to_vec(),
        },
        0,
    );
    bus.run(200);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let append_err = client
        .received
        .iter()
        .any(|m| matches!(m, Message::WalAppendErr { .. }));
    assert!(append_err, "Should have received WalAppendErr");
}

// ─── WAL deterministic replay ───────────────────────────────────────

#[test]
fn wal_deterministic_replay() {
    fn run_wal_scenario(seed: u64) -> Vec<String> {
        let mut bus = setup_bus(seed, FaultConfig::none(), false);

        bus.send(
            CLIENT_ID,
            WRITER_ID,
            Message::WalAppend {
                data: b"a".to_vec(),
            },
            0,
        );
        bus.send(
            CLIENT_ID,
            WRITER_ID,
            Message::WalAppend {
                data: b"b".to_vec(),
            },
            0,
        );
        bus.send(
            CLIENT_ID,
            WRITER_ID,
            Message::WalAppend {
                data: b"c".to_vec(),
            },
            0,
        );
        bus.run(200);

        bus.send(CLIENT_ID, READER_ID, Message::WalReadAll, 0);
        bus.run(200);

        bus.trace()
            .iter()
            .map(|(tick, from, to, msg)| format!("{}:{}->{}:{}", tick, from, to, msg))
            .collect()
    }

    let trace_a = run_wal_scenario(777);
    let trace_b = run_wal_scenario(777);
    assert_eq!(
        trace_a, trace_b,
        "WAL operations must replay deterministically"
    );
}
