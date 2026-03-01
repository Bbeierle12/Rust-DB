// All numeric constants live here (Guardrail 6).

/// Page size in bytes — matches OS pages and SSD blocks.
pub const PAGE_SIZE: usize = 4096;

/// Default buffer pool capacity in pages.
pub const DEFAULT_BUFFER_POOL_PAGES: usize = 1024;

/// WAL record header size: length (u32) + CRC32 (u32).
pub const WAL_HEADER_SIZE: usize = 8;

/// Maximum WAL record payload size.
pub const WAL_MAX_RECORD_SIZE: usize = PAGE_SIZE * 4;

/// Default fault injection probability (0.0–1.0) in simulation.
pub const DEFAULT_FAULT_PROBABILITY: f64 = 0.05;

/// Default message bus queue depth.
pub const DEFAULT_QUEUE_DEPTH: usize = 16384;

/// Simulated fsync latency in ticks.
pub const SIM_FSYNC_LATENCY_TICKS: u64 = 5;

/// Simulated disk read latency in ticks.
pub const SIM_DISK_READ_LATENCY_TICKS: u64 = 2;

/// Simulated disk write latency in ticks.
pub const SIM_DISK_WRITE_LATENCY_TICKS: u64 = 3;

/// Maximum key-value entries in a B-tree leaf node before splitting.
pub const BTREE_MAX_LEAF_ENTRIES: usize = 32;

/// Maximum keys in a B-tree internal node before splitting.
pub const BTREE_MAX_INTERNAL_KEYS: usize = 32;

/// Minimum entries before a leaf node is considered for merge (floor of max/2).
pub const BTREE_MIN_LEAF_ENTRIES: usize = BTREE_MAX_LEAF_ENTRIES / 2;

/// Minimum keys before an internal node is considered for merge.
pub const BTREE_MIN_INTERNAL_KEYS: usize = BTREE_MAX_INTERNAL_KEYS / 2;

/// File ID used for the B-tree page storage on the simulated disk.
pub const BTREE_DATA_FILE_ID: u64 = 1;

/// Page header size in bytes: page_id(8) + page_type(1) + lsn(8) + entry_count(2) + crc32(4) = 23, padded to 32.
pub const PAGE_HEADER_SIZE: usize = 32;
