// All numeric constants live here (Guardrail 6).

/// Page size in bytes — matches OS pages and SSD blocks.
pub const PAGE_SIZE: usize = 4096;

// Raft timing constants.
pub const RAFT_ELECTION_TIMEOUT_MIN_TICKS: u64 = 150;
pub const RAFT_ELECTION_TIMEOUT_MAX_TICKS: u64 = 300;
pub const RAFT_HEARTBEAT_INTERVAL_TICKS: u64 = 50;
pub const RAFT_MAX_ENTRIES_PER_APPEND: usize = 64;

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

/// Simulated network base latency in ticks.
pub const SIM_NETWORK_LATENCY_TICKS: u64 = 3;

/// Simulated network latency variance in ticks.
pub const SIM_NETWORK_LATENCY_VARIANCE_TICKS: u64 = 2;

/// Default PostgreSQL wire protocol port.
pub const NET_DEFAULT_PORT: u16 = 5432;

/// Runtime-configurable database parameters.
///
/// Construct with `DatabaseConfig::default()` to get production defaults,
/// then override fields for simulation or tuning.
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseConfig {
    // Storage
    pub page_size: usize,
    pub buffer_pool_pages: usize,
    pub btree_max_leaf_entries: usize,
    pub btree_max_internal_keys: usize,
    pub btree_data_file_id: u64,

    // WAL
    pub wal_header_size: usize,
    pub wal_max_record_size: usize,
    pub wal_file_id: u64,

    // Simulation latency
    pub sim_fsync_latency_ticks: u64,
    pub sim_disk_read_latency_ticks: u64,
    pub sim_disk_write_latency_ticks: u64,
    pub sim_network_latency_ticks: u64,
    pub sim_network_latency_variance_ticks: u64,

    // Fault injection defaults
    pub default_fault_probability: f64,

    // Bus
    pub default_queue_depth: usize,

    // Network
    pub net_default_port: u16,

    // Raft
    pub raft_election_timeout_min: u64,
    pub raft_election_timeout_max: u64,
    pub raft_heartbeat_interval: u64,
    pub raft_max_entries_per_append: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            page_size: PAGE_SIZE,
            buffer_pool_pages: DEFAULT_BUFFER_POOL_PAGES,
            btree_max_leaf_entries: BTREE_MAX_LEAF_ENTRIES,
            btree_max_internal_keys: BTREE_MAX_INTERNAL_KEYS,
            btree_data_file_id: BTREE_DATA_FILE_ID,
            wal_header_size: WAL_HEADER_SIZE,
            wal_max_record_size: WAL_MAX_RECORD_SIZE,
            wal_file_id: 0,
            sim_fsync_latency_ticks: SIM_FSYNC_LATENCY_TICKS,
            sim_disk_read_latency_ticks: SIM_DISK_READ_LATENCY_TICKS,
            sim_disk_write_latency_ticks: SIM_DISK_WRITE_LATENCY_TICKS,
            sim_network_latency_ticks: SIM_NETWORK_LATENCY_TICKS,
            sim_network_latency_variance_ticks: SIM_NETWORK_LATENCY_VARIANCE_TICKS,
            default_fault_probability: DEFAULT_FAULT_PROBABILITY,
            default_queue_depth: DEFAULT_QUEUE_DEPTH,
            net_default_port: NET_DEFAULT_PORT,
            raft_election_timeout_min: RAFT_ELECTION_TIMEOUT_MIN_TICKS,
            raft_election_timeout_max: RAFT_ELECTION_TIMEOUT_MAX_TICKS,
            raft_heartbeat_interval: RAFT_HEARTBEAT_INTERVAL_TICKS,
            raft_max_entries_per_append: RAFT_MAX_ENTRIES_PER_APPEND,
        }
    }
}
