use std::fmt;

/// Unique identifier for a state machine in the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActorId(pub u64);

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Actor({})", self.0)
    }
}

/// Where a message should be delivered.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Destination {
    pub actor: ActorId,
    /// Delivery delay in ticks (0 = immediate next tick).
    pub delay: u64,
}

/// Envelope wrapping a message with routing info.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub from: ActorId,
    pub to: ActorId,
    pub deliver_at: u64,
    pub message: Message,
}

impl PartialEq for Envelope {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at
            && self.from == other.from
            && self.to == other.to
    }
}

impl Eq for Envelope {}

impl PartialOrd for Envelope {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Envelope {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Earlier delivery time comes first, then break ties by sender, then receiver.
        self.deliver_at
            .cmp(&other.deliver_at)
            .then_with(|| self.from.cmp(&other.from))
            .then_with(|| self.to.cmp(&other.to))
    }
}

/// All messages in the system.
///
/// Every inter–state-machine communication is a variant here.
/// This enum will grow as stages are added.
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    // --- Simulation lifecycle ---
    /// Tick heartbeat sent by the bus each step.
    Tick,

    // --- WAL messages ---
    /// Request to append a record to the WAL.
    WalAppend { data: Vec<u8> },
    /// WAL append succeeded; returns the LSN of the written record.
    WalAppendOk { lsn: u64 },
    /// WAL append failed.
    WalAppendErr { reason: String },
    /// Request to read all records from the WAL (recovery).
    WalReadAll,
    /// Response containing all recovered WAL records.
    WalRecords { records: Vec<(u64, Vec<u8>)> },
    /// Request to fsync the WAL.
    WalFsync,
    /// Fsync completed.
    WalFsyncOk,
    /// Fsync failed.
    WalFsyncErr { reason: String },

    // --- IO messages (between state machines and simulated IO) ---
    /// Write bytes to a file at an offset.
    DiskWrite {
        file_id: u64,
        offset: u64,
        data: Vec<u8>,
    },
    /// Disk write completed.
    DiskWriteOk { file_id: u64, offset: u64 },
    /// Disk write failed.
    DiskWriteErr {
        file_id: u64,
        offset: u64,
        reason: String,
    },
    /// Read bytes from a file at an offset.
    DiskRead {
        file_id: u64,
        offset: u64,
        len: u64,
    },
    /// Disk read completed with data.
    DiskReadOk {
        file_id: u64,
        offset: u64,
        data: Vec<u8>,
    },
    /// Disk read failed.
    DiskReadErr {
        file_id: u64,
        offset: u64,
        reason: String,
    },
    /// Fsync a file.
    DiskFsync { file_id: u64 },
    /// Disk fsync completed.
    DiskFsyncOk { file_id: u64 },
    /// Disk fsync failed.
    DiskFsyncErr { file_id: u64, reason: String },

    // --- Buffer pool messages ---
    /// Write a page to the buffer pool.
    BufPoolWritePage { page_id: u64, data: Vec<u8> },
    /// Page write acknowledged.
    BufPoolWriteOk { page_id: u64 },
    /// Read a page from the buffer pool.
    BufPoolReadPage { page_id: u64 },
    /// Page data returned from the buffer pool.
    BufPoolPageData { page_id: u64, data: Vec<u8> },
    /// Page not found in buffer pool or on disk.
    BufPoolPageNotFound { page_id: u64 },
    /// Flush all dirty pages to disk.
    BufPoolFlush,
    /// Flush completed.
    BufPoolFlushOk,
    /// Flush failed.
    BufPoolFlushErr { reason: String },

    // --- B-tree client interface ---
    /// Get a value by key.
    BTreeGet { key: Vec<u8> },
    /// Get result.
    BTreeGetResult { key: Vec<u8>, value: Option<Vec<u8>> },
    /// Insert or update a key-value pair.
    BTreePut { key: Vec<u8>, value: Vec<u8> },
    /// Put succeeded.
    BTreePutOk,
    /// Delete a key.
    BTreeDelete { key: Vec<u8> },
    /// Delete result.
    BTreeDeleteOk { found: bool },
    /// Scan a range of keys. None means unbounded on that side.
    BTreeScan { start: Option<Vec<u8>>, end: Option<Vec<u8>> },
    /// Scan results.
    BTreeScanResult { entries: Vec<(Vec<u8>, Vec<u8>)> },

    // --- Transaction messages ---
    /// Begin a new transaction.
    TxnBegin,
    /// Transaction begun; returns the assigned transaction ID.
    TxnBeginOk { txn_id: u64 },
    /// Read a key within a transaction.
    TxnGet { txn_id: u64, key: Vec<u8> },
    /// Transaction read result.
    TxnGetResult { txn_id: u64, key: Vec<u8>, value: Option<Vec<u8>> },
    /// Write a key-value pair within a transaction (buffered until commit).
    TxnPut { txn_id: u64, key: Vec<u8>, value: Vec<u8> },
    /// Transaction write buffered.
    TxnPutOk { txn_id: u64 },
    /// Delete a key within a transaction (buffered until commit).
    TxnDelete { txn_id: u64, key: Vec<u8> },
    /// Transaction delete buffered.
    TxnDeleteOk { txn_id: u64 },
    /// Commit a transaction (OCC validation + apply).
    TxnCommit { txn_id: u64 },
    /// Transaction committed successfully.
    TxnCommitOk { txn_id: u64 },
    /// Transaction commit failed (conflict or error).
    TxnCommitErr { txn_id: u64, reason: String },
    /// Abort a transaction (discard write set).
    TxnAbort { txn_id: u64 },
    /// Transaction aborted.
    TxnAbortOk { txn_id: u64 },
    /// Scan a range within a transaction (snapshot read).
    TxnScan { txn_id: u64, start: Option<Vec<u8>>, end: Option<Vec<u8>> },
    /// Transaction scan result.
    TxnScanResult { txn_id: u64, entries: Vec<(Vec<u8>, Vec<u8>)> },
}
