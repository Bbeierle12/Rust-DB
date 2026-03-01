/// A single entry in the Raft log.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
}

/// In-memory Raft log. Indices are 1-based.
pub struct RaftLog {
    entries: Vec<LogEntry>,
    /// Index of last entry replaced by a snapshot (0 if none).
    pub snapshot_last_index: u64,
    pub snapshot_last_term: u64,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            snapshot_last_index: 0,
            snapshot_last_term: 0,
        }
    }

    /// Append an entry and return its index (1-based).
    pub fn append(&mut self, term: u64, data: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { index, term, data });
        index
    }

    /// Get an entry by index.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 || index <= self.snapshot_last_index {
            return None;
        }
        let offset = (index - self.snapshot_last_index - 1) as usize;
        self.entries.get(offset)
    }

    /// Last log index (0 if empty).
    pub fn last_index(&self) -> u64 {
        if let Some(last) = self.entries.last() {
            last.index
        } else {
            self.snapshot_last_index
        }
    }

    /// Term of the last entry (0 if empty).
    pub fn last_term(&self) -> u64 {
        if let Some(last) = self.entries.last() {
            last.term
        } else {
            self.snapshot_last_term
        }
    }

    /// Term of the entry at `index`, or None if not found.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        if index == self.snapshot_last_index {
            return Some(self.snapshot_last_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Remove all entries with index >= from_index.
    pub fn truncate_from(&mut self, from_index: u64) {
        if from_index <= self.snapshot_last_index {
            self.entries.clear();
            return;
        }
        let offset = (from_index - self.snapshot_last_index - 1) as usize;
        self.entries.truncate(offset);
    }

    /// True if (last_term, last_index) is at least as up-to-date as this log.
    pub fn is_up_to_date(&self, last_term: u64, last_index: u64) -> bool {
        if last_term != self.last_term() {
            last_term > self.last_term()
        } else {
            last_index >= self.last_index()
        }
    }

    /// Compact: replace all entries up through snapshot_index with a snapshot marker.
    pub fn compact(&mut self, snapshot_last_index: u64, snapshot_last_term: u64) {
        // Remove all entries with index <= snapshot_last_index.
        self.entries.retain(|e| e.index > snapshot_last_index);
        self.snapshot_last_index = snapshot_last_index;
        self.snapshot_last_term = snapshot_last_term;
    }

    /// Return (term, data) pairs for all entries with index >= from_index.
    pub fn get_entries_from(&self, from_index: u64) -> Vec<(u64, Vec<u8>)> {
        self.entries
            .iter()
            .filter(|e| e.index >= from_index)
            .map(|e| (e.term, e.data.clone()))
            .collect()
    }
}
