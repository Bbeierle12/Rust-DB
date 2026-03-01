use std::collections::BTreeSet;

use crate::storage::types::PageId;

/// Simple page ID allocator.
///
/// Allocates monotonically increasing PageIds, reusing freed IDs.
/// BTreeSet ensures deterministic ordering of freed IDs.
#[derive(Debug)]
pub struct PageAllocator {
    next_id: u64,
    free_list: BTreeSet<u64>,
}

impl PageAllocator {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            free_list: BTreeSet::new(),
        }
    }

    /// Start allocating from a given ID (for recovery).
    pub fn new_starting_at(next_id: u64) -> Self {
        Self {
            next_id,
            free_list: BTreeSet::new(),
        }
    }

    /// Allocate a new PageId.
    pub fn alloc(&mut self) -> PageId {
        if let Some(&id) = self.free_list.iter().next() {
            self.free_list.remove(&id);
            PageId(id)
        } else {
            let id = self.next_id;
            self.next_id += 1;
            PageId(id)
        }
    }

    /// Free a PageId for reuse.
    pub fn free(&mut self, page_id: PageId) {
        self.free_list.insert(page_id.0);
    }

    /// Return the next ID that would be allocated (for recovery).
    pub fn next_id(&self) -> u64 {
        self.next_id
    }
}

impl Default for PageAllocator {
    fn default() -> Self {
        Self::new()
    }
}
