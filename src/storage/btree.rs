use std::collections::BTreeMap;

use crate::config::DatabaseConfig;
use crate::storage::page::BTreeNode;
use crate::storage::page_alloc::PageAllocator;
use crate::storage::types::PageId;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// WAL record types for crash recovery.
const WAL_OP_PUT: u8 = 1;
const WAL_OP_DELETE: u8 = 2;

/// B+tree engine state machine.
///
/// Maintains an in-memory B+tree and persists changes through the WAL
/// and BufferPool. All values live in leaf nodes. Internal nodes hold
/// separator keys and child pointers.
///
/// On mutation, the engine:
/// 1. Logs the operation to the WAL (write-ahead)
/// 2. Applies the change to the in-memory tree
/// 3. Sends dirty pages to the BufferPool for persistence
/// 4. Replies to the client
pub struct BTreeEngine {
    id: ActorId,
    buf_pool_actor: ActorId,
    wal_actor: ActorId,
    /// All nodes in the tree, indexed by PageId.
    nodes: BTreeMap<PageId, BTreeNode>,
    /// Page ID of the root node (None if tree is empty).
    root: Option<PageId>,
    /// Page allocator for new nodes.
    allocator: PageAllocator,
    /// Max entries per leaf before split.
    max_leaf: usize,
    /// Max keys per internal node before split.
    max_internal: usize,
    /// Whether the engine has been initialized (root created).
    initialized: bool,
}

impl BTreeEngine {
    pub fn new(
        id: ActorId,
        buf_pool_actor: ActorId,
        wal_actor: ActorId,
        cfg: &DatabaseConfig,
    ) -> Self {
        Self {
            id,
            buf_pool_actor,
            wal_actor,
            nodes: BTreeMap::new(),
            root: None,
            allocator: PageAllocator::new(),
            max_leaf: cfg.btree_max_leaf_entries,
            max_internal: cfg.btree_max_internal_keys,
            initialized: false,
        }
    }

    fn ensure_root(&mut self) {
        if !self.initialized {
            let page_id = self.allocator.alloc();
            let root = BTreeNode::new_leaf(page_id);
            self.nodes.insert(page_id, root);
            self.root = Some(page_id);
            self.initialized = true;
        }
    }

    /// Find the leaf node that should contain the given key.
    /// Returns the PageId of the leaf.
    fn find_leaf(&self, key: &[u8]) -> Option<PageId> {
        let mut current = self.root?;

        loop {
            let node = self.nodes.get(&current)?;
            match node {
                BTreeNode::Leaf { .. } => return Some(current),
                BTreeNode::Internal { keys, children, .. } => {
                    // Binary search for the child to follow.
                    let idx = keys.partition_point(|k| k.as_slice() <= key);
                    current = *children.get(idx)?;
                }
            }
        }
    }

    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let leaf_id = self.find_leaf(key)?;
        let node = self.nodes.get(&leaf_id)?;

        if let BTreeNode::Leaf { entries, .. } = node {
            entries
                .iter()
                .find(|(k, _)| k.as_slice() == key)
                .map(|(_, v)| v.clone())
        } else {
            None
        }
    }

    /// Insert or update a key-value pair.
    /// Returns the set of dirty page IDs that need to be persisted.
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Vec<PageId> {
        self.ensure_root();
        let mut dirty = Vec::new();

        let leaf_id = match self.find_leaf(&key) {
            Some(id) => id,
            None => return dirty, // Should not happen after ensure_root, but avoid panic.
        };

        // Insert into the leaf.
        if let Some(BTreeNode::Leaf { entries, .. }) = self.nodes.get_mut(&leaf_id) {
            match entries.binary_search_by(|(k, _)| k.as_slice().cmp(&key)) {
                Ok(idx) => {
                    // Update existing key.
                    entries[idx].1 = value;
                }
                Err(idx) => {
                    // Insert new key.
                    entries.insert(idx, (key, value));
                }
            }
            dirty.push(leaf_id);
        }

        // Check if the leaf needs splitting.
        self.maybe_split(leaf_id, &mut dirty);

        dirty
    }

    /// Delete a key. Returns (found, dirty_pages).
    fn delete(&mut self, key: &[u8]) -> (bool, Vec<PageId>) {
        let mut dirty = Vec::new();

        let Some(leaf_id) = self.find_leaf(key) else {
            return (false, dirty);
        };

        let found;
        if let Some(BTreeNode::Leaf { entries, .. }) = self.nodes.get_mut(&leaf_id) {
            if let Ok(idx) = entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
                entries.remove(idx);
                found = true;
                dirty.push(leaf_id);
            } else {
                found = false;
            }
        } else {
            found = false;
        }

        (found, dirty)
    }

    /// Scan a range of keys.
    fn scan(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();

        // Collect all leaf entries in order.
        // Since our nodes are in a BTreeMap, we traverse all leaves.
        self.collect_leaves(self.root, &mut results);

        // Filter by range.
        results
            .into_iter()
            .filter(|(k, _)| {
                if let Some(s) = start {
                    if k.as_slice() < s {
                        return false;
                    }
                }
                if let Some(e) = end {
                    if k.as_slice() >= e {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Recursively collect all key-value pairs from leaf nodes in order.
    fn collect_leaves(
        &self,
        page_id: Option<PageId>,
        results: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) {
        let Some(pid) = page_id else { return };
        let Some(node) = self.nodes.get(&pid) else {
            return;
        };

        match node {
            BTreeNode::Leaf { entries, .. } => {
                results.extend(entries.iter().cloned());
            }
            BTreeNode::Internal { children, .. } => {
                for child in children {
                    self.collect_leaves(Some(*child), results);
                }
            }
        }
    }

    /// Check if a node needs splitting. If so, split it and propagate up.
    fn maybe_split(&mut self, page_id: PageId, dirty: &mut Vec<PageId>) {
        let needs_split = match self.nodes.get(&page_id) {
            Some(BTreeNode::Leaf { entries, .. }) => entries.len() > self.max_leaf,
            Some(BTreeNode::Internal { keys, .. }) => keys.len() > self.max_internal,
            None => false,
        };

        if !needs_split {
            return;
        }

        let node = match self.nodes.get(&page_id) {
            Some(n) => n.clone(),
            None => return, // Node disappeared; skip split.
        };

        match node {
            BTreeNode::Leaf {
                page_id: leaf_id,
                mut entries,
            } => {
                let mid = entries.len() / 2;
                let right_entries = entries.split_off(mid);
                let separator = right_entries[0].0.clone();

                // Create new right leaf.
                let right_id = self.allocator.alloc();
                let right_leaf = BTreeNode::Leaf {
                    page_id: right_id,
                    entries: right_entries,
                };

                // Update existing leaf (becomes left).
                let left_leaf = BTreeNode::Leaf {
                    page_id: leaf_id,
                    entries,
                };

                self.nodes.insert(leaf_id, left_leaf);
                self.nodes.insert(right_id, right_leaf);
                dirty.push(right_id);

                // Insert separator into parent.
                self.insert_into_parent(leaf_id, separator, right_id, dirty);
            }
            BTreeNode::Internal {
                page_id: int_id,
                mut keys,
                mut children,
            } => {
                let mid = keys.len() / 2;
                let separator = keys[mid].clone();

                // Right side gets keys after mid, children after mid.
                let right_keys = keys.split_off(mid + 1);
                keys.pop(); // Remove the separator key from left.
                let right_children = children.split_off(mid + 1);

                let right_id = self.allocator.alloc();
                let right_node = BTreeNode::Internal {
                    page_id: right_id,
                    keys: right_keys,
                    children: right_children,
                };

                let left_node = BTreeNode::Internal {
                    page_id: int_id,
                    keys,
                    children,
                };

                self.nodes.insert(int_id, left_node);
                self.nodes.insert(right_id, right_node);
                dirty.push(right_id);

                self.insert_into_parent(int_id, separator, right_id, dirty);
            }
        }
    }

    /// Insert a separator key and right child into the parent of `left_id`.
    /// If `left_id` is the root, create a new root.
    fn insert_into_parent(
        &mut self,
        left_id: PageId,
        separator: Vec<u8>,
        right_id: PageId,
        dirty: &mut Vec<PageId>,
    ) {
        // Find the parent of left_id.
        let parent_id = self.find_parent(left_id);

        match parent_id {
            None => {
                // left_id is the root — create a new root.
                let new_root_id = self.allocator.alloc();
                let new_root = BTreeNode::Internal {
                    page_id: new_root_id,
                    keys: vec![separator],
                    children: vec![left_id, right_id],
                };
                self.nodes.insert(new_root_id, new_root);
                self.root = Some(new_root_id);
                dirty.push(new_root_id);
            }
            Some(parent_id) => {
                // Insert into existing parent.
                if let Some(BTreeNode::Internal { keys, children, .. }) =
                    self.nodes.get_mut(&parent_id)
                {
                    // Find where left_id is in children.
                    let pos = match children.iter().position(|c| *c == left_id) {
                        Some(p) => p,
                        None => return, // Child not found in parent; skip.
                    };
                    keys.insert(pos, separator);
                    children.insert(pos + 1, right_id);
                    dirty.push(parent_id);
                }

                // The parent may now need splitting too.
                self.maybe_split(parent_id, dirty);
            }
        }
    }

    /// Find the parent of a given page. Returns None if the page is the root.
    fn find_parent(&self, child_id: PageId) -> Option<PageId> {
        for (pid, node) in &self.nodes {
            if let BTreeNode::Internal { children, .. } = node {
                if children.contains(&child_id) {
                    return Some(*pid);
                }
            }
        }
        None
    }

    /// Persist dirty pages to the buffer pool.
    fn persist_pages(&self, dirty: &[PageId]) -> Vec<(Message, Destination)> {
        let mut msgs = Vec::new();
        for &page_id in dirty {
            if let Some(node) = self.nodes.get(&page_id) {
                let data = node.serialize();
                msgs.push((
                    Message::BufPoolWritePage {
                        page_id: page_id.0,
                        data,
                    },
                    Destination {
                        actor: self.buf_pool_actor,
                        delay: 0,
                    },
                ));
            }
        }
        msgs
    }

    /// Encode a WAL record for a Put operation.
    fn encode_wal_put(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut data = Vec::with_capacity(1 + 4 + key.len() + 4 + value.len());
        data.push(WAL_OP_PUT);
        data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        data.extend_from_slice(key);
        data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        data.extend_from_slice(value);
        data
    }

    /// Encode a WAL record for a Delete operation.
    fn encode_wal_delete(key: &[u8]) -> Vec<u8> {
        let mut data = Vec::with_capacity(1 + 4 + key.len());
        data.push(WAL_OP_DELETE);
        data.extend_from_slice(&(key.len() as u32).to_le_bytes());
        data.extend_from_slice(key);
        data
    }

    /// Decode a WAL record. Returns (op_type, key, optional_value).
    pub fn decode_wal_record(data: &[u8]) -> Option<(u8, Vec<u8>, Option<Vec<u8>>)> {
        if data.is_empty() {
            return None;
        }
        let op = data[0];
        let mut pos = 1;

        let key_len = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;
        let key = data.get(pos..pos + key_len)?.to_vec();
        pos += key_len;

        match op {
            WAL_OP_PUT => {
                let val_len =
                    u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let value = data.get(pos..pos + val_len)?.to_vec();
                Some((op, key, Some(value)))
            }
            WAL_OP_DELETE => Some((op, key, None)),
            _ => None,
        }
    }

    /// Replay WAL records to rebuild in-memory state.
    pub fn replay_wal(&mut self, records: &[(u64, Vec<u8>)]) {
        for (_lsn, data) in records {
            if let Some((op, key, value)) = Self::decode_wal_record(data) {
                match op {
                    WAL_OP_PUT => {
                        if let Some(v) = value {
                            self.put(key, v);
                        }
                    }
                    WAL_OP_DELETE => {
                        self.delete(&key);
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn root_page_id(&self) -> Option<PageId> {
        self.root
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl StateMachine for BTreeEngine {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::BTreeGet { key } => {
                let value = self.get(&key);
                Some(vec![(
                    Message::BTreeGetResult { key, value },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            Message::BTreePut { key, value } => {
                let mut outgoing = Vec::new();

                // Write-ahead: log to WAL first.
                let wal_data = Self::encode_wal_put(&key, &value);
                outgoing.push((
                    Message::WalAppend { data: wal_data },
                    Destination {
                        actor: self.wal_actor,
                        delay: 0,
                    },
                ));

                // Apply to in-memory tree.
                let dirty = self.put(key, value);

                // Persist dirty pages to buffer pool.
                outgoing.extend(self.persist_pages(&dirty));

                // Reply to client.
                outgoing.push((
                    Message::BTreePutOk,
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                ));

                Some(outgoing)
            }

            Message::BTreeDelete { key } => {
                let mut outgoing = Vec::new();

                // Write-ahead: log to WAL.
                let wal_data = Self::encode_wal_delete(&key);
                outgoing.push((
                    Message::WalAppend { data: wal_data },
                    Destination {
                        actor: self.wal_actor,
                        delay: 0,
                    },
                ));

                // Apply to in-memory tree.
                let (found, dirty) = self.delete(&key);

                // Persist dirty pages.
                outgoing.extend(self.persist_pages(&dirty));

                // Reply to client.
                outgoing.push((
                    Message::BTreeDeleteOk { found },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                ));

                Some(outgoing)
            }

            Message::BTreeScan { start, end } => {
                let entries = self.scan(
                    start.as_deref(),
                    end.as_deref(),
                );
                Some(vec![(
                    Message::BTreeScanResult { entries },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            // WAL replay for crash recovery.
            Message::WalRecords { records } => {
                self.replay_wal(&records);
                None
            }

            // Ignore buffer pool acknowledgments.
            Message::BufPoolWriteOk { .. } | Message::WalAppendOk { .. } => None,

            _ => None,
        }
    }

    fn tick(&mut self, _now: u64) -> Option<Vec<(Message, Destination)>> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
