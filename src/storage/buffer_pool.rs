use std::collections::BTreeMap;

use crate::config::{self, DatabaseConfig};
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// LRU-2 access history for a cached page.
#[derive(Debug, Clone)]
struct PageEntry {
    data: Vec<u8>,
    dirty: bool,
    /// Last two access timestamps. history[0] is older, history[1] is more recent.
    history: [Option<u64>; 2],
}

impl PageEntry {
    fn new(data: Vec<u8>, now: u64) -> Self {
        Self {
            data,
            dirty: false,
            history: [None, Some(now)],
        }
    }

    fn touch(&mut self, now: u64) {
        self.history[0] = self.history[1];
        self.history[1] = Some(now);
    }

    /// LRU-2 eviction key: the second-to-last access time.
    /// Pages accessed only once (history[0] = None) are evicted first.
    fn eviction_key(&self) -> u64 {
        self.history[0].unwrap_or(0)
    }
}

/// Buffer pool state machine.
///
/// Caches pages in memory with LRU-2 eviction. Reads from and writes to
/// the simulated disk through the message bus. Uses BTreeMap for
/// deterministic iteration order.
pub struct BufferPool {
    id: ActorId,
    disk_actor: ActorId,
    file_id: u64,
    capacity: usize,
    /// Cached pages: page_id → entry.
    cache: BTreeMap<u64, PageEntry>,
    /// Current simulation tick (updated on each tick/receive).
    now: u64,
    /// Pending read requests: page_id → list of requesters.
    pending_reads: BTreeMap<u64, Vec<ActorId>>,
    /// Pending flush: number of dirty pages still being written.
    flush_pending: Option<(ActorId, usize)>,
}

impl BufferPool {
    pub fn new(id: ActorId, disk_actor: ActorId, cfg: &DatabaseConfig) -> Self {
        Self {
            id,
            disk_actor,
            file_id: cfg.btree_data_file_id,
            capacity: cfg.buffer_pool_pages,
            cache: BTreeMap::new(),
            now: 0,
            pending_reads: BTreeMap::new(),
            flush_pending: None,
        }
    }

    pub fn cached_count(&self) -> usize {
        self.cache.len()
    }

    pub fn dirty_count(&self) -> usize {
        self.cache.values().filter(|e| e.dirty).count()
    }

    /// Evict the LRU-2 page to make room. If the evicted page is dirty,
    /// returns a DiskWrite message to flush it first.
    fn evict_one(&mut self) -> Option<(Message, Destination)> {
        if self.cache.len() < self.capacity {
            return None;
        }

        // Find the page with the smallest eviction key (oldest second-to-last access).
        let victim_id = self
            .cache
            .iter()
            .min_by_key(|(_, entry)| entry.eviction_key())
            .map(|(&id, _)| id)?;

        let entry = self.cache.remove(&victim_id)?;

        if entry.dirty {
            // Write dirty page to disk before evicting.
            let offset = victim_id * config::PAGE_SIZE as u64;
            Some((
                Message::DiskWrite {
                    file_id: self.file_id,
                    offset,
                    data: entry.data,
                },
                Destination {
                    actor: self.disk_actor,
                    delay: 0,
                },
            ))
        } else {
            None
        }
    }

    /// Flush all dirty pages. Returns DiskWrite messages for each.
    fn flush_dirty(&self) -> Vec<(Message, Destination)> {
        let mut writes = Vec::new();
        for (&page_id, entry) in &self.cache {
            if entry.dirty {
                let offset = page_id * config::PAGE_SIZE as u64;
                writes.push((
                    Message::DiskWrite {
                        file_id: self.file_id,
                        offset,
                        data: entry.data.clone(),
                    },
                    Destination {
                        actor: self.disk_actor,
                        delay: 0,
                    },
                ));
            }
        }
        writes
    }
}

impl StateMachine for BufferPool {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::BufPoolWritePage { page_id, data } => {
                let mut outgoing = Vec::new();

                // Evict if at capacity and this is a new page.
                if !self.cache.contains_key(&page_id)
                    && let Some(write_msg) = self.evict_one()
                {
                    outgoing.push(write_msg);
                }

                // Insert or update the page.
                if let Some(entry) = self.cache.get_mut(&page_id) {
                    entry.data = data;
                    entry.dirty = true;
                    entry.touch(self.now);
                } else {
                    let mut entry = PageEntry::new(data, self.now);
                    entry.dirty = true;
                    self.cache.insert(page_id, entry);
                }

                outgoing.push((
                    Message::BufPoolWriteOk { page_id },
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                ));
                Some(outgoing)
            }

            Message::BufPoolReadPage { page_id } => {
                if let Some(entry) = self.cache.get_mut(&page_id) {
                    // Cache hit.
                    entry.touch(self.now);
                    Some(vec![(
                        Message::BufPoolPageData {
                            page_id,
                            data: entry.data.clone(),
                        },
                        Destination {
                            actor: from,
                            delay: 0,
                        },
                    )])
                } else {
                    // Cache miss — read from disk.
                    self.pending_reads.entry(page_id).or_default().push(from);
                    Some(vec![(
                        Message::DiskRead {
                            file_id: self.file_id,
                            offset: page_id * config::PAGE_SIZE as u64,
                            len: config::PAGE_SIZE as u64,
                        },
                        Destination {
                            actor: self.disk_actor,
                            delay: 0,
                        },
                    )])
                }
            }

            Message::DiskReadOk {
                file_id,
                offset,
                data,
            } if file_id == self.file_id => {
                let page_id = offset / config::PAGE_SIZE as u64;

                if data.is_empty() {
                    // Page doesn't exist on disk.
                    if let Some(requesters) = self.pending_reads.remove(&page_id) {
                        let msgs = requesters
                            .into_iter()
                            .map(|r| {
                                (
                                    Message::BufPoolPageNotFound { page_id },
                                    Destination { actor: r, delay: 0 },
                                )
                            })
                            .collect();
                        return Some(msgs);
                    }
                    return None;
                }

                // Evict if needed.
                let mut outgoing = Vec::new();
                if !self.cache.contains_key(&page_id)
                    && let Some(write_msg) = self.evict_one()
                {
                    outgoing.push(write_msg);
                }

                // Cache the page.
                let entry = PageEntry::new(data.clone(), self.now);
                self.cache.insert(page_id, entry);

                // Reply to all waiting readers.
                if let Some(requesters) = self.pending_reads.remove(&page_id) {
                    for requester in requesters {
                        outgoing.push((
                            Message::BufPoolPageData {
                                page_id,
                                data: data.clone(),
                            },
                            Destination {
                                actor: requester,
                                delay: 0,
                            },
                        ));
                    }
                }

                Some(outgoing)
            }

            Message::DiskReadErr {
                file_id,
                offset,
                reason,
            } if file_id == self.file_id => {
                let page_id = offset / config::PAGE_SIZE as u64;
                if let Some(requesters) = self.pending_reads.remove(&page_id) {
                    let msgs = requesters
                        .into_iter()
                        .map(|r| {
                            (
                                Message::BufPoolPageNotFound { page_id },
                                Destination { actor: r, delay: 0 },
                            )
                        })
                        .collect();
                    return Some(msgs);
                }
                let _ = reason;
                None
            }

            Message::BufPoolFlush => {
                let writes = self.flush_dirty();
                let dirty_count = writes.len();
                if dirty_count == 0 {
                    return Some(vec![(
                        Message::BufPoolFlushOk,
                        Destination {
                            actor: from,
                            delay: 0,
                        },
                    )]);
                }
                self.flush_pending = Some((from, dirty_count));
                Some(writes)
            }

            Message::DiskWriteOk { file_id, offset } if file_id == self.file_id => {
                let page_id = offset / config::PAGE_SIZE as u64;
                // Mark page as clean.
                if let Some(entry) = self.cache.get_mut(&page_id) {
                    entry.dirty = false;
                }

                // Track flush completion.
                if let Some((requester, ref mut remaining)) = self.flush_pending {
                    *remaining = remaining.saturating_sub(1);
                    if *remaining == 0 {
                        self.flush_pending = None;
                        return Some(vec![(
                            Message::BufPoolFlushOk,
                            Destination {
                                actor: requester,
                                delay: 0,
                            },
                        )]);
                    }
                }
                None
            }

            Message::DiskWriteErr {
                file_id,
                offset: _,
                reason,
            } if file_id == self.file_id => {
                if let Some((requester, _)) = self.flush_pending.take() {
                    return Some(vec![(
                        Message::BufPoolFlushErr { reason },
                        Destination {
                            actor: requester,
                            delay: 0,
                        },
                    )]);
                }
                None
            }

            _ => None,
        }
    }

    fn tick(&mut self, now: u64) -> Option<Vec<(Message, Destination)>> {
        self.now = now;
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
