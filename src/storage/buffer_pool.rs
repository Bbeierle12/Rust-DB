use std::collections::BTreeMap;

use crate::config::{self, DatabaseConfig};
use crate::storage::page::PAGE_TYPE_INTERNAL;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// Eviction-policy weights discovered by the edge-loop db-eviction
/// autoresearch loop (gemma iteration-300 vector, promoted 2026-07-11 on
/// holdout generalization: −23.5% disk traffic vs the previous LRU-2 on
/// unseen traces). Do not hand-tune: any change must re-run the loop's
/// T3 evaluation, and the differential test against
/// tests/fixtures/discovered_policy_vectors.json pins these exact values.
const W_AGE_PENULT: f64 = 4.0;
const W_AGE_LAST: f64 = 8.5;
const W_DIRTY: f64 = -1.5;
const W_INTERNAL: f64 = -7.0;
const W_FREQ: f64 = 3.5;
const W_GAP: f64 = 2.0;
const W_AGE_INSERT: f64 = -3.0;

/// Access statistics for a cached page.
#[derive(Debug, Clone)]
struct PageEntry {
    data: Vec<u8>,
    dirty: bool,
    /// Last two access timestamps. history[0] is older, history[1] is more recent.
    history: [Option<u64>; 2],
    /// Total accesses, tick of first insertion, and summed inter-access
    /// gaps — inputs to the discovered eviction score.
    count: u64,
    first: u64,
    sum_gaps: u64,
}

impl PageEntry {
    fn new(data: Vec<u8>, now: u64) -> Self {
        Self {
            data,
            dirty: false,
            history: [None, Some(now)],
            count: 1,
            first: now,
            sum_gaps: 0,
        }
    }

    fn touch(&mut self, now: u64) {
        // Gap accumulates before the history shift (same order as the
        // research scaffold, which the differential vectors encode).
        self.sum_gaps += now - self.history[1].unwrap_or(0);
        self.history[0] = self.history[1];
        self.history[1] = Some(now);
        self.count += 1;
    }

    /// Evict-desirability under the discovered policy: HIGHEST score is
    /// evicted. Features are normalized by `now`; f64 evaluation order
    /// mirrors the research genome so victim choice is bit-identical.
    /// Retains internal pages and hot pages, defers dirty write-backs,
    /// and evicts recently-inserted (one-shot burst) pages young.
    fn evict_score(&self, now: u64) -> f64 {
        let nowf = now as f64;
        let mut s = W_AGE_PENULT * (now - self.history[0].unwrap_or(0)) as f64 / nowf;
        s += W_AGE_LAST * (now - self.history[1].unwrap_or(0)) as f64 / nowf;
        if self.dirty {
            s += W_DIRTY;
        }
        if self.data.first() == Some(&PAGE_TYPE_INTERNAL) {
            s += W_INTERNAL;
        }
        s += W_FREQ * self.count as f64 / nowf;
        let gap = if self.count > 1 {
            (self.sum_gaps as f64 / (self.count - 1) as f64) / nowf
        } else {
            1.0
        };
        s += W_GAP * gap;
        s += W_AGE_INSERT * (now - self.first) as f64 / nowf;
        s
    }
}

/// Buffer pool state machine.
///
/// Caches pages in memory; eviction uses the discovered weighted score
/// (see the W_* constants above). Reads from and writes to the simulated
/// disk through the message bus. Uses BTreeMap for deterministic
/// iteration order — ascending page id, which is also the tie-break rule.
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
    /// Eviction victims in order (feature `trace-capture` only) — read by
    /// the differential test that pins victim-for-victim parity with the
    /// research reference.
    #[cfg(feature = "trace-capture")]
    evict_log: Vec<u64>,
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
            #[cfg(feature = "trace-capture")]
            evict_log: Vec::new(),
        }
    }

    /// Drain and return the eviction victim sequence.
    #[cfg(feature = "trace-capture")]
    pub fn take_evictions(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.evict_log)
    }

    pub fn cached_count(&self) -> usize {
        self.cache.len()
    }

    pub fn dirty_count(&self) -> usize {
        self.cache.values().filter(|e| e.dirty).count()
    }

    /// Evict one page under the discovered policy to make room. If the
    /// evicted page is dirty, returns a DiskWrite message to flush it first.
    fn evict_one(&mut self) -> Option<(Message, Destination)> {
        if self.cache.len() < self.capacity {
            return None;
        }

        // Highest score evicts; ascending iteration + strict > keeps the
        // first maximum, so ties go to the lowest page id.
        let now = self.now.max(1);
        let mut victim: Option<(f64, u64)> = None;
        for (&id, entry) in &self.cache {
            let s = entry.evict_score(now);
            match victim {
                Some((best, _)) if s <= best => {}
                _ => victim = Some((s, id)),
            }
        }
        let victim_id = victim.map(|(_, id)| id)?;
        #[cfg(feature = "trace-capture")]
        self.evict_log.push(victim_id);

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
