use std::collections::BTreeMap;

use crate::config::DatabaseConfig;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// Simulated disk — an in-memory store that state machines interact with
/// through the message bus, just like a real disk via syscalls.
///
/// Each file is a `Vec<u8>` keyed by file_id. Supports read, write, and
/// fsync operations delivered as messages.
pub struct SimDisk {
    id: ActorId,
    /// File contents: file_id → data.
    files: BTreeMap<u64, Vec<u8>>,
    /// Buffered (unflushed) writes: file_id → (offset, data) pairs.
    /// These are lost on simulated crash.
    write_buffer: BTreeMap<u64, Vec<(u64, Vec<u8>)>>,
    /// Whether to buffer writes (true) or apply immediately (false).
    /// When true, data is only visible after fsync.
    buffered: bool,
    read_latency: u64,
    write_latency: u64,
    fsync_latency: u64,
}

impl SimDisk {
    pub fn new(id: ActorId, buffered: bool, cfg: &DatabaseConfig) -> Self {
        Self {
            id,
            files: BTreeMap::new(),
            write_buffer: BTreeMap::new(),
            buffered,
            read_latency: cfg.sim_disk_read_latency_ticks,
            write_latency: cfg.sim_disk_write_latency_ticks,
            fsync_latency: cfg.sim_fsync_latency_ticks,
        }
    }

    /// Get the persisted contents of a file (only fsynced data).
    pub fn file_contents(&self, file_id: u64) -> Option<&Vec<u8>> {
        self.files.get(&file_id)
    }

    /// Simulate a crash: discard all buffered (unflushed) writes.
    pub fn crash(&mut self) {
        self.write_buffer.clear();
    }

    fn apply_write(&mut self, file_id: u64, offset: u64, data: &[u8]) {
        let file = self.files.entry(file_id).or_default();
        let end = offset as usize + data.len();
        if file.len() < end {
            file.resize(end, 0);
        }
        file[offset as usize..end].copy_from_slice(data);
    }
}

impl StateMachine for SimDisk {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::DiskWrite {
                file_id,
                offset,
                data,
            } => {
                if self.buffered {
                    self.write_buffer
                        .entry(file_id)
                        .or_default()
                        .push((offset, data));
                } else {
                    self.apply_write(file_id, offset, &data);
                }
                Some(vec![(
                    Message::DiskWriteOk { file_id, offset },
                    Destination {
                        actor: from,
                        delay: self.write_latency,
                    },
                )])
            }
            Message::DiskRead {
                file_id,
                offset,
                len,
            } => {
                let data = if let Some(file) = self.files.get(&file_id) {
                    let start = offset as usize;
                    let end = (offset + len) as usize;
                    if start >= file.len() {
                        vec![]
                    } else {
                        let actual_end = end.min(file.len());
                        file[start..actual_end].to_vec()
                    }
                } else {
                    vec![]
                };
                Some(vec![(
                    Message::DiskReadOk {
                        file_id,
                        offset,
                        data,
                    },
                    Destination {
                        actor: from,
                        delay: self.read_latency,
                    },
                )])
            }
            Message::DiskFsync { file_id } => {
                // Flush buffered writes for this file.
                if let Some(writes) = self.write_buffer.remove(&file_id) {
                    for (offset, data) in writes {
                        self.apply_write(file_id, offset, &data);
                    }
                }
                Some(vec![(
                    Message::DiskFsyncOk { file_id },
                    Destination {
                        actor: from,
                        delay: self.fsync_latency,
                    },
                )])
            }
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
