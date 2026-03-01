use crate::config;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// WAL record format (on disk):
///   length: u32 (payload length, not including header)
///   crc32:  u32 (checksum of payload only)
///   payload: [u8; length]

/// WAL writer state machine.
///
/// Accepts WalAppend messages, formats records with length + CRC32 headers,
/// and writes them through the simulated disk via DiskWrite messages.
/// Tracks the current LSN (log sequence number) for each record.
pub struct WalWriter {
    id: ActorId,
    disk_actor: ActorId,
    reply_to: ActorId,
    file_id: u64,
    /// Current write offset in the WAL file.
    offset: u64,
    /// Next LSN to assign.
    next_lsn: u64,
    /// LSN of the last fsynced record.
    fsynced_lsn: u64,
    /// Pending appends waiting for disk write confirmation.
    /// Maps offset → (lsn, requester) for correlating DiskWriteOk responses.
    pending_writes: Vec<(u64, u64)>,
    /// Whether we're waiting for an fsync response.
    fsync_pending: bool,
}

impl WalWriter {
    pub fn new(id: ActorId, disk_actor: ActorId, reply_to: ActorId, file_id: u64) -> Self {
        Self {
            id,
            disk_actor,
            reply_to,
            file_id,
            offset: 0,
            next_lsn: 0,
            fsynced_lsn: 0,
            pending_writes: Vec::new(),
            fsync_pending: false,
        }
    }

    /// Encode a WAL record: [length: u32][crc32: u32][payload].
    fn encode_record(data: &[u8]) -> Vec<u8> {
        let len = data.len() as u32;
        let crc = crc32fast::hash(data);
        let mut record = Vec::with_capacity(config::WAL_HEADER_SIZE + data.len());
        record.extend_from_slice(&len.to_le_bytes());
        record.extend_from_slice(&crc.to_le_bytes());
        record.extend_from_slice(data);
        record
    }

    pub fn current_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn fsynced_lsn(&self) -> u64 {
        self.fsynced_lsn
    }
}

impl StateMachine for WalWriter {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, _from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::WalAppend { data } => {
                let lsn = self.next_lsn;
                self.next_lsn += 1;

                let record = Self::encode_record(&data);
                let write_offset = self.offset;
                self.offset += record.len() as u64;

                self.pending_writes.push((write_offset, lsn));

                Some(vec![(
                    Message::DiskWrite {
                        file_id: self.file_id,
                        offset: write_offset,
                        data: record,
                    },
                    Destination {
                        actor: self.disk_actor,
                        delay: 0,
                    },
                )])
            }
            Message::DiskWriteOk { file_id, offset } if file_id == self.file_id => {
                // Find the pending write for this offset.
                if let Some(pos) = self.pending_writes.iter().position(|(o, _)| *o == offset) {
                    let (_offset, lsn) = self.pending_writes.remove(pos);
                    Some(vec![(
                        Message::WalAppendOk { lsn },
                        Destination {
                            actor: self.reply_to,
                            delay: 0,
                        },
                    )])
                } else {
                    None
                }
            }
            Message::DiskWriteErr {
                file_id,
                offset,
                reason,
            } if file_id == self.file_id => {
                // Remove the pending write.
                self.pending_writes.retain(|(o, _)| *o != offset);
                Some(vec![(
                    Message::WalAppendErr { reason },
                    Destination {
                        actor: self.reply_to,
                        delay: 0,
                    },
                )])
            }
            Message::WalFsync => {
                self.fsync_pending = true;
                Some(vec![(
                    Message::DiskFsync {
                        file_id: self.file_id,
                    },
                    Destination {
                        actor: self.disk_actor,
                        delay: 0,
                    },
                )])
            }
            Message::DiskFsyncOk { file_id } if file_id == self.file_id => {
                self.fsync_pending = false;
                self.fsynced_lsn = self.next_lsn;
                Some(vec![(
                    Message::WalFsyncOk,
                    Destination {
                        actor: self.reply_to,
                        delay: 0,
                    },
                )])
            }
            Message::DiskFsyncErr { file_id, reason } if file_id == self.file_id => {
                self.fsync_pending = false;
                Some(vec![(
                    Message::WalFsyncErr { reason },
                    Destination {
                        actor: self.reply_to,
                        delay: 0,
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
