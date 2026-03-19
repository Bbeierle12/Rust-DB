use crate::config::DatabaseConfig;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// WAL reader state machine.
///
/// Reads the WAL file from disk, validates CRC32 checksums, and returns
/// recovered records. Truncates at the first corrupted record (recovery
/// to last valid record).
pub struct WalReader {
    id: ActorId,
    disk_actor: ActorId,
    reply_to: ActorId,
    file_id: u64,
    wal_header_size: usize,
    /// State for progressive read-back.
    read_state: ReadState,
}

#[derive(Debug)]
enum ReadState {
    Idle,
    /// We've requested the full file and are waiting for the response.
    WaitingForData,
}

impl WalReader {
    pub fn new(id: ActorId, disk_actor: ActorId, reply_to: ActorId, cfg: &DatabaseConfig) -> Self {
        Self {
            id,
            disk_actor,
            reply_to,
            file_id: cfg.wal_file_id,
            wal_header_size: cfg.wal_header_size,
            read_state: ReadState::Idle,
        }
    }

    /// Parse WAL records from raw bytes.
    /// Returns (lsn, payload) pairs. Stops at first corruption.
    fn parse_records(data: &[u8], header_size: usize) -> Vec<(u64, Vec<u8>)> {
        let mut records = Vec::new();
        let mut pos = 0;
        let mut lsn = 0u64;

        while pos + header_size <= data.len() {
            // Read length.
            let len_bytes: [u8; 4] = match data.get(pos..pos + 4).and_then(|s| s.try_into().ok()) {
                Some(b) => b,
                None => break, // Truncated header — stop.
            };
            let payload_len = u32::from_le_bytes(len_bytes) as usize;

            // Read CRC32.
            let crc_bytes: [u8; 4] = match data.get(pos + 4..pos + 8).and_then(|s| s.try_into().ok()) {
                Some(b) => b,
                None => break, // Truncated header — stop.
            };
            let stored_crc = u32::from_le_bytes(crc_bytes);

            // Bounds check.
            let payload_start = pos + header_size;
            let payload_end = payload_start + payload_len;
            if payload_end > data.len() {
                // Truncated record — stop here.
                break;
            }

            // Verify checksum.
            let payload = &data[payload_start..payload_end];
            let computed_crc = crc32fast::hash(payload);
            if computed_crc != stored_crc {
                // Corruption detected — stop here (truncate at last valid).
                break;
            }

            records.push((lsn, payload.to_vec()));
            lsn += 1;
            pos = payload_end;
        }

        records
    }
}

impl StateMachine for WalReader {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, _from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            Message::WalReadAll => {
                self.read_state = ReadState::WaitingForData;
                // Request the entire file. Use a large length.
                Some(vec![(
                    Message::DiskRead {
                        file_id: self.file_id,
                        offset: 0,
                        len: u64::MAX,
                    },
                    Destination {
                        actor: self.disk_actor,
                        delay: 0,
                    },
                )])
            }
            Message::DiskReadOk {
                file_id,
                offset: _,
                data,
            } if file_id == self.file_id => {
                self.read_state = ReadState::Idle;
                let records = Self::parse_records(&data, self.wal_header_size);
                Some(vec![(
                    Message::WalRecords { records },
                    Destination {
                        actor: self.reply_to,
                        delay: 0,
                    },
                )])
            }
            Message::DiskReadErr {
                file_id,
                offset: _,
                reason,
            } if file_id == self.file_id => {
                self.read_state = ReadState::Idle;
                Some(vec![(
                    Message::WalAppendErr { reason },
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
