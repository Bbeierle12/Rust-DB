use std::collections::BTreeMap;

use crate::backup::checkpoint::Checkpoint;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// Internal state of an in-progress backup capture.
struct PartialCapture {
    checkpoint_id: u64,
    tick: u64,
    files: BTreeMap<u64, Vec<u8>>,
    fsynced_lsn: u64,
}

/// File IDs to capture during backup (WAL=0, B-tree pages=1).
const BACKUP_FILE_IDS: [u64; 2] = [0, 1];

enum BackupState {
    Idle,
    WaitingForFlush,
    WaitingForFsync,
    /// Reading disk files. `files_read` tracks how many we've completed.
    WaitingForDiskRead { files_read: usize },
    /// Writing files back to disk during restore. tracks remaining writes.
    WaitingForRestoreWrites { remaining: usize, file_ids_to_fsync: Vec<u64> },
    /// Fsyncing restored files.
    WaitingForRestoreFsync { remaining: usize },
}

/// BackupManager state machine.
///
/// Coordinates a consistent checkpoint: flush dirty pages, fsync WAL,
/// then capture full disk state. Can also restore from a checkpoint.
pub struct BackupManager {
    id: ActorId,
    buf_pool_actor: ActorId,
    wal_actor: ActorId,
    disk_actor: ActorId,
    requester: Option<ActorId>,
    state: BackupState,
    next_checkpoint_id: u64,
    checkpoints: BTreeMap<u64, Checkpoint>,
    current_capture: Option<PartialCapture>,
}

impl BackupManager {
    pub fn new(
        id: ActorId,
        buf_pool_actor: ActorId,
        wal_actor: ActorId,
        disk_actor: ActorId,
    ) -> Self {
        Self {
            id,
            buf_pool_actor,
            wal_actor,
            disk_actor,
            requester: None,
            state: BackupState::Idle,
            next_checkpoint_id: 0,
            checkpoints: BTreeMap::new(),
            current_capture: None,
        }
    }

    /// Get a checkpoint by ID.
    pub fn get_checkpoint(&self, id: u64) -> Option<&Checkpoint> {
        self.checkpoints.get(&id)
    }

    /// Number of stored checkpoints.
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }

    fn send_disk_read_for_index(
        &self,
        file_index: usize,
    ) -> Option<(Message, Destination)> {
        let file_id = *BACKUP_FILE_IDS.get(file_index)?;
        Some((
            Message::DiskRead { file_id, offset: 0, len: u64::MAX },
            Destination { actor: self.disk_actor, delay: 0 },
        ))
    }

    fn err_msg(&self, reason: impl Into<String>) -> (Message, Destination) {
        (
            Message::BackupErr { reason: reason.into() },
            Destination { actor: self.requester.unwrap_or(self.id), delay: 0 },
        )
    }
}

impl StateMachine for BackupManager {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            // ── Initiate backup ───────────────────────────────────────────
            Message::BackupCreate => {
                if !matches!(self.state, BackupState::Idle) {
                    return Some(vec![self.err_msg("backup already in progress")]);
                }
                self.requester = Some(from);
                self.state = BackupState::WaitingForFlush;
                self.current_capture = Some(PartialCapture {
                    checkpoint_id: self.next_checkpoint_id,
                    tick: 0,
                    files: BTreeMap::new(),
                    fsynced_lsn: 0,
                });
                Some(vec![(
                    Message::BufPoolFlush,
                    Destination { actor: self.buf_pool_actor, delay: 0 },
                )])
            }

            Message::BufPoolFlushOk if matches!(self.state, BackupState::WaitingForFlush) => {
                self.state = BackupState::WaitingForFsync;
                Some(vec![(
                    Message::WalFsync,
                    Destination { actor: self.wal_actor, delay: 0 },
                )])
            }

            Message::BufPoolFlushErr { reason } if matches!(self.state, BackupState::WaitingForFlush) => {
                self.state = BackupState::Idle;
                self.current_capture = None;
                Some(vec![self.err_msg(format!("flush failed: {}", reason))])
            }

            Message::WalFsyncOk if matches!(self.state, BackupState::WaitingForFsync) => {
                // Record fsynced LSN (we don't have direct access to WalWriter state,
                // so we use next_checkpoint_id as a proxy LSN counter for now).
                if let Some(capture) = &mut self.current_capture {
                    capture.fsynced_lsn = self.next_checkpoint_id;
                }
                self.state = BackupState::WaitingForDiskRead { files_read: 0 };
                // Start reading disk files.
                self.send_disk_read_for_index(0).map(|m| vec![m])
            }

            Message::WalFsyncErr { reason } if matches!(self.state, BackupState::WaitingForFsync) => {
                self.state = BackupState::Idle;
                self.current_capture = None;
                Some(vec![self.err_msg(format!("fsync failed: {}", reason))])
            }

            Message::DiskReadOk { file_id, data, .. } => {
                let files_read = match &self.state {
                    BackupState::WaitingForDiskRead { files_read } => *files_read,
                    _ => return None,
                };

                // Verify this is a file we're waiting for.
                if file_id != BACKUP_FILE_IDS[files_read] {
                    return None;
                }

                if let Some(capture) = &mut self.current_capture {
                    capture.files.insert(file_id, data);
                }

                let next_index = files_read + 1;
                if next_index < BACKUP_FILE_IDS.len() {
                    // More files to read.
                    self.state = BackupState::WaitingForDiskRead { files_read: next_index };
                    self.send_disk_read_for_index(next_index).map(|m| vec![m])
                } else {
                    // All files read — assemble checkpoint.
                    let capture = self.current_capture.take()?;
                    let checkpoint = Checkpoint {
                        id: capture.checkpoint_id,
                        captured_at_tick: capture.tick,
                        last_fsynced_lsn: capture.fsynced_lsn,
                        files: capture.files,
                    };
                    let checkpoint_id = checkpoint.id;
                    self.checkpoints.insert(checkpoint_id, checkpoint);
                    self.next_checkpoint_id += 1;
                    self.state = BackupState::Idle;
                    let requester = self.requester.take().unwrap_or(self.id);
                    Some(vec![(
                        Message::BackupCreated { checkpoint_id },
                        Destination { actor: requester, delay: 0 },
                    )])
                }
            }

            // ── Restore from checkpoint ───────────────────────────────────
            Message::BackupRestore { checkpoint_id } => {
                if !matches!(self.state, BackupState::Idle) {
                    return Some(vec![self.err_msg("backup in progress, cannot restore")]);
                }
                let checkpoint = match self.checkpoints.get(&checkpoint_id) {
                    Some(c) => c.clone(),
                    None => {
                        return Some(vec![self.err_msg(format!("checkpoint {} not found", checkpoint_id))]);
                    }
                };
                self.requester = Some(from);

                let file_count = checkpoint.files.len();
                if file_count == 0 {
                    self.state = BackupState::Idle;
                    return Some(vec![(
                        Message::BackupRestored,
                        Destination { actor: from, delay: 0 },
                    )]);
                }

                let file_ids_to_fsync: Vec<u64> = checkpoint.files.keys().cloned().collect();
                self.state = BackupState::WaitingForRestoreWrites {
                    remaining: file_count,
                    file_ids_to_fsync,
                };

                // Write all files back to disk.
                let writes: Vec<(Message, Destination)> = checkpoint
                    .files
                    .iter()
                    .map(|(&fid, data)| {
                        (
                            Message::DiskWrite { file_id: fid, offset: 0, data: data.clone() },
                            Destination { actor: self.disk_actor, delay: 0 },
                        )
                    })
                    .collect();
                Some(writes)
            }

            Message::DiskWriteOk { .. } => {
                let (remaining, file_ids_to_fsync) = match &mut self.state {
                    BackupState::WaitingForRestoreWrites { remaining, file_ids_to_fsync } => {
                        *remaining -= 1;
                        let rem = *remaining;
                        let ids = file_ids_to_fsync.clone();
                        (rem, ids)
                    }
                    _ => return None,
                };

                if remaining == 0 {
                    let fsync_count = file_ids_to_fsync.len();
                    self.state = BackupState::WaitingForRestoreFsync { remaining: fsync_count };
                    let fsyncs: Vec<(Message, Destination)> = file_ids_to_fsync
                        .into_iter()
                        .map(|fid| {
                            (
                                Message::DiskFsync { file_id: fid },
                                Destination { actor: self.disk_actor, delay: 0 },
                            )
                        })
                        .collect();
                    Some(fsyncs)
                } else {
                    None
                }
            }

            Message::DiskFsyncOk { .. } => {
                let remaining = match &mut self.state {
                    BackupState::WaitingForRestoreFsync { remaining } => {
                        *remaining -= 1;
                        *remaining
                    }
                    _ => return None,
                };

                if remaining == 0 {
                    self.state = BackupState::Idle;
                    let requester = self.requester.take().unwrap_or(self.id);
                    Some(vec![(
                        Message::BackupRestored,
                        Destination { actor: requester, delay: 0 },
                    )])
                } else {
                    None
                }
            }

            Message::BackupReadDisk { file_id } => {
                // Convenience: read an entire file from disk.
                Some(vec![(
                    Message::DiskRead { file_id, offset: 0, len: u64::MAX },
                    Destination { actor: self.disk_actor, delay: 0 },
                )])
            }

            _ => None,
        }
    }

    fn tick(&mut self, now: u64) -> Option<Vec<(Message, Destination)>> {
        // Update tick in current capture.
        if let Some(capture) = &mut self.current_capture {
            capture.tick = now;
        }
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
