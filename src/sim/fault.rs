use crate::sim::rng::SeededRng;
use crate::traits::message::{Envelope, Message};

/// Fault injection modes that can be enabled independently.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability of dropping a message entirely.
    pub drop_probability: f64,
    /// Probability of injecting an IO error on disk writes.
    pub disk_write_error_probability: f64,
    /// Probability of injecting an IO error on disk reads.
    pub disk_read_error_probability: f64,
    /// Probability of injecting an fsync failure.
    pub fsync_error_probability: f64,
    /// Probability of corrupting data on read.
    pub corruption_probability: f64,
}

impl FaultConfig {
    /// No faults — clean execution.
    pub fn none() -> Self {
        Self {
            drop_probability: 0.0,
            disk_write_error_probability: 0.0,
            disk_read_error_probability: 0.0,
            fsync_error_probability: 0.0,
            corruption_probability: 0.0,
        }
    }
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self::none()
    }
}

/// Injects faults into messages flowing through the bus.
///
/// Sits between the bus and state machines. Decisions are deterministic
/// because they flow through SeededRng.
#[derive(Debug)]
pub struct FaultInjector {
    config: FaultConfig,
}

impl FaultInjector {
    pub fn new(config: FaultConfig) -> Self {
        Self { config }
    }

    /// Inspect an envelope and decide whether to drop, corrupt, or pass it.
    /// Returns None if the message should be dropped.
    pub fn maybe_fault(
        &self,
        envelope: &mut Envelope,
        rng: &mut SeededRng,
    ) -> FaultAction {
        // Check for message drop.
        if rng.chance(self.config.drop_probability) {
            return FaultAction::Drop;
        }

        // Check for IO fault injection based on message type.
        match &envelope.message {
            Message::DiskWriteOk { file_id, offset } => {
                if rng.chance(self.config.disk_write_error_probability) {
                    let file_id = *file_id;
                    let offset = *offset;
                    envelope.message = Message::DiskWriteErr {
                        file_id,
                        offset,
                        reason: "simulated disk write failure".into(),
                    };
                    return FaultAction::Mutated;
                }
            }
            Message::DiskReadOk { file_id, offset, data } => {
                if rng.chance(self.config.corruption_probability) {
                    let mut corrupted = data.clone();
                    if !corrupted.is_empty() {
                        let idx = rng.next_range(corrupted.len() as u64) as usize;
                        corrupted[idx] ^= 0xFF;
                    }
                    let file_id = *file_id;
                    let offset = *offset;
                    envelope.message = Message::DiskReadOk {
                        file_id,
                        offset,
                        data: corrupted,
                    };
                    return FaultAction::Mutated;
                }
                if rng.chance(self.config.disk_read_error_probability) {
                    let file_id = *file_id;
                    let offset = *offset;
                    envelope.message = Message::DiskReadErr {
                        file_id,
                        offset,
                        reason: "simulated disk read failure".into(),
                    };
                    return FaultAction::Mutated;
                }
            }
            Message::DiskFsyncOk { file_id } => {
                if rng.chance(self.config.fsync_error_probability) {
                    let file_id = *file_id;
                    envelope.message = Message::DiskFsyncErr {
                        file_id,
                        reason: "simulated fsync failure".into(),
                    };
                    return FaultAction::Mutated;
                }
            }
            _ => {}
        }

        FaultAction::Pass
    }

    pub fn config(&self) -> &FaultConfig {
        &self.config
    }
}

/// What the fault injector decided.
#[derive(Debug, PartialEq, Eq)]
pub enum FaultAction {
    /// Deliver the message unchanged.
    Pass,
    /// Message was mutated in place (e.g., IO success → IO error).
    Mutated,
    /// Drop the message entirely.
    Drop,
}
