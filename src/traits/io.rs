/// IO traits with real and simulated implementations.
///
/// In the state-machine architecture, IO operations are modeled as messages.
/// These traits exist for the *real* (non-simulated) production path.
/// In simulation, all IO goes through the message bus instead.
use std::io;

/// Disk IO operations.
pub trait DiskIO {
    fn write(&mut self, file_id: u64, offset: u64, data: &[u8]) -> io::Result<()>;
    fn read(&self, file_id: u64, offset: u64, len: u64) -> io::Result<Vec<u8>>;
    fn fsync(&mut self, file_id: u64) -> io::Result<()>;
}

/// Network IO operations (Stage 5+).
pub trait NetworkIO {
    fn send(&mut self, dest: &str, data: &[u8]) -> io::Result<()>;
    fn recv(&mut self) -> io::Result<Option<Vec<u8>>>;
}

/// Clock abstraction — replaced by SimClock in simulation.
pub trait Clock {
    fn now(&self) -> u64;
}
