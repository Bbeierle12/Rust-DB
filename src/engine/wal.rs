//! Real filesystem WAL (Write-Ahead Log).
//!
//! Writes records to an actual file on disk with CRC32 checksums and fsync.
//! Format per record: [length: u32][crc32: u32][payload: [u8; length]]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Real filesystem-backed WAL.
pub struct FileWal {
    path: PathBuf,
    file: File,
    /// Current write offset.
    offset: u64,
    /// Next LSN to assign.
    next_lsn: u64,
}

impl FileWal {
    /// Open or create a WAL file at the given path.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let offset = file.metadata()?.len();

        // Count existing records to determine next LSN.
        let mut wal = Self {
            path,
            file,
            offset,
            next_lsn: 0,
        };

        let records = wal.read_all()?;
        wal.next_lsn = records.len() as u64;

        Ok(wal)
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    pub fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let lsn = self.next_lsn;
        self.next_lsn += 1;

        let len = data.len() as u32;
        let crc = crc32fast::hash(data);

        self.file.seek(SeekFrom::Start(self.offset))?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(data)?;

        self.offset += 8 + data.len() as u64;

        Ok(lsn)
    }

    /// Fsync the WAL to disk.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Append and immediately fsync. Returns the LSN.
    pub fn append_sync(&mut self, data: &[u8]) -> io::Result<u64> {
        let lsn = self.append(data)?;
        self.fsync()?;
        Ok(lsn)
    }

    /// Read all valid records from the WAL.
    /// Returns (lsn, payload) pairs. Stops at first corrupted record.
    pub fn read_all(&mut self) -> io::Result<Vec<(u64, Vec<u8>)>> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf)?;

        let mut records = Vec::new();
        let mut pos = 0usize;
        let mut lsn = 0u64;

        while pos + 8 <= buf.len() {
            let len = u32::from_le_bytes(
                buf[pos..pos + 4].try_into().unwrap(),
            ) as usize;
            let expected_crc = u32::from_le_bytes(
                buf[pos + 4..pos + 8].try_into().unwrap(),
            );

            if pos + 8 + len > buf.len() {
                // Truncated record — stop here.
                break;
            }

            let payload = &buf[pos + 8..pos + 8 + len];
            let actual_crc = crc32fast::hash(payload);

            if actual_crc != expected_crc {
                // Corrupted record — stop here.
                break;
            }

            records.push((lsn, payload.to_vec()));
            lsn += 1;
            pos += 8 + len;
        }

        Ok(records)
    }

    /// Truncate the WAL (e.g., after a successful checkpoint).
    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.offset = 0;
        self.next_lsn = 0;
        self.fsync()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }
}
