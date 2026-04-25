//! Snapshot (checkpoint) format for Database.
//!
//! A snapshot captures the latest committed value for every key in the
//! MVCC store so the WAL can be truncated. On open, the snapshot is loaded
//! first and the WAL is replayed on top.
//!
//! # File layout
//!
//! ```text
//! data_dir/
//!   snapshot.current   — current valid snapshot (read on open)
//!   snapshot.new       — in-progress write; atomically renamed on commit
//!   wal.log            — truncated after a successful snapshot
//! ```
//!
//! # Binary format (little-endian)
//!
//! ```text
//! [magic:       b"RDBSNP\x00\x01"  8 bytes]
//! [version:     u32                4 bytes]   = 1
//! [next_ts:     u64                8 bytes]
//! [entry_count: u64                8 bytes]
//! for each entry:
//!   [key_len:   u32]
//!   [key bytes]
//!   [flag:      u8]                0 = tombstone, 1 = value present
//!   if flag == 1:
//!     [val_len: u32]
//!     [val bytes]
//! [crc32: u32]     over every byte up to this point
//! ```
//!
//! The CRC covers the header and all entries so a torn write is detected.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

pub const MAGIC: &[u8; 8] = b"RDBSNP\x00\x01";
pub const VERSION: u32 = 1;

pub const SNAPSHOT_CURRENT: &str = "snapshot.current";
pub const SNAPSHOT_NEW: &str = "snapshot.new";

/// A single snapshot entry: key, optional value (None = tombstone), and the
/// commit timestamp under which it's installed into the restored MVCC store.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

/// In-memory view of a snapshot file.
pub struct SnapshotReader {
    pub next_ts: u64,
    pub entries: Vec<SnapshotEntry>,
}

impl SnapshotReader {
    /// Read and fully validate the current snapshot at `data_dir/snapshot.current`.
    /// Returns Ok(None) if the file is absent. Returns Err on torn / mismatched files.
    pub fn read(data_dir: &Path) -> io::Result<Option<Self>> {
        let path = data_dir.join(SNAPSHOT_CURRENT);
        if !path.exists() {
            return Ok(None);
        }
        let mut file = File::open(&path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        parse_snapshot(&buf).map(Some)
    }
}

fn parse_snapshot(buf: &[u8]) -> io::Result<SnapshotReader> {
    let min_len = 8 + 4 + 8 + 8 + 4;
    if buf.len() < min_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot too short: {} bytes", buf.len()),
        ));
    }
    let payload_end = buf.len() - 4;
    let stored_crc = u32::from_le_bytes(buf[payload_end..payload_end + 4].try_into().unwrap());
    let computed_crc = crc32fast::hash(&buf[..payload_end]);
    if stored_crc != computed_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot crc mismatch",
        ));
    }
    if &buf[..8] != MAGIC.as_slice() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot magic mismatch",
        ));
    }
    let version = u32::from_le_bytes(buf[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot version {} unsupported", version),
        ));
    }
    let next_ts = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let entry_count = u64::from_le_bytes(buf[20..28].try_into().unwrap()) as usize;

    let mut pos = 28usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        if pos + 4 > payload_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot truncated at key_len",
            ));
        }
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + key_len + 1 > payload_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot truncated at key body",
            ));
        }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;
        let flag = buf[pos];
        pos += 1;
        let value = match flag {
            0 => None,
            1 => {
                if pos + 4 > payload_end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "snapshot truncated at val_len",
                    ));
                }
                let val_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + val_len > payload_end {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "snapshot truncated at val body",
                    ));
                }
                let v = buf[pos..pos + val_len].to_vec();
                pos += val_len;
                Some(v)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot invalid entry flag {}", flag),
                ));
            }
        };
        entries.push(SnapshotEntry { key, value });
    }
    if pos != payload_end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("snapshot trailing bytes: {} unread", payload_end - pos),
        ));
    }
    Ok(SnapshotReader { next_ts, entries })
}

/// Atomically write a snapshot for the given MVCC state to `data_dir`.
///
/// Steps:
/// 1. Write all bytes to `snapshot.new`.
/// 2. `sync_all` on that file.
/// 3. `fs::rename` to `snapshot.current`.
/// 4. `fsync` the directory so the rename is durable.
///
/// If the process dies before step 3, `snapshot.current` is unchanged and
/// `snapshot.new` is ignored on next open.
pub fn write_snapshot(data_dir: &Path, next_ts: u64, entries: &[SnapshotEntry]) -> io::Result<()> {
    let new_path: PathBuf = data_dir.join(SNAPSHOT_NEW);
    let current_path: PathBuf = data_dir.join(SNAPSHOT_CURRENT);

    let mut payload: Vec<u8> = Vec::new();
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&VERSION.to_le_bytes());
    payload.extend_from_slice(&next_ts.to_le_bytes());
    payload.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        payload.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        payload.extend_from_slice(&entry.key);
        match &entry.value {
            None => payload.push(0),
            Some(v) => {
                payload.push(1);
                payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
                payload.extend_from_slice(v);
            }
        }
    }
    let crc = crc32fast::hash(&payload);
    payload.extend_from_slice(&crc.to_le_bytes());

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&new_path)?;
        file.write_all(&payload)?;
        file.sync_all()?;
    }

    fs::rename(&new_path, &current_path)?;

    // Fsync the directory so the rename survives crash.
    if let Ok(dir) = File::open(data_dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Remove a stale `snapshot.new` if present. Called on open.
pub fn clear_partial(data_dir: &Path) -> io::Result<()> {
    let p = data_dir.join(SNAPSHOT_NEW);
    if p.exists() {
        fs::remove_file(p)?;
    }
    Ok(())
}
