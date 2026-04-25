use std::collections::BTreeMap;

/// A consistent point-in-time snapshot of the entire database state.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// Monotonically increasing checkpoint identifier.
    pub id: u64,
    /// Simulation tick at capture time.
    pub captured_at_tick: u64,
    /// Last WAL LSN fsynced before capture.
    pub last_fsynced_lsn: u64,
    /// Raw bytes of each disk file at capture time. key = file_id (0 = WAL, 1 = B-tree pages).
    pub files: BTreeMap<u64, Vec<u8>>,
}

impl Checkpoint {
    /// Encode to bytes:
    /// [id:u64][tick:u64][lsn:u64][file_count:u32]
    /// for each file: [file_id:u64][len:u64][data]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.extend_from_slice(&self.captured_at_tick.to_le_bytes());
        buf.extend_from_slice(&self.last_fsynced_lsn.to_le_bytes());
        buf.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for (&file_id, data) in &self.files {
            buf.extend_from_slice(&file_id.to_le_bytes());
            buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
            buf.extend_from_slice(data);
        }
        buf
    }

    /// Decode from bytes produced by encode().
    pub fn decode(data: &[u8]) -> Option<Self> {
        let mut pos = 0;

        let id = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let captured_at_tick = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let last_fsynced_lsn = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
        pos += 8;
        let file_count = u32::from_le_bytes(data.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;

        let mut files = BTreeMap::new();
        for _ in 0..file_count {
            let file_id = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?);
            pos += 8;
            let len = u64::from_le_bytes(data.get(pos..pos + 8)?.try_into().ok()?) as usize;
            pos += 8;
            let file_data = data.get(pos..pos + len)?.to_vec();
            pos += len;
            files.insert(file_id, file_data);
        }

        Some(Self {
            id,
            captured_at_tick,
            last_fsynced_lsn,
            files,
        })
    }
}
