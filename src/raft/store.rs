/// State that must survive crashes (written to WAL before responding).
#[derive(Debug, Clone)]
pub struct RaftPersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
}

impl Default for RaftPersistentState {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftPersistentState {
    pub fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.current_term.to_le_bytes());
        match &self.voted_for {
            None => buf.push(0),
            Some(s) => {
                buf.push(1);
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 9 {
            return None;
        }
        let current_term = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let tag = data[8];
        let voted_for = if tag == 0 {
            None
        } else {
            let len = u32::from_le_bytes(data.get(9..13)?.try_into().ok()?) as usize;
            let s = std::str::from_utf8(data.get(13..13 + len)?)
                .ok()?
                .to_string();
            Some(s)
        };
        Some(Self {
            current_term,
            voted_for,
        })
    }
}
