use crate::storage::types::PageId;

/// In-memory representation of a B-tree node.
///
/// Leaf nodes store sorted key-value pairs.
/// Internal nodes store sorted keys with child page pointers.
/// The tree is a B+tree: all values live in leaves, internal nodes
/// hold separator keys and child pointers.
#[derive(Debug, Clone, PartialEq)]
pub enum BTreeNode {
    Leaf {
        page_id: PageId,
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Internal {
        page_id: PageId,
        /// Separator keys. `keys[i]` is the smallest key in `children[i+1]`.
        keys: Vec<Vec<u8>>,
        /// Child page IDs. Always `keys.len() + 1` children.
        children: Vec<PageId>,
    },
}

const PAGE_TYPE_LEAF: u8 = 0;
const PAGE_TYPE_INTERNAL: u8 = 1;

impl BTreeNode {
    pub fn page_id(&self) -> PageId {
        match self {
            BTreeNode::Leaf { page_id, .. } => *page_id,
            BTreeNode::Internal { page_id, .. } => *page_id,
        }
    }

    pub fn new_leaf(page_id: PageId) -> Self {
        BTreeNode::Leaf {
            page_id,
            entries: Vec::new(),
        }
    }

    pub fn new_internal(page_id: PageId, first_child: PageId) -> Self {
        BTreeNode::Internal {
            page_id,
            keys: Vec::new(),
            children: vec![first_child],
        }
    }

    /// Serialize a BTreeNode to bytes.
    ///
    /// Format:
    ///   [page_type: u8]
    ///   [page_id: u64]
    ///   [entry_count: u32]
    ///   ...entries...
    ///   [crc32: u32] (trailer, covers everything before it)
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            BTreeNode::Leaf { page_id, entries } => {
                buf.push(PAGE_TYPE_LEAF);
                buf.extend_from_slice(&page_id.0.to_le_bytes());
                buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

                for (key, value) in entries {
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    buf.extend_from_slice(value);
                }
            }
            BTreeNode::Internal {
                page_id,
                keys,
                children,
            } => {
                buf.push(PAGE_TYPE_INTERNAL);
                buf.extend_from_slice(&page_id.0.to_le_bytes());
                buf.extend_from_slice(&(keys.len() as u32).to_le_bytes());

                // Write children (keys.len() + 1 children).
                for child in children {
                    buf.extend_from_slice(&child.0.to_le_bytes());
                }
                // Write keys.
                for key in keys {
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                }
            }
        }

        // CRC32 trailer.
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a BTreeNode from bytes.
    /// Returns None if the data is corrupted (CRC mismatch or malformed).
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }

        // Verify CRC32 trailer.
        let crc_offset = data.len() - 4;
        let stored_crc = u32::from_le_bytes(data[crc_offset..].try_into().ok()?);
        let computed_crc = crc32fast::hash(&data[..crc_offset]);
        if stored_crc != computed_crc {
            return None;
        }

        let payload = &data[..crc_offset];
        let mut pos = 0;

        let page_type = *payload.get(pos)?;
        pos += 1;

        let page_id = PageId(u64::from_le_bytes(
            payload.get(pos..pos + 8)?.try_into().ok()?,
        ));
        pos += 8;

        let count = u32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?) as usize;
        pos += 4;

        match page_type {
            PAGE_TYPE_LEAF => {
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_len =
                        u32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?) as usize;
                    pos += 4;
                    let key = payload.get(pos..pos + key_len)?.to_vec();
                    pos += key_len;

                    let val_len =
                        u32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?) as usize;
                    pos += 4;
                    let value = payload.get(pos..pos + val_len)?.to_vec();
                    pos += val_len;

                    entries.push((key, value));
                }
                Some(BTreeNode::Leaf { page_id, entries })
            }
            PAGE_TYPE_INTERNAL => {
                let num_children = count + 1;
                let mut children = Vec::with_capacity(num_children);
                for _ in 0..num_children {
                    let child = PageId(u64::from_le_bytes(
                        payload.get(pos..pos + 8)?.try_into().ok()?,
                    ));
                    pos += 8;
                    children.push(child);
                }

                let mut keys = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_len =
                        u32::from_le_bytes(payload.get(pos..pos + 4)?.try_into().ok()?) as usize;
                    pos += 4;
                    let key = payload.get(pos..pos + key_len)?.to_vec();
                    pos += key_len;
                    keys.push(key);
                }
                Some(BTreeNode::Internal {
                    page_id,
                    keys,
                    children,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_roundtrip() {
        let node = BTreeNode::Leaf {
            page_id: PageId(42),
            entries: vec![
                (b"apple".to_vec(), b"red".to_vec()),
                (b"banana".to_vec(), b"yellow".to_vec()),
            ],
        };
        let bytes = node.serialize();
        let restored = BTreeNode::deserialize(&bytes).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn internal_roundtrip() {
        let node = BTreeNode::Internal {
            page_id: PageId(1),
            keys: vec![b"middle".to_vec()],
            children: vec![PageId(2), PageId(3)],
        };
        let bytes = node.serialize();
        let restored = BTreeNode::deserialize(&bytes).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn empty_leaf_roundtrip() {
        let node = BTreeNode::new_leaf(PageId(0));
        let bytes = node.serialize();
        let restored = BTreeNode::deserialize(&bytes).unwrap();
        assert_eq!(node, restored);
    }

    #[test]
    fn corrupted_data_returns_none() {
        let node = BTreeNode::new_leaf(PageId(0));
        let mut bytes = node.serialize();
        // Corrupt a byte.
        if let Some(b) = bytes.get_mut(5) {
            *b ^= 0xFF;
        }
        assert!(BTreeNode::deserialize(&bytes).is_none());
    }
}
