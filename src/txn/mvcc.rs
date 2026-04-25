use std::collections::BTreeMap;

/// A single committed version of a key.
#[derive(Debug, Clone, PartialEq)]
pub struct Version {
    /// Timestamp at which this version was committed.
    pub commit_ts: u64,
    /// The value, or None if this version is a tombstone (delete).
    pub value: Option<Vec<u8>>,
}

/// MVCC version chain store.
///
/// Each key maps to an append-only list of versions, ordered newest-first.
/// Readers at a given snapshot timestamp see the most recent version
/// committed at or before their snapshot.
///
/// Uses BTreeMap for deterministic iteration order.
pub struct MvccStore {
    /// Key → version chain (newest version first).
    versions: BTreeMap<Vec<u8>, Vec<Version>>,
    /// Oldest active snapshot timestamp. Versions committed before this
    /// and superseded by a newer version can be garbage collected.
    gc_watermark: u64,
}

impl MvccStore {
    pub fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            gc_watermark: 0,
        }
    }

    /// Read the value of a key at the given snapshot timestamp.
    ///
    /// Returns the most recent version with `commit_ts <= snapshot_ts`.
    /// Returns None if no such version exists or if the version is a tombstone.
    pub fn read(&self, key: &[u8], snapshot_ts: u64) -> Option<Vec<u8>> {
        let chain = self.versions.get(key)?;
        for version in chain {
            if version.commit_ts <= snapshot_ts {
                return version.value.clone();
            }
        }
        None
    }

    /// Check if a key has been written (committed) after the given timestamp.
    ///
    /// Used for write-write conflict detection in OCC.
    pub fn has_write_after(&self, key: &[u8], after_ts: u64) -> bool {
        if let Some(chain) = self.versions.get(key)
            && let Some(latest) = chain.first()
        {
            return latest.commit_ts > after_ts;
        }
        false
    }

    /// Apply a write: prepend a new version to the key's chain.
    ///
    /// `value` is None for a delete (tombstone).
    pub fn write(&mut self, key: Vec<u8>, commit_ts: u64, value: Option<Vec<u8>>) {
        let chain = self.versions.entry(key).or_default();
        // Prepend (newest first).
        chain.insert(0, Version { commit_ts, value });
    }

    /// Scan all keys in the given range at the snapshot timestamp.
    ///
    /// Returns (key, value) pairs for keys that have a visible, non-tombstone
    /// version at `snapshot_ts`. Range is `[start, end)` (start inclusive,
    /// end exclusive). None bounds mean unbounded.
    pub fn scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_ts: u64,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();

        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Vec<Version>)>> = match start {
            Some(s) => Box::new(self.versions.range(s.to_vec()..)),
            None => Box::new(self.versions.iter()),
        };

        for (key, chain) in iter {
            // Check end bound.
            if let Some(e) = end
                && key.as_slice() >= e
            {
                break;
            }

            // Find the visible version at snapshot_ts.
            for version in chain {
                if version.commit_ts <= snapshot_ts {
                    if let Some(ref value) = version.value {
                        results.push((key.clone(), value.clone()));
                    }
                    // Found the visible version (tombstone or not), stop.
                    break;
                }
            }
        }

        results
    }

    /// Update the GC watermark and remove obsolete versions.
    ///
    /// For each key, keep only versions that might be visible to any
    /// active snapshot (commit_ts >= watermark), plus at most one version
    /// older than the watermark (the one visible to the watermark snapshot).
    pub fn update_gc_watermark(&mut self, watermark: u64) {
        self.gc_watermark = watermark;

        let keys: Vec<Vec<u8>> = self.versions.keys().cloned().collect();
        for key in keys {
            if let Some(chain) = self.versions.get_mut(&key) {
                // Find the first version visible at the watermark.
                let mut keep_until = chain.len();
                let mut found_watermark_version = false;

                for (i, version) in chain.iter().enumerate() {
                    if version.commit_ts <= watermark {
                        if !found_watermark_version {
                            // Keep this one (visible to watermark readers).
                            found_watermark_version = true;
                            keep_until = i + 1;
                        } else {
                            // Older than the watermark version — can GC.
                            keep_until = i;
                            break;
                        }
                    }
                }

                chain.truncate(keep_until);

                // Remove entirely empty chains.
                if chain.is_empty() {
                    self.versions.remove(&key);
                }
            }
        }
    }

    /// Return the number of keys with at least one version.
    pub fn key_count(&self) -> usize {
        self.versions.len()
    }

    /// Return the total number of versions across all keys.
    pub fn version_count(&self) -> usize {
        self.versions.values().map(|c| c.len()).sum()
    }

    pub fn gc_watermark(&self) -> u64 {
        self.gc_watermark
    }

    /// Snapshot the latest committed version of every key.
    ///
    /// Returns (key, value_or_tombstone, commit_ts) triples for every key
    /// that has at least one version. Tombstones are included so that a
    /// subsequent WAL replay doesn't resurrect deleted keys.
    pub fn snapshot_latest(&self) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64)> {
        let mut out = Vec::with_capacity(self.versions.len());
        for (key, chain) in &self.versions {
            if let Some(latest) = chain.first() {
                out.push((key.clone(), latest.value.clone(), latest.commit_ts));
            }
        }
        out
    }

    /// Replace the version chain for each given key with a single version at
    /// `commit_ts`. Used during snapshot load: any prior history is discarded.
    pub fn install_snapshot_entries(
        &mut self,
        entries: impl IntoIterator<Item = (Vec<u8>, Option<Vec<u8>>, u64)>,
    ) {
        for (key, value, commit_ts) in entries {
            self.versions
                .insert(key, vec![Version { commit_ts, value }]);
        }
    }
}

impl Default for MvccStore {
    fn default() -> Self {
        Self::new()
    }
}
