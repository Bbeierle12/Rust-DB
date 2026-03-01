use std::collections::{BTreeMap, BTreeSet};

use crate::config::DatabaseConfig;
use crate::raft::log::RaftLog;
use crate::sim::rng::SeededRng;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// Raft server role.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerRole {
    Follower,
    Candidate,
    Leader,
}

/// Canonical (alphabetically ordered) partition key.
fn canonical_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

/// Encode a client command as bytes.
fn encode_command(msg: &Message) -> Vec<u8> {
    match msg {
        Message::TxnBegin => vec![0],
        Message::TxnPut { txn_id, key, value } => {
            let mut buf = vec![1];
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
            buf
        }
        Message::TxnCommit { txn_id } => {
            let mut buf = vec![2];
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf
        }
        Message::TxnAbort { txn_id } => {
            let mut buf = vec![3];
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf
        }
        Message::TxnDelete { txn_id, key } => {
            let mut buf = vec![4];
            buf.extend_from_slice(&txn_id.to_le_bytes());
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf
        }
        _ => vec![255], // unknown
    }
}

/// Decode bytes back to a client command.
fn decode_command(data: &[u8]) -> Option<Message> {
    let tag = *data.first()?;
    match tag {
        0 => Some(Message::TxnBegin),
        1 => {
            if data.len() < 9 { return None; }
            let txn_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
            let key_len = u32::from_le_bytes(data.get(9..13)?.try_into().ok()?) as usize;
            let key = data.get(13..13 + key_len)?.to_vec();
            let val_start = 13 + key_len;
            let val_len = u32::from_le_bytes(data.get(val_start..val_start + 4)?.try_into().ok()?) as usize;
            let value = data.get(val_start + 4..val_start + 4 + val_len)?.to_vec();
            Some(Message::TxnPut { txn_id, key, value })
        }
        2 => {
            if data.len() < 9 { return None; }
            let txn_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
            Some(Message::TxnCommit { txn_id })
        }
        3 => {
            if data.len() < 9 { return None; }
            let txn_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
            Some(Message::TxnAbort { txn_id })
        }
        4 => {
            if data.len() < 9 { return None; }
            let txn_id = u64::from_le_bytes(data[1..9].try_into().ok()?);
            let key_len = u32::from_le_bytes(data.get(9..13)?.try_into().ok()?) as usize;
            let key = data.get(13..13 + key_len)?.to_vec();
            Some(Message::TxnDelete { txn_id, key })
        }
        _ => None,
    }
}

/// Raft server state machine.
///
/// Implements leader election, log replication, and transaction forwarding.
/// All communication is via the message bus. Partitions are tracked locally
/// (via NetPartition/NetHeal messages) for DST-compatible testing.
pub struct RaftServer {
    id: ActorId,
    pub node_id: String,
    cfg: DatabaseConfig,

    // Persistent state
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub log: RaftLog,

    // Volatile state (all servers)
    pub commit_index: u64,
    last_applied: u64,
    pub role: ServerRole,
    pub leader_id: Option<String>,

    // Candidate state
    votes_received: BTreeSet<String>,
    election_start_tick: u64,

    // Leader state
    next_index: BTreeMap<String, u64>,
    match_index: BTreeMap<String, u64>,

    // Timing
    last_heartbeat_tick: u64,
    election_timeout: u64,

    // Wiring
    /// peer node_id → peer RaftServer ActorId (direct bus communication).
    peers: BTreeMap<String, ActorId>,
    txn_actor: ActorId,

    // Client request tracking (leader only): log_index → requester ActorId.
    pending: BTreeMap<u64, ActorId>,
    /// Currently waiting for TxnManager response to forward to client.
    waiting_for_txn: Option<ActorId>,

    // Partition state (updated via NetPartition/NetHeal).
    partitions: BTreeSet<(String, String)>,

    rng: SeededRng,

    /// Current simulation tick (updated at start of each tick()).
    now: u64,
}

impl RaftServer {
    pub fn new(
        id: ActorId,
        node_id: String,
        txn_actor: ActorId,
        cfg: &DatabaseConfig,
        rng_seed: u64,
    ) -> Self {
        let mut rng = SeededRng::new(rng_seed);
        let timeout = random_timeout(cfg, &mut rng);
        Self {
            id,
            node_id,
            cfg: cfg.clone(),
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            role: ServerRole::Follower,
            leader_id: None,
            votes_received: BTreeSet::new(),
            election_start_tick: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            last_heartbeat_tick: 0,
            election_timeout: timeout,
            peers: BTreeMap::new(),
            txn_actor,
            pending: BTreeMap::new(),
            waiting_for_txn: None,
            partitions: BTreeSet::new(),
            rng,
            now: 0,
        }
    }

    pub fn add_peer(&mut self, node_id: impl Into<String>, actor: ActorId) {
        self.peers.insert(node_id.into(), actor);
    }

    pub fn role(&self) -> &ServerRole { &self.role }
    pub fn leader_id(&self) -> Option<&str> { self.leader_id.as_deref() }
    pub fn log_last_index(&self) -> u64 { self.log.last_index() }

    fn cluster_size(&self) -> usize { self.peers.len() + 1 }

    fn has_majority(&self, count: usize) -> bool {
        count >= self.cluster_size() / 2 + 1
    }

    fn is_partitioned_from(&self, peer: &str) -> bool {
        self.partitions.contains(&canonical_pair(&self.node_id, peer))
    }

    fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
        self.role = ServerRole::Follower;
        self.votes_received.clear();
        // Reset election timer so we don't immediately start a new election.
        self.last_heartbeat_tick = self.now;
    }

    fn start_election(&mut self, now: u64, msgs: &mut Vec<(Message, Destination)>) {
        self.current_term += 1;
        self.role = ServerRole::Candidate;
        self.voted_for = Some(self.node_id.clone());
        self.votes_received.clear();
        self.votes_received.insert(self.node_id.clone());
        self.election_start_tick = now;
        self.election_timeout = random_timeout(&self.cfg, &mut self.rng);

        // Check for immediate self-win (single node cluster).
        if self.has_majority(self.votes_received.len()) {
            self.become_leader(now, msgs);
            return;
        }

        let term = self.current_term;
        let candidate_id = self.node_id.clone();
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        for (peer_id, &peer_actor) in &self.peers {
            if self.is_partitioned_from(peer_id) { continue; }
            msgs.push((
                Message::RaftRequestVote { term, candidate_id: candidate_id.clone(), last_log_index, last_log_term },
                Destination { actor: peer_actor, delay: 0 },
            ));
        }
    }

    fn become_leader(&mut self, now: u64, msgs: &mut Vec<(Message, Destination)>) {
        self.role = ServerRole::Leader;
        self.leader_id = Some(self.node_id.clone());
        self.votes_received.clear();

        let next = self.log.last_index() + 1;
        for peer_id in self.peers.keys().cloned().collect::<Vec<_>>() {
            self.next_index.insert(peer_id.clone(), next);
            self.match_index.insert(peer_id, 0);
        }

        self.last_heartbeat_tick = now;
        let heartbeats = self.build_heartbeats();
        msgs.extend(heartbeats);
    }

    fn build_heartbeats(&self) -> Vec<(Message, Destination)> {
        let mut msgs = Vec::new();
        for (peer_id, &peer_actor) in &self.peers {
            if self.is_partitioned_from(peer_id) { continue; }
            let next = *self.next_index.get(peer_id.as_str()).unwrap_or(&1);

            // Check if follower needs a snapshot.
            if next <= self.log.snapshot_last_index && self.log.snapshot_last_index > 0 {
                // Would need to send snapshot; skip for now (test 11 handles this separately).
                continue;
            }

            let entries = self.log.get_entries_from(next);
            let prev_log_index = if next > 1 { next - 1 } else { 0 };
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
            msgs.push((
                Message::RaftAppendEntries {
                    term: self.current_term,
                    leader_id: self.node_id.clone(),
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
                Destination { actor: peer_actor, delay: 0 },
            ));
        }
        msgs
    }

    /// Advance commit_index and apply entries to txn_actor.
    fn maybe_advance_commit(&mut self, msgs: &mut Vec<(Message, Destination)>) {
        // Find the highest index that a majority of nodes have matched.
        let last = self.log.last_index();
        let mut new_commit = self.commit_index;

        for idx in (self.commit_index + 1)..=last {
            if self.log.term_at(idx) != Some(self.current_term) { continue; }
            let mut count = 1; // self
            for match_idx in self.match_index.values() {
                if *match_idx >= idx { count += 1; }
            }
            if self.has_majority(count) {
                new_commit = idx;
            }
        }

        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            let apply_msgs = self.apply_committed_entries();
            msgs.extend(apply_msgs);
        }
    }

    /// Apply all committed-but-not-applied entries.
    fn apply_committed_entries(&mut self) -> Vec<(Message, Destination)> {
        let mut msgs = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let index = self.last_applied;
            let data = match self.log.get(index) {
                Some(e) => e.data.clone(),
                None => continue,
            };

            // Decode and send to txn_actor.
            if let Some(txn_msg) = decode_command(&data) {
                // Look up pending requester (only on leader).
                let requester = self.pending.remove(&index);
                if let Some(req) = requester {
                    // This node is (or was) the leader — track for response forwarding.
                    self.waiting_for_txn = Some(req);
                }
                msgs.push((txn_msg, Destination { actor: self.txn_actor, delay: 0 }));
            }
        }
        msgs
    }

    /// Update follower's commit index from leader's.
    fn follower_advance_commit(&mut self, leader_commit: u64) -> Vec<(Message, Destination)> {
        let new_commit = leader_commit.min(self.log.last_index());
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            return self.apply_committed_entries();
        }
        Vec::new()
    }
}

fn random_timeout(cfg: &DatabaseConfig, rng: &mut SeededRng) -> u64 {
    let range = cfg.raft_election_timeout_max - cfg.raft_election_timeout_min;
    cfg.raft_election_timeout_min + rng.next_range(range + 1)
}

impl StateMachine for RaftServer {
    fn id(&self) -> ActorId { self.id }

    fn tick(&mut self, now: u64) -> Option<Vec<(Message, Destination)>> {
        self.now = now;
        let mut msgs = Vec::new();
        match self.role {
            ServerRole::Follower => {
                let elapsed = now.saturating_sub(self.last_heartbeat_tick);
                if elapsed >= self.election_timeout {
                    self.start_election(now, &mut msgs);
                }
            }
            ServerRole::Candidate => {
                let elapsed = now.saturating_sub(self.election_start_tick);
                if elapsed >= self.election_timeout {
                    // Restart election.
                    self.start_election(now, &mut msgs);
                }
            }
            ServerRole::Leader => {
                let elapsed = now.saturating_sub(self.last_heartbeat_tick);
                if elapsed >= self.cfg.raft_heartbeat_interval {
                    self.last_heartbeat_tick = now;
                    let hb = self.build_heartbeats();
                    msgs.extend(hb);
                }
            }
        }
        if msgs.is_empty() { None } else { Some(msgs) }
    }

    fn receive(&mut self, _from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        let mut msgs = Vec::new();
        match msg {
            // ── Partition control ─────────────────────────────────────────
            Message::NetPartition { node_a, node_b } => {
                self.partitions.insert(canonical_pair(&node_a, &node_b));
                return None;
            }
            Message::NetHeal { node_a, node_b } => {
                self.partitions.remove(&canonical_pair(&node_a, &node_b));
                return None;
            }

            // ── RequestVote ───────────────────────────────────────────────
            Message::RaftRequestVote { term, candidate_id, last_log_index, last_log_term } => {
                if term > self.current_term {
                    self.become_follower(term);
                }

                let can_vote = term >= self.current_term
                    && (self.voted_for.is_none() || self.voted_for.as_deref() == Some(&candidate_id))
                    && self.log.is_up_to_date(last_log_term, last_log_index);

                if can_vote {
                    self.voted_for = Some(candidate_id.clone());
                    self.last_heartbeat_tick = self.now; // reset election timer
                    let sender = self.peers.get(&candidate_id).copied();
                    if let Some(actor) = sender {
                        msgs.push((
                            Message::RaftVoteGranted { term: self.current_term, from: self.node_id.clone() },
                            Destination { actor, delay: 0 },
                        ));
                    }
                } else {
                    let sender = self.peers.get(&candidate_id).copied();
                    if let Some(actor) = sender {
                        msgs.push((
                            Message::RaftVoteDenied { term: self.current_term, from: self.node_id.clone() },
                            Destination { actor, delay: 0 },
                        ));
                    }
                }
            }

            Message::RaftVoteGranted { term, from } => {
                if term != self.current_term || !matches!(self.role, ServerRole::Candidate) {
                    return None;
                }
                self.votes_received.insert(from);
                if self.has_majority(self.votes_received.len()) {
                    // Capture now from last tick (we don't have it here; use 0 as placeholder).
                    // The leader will send heartbeats on next tick anyway.
                    self.become_leader(0, &mut msgs);
                }
            }

            Message::RaftVoteDenied { term, .. } => {
                if term > self.current_term {
                    self.become_follower(term);
                }
            }

            // ── AppendEntries ─────────────────────────────────────────────
            Message::RaftAppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                let sender = self.peers.get(&leader_id).copied();

                if term < self.current_term {
                    if let Some(actor) = sender {
                        msgs.push((
                            Message::RaftAppendEntriesErr { term: self.current_term, from: self.node_id.clone(), reason: "stale term".into() },
                            Destination { actor, delay: 0 },
                        ));
                    }
                    return Some(msgs);
                }

                // Update term and revert to follower if needed.
                if term > self.current_term {
                    self.become_follower(term);
                }
                if matches!(self.role, ServerRole::Candidate) {
                    self.role = ServerRole::Follower;
                    self.votes_received.clear();
                }

                self.leader_id = Some(leader_id.clone());
                self.last_heartbeat_tick = self.now; // Reset election timer.

                // Check prev_log consistency.
                if prev_log_index > 0 {
                    match self.log.term_at(prev_log_index) {
                        Some(t) if t == prev_log_term => {}
                        _ => {
                            if let Some(actor) = sender {
                                msgs.push((
                                    Message::RaftAppendEntriesErr { term: self.current_term, from: self.node_id.clone(), reason: "log inconsistency".into() },
                                    Destination { actor, delay: 0 },
                                ));
                            }
                            return Some(msgs);
                        }
                    }
                }

                // Append entries.
                let mut idx = prev_log_index;
                for (entry_term, entry_data) in entries {
                    idx += 1;
                    match self.log.term_at(idx) {
                        Some(t) if t == entry_term => {} // already consistent
                        Some(_) => {
                            // Conflict: truncate and append.
                            self.log.truncate_from(idx);
                            self.log.append(entry_term, entry_data);
                        }
                        None => {
                            self.log.append(entry_term, entry_data);
                        }
                    }
                }

                // Advance commit.
                let apply_msgs = self.follower_advance_commit(leader_commit);
                msgs.extend(apply_msgs);

                if let Some(actor) = sender {
                    msgs.push((
                        Message::RaftAppendEntriesOk { term: self.current_term, from: self.node_id.clone(), match_index: self.log.last_index() },
                        Destination { actor, delay: 0 },
                    ));
                }
            }

            Message::RaftAppendEntriesOk { term, from, match_index } => {
                if term > self.current_term {
                    self.become_follower(term);
                    return None;
                }
                if !matches!(self.role, ServerRole::Leader) { return None; }

                // Update match/next index.
                self.match_index.insert(from.clone(), match_index);
                self.next_index.insert(from, match_index + 1);

                // Check for new commit.
                self.maybe_advance_commit(&mut msgs);
            }

            Message::RaftAppendEntriesErr { term, from, .. } => {
                if term > self.current_term {
                    self.become_follower(term);
                    return None;
                }
                if !matches!(self.role, ServerRole::Leader) { return None; }

                // Decrement next_index and retry.
                let next = self.next_index.entry(from.clone()).or_insert(1);
                if *next > 1 { *next -= 1; }
                let retry_next = *next;

                if let Some(&peer_actor) = self.peers.get(&from) {
                    let entries = self.log.get_entries_from(retry_next);
                    let prev_log_index = if retry_next > 1 { retry_next - 1 } else { 0 };
                    let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
                    msgs.push((
                        Message::RaftAppendEntries {
                            term: self.current_term,
                            leader_id: self.node_id.clone(),
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit: self.commit_index,
                        },
                        Destination { actor: peer_actor, delay: 0 },
                    ));
                }
            }

            // ── Snapshot install ──────────────────────────────────────────
            Message::RaftInstallSnapshot { term, leader_id, last_included_index, last_included_term, .. } => {
                if term < self.current_term { return None; }
                if term > self.current_term {
                    self.become_follower(term);
                }
                self.leader_id = Some(leader_id.clone());
                self.last_heartbeat_tick = 0;

                // Apply snapshot to log.
                self.log.compact(last_included_index, last_included_term);
                self.commit_index = last_included_index.max(self.commit_index);
                self.last_applied = last_included_index.max(self.last_applied);

                if let Some(&peer_actor) = self.peers.get(&leader_id) {
                    msgs.push((
                        Message::RaftInstallSnapshotOk { term: self.current_term, from: self.node_id.clone() },
                        Destination { actor: peer_actor, delay: 0 },
                    ));
                }
            }

            Message::RaftInstallSnapshotOk { term, from } => {
                if term > self.current_term {
                    self.become_follower(term);
                    return None;
                }
                if !matches!(self.role, ServerRole::Leader) { return None; }
                // Update match/next index after snapshot.
                let snap_idx = self.log.snapshot_last_index;
                self.match_index.insert(from.clone(), snap_idx);
                self.next_index.insert(from, snap_idx + 1);
                self.maybe_advance_commit(&mut msgs);
            }

            // ── Client commands ───────────────────────────────────────────
            Message::TxnBegin | Message::TxnPut { .. } | Message::TxnCommit { .. }
            | Message::TxnAbort { .. } | Message::TxnDelete { .. } => {
                match self.role {
                    ServerRole::Leader => {
                        let data = encode_command(&msg);
                        let index = self.log.append(self.current_term, data);
                        self.pending.insert(index, _from);

                        // Send AppendEntries to replicate.
                        let heartbeats = self.build_heartbeats();
                        msgs.extend(heartbeats);

                        // Single-node cluster: commit immediately.
                        if self.cluster_size() == 1 {
                            self.commit_index = index;
                            let apply = self.apply_committed_entries();
                            msgs.extend(apply);
                        }
                    }
                    _ => {
                        // Redirect to leader.
                        msgs.push((
                            Message::RaftRedirect { leader_id: self.leader_id.clone() },
                            Destination { actor: _from, delay: 0 },
                        ));
                    }
                }
            }

            // ── TxnManager responses (forward to pending client) ──────────
            Message::TxnBeginOk { .. } | Message::TxnPutOk { .. } | Message::TxnCommitOk { .. }
            | Message::TxnCommitErr { .. } | Message::TxnAbortOk { .. } => {
                if let Some(requester) = self.waiting_for_txn.take() {
                    msgs.push((msg, Destination { actor: requester, delay: 0 }));
                }
            }

            _ => {}
        }

        if msgs.is_empty() { None } else { Some(msgs) }
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}
