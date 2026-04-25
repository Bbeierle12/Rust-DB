//! Stage 11: Distributed Raft consensus tests.
//!
//! Each test creates n-node clusters via make_cluster() and exercises
//! leader election, log replication, partitioning, and failover.

mod helpers;

use helpers::Collector;
use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::raft::server::{RaftServer, ServerRole};
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::disk::SimDisk;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};
use rust_dst_db::txn::manager::TransactionManager;
use rust_dst_db::wal::writer::WalWriter;

// ─── Cluster wiring ───────────────────────────────────────────────────────────
//
// For node i, actor IDs are:
//   SimDisk:            ActorId(i*10 + 1)
//   WalWriter:          ActorId(i*10 + 2)
//   TransactionManager: ActorId(i*10 + 3)
//   RaftServer:         ActorId(i*10 + 4)
//
// CLIENT (observer):    ActorId(100)

fn disk_id(node: usize) -> ActorId {
    ActorId((node * 10 + 1) as u64)
}
fn wal_id(node: usize) -> ActorId {
    ActorId((node * 10 + 2) as u64)
}
fn txn_id(node: usize) -> ActorId {
    ActorId((node * 10 + 3) as u64)
}
fn raft_id(node: usize) -> ActorId {
    ActorId((node * 10 + 4) as u64)
}

const CLIENT_ID: ActorId = ActorId(100);

/// Run the bus for exactly `n` ticks.
///
/// This is required for Raft tests because bus.run() stops early when
/// no messages are in flight (which would prevent election timers from firing).
fn ticks(bus: &mut MessageBus, n: u64) {
    for _ in 0..n {
        bus.tick();
    }
}

/// Build an n-node Raft cluster on a single bus.
fn make_cluster(n: usize, seed: u64) -> (MessageBus, DatabaseConfig) {
    let cfg = DatabaseConfig {
        // Fast election/heartbeat for tests.
        raft_election_timeout_min: 10,
        raft_election_timeout_max: 20,
        raft_heartbeat_interval: 3,
        // Zero disk latency for speed.
        sim_disk_read_latency_ticks: 0,
        sim_disk_write_latency_ticks: 0,
        sim_fsync_latency_ticks: 0,
        ..DatabaseConfig::default()
    };

    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(seed, injector, &cfg);

    let mut raft_servers: Vec<RaftServer> = Vec::new();
    for i in 0..n {
        bus.register(Box::new(SimDisk::new(disk_id(i), false, &cfg)));
        bus.register(Box::new(WalWriter::new(
            wal_id(i),
            disk_id(i),
            txn_id(i),
            &cfg,
        )));
        bus.register(Box::new(TransactionManager::new(txn_id(i), wal_id(i))));

        let raft_seed = seed.wrapping_add(i as u64 * 1000);
        let mut server =
            RaftServer::new(raft_id(i), format!("node{}", i), txn_id(i), &cfg, raft_seed);
        for j in 0..n {
            if i != j {
                server.add_peer(format!("node{}", j), raft_id(j));
            }
        }
        raft_servers.push(server);
    }
    for server in raft_servers {
        bus.register(Box::new(server));
    }
    bus.register(Box::new(Collector::new(CLIENT_ID)));

    (bus, cfg)
}

fn count_leaders(bus: &MessageBus, n: usize) -> usize {
    (0..n)
        .filter(|&i| {
            bus.actor::<RaftServer>(raft_id(i))
                .map(|r| *r.role() == ServerRole::Leader)
                .unwrap_or(false)
        })
        .count()
}

fn find_leader(bus: &MessageBus, n: usize) -> Option<usize> {
    (0..n).find(|&i| {
        bus.actor::<RaftServer>(raft_id(i))
            .map(|r| *r.role() == ServerRole::Leader)
            .unwrap_or(false)
    })
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Test 1: 1-node cluster self-elects as leader within election_timeout ticks.
#[test]
fn raft_single_node_becomes_leader() {
    let (mut bus, _) = make_cluster(1, 42);
    ticks(&mut bus, 100); // > max election timeout (20 ticks)

    let raft = bus.actor::<RaftServer>(raft_id(0)).unwrap();
    assert_eq!(
        *raft.role(),
        ServerRole::Leader,
        "Single node must become leader"
    );
}

/// Test 2: 3-node cluster elects exactly one leader.
#[test]
fn raft_three_node_elects_leader() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200); // Enough for election + stabilization

    let leaders = count_leaders(&bus, 3);
    assert_eq!(leaders, 1, "Exactly one leader expected, got {}", leaders);
}

/// Test 3: After election, leader sends heartbeats (AppendEntries).
#[test]
fn raft_leader_sends_heartbeats() {
    let (mut bus, cfg) = make_cluster(3, 42);
    ticks(&mut bus, 200); // Wait for election

    assert_eq!(count_leaders(&bus, 3), 1, "Should have a leader");

    let before = bus.trace().len();
    // Run a few more ticks to capture heartbeats.
    ticks(&mut bus, cfg.raft_heartbeat_interval * 3);

    let trace = bus.trace();
    let heartbeats = trace[before..]
        .iter()
        .filter(|(_, _, _, msg)| msg.contains("RaftAppendEntries"))
        .count();
    assert!(
        heartbeats > 0,
        "Leader should send heartbeats, found {}",
        heartbeats
    );
}

/// Test 4: Leader receives command → followers' logs match leader's.
#[test]
fn raft_log_replicated_to_followers() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 100);

    let leader_log = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .log_last_index();
    assert!(
        leader_log >= 1,
        "Leader should have >= 1 log entry, got {}",
        leader_log
    );

    for i in 0..3 {
        if i == leader_idx {
            continue;
        }
        let f_log = bus
            .actor::<RaftServer>(raft_id(i))
            .unwrap()
            .log_last_index();
        assert_eq!(
            f_log, leader_log,
            "Follower {} log {} should match leader {}",
            i, f_log, leader_log
        );
    }
}

/// Test 5: Entry committed only after majority (2/3) ack.
#[test]
fn raft_commit_requires_majority() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 100);

    let commit = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;
    assert!(
        commit >= 1,
        "Entry should be committed after majority ack (commit={})",
        commit
    );
}

/// Test 6: TxnBegin to follower → RaftRedirect with leader_id.
#[test]
fn raft_client_redirect_on_follower() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();

    bus.send(CLIENT_ID, raft_id(follower_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 50);

    let client = bus.actor::<Collector>(CLIENT_ID).unwrap();
    let redirect = client
        .received
        .iter()
        .find(|m| matches!(m, Message::RaftRedirect { .. }));
    assert!(redirect.is_some(), "Follower should RaftRedirect");

    if let Some(Message::RaftRedirect { leader_id }) = redirect {
        assert_eq!(
            leader_id.as_deref(),
            Some(format!("node{}", leader_idx).as_str())
        );
    }
}

/// Test 7: Minority partition cannot advance commit.
#[test]
fn raft_partition_prevents_commit() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    let commit_before = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;

    // Partition leader from both followers.
    for i in 0..3 {
        if i != leader_idx {
            let a = format!("node{}", leader_idx);
            let b = format!("node{}", i);
            bus.send(
                CLIENT_ID,
                raft_id(leader_idx),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
            bus.send(
                CLIENT_ID,
                raft_id(i),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
        }
    }
    ticks(&mut bus, 50);

    // Try to commit while partitioned (minority can't commit).
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 100);

    let commit_after = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;
    assert_eq!(
        commit_after, commit_before,
        "Partitioned minority leader must not advance commit (before={}, after={})",
        commit_before, commit_after
    );
}

/// Test 8: Majority partition elects a new leader.
#[test]
fn raft_new_leader_after_partition() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let old_leader = find_leader(&bus, 3).expect("No initial leader");

    // Partition old leader from the majority.
    for i in 0..3 {
        if i != old_leader {
            let a = format!("node{}", old_leader);
            let b = format!("node{}", i);
            bus.send(
                CLIENT_ID,
                raft_id(old_leader),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
            bus.send(
                CLIENT_ID,
                raft_id(i),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
        }
    }
    ticks(&mut bus, 300); // Allow new election.

    let majority: Vec<usize> = (0..3).filter(|&i| i != old_leader).collect();
    let new_leader = majority.iter().any(|&i| {
        bus.actor::<RaftServer>(raft_id(i))
            .map(|r| *r.role() == ServerRole::Leader)
            .unwrap_or(false)
    });
    assert!(new_leader, "Majority partition should elect a new leader");
}

/// Test 9: Old leader gets higher-term messages and steps down after heal.
#[test]
fn raft_stale_leader_rejected() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let old_leader = find_leader(&bus, 3).expect("No leader");
    let old_term = bus
        .actor::<RaftServer>(raft_id(old_leader))
        .unwrap()
        .current_term;

    // Partition old leader.
    for i in 0..3 {
        if i != old_leader {
            let a = format!("node{}", old_leader);
            let b = format!("node{}", i);
            bus.send(
                CLIENT_ID,
                raft_id(old_leader),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
            bus.send(
                CLIENT_ID,
                raft_id(i),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
        }
    }
    ticks(&mut bus, 300); // New election in majority.

    // Heal.
    for i in 0..3 {
        if i != old_leader {
            let a = format!("node{}", old_leader);
            let b = format!("node{}", i);
            bus.send(
                CLIENT_ID,
                raft_id(old_leader),
                Message::NetHeal {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
            bus.send(
                CLIENT_ID,
                raft_id(i),
                Message::NetHeal {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
        }
    }
    ticks(&mut bus, 200); // Let old leader receive higher-term messages.

    let new_role = bus
        .actor::<RaftServer>(raft_id(old_leader))
        .unwrap()
        .role()
        .clone();
    let new_term = bus
        .actor::<RaftServer>(raft_id(old_leader))
        .unwrap()
        .current_term;
    assert_ne!(new_role, ServerRole::Leader, "Old leader should step down");
    assert!(
        new_term > old_term,
        "Old leader term should have advanced ({} → {})",
        old_term,
        new_term
    );
}

/// Test 10: Partitioned follower catches up after rejoin.
#[test]
fn raft_log_catch_up_after_rejoin() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();

    let a = format!("node{}", leader_idx);
    let b = format!("node{}", follower_idx);

    // Partition one follower.
    bus.send(
        CLIENT_ID,
        raft_id(leader_idx),
        Message::NetPartition {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        raft_id(follower_idx),
        Message::NetPartition {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    ticks(&mut bus, 20);

    // Leader commits an entry.
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 100);

    let leader_commit = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;

    // Heal.
    bus.send(
        CLIENT_ID,
        raft_id(leader_idx),
        Message::NetHeal {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        raft_id(follower_idx),
        Message::NetHeal {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    ticks(&mut bus, 200);

    let follower_log = bus
        .actor::<RaftServer>(raft_id(follower_idx))
        .unwrap()
        .log_last_index();
    assert!(
        follower_log >= leader_commit,
        "Follower should catch up (follower_log={}, leader_commit={})",
        follower_log,
        leader_commit
    );
}

/// Test 11: Follower too far behind receives snapshot install and catches up.
#[test]
fn raft_snapshot_install_for_lagged_follower() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();

    let a = format!("node{}", leader_idx);
    let b = format!("node{}", follower_idx);

    // Partition follower.
    bus.send(
        CLIENT_ID,
        raft_id(leader_idx),
        Message::NetPartition {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    bus.send(
        CLIENT_ID,
        raft_id(follower_idx),
        Message::NetPartition {
            node_a: a.clone(),
            node_b: b.clone(),
        },
        0,
    );
    ticks(&mut bus, 20);

    // Commit several entries (these won't reach the partitioned follower).
    for _ in 0..3 {
        bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
        ticks(&mut bus, 50);
    }

    let leader_commit = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;
    let leader_term = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .current_term;

    // Manually install snapshot to the lagged follower.
    bus.send(
        raft_id(leader_idx),
        raft_id(follower_idx),
        Message::RaftInstallSnapshot {
            term: leader_term,
            leader_id: format!("node{}", leader_idx),
            last_included_index: leader_commit,
            last_included_term: leader_term,
            data: vec![],
        },
        0,
    );
    ticks(&mut bus, 50);

    // Follower's commit_index should now reflect the snapshot.
    let follower_commit = bus
        .actor::<RaftServer>(raft_id(follower_idx))
        .unwrap()
        .commit_index;
    assert!(
        follower_commit >= leader_commit,
        "Follower should have applied snapshot (follower={}, leader={})",
        follower_commit,
        leader_commit
    );
}

/// Test 12: Committed txn visible after leader changes.
#[test]
fn raft_transaction_survives_leader_change() {
    let (mut bus, _) = make_cluster(3, 42);
    ticks(&mut bus, 200);

    let leader_idx = find_leader(&bus, 3).expect("No leader");

    // Commit a txn.
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 100);

    let commit_before = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;
    assert!(
        commit_before >= 1,
        "Txn should be committed (commit={})",
        commit_before
    );

    // Kill old leader (partition from everyone).
    for i in 0..3 {
        if i != leader_idx {
            let a = format!("node{}", leader_idx);
            let b = format!("node{}", i);
            bus.send(
                CLIENT_ID,
                raft_id(leader_idx),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
            bus.send(
                CLIENT_ID,
                raft_id(i),
                Message::NetPartition {
                    node_a: a.clone(),
                    node_b: b.clone(),
                },
                0,
            );
        }
    }
    ticks(&mut bus, 300); // New election.

    let new_leader = (0..3).find(|&i| {
        i != leader_idx
            && bus
                .actor::<RaftServer>(raft_id(i))
                .map(|r| *r.role() == ServerRole::Leader)
                .unwrap_or(false)
    });
    assert!(new_leader.is_some(), "New leader should be elected");

    let new_commit = bus
        .actor::<RaftServer>(raft_id(new_leader.unwrap()))
        .unwrap()
        .commit_index;
    assert!(
        new_commit >= 1,
        "New leader should have the committed entry (new_commit={})",
        new_commit
    );
}

/// Test 13: 5-node cluster continues with 2 nodes failed.
#[test]
fn raft_five_node_tolerates_two_failures() {
    let (mut bus, _) = make_cluster(5, 42);
    ticks(&mut bus, 300);

    let leader_idx = find_leader(&bus, 5).expect("No leader in 5-node cluster");

    // Kill 2 non-leader nodes.
    let failed: Vec<usize> = (0..5).filter(|&i| i != leader_idx).take(2).collect();
    for &f in &failed {
        for i in 0..5 {
            if i != f {
                let a = format!("node{}", f);
                let b = format!("node{}", i);
                bus.send(
                    CLIENT_ID,
                    raft_id(f),
                    Message::NetPartition {
                        node_a: a.clone(),
                        node_b: b.clone(),
                    },
                    0,
                );
                bus.send(
                    CLIENT_ID,
                    raft_id(i),
                    Message::NetPartition {
                        node_a: a.clone(),
                        node_b: b.clone(),
                    },
                    0,
                );
            }
        }
    }
    ticks(&mut bus, 50);

    // Cluster should still commit (3/5 nodes alive).
    bus.send(CLIENT_ID, raft_id(leader_idx), Message::TxnBegin, 0);
    ticks(&mut bus, 200);

    let commit = bus
        .actor::<RaftServer>(raft_id(leader_idx))
        .unwrap()
        .commit_index;
    assert!(
        commit >= 1,
        "5-node cluster should commit with 2 failures (commit={})",
        commit
    );
}

/// Test 14: Same seed → identical trace (DST correctness proof).
#[test]
fn raft_deterministic_replay() {
    fn run(seed: u64) -> Vec<String> {
        let (mut bus, _) = make_cluster(3, seed);
        ticks(&mut bus, 300);
        bus.trace()
            .iter()
            .map(|(t, f, to, m)| format!("{}:{}->{}:{}", t, f, to, m))
            .collect()
    }

    let trace_a = run(12345);
    let trace_b = run(12345);
    assert_eq!(trace_a, trace_b, "Same seed must produce identical trace");
    assert!(!trace_a.is_empty());
}

/// Test 15: 10 seeds × 20% drop probability → no trace divergence within a seed.
#[test]
fn raft_chaos_fuzz() {
    for seed in 0u64..10 {
        let run_trace = |s: u64| -> Vec<String> {
            let cfg = DatabaseConfig {
                raft_election_timeout_min: 10,
                raft_election_timeout_max: 20,
                raft_heartbeat_interval: 3,
                sim_disk_read_latency_ticks: 0,
                sim_disk_write_latency_ticks: 0,
                sim_fsync_latency_ticks: 0,
                ..DatabaseConfig::default()
            };
            let fault = FaultConfig {
                drop_probability: 0.2,
                ..FaultConfig::none()
            };
            let injector = FaultInjector::new(fault);
            let mut bus = MessageBus::new(s, injector, &cfg);

            for i in 0usize..3 {
                bus.register(Box::new(SimDisk::new(disk_id(i), false, &cfg)));
                bus.register(Box::new(WalWriter::new(
                    wal_id(i),
                    disk_id(i),
                    txn_id(i),
                    &cfg,
                )));
                bus.register(Box::new(TransactionManager::new(txn_id(i), wal_id(i))));

                let raft_seed = s.wrapping_add(i as u64 * 1000);
                let mut server =
                    RaftServer::new(raft_id(i), format!("node{}", i), txn_id(i), &cfg, raft_seed);
                for j in 0usize..3 {
                    if i != j {
                        server.add_peer(format!("node{}", j), raft_id(j));
                    }
                }
                bus.register(Box::new(server));
            }
            bus.register(Box::new(Collector::new(CLIENT_ID)));

            for _ in 0..300 {
                bus.tick();
            }
            bus.trace()
                .iter()
                .map(|(t, f, to, m)| format!("{}:{}->{}:{}", t, f, to, m))
                .collect()
        };

        let trace_a = run_trace(seed);
        let trace_b = run_trace(seed);
        assert_eq!(
            trace_a, trace_b,
            "Chaos fuzz seed={}: determinism regression",
            seed
        );
    }
}
