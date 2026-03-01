//! Stage 5 tests: SimNetwork state machine.

mod helpers;

use helpers::Collector;
use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::net::sim_network::SimNetwork;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};

// ─── Actor IDs used throughout ────────────────────────────────────────────────

const SIM_A_ID: ActorId = ActorId(1);
const SIM_B_ID: ActorId = ActorId(2);
const COLL_A_ID: ActorId = ActorId(3);  // client / "node A application"
const COLL_B_ID: ActorId = ActorId(4);  // "node B application"

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Single-SimNetwork setup: sim_a knows node "b" → `COLL_B_ID` directly.
/// Useful for tests that only care about sending from A.
fn single_node_bus(seed: u64, base_latency: u64) -> MessageBus {
    let injector = FaultInjector::new(FaultConfig::none());
    let cfg = DatabaseConfig {
        sim_network_latency_ticks: base_latency,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let mut bus = MessageBus::new(seed, injector, &cfg);

    let mut sim_a = SimNetwork::new(SIM_A_ID, "a", seed, &cfg);
    sim_a.add_peer("b", COLL_B_ID);

    bus.register(Box::new(sim_a));
    bus.register(Box::new(Collector::new(COLL_A_ID)));
    bus.register(Box::new(Collector::new(COLL_B_ID)));
    bus
}

/// Two-SimNetwork setup: sim_a and sim_b each forward NetRecv to their
/// respective Collector via `set_app_actor`.
fn two_node_bus(seed: u64) -> MessageBus {
    let injector = FaultInjector::new(FaultConfig::none());
    let cfg = DatabaseConfig {
        sim_network_latency_ticks: 1,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let mut bus = MessageBus::new(seed, injector, &cfg);

    let mut sim_a = SimNetwork::new(SIM_A_ID, "a", seed, &cfg)
        .set_app_actor(COLL_A_ID);
    sim_a.add_peer("b", SIM_B_ID);

    let mut sim_b = SimNetwork::new(SIM_B_ID, "b", seed.wrapping_add(1), &cfg)
        .set_app_actor(COLL_B_ID);
    sim_b.add_peer("a", SIM_A_ID);

    bus.register(Box::new(sim_a));
    bus.register(Box::new(sim_b));
    bus.register(Box::new(Collector::new(COLL_A_ID)));
    bus.register(Box::new(Collector::new(COLL_B_ID)));
    bus
}

// ─── Tests ───────────────────────────────────────────────────────────────────

/// Basic send/recv: A sends data to B, B's collector receives NetRecv.
#[test]
fn simnet_two_nodes_communicate() {
    let mut bus = single_node_bus(42, 1); // latency=1 tick, no variance

    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend {
            conn_id: 1,
            to_node: "b".into(),
            data: b"hello".to_vec(),
        },
        0,
    );

    bus.run(20);

    // Sender (COLL_A) should receive NetSendOk.
    let coll_a = bus.actor::<Collector>(COLL_A_ID).unwrap();
    let oks: Vec<_> = coll_a
        .received
        .iter()
        .filter(|m| matches!(m, Message::NetSendOk { .. }))
        .collect();
    assert_eq!(oks.len(), 1, "expected NetSendOk to sender");

    // Receiver (COLL_B) should receive NetRecv with the original data.
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert_eq!(coll_b.received.len(), 1, "expected NetRecv at B");
    match &coll_b.received[0] {
        Message::NetRecv { from_node, data, .. } => {
            assert_eq!(from_node, "a");
            assert_eq!(data, b"hello");
        }
        other => panic!("expected NetRecv, got {:?}", other),
    }
}

/// Messages sent while a partition is active are dropped (NetSendErr returned).
#[test]
fn simnet_partition_drops_messages() {
    let mut bus = single_node_bus(42, 1);

    // Inject partition: A <-> B.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetPartition { node_a: "a".into(), node_b: "b".into() },
        0,
    );
    bus.run(5);

    // Try to send while partitioned.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend {
            conn_id: 2,
            to_node: "b".into(),
            data: b"dropped".to_vec(),
        },
        0,
    );
    bus.run(20);

    // Sender should get NetSendErr.
    let coll_a = bus.actor::<Collector>(COLL_A_ID).unwrap();
    let errs: Vec<_> = coll_a
        .received
        .iter()
        .filter(|m| matches!(m, Message::NetSendErr { .. }))
        .collect();
    assert_eq!(errs.len(), 1, "expected NetSendErr while partitioned");

    // B's collector should receive NOTHING.
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert!(
        coll_b.received.is_empty(),
        "B must not receive anything while partitioned"
    );
}

/// After healing a partition, messages flow again.
#[test]
fn simnet_heal_restores_communication() {
    let mut bus = single_node_bus(42, 1);

    // 1. Inject partition.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetPartition { node_a: "a".into(), node_b: "b".into() },
        0,
    );
    bus.run(5);

    // 2. Send while partitioned → error.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend { conn_id: 1, to_node: "b".into(), data: b"x".to_vec() },
        0,
    );
    bus.run(10);

    // 3. Heal the partition.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetHeal { node_a: "a".into(), node_b: "b".into() },
        0,
    );
    bus.run(5);

    // 4. Send again → success.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend { conn_id: 2, to_node: "b".into(), data: b"hello".to_vec() },
        0,
    );
    bus.run(20);

    // B should receive exactly one NetRecv (the second send, after healing).
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    let recvs: Vec<_> = coll_b
        .received
        .iter()
        .filter(|m| matches!(m, Message::NetRecv { .. }))
        .collect();
    assert_eq!(recvs.len(), 1, "expected exactly one successful delivery");
    match &recvs[0] {
        Message::NetRecv { data, .. } => assert_eq!(data, b"hello"),
        _ => unreachable!(),
    }
}

/// NetRecv arrives after `base_latency` ticks, not before.
#[test]
fn simnet_latency_applied() {
    let base_latency = 3u64;
    let mut bus = single_node_bus(42, base_latency); // no variance

    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend { conn_id: 1, to_node: "b".into(), data: b"ping".to_vec() },
        0,
    );

    // Tick 0 processes NetSend → SimNetwork enqueues NetRecv at tick `base_latency`.
    // Ticks 1..base_latency-1 are silent for B.
    for _ in 0..base_latency {
        bus.tick();
    }
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert!(
        coll_b.received.is_empty(),
        "NetRecv must not arrive before base_latency ticks"
    );

    // One more tick delivers the message.
    bus.tick();
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert_eq!(coll_b.received.len(), 1, "NetRecv must arrive at base_latency ticks");
}

/// Running the same seed twice produces byte-for-byte identical traces.
#[test]
fn simnet_deterministic_replay() {
    fn run_with_seed(seed: u64) -> Vec<String> {
        let injector = FaultInjector::new(FaultConfig {
            drop_probability: 0.05,
            ..FaultConfig::none()
        });
        let cfg = DatabaseConfig {
            sim_network_latency_ticks: 2,
            sim_network_latency_variance_ticks: 1,
            ..DatabaseConfig::default()
        };
        let mut bus = MessageBus::new(seed, injector, &cfg);

        let mut sim_a = SimNetwork::new(SIM_A_ID, "a", seed, &cfg);
        sim_a.add_peer("b", COLL_B_ID);
        bus.register(Box::new(sim_a));
        bus.register(Box::new(Collector::new(COLL_A_ID)));
        bus.register(Box::new(Collector::new(COLL_B_ID)));

        for i in 0u64..5 {
            bus.send(
                COLL_A_ID,
                SIM_A_ID,
                Message::NetSend {
                    conn_id: i,
                    to_node: "b".into(),
                    data: vec![i as u8],
                },
                i,
            );
        }
        bus.run(50);

        bus.trace()
            .iter()
            .map(|(t, f, to, m)| format!("{:05}:{}->{}:{}", t, f, to, m))
            .collect()
    }

    let trace_a = run_with_seed(999);
    let trace_b = run_with_seed(999);

    assert!(!trace_a.is_empty());
    assert_eq!(
        trace_a, trace_b,
        "DETERMINISM REGRESSION: SimNetwork produced different traces with same seed"
    );
}

/// Bus-level fault injection (message drop) interacts deterministically with SimNetwork.
#[test]
fn simnet_fault_injector_interacts() {
    let fault_cfg = FaultConfig {
        drop_probability: 0.3,
        ..FaultConfig::none()
    };
    let injector = FaultInjector::new(fault_cfg.clone());
    let net_cfg = DatabaseConfig {
        sim_network_latency_ticks: 2,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let mut bus = MessageBus::new(1234, injector, &net_cfg);

    let mut sim_a = SimNetwork::new(SIM_A_ID, "a", 1234, &net_cfg);
    sim_a.add_peer("b", COLL_B_ID);
    bus.register(Box::new(sim_a));
    bus.register(Box::new(Collector::new(COLL_A_ID)));
    bus.register(Box::new(Collector::new(COLL_B_ID)));

    // Send several messages; some will be dropped by the bus fault injector.
    for i in 0u64..10 {
        bus.send(
            COLL_A_ID,
            SIM_A_ID,
            Message::NetSend { conn_id: i, to_node: "b".into(), data: vec![i as u8] },
            0,
        );
    }
    bus.run(50);

    // Determinism check: same seed must replay identically.
    let trace1 = bus
        .trace()
        .iter()
        .map(|(t, f, to, m)| format!("{:05}:{}->{}:{}", t, f, to, m))
        .collect::<Vec<_>>();

    // Rebuild with the same seed.
    let injector2 = FaultInjector::new(fault_cfg);
    let mut bus2 = MessageBus::new(1234, injector2, &net_cfg);
    let mut sim_a2 = SimNetwork::new(SIM_A_ID, "a", 1234, &net_cfg);
    sim_a2.add_peer("b", COLL_B_ID);
    bus2.register(Box::new(sim_a2));
    bus2.register(Box::new(Collector::new(COLL_A_ID)));
    bus2.register(Box::new(Collector::new(COLL_B_ID)));
    for i in 0u64..10 {
        bus2.send(
            COLL_A_ID,
            SIM_A_ID,
            Message::NetSend { conn_id: i, to_node: "b".into(), data: vec![i as u8] },
            0,
        );
    }
    bus2.run(50);
    let trace2 = bus2
        .trace()
        .iter()
        .map(|(t, f, to, m)| format!("{:05}:{}->{}:{}", t, f, to, m))
        .collect::<Vec<_>>();

    assert_eq!(trace1, trace2, "fault injection must be deterministic");
}

/// Partitioning A-B does not affect A-C; multiple partitions are independent.
#[test]
fn simnet_multiple_partitions() {
    const COLL_C_ID: ActorId = ActorId(5);

    let injector = FaultInjector::new(FaultConfig::none());
    let cfg = DatabaseConfig {
        sim_network_latency_ticks: 1,
        sim_network_latency_variance_ticks: 0,
        ..DatabaseConfig::default()
    };
    let mut bus = MessageBus::new(42, injector, &cfg);

    let mut sim_a = SimNetwork::new(SIM_A_ID, "a", 42, &cfg);
    sim_a.add_peer("b", COLL_B_ID);
    sim_a.add_peer("c", COLL_C_ID);

    bus.register(Box::new(sim_a));
    bus.register(Box::new(Collector::new(COLL_A_ID)));
    bus.register(Box::new(Collector::new(COLL_B_ID)));
    bus.register(Box::new(Collector::new(COLL_C_ID)));

    // Partition A-B only.
    bus.send(COLL_A_ID, SIM_A_ID, Message::NetPartition { node_a: "a".into(), node_b: "b".into() }, 0);
    bus.run(5);

    // A→B fails.
    bus.send(COLL_A_ID, SIM_A_ID, Message::NetSend { conn_id: 1, to_node: "b".into(), data: b"x".to_vec() }, 0);
    // A→C succeeds.
    bus.send(COLL_A_ID, SIM_A_ID, Message::NetSend { conn_id: 2, to_node: "c".into(), data: b"y".to_vec() }, 0);
    bus.run(20);

    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert!(coll_b.received.is_empty(), "B must not receive while A-B partitioned");

    let coll_c = bus.actor::<Collector>(COLL_C_ID).unwrap();
    assert_eq!(coll_c.received.len(), 1, "C must receive; A-C is not partitioned");
}

/// Partition is bidirectional: A→B and B→A both fail when both SimNetworks
/// have the partition applied.
#[test]
fn simnet_partition_is_bidirectional() {
    let mut bus = two_node_bus(42);

    // Apply partition to BOTH sim_a and sim_b.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetPartition { node_a: "a".into(), node_b: "b".into() },
        0,
    );
    bus.send(
        COLL_B_ID,
        SIM_B_ID,
        Message::NetPartition { node_a: "a".into(), node_b: "b".into() },
        0,
    );
    bus.run(5);

    // A → B fails.
    bus.send(
        COLL_A_ID,
        SIM_A_ID,
        Message::NetSend { conn_id: 1, to_node: "b".into(), data: b"from-a".to_vec() },
        0,
    );
    // B → A fails.
    bus.send(
        COLL_B_ID,
        SIM_B_ID,
        Message::NetSend { conn_id: 2, to_node: "a".into(), data: b"from-b".to_vec() },
        0,
    );
    bus.run(20);

    // A's collector should have NetSendErr (not NetRecv from B).
    let coll_a = bus.actor::<Collector>(COLL_A_ID).unwrap();
    assert!(
        coll_a.received.iter().any(|m| matches!(m, Message::NetSendErr { .. })),
        "A must get NetSendErr (A→B blocked)"
    );
    assert!(
        !coll_a.received.iter().any(|m| matches!(m, Message::NetRecv { .. })),
        "A must not receive NetRecv (B→A blocked)"
    );

    // B's collector should have NetSendErr (not NetRecv from A).
    let coll_b = bus.actor::<Collector>(COLL_B_ID).unwrap();
    assert!(
        coll_b.received.iter().any(|m| matches!(m, Message::NetSendErr { .. })),
        "B must get NetSendErr (B→A blocked)"
    );
    assert!(
        !coll_b.received.iter().any(|m| matches!(m, Message::NetRecv { .. })),
        "B must not receive NetRecv (A→B blocked)"
    );
}
