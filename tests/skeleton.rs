//! Walking skeleton: boots the simulation harness, creates a state machine,
//! sends a message through the bus, and verifies deterministic replay.

mod helpers;

use helpers::{Collector, Echo};
use rust_dst_db::config::DatabaseConfig;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::fault::{FaultConfig, FaultInjector};
use rust_dst_db::traits::message::{ActorId, Message};

#[test]
fn skeleton_boots_and_delivers_message() {
    let injector = FaultInjector::new(FaultConfig::none());
    let cfg = DatabaseConfig::default();
    let mut bus = MessageBus::new(42, injector, &cfg);

    let echo_id = ActorId(1);
    let collector_id = ActorId(2);

    bus.register(Box::new(Echo::new(echo_id)));
    bus.register(Box::new(Collector::new(collector_id)));

    // Send a Tick from collector to echo; echo will bounce it back.
    bus.send(collector_id, echo_id, Message::Tick, 0);
    bus.run(100);

    let collector = bus.actor::<Collector>(collector_id).unwrap();
    assert_eq!(collector.received.len(), 1);
    assert_eq!(collector.received[0], Message::Tick);
}

#[test]
fn deterministic_replay_same_seed() {
    // Run the same scenario twice with the same seed.
    // Trace logs must be byte-identical.
    fn run_scenario(seed: u64) -> Vec<String> {
        let injector = FaultInjector::new(FaultConfig::none());
        let cfg = DatabaseConfig::default();
        let mut bus = MessageBus::new(seed, injector, &cfg);

        let echo_id = ActorId(1);
        let collector_id = ActorId(2);

        bus.register(Box::new(Echo::new(echo_id)));
        bus.register(Box::new(Collector::new(collector_id)));

        // Send several messages.
        bus.send(collector_id, echo_id, Message::Tick, 0);
        bus.send(collector_id, echo_id, Message::Tick, 1);
        bus.send(collector_id, echo_id, Message::Tick, 2);
        bus.run(100);

        bus.trace()
            .iter()
            .map(|(tick, from, to, msg)| format!("{}:{}->{}:{}", tick, from, to, msg))
            .collect()
    }

    let trace_a = run_scenario(12345);
    let trace_b = run_scenario(12345);

    assert_eq!(trace_a, trace_b, "Same seed must produce identical traces");
    assert!(!trace_a.is_empty(), "Trace should not be empty");
}

#[test]
fn different_seeds_may_differ_under_faults() {
    fn run_with_faults(seed: u64) -> Vec<String> {
        let config = FaultConfig {
            drop_probability: 0.3,
            ..FaultConfig::none()
        };
        let injector = FaultInjector::new(config);
        let cfg = DatabaseConfig::default();
        let mut bus = MessageBus::new(seed, injector, &cfg);

        let echo_id = ActorId(1);
        let collector_id = ActorId(2);

        bus.register(Box::new(Echo::new(echo_id)));
        bus.register(Box::new(Collector::new(collector_id)));

        for i in 0..10 {
            bus.send(collector_id, echo_id, Message::Tick, i);
        }
        bus.run(200);

        bus.trace()
            .iter()
            .map(|(tick, from, to, msg)| format!("{}:{}->{}:{}", tick, from, to, msg))
            .collect()
    }

    let trace_a = run_with_faults(111);
    let trace_b = run_with_faults(222);

    // With different seeds and fault injection, traces will likely differ.
    // (This is probabilistic but extremely unlikely to be equal.)
    // More importantly, same seed still replays identically:
    let trace_a2 = run_with_faults(111);
    assert_eq!(trace_a, trace_a2, "Same seed must replay identically even with faults");

    // Different seeds should (almost certainly) produce different traces.
    assert_ne!(trace_a, trace_b, "Different seeds should produce different fault patterns");
}
