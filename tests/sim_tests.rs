//! Tests for the simulation harness: clock, RNG, fault injector, bus.

mod helpers;

use helpers::Collector;
use rust_dst_db::sim::bus::MessageBus;
use rust_dst_db::sim::clock::SimClock;
use rust_dst_db::sim::fault::{FaultAction, FaultConfig, FaultInjector};
use rust_dst_db::sim::rng::SeededRng;
use rust_dst_db::traits::io::Clock;
use rust_dst_db::traits::message::{ActorId, Envelope, Message};

// ─── SimClock ────────────────────────────────────────────────────────

#[test]
fn clock_starts_at_zero() {
    let clock = SimClock::new();
    assert_eq!(clock.now(), 0);
}

#[test]
fn clock_advances_only_on_tick() {
    let mut clock = SimClock::new();
    assert_eq!(clock.now(), 0);
    clock.tick();
    assert_eq!(clock.now(), 1);
    clock.tick();
    assert_eq!(clock.now(), 2);
    // No wall-clock time passes — only explicit ticks.
}

#[test]
fn clock_advance_to() {
    let mut clock = SimClock::new();
    clock.advance_to(42);
    assert_eq!(clock.now(), 42);
}

// ─── SeededRng ───────────────────────────────────────────────────────

#[test]
fn rng_same_seed_same_sequence() {
    let mut rng_a = SeededRng::new(999);
    let mut rng_b = SeededRng::new(999);

    let seq_a: Vec<u64> = (0..100).map(|_| rng_a.next_u64()).collect();
    let seq_b: Vec<u64> = (0..100).map(|_| rng_b.next_u64()).collect();

    assert_eq!(seq_a, seq_b);
}

#[test]
fn rng_different_seed_different_sequence() {
    let mut rng_a = SeededRng::new(1);
    let mut rng_b = SeededRng::new(2);

    let seq_a: Vec<u64> = (0..10).map(|_| rng_a.next_u64()).collect();
    let seq_b: Vec<u64> = (0..10).map(|_| rng_b.next_u64()).collect();

    assert_ne!(seq_a, seq_b);
}

#[test]
fn rng_f64_in_range() {
    let mut rng = SeededRng::new(42);
    for _ in 0..1000 {
        let v = rng.next_f64();
        assert!((0.0..1.0).contains(&v), "f64 out of range: {}", v);
    }
}

#[test]
fn rng_chance_respects_probability() {
    let mut rng = SeededRng::new(42);
    let trials = 10_000;

    let hits_zero = (0..trials).filter(|_| rng.chance(0.0)).count();
    assert_eq!(hits_zero, 0);

    let hits_one = (0..trials).filter(|_| rng.chance(1.0)).count();
    assert_eq!(hits_one, trials);

    let hits_half = (0..trials).filter(|_| rng.chance(0.5)).count();
    // Should be roughly 5000 ± a few hundred.
    assert!(
        (4000..6000).contains(&hits_half),
        "Expected ~5000 hits, got {}",
        hits_half
    );
}

// ─── FaultInjector ───────────────────────────────────────────────────

#[test]
fn fault_injector_passes_when_no_faults() {
    let injector = FaultInjector::new(FaultConfig::none());
    let mut rng = SeededRng::new(42);

    let mut envelope = Envelope {
        from: ActorId(1),
        to: ActorId(2),
        deliver_at: 0,
        message: Message::Tick,
    };

    let action = injector.maybe_fault(&mut envelope, &mut rng);
    assert_eq!(action, FaultAction::Pass);
}

#[test]
fn fault_injector_drops_messages() {
    let config = FaultConfig {
        drop_probability: 1.0,
        ..FaultConfig::none()
    };
    let injector = FaultInjector::new(config);
    let mut rng = SeededRng::new(42);

    let mut envelope = Envelope {
        from: ActorId(1),
        to: ActorId(2),
        deliver_at: 0,
        message: Message::Tick,
    };

    let action = injector.maybe_fault(&mut envelope, &mut rng);
    assert_eq!(action, FaultAction::Drop);
}

#[test]
fn fault_injector_corrupts_disk_reads() {
    let config = FaultConfig {
        corruption_probability: 1.0,
        ..FaultConfig::none()
    };
    let injector = FaultInjector::new(config);
    let mut rng = SeededRng::new(42);

    let original_data = vec![1, 2, 3, 4, 5];
    let mut envelope = Envelope {
        from: ActorId(1),
        to: ActorId(2),
        deliver_at: 0,
        message: Message::DiskReadOk {
            file_id: 0,
            offset: 0,
            data: original_data.clone(),
        },
    };

    let action = injector.maybe_fault(&mut envelope, &mut rng);
    assert_eq!(action, FaultAction::Mutated);

    // Data should be corrupted.
    if let Message::DiskReadOk { data, .. } = &envelope.message {
        assert_ne!(data, &original_data, "Data should have been corrupted");
    } else {
        panic!("Message should still be DiskReadOk");
    }
}

#[test]
fn fault_injector_injects_io_errors() {
    let config = FaultConfig {
        disk_write_error_probability: 1.0,
        ..FaultConfig::none()
    };
    let injector = FaultInjector::new(config);
    let mut rng = SeededRng::new(42);

    let mut envelope = Envelope {
        from: ActorId(1),
        to: ActorId(2),
        deliver_at: 0,
        message: Message::DiskWriteOk {
            file_id: 0,
            offset: 0,
        },
    };

    let action = injector.maybe_fault(&mut envelope, &mut rng);
    assert_eq!(action, FaultAction::Mutated);

    match &envelope.message {
        Message::DiskWriteErr { reason, .. } => {
            assert!(reason.contains("simulated"));
        }
        other => panic!("Expected DiskWriteErr, got {:?}", other),
    }
}

#[test]
fn fault_injector_injects_fsync_errors() {
    let config = FaultConfig {
        fsync_error_probability: 1.0,
        ..FaultConfig::none()
    };
    let injector = FaultInjector::new(config);
    let mut rng = SeededRng::new(42);

    let mut envelope = Envelope {
        from: ActorId(1),
        to: ActorId(2),
        deliver_at: 0,
        message: Message::DiskFsyncOk { file_id: 0 },
    };

    let action = injector.maybe_fault(&mut envelope, &mut rng);
    assert_eq!(action, FaultAction::Mutated);

    match &envelope.message {
        Message::DiskFsyncErr { reason, .. } => {
            assert!(reason.contains("simulated"));
        }
        other => panic!("Expected DiskFsyncErr, got {:?}", other),
    }
}

// ─── MessageBus ──────────────────────────────────────────────────────

#[test]
fn bus_two_actors_communicate() {
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector);

    let a_id = ActorId(1);
    let b_id = ActorId(2);

    bus.register(Box::new(Collector::new(a_id)));
    bus.register(Box::new(Collector::new(b_id)));

    // A sends to B.
    bus.send(a_id, b_id, Message::Tick, 0);
    bus.run(10);

    let b = bus.actor::<Collector>(b_id).unwrap();
    assert_eq!(b.received.len(), 1);
    assert_eq!(b.received[0], Message::Tick);

    // A should not have received anything.
    let a = bus.actor::<Collector>(a_id).unwrap();
    assert!(a.received.is_empty());
}

#[test]
fn bus_respects_delivery_delay() {
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector);

    let collector_id = ActorId(1);
    bus.register(Box::new(Collector::new(collector_id)));

    // Send with delay=5.
    bus.send(ActorId(99), collector_id, Message::Tick, 5);

    // Run 4 ticks — should not be delivered yet.
    for _ in 0..5 {
        bus.tick();
    }
    let c = bus.actor::<Collector>(collector_id).unwrap();
    assert!(c.received.is_empty(), "Message delivered too early");

    // Tick 5 — should deliver now.
    bus.tick();
    let c = bus.actor::<Collector>(collector_id).unwrap();
    assert_eq!(c.received.len(), 1);
}

#[test]
fn bus_deterministic_delivery_order() {
    // Send multiple messages at the same tick from different senders.
    // Delivery order must be deterministic (sorted by from, then to).
    let injector = FaultInjector::new(FaultConfig::none());
    let mut bus = MessageBus::new(42, injector);

    let collector_id = ActorId(100);
    bus.register(Box::new(Collector::new(collector_id)));

    // Three senders, all at delay=0.
    bus.send(ActorId(3), collector_id, Message::WalAppend { data: vec![3] }, 0);
    bus.send(ActorId(1), collector_id, Message::WalAppend { data: vec![1] }, 0);
    bus.send(ActorId(2), collector_id, Message::WalAppend { data: vec![2] }, 0);

    bus.tick();

    let c = bus.actor::<Collector>(collector_id).unwrap();
    assert_eq!(c.received.len(), 3);

    // Should be ordered by sender ActorId (1, 2, 3).
    let data: Vec<Vec<u8>> = c.received.iter().map(|m| {
        if let Message::WalAppend { data } = m { data.clone() } else { panic!() }
    }).collect();
    assert_eq!(data, vec![vec![1], vec![2], vec![3]]);
}
