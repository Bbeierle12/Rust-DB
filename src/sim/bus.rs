use std::collections::BTreeMap;

use crate::config::DatabaseConfig;
use crate::sim::clock::SimClock;
use crate::sim::fault::{FaultAction, FaultInjector};
use crate::sim::rng::SeededRng;
use crate::traits::io::Clock;
use crate::traits::message::{ActorId, Destination, Envelope, Message};
use crate::traits::state_machine::StateMachine;

/// The deterministic event-driven message bus.
///
/// All state machines register here. The bus delivers messages in
/// deterministic order (by delivery time, then sender, then receiver).
/// The same seed produces the exact same execution trace.
pub struct MessageBus {
    clock: SimClock,
    rng: SeededRng,
    fault_injector: FaultInjector,
    /// State machines indexed by ActorId.
    actors: BTreeMap<ActorId, Box<dyn StateMachine>>,
    /// Priority queue of pending messages, keyed by delivery tick.
    /// BTreeMap guarantees deterministic iteration order.
    queue: BTreeMap<u64, Vec<Envelope>>,
    /// Trace log of delivered messages for determinism verification.
    trace: Vec<(u64, ActorId, ActorId, String)>,
    /// Total messages delivered.
    delivered_count: u64,
}

impl MessageBus {
    pub fn new(seed: u64, fault_injector: FaultInjector, _cfg: &DatabaseConfig) -> Self {
        Self {
            clock: SimClock::new(),
            rng: SeededRng::new(seed),
            fault_injector,
            actors: BTreeMap::new(),
            queue: BTreeMap::new(),
            trace: Vec::new(),
            delivered_count: 0,
        }
    }

    /// Register a state machine with the bus.
    pub fn register(&mut self, actor: Box<dyn StateMachine>) {
        let id = actor.id();
        self.actors.insert(id, actor);
    }

    /// Enqueue a message for delivery.
    pub fn send(&mut self, from: ActorId, to: ActorId, msg: Message, delay: u64) {
        let deliver_at = self.clock.now() + delay;
        let envelope = Envelope {
            from,
            to,
            deliver_at,
            message: msg,
        };
        self.queue.entry(deliver_at).or_default().push(envelope);
    }

    /// Enqueue outgoing messages produced by a state machine.
    fn enqueue_outgoing(&mut self, from: ActorId, outgoing: Vec<(Message, Destination)>) {
        for (msg, dest) in outgoing {
            self.send(from, dest.actor, msg, dest.delay);
        }
    }

    /// Run a single tick: deliver all messages scheduled for the current time,
    /// then advance the clock.
    ///
    /// Messages produced with delay=0 during this tick are also delivered
    /// within the same tick (processed in waves until quiescent).
    ///
    /// Returns the number of messages delivered in this tick.
    pub fn tick(&mut self) -> usize {
        let now = self.clock.now();
        let mut delivered = 0;

        // Process waves: deliver all messages at `now`, including any new
        // messages enqueued at `now` by handlers (delay=0 responses).
        while let Some(mut envelopes) = self.queue.remove(&now) {
            envelopes.sort();

            for mut envelope in envelopes {
                // Run fault injection.
                let action = self
                    .fault_injector
                    .maybe_fault(&mut envelope, &mut self.rng);
                match action {
                    FaultAction::Drop => {
                        self.trace.push((
                            now,
                            envelope.from,
                            envelope.to,
                            format!("DROPPED: {:?}", envelope.message),
                        ));
                        continue;
                    }
                    FaultAction::Mutated | FaultAction::Pass => {}
                }

                let msg_debug = format!("{:?}", envelope.message);
                self.trace
                    .push((now, envelope.from, envelope.to, msg_debug));

                // Deliver to the target actor.
                if let Some(actor) = self.actors.remove(&envelope.to) {
                    let mut actor = actor;
                    let outgoing = actor.receive(envelope.from, envelope.message);
                    let actor_id = actor.id();
                    self.actors.insert(actor_id, actor);

                    if let Some(msgs) = outgoing {
                        self.enqueue_outgoing(actor_id, msgs);
                    }
                }

                delivered += 1;
                self.delivered_count += 1;
            }
        }

        // Tick all actors (may produce messages scheduled at `now`).
        let actor_ids: Vec<ActorId> = self.actors.keys().copied().collect();
        for id in actor_ids {
            if let Some(mut actor) = self.actors.remove(&id) {
                let outgoing = actor.tick(now);
                self.actors.insert(id, actor);
                if let Some(msgs) = outgoing {
                    self.enqueue_outgoing(id, msgs);
                }
            }
        }

        // Second wave: process any messages that actor ticks just enqueued at `now`.
        while let Some(mut envelopes) = self.queue.remove(&now) {
            envelopes.sort();

            for mut envelope in envelopes {
                let action = self
                    .fault_injector
                    .maybe_fault(&mut envelope, &mut self.rng);
                match action {
                    FaultAction::Drop => {
                        self.trace.push((
                            now,
                            envelope.from,
                            envelope.to,
                            format!("DROPPED: {:?}", envelope.message),
                        ));
                        continue;
                    }
                    FaultAction::Mutated | FaultAction::Pass => {}
                }

                let msg_debug = format!("{:?}", envelope.message);
                self.trace
                    .push((now, envelope.from, envelope.to, msg_debug));

                if let Some(actor) = self.actors.remove(&envelope.to) {
                    let mut actor = actor;
                    let outgoing = actor.receive(envelope.from, envelope.message);
                    let actor_id = actor.id();
                    self.actors.insert(actor_id, actor);
                    if let Some(msgs) = outgoing {
                        self.enqueue_outgoing(actor_id, msgs);
                    }
                }

                delivered += 1;
                self.delivered_count += 1;
            }
        }

        self.clock.tick();
        delivered
    }

    /// Run the simulation for up to `max_ticks` ticks, or until the queue
    /// is drained and no new messages are produced.
    pub fn run(&mut self, max_ticks: u64) -> u64 {
        let mut total_delivered = 0u64;
        let mut idle_ticks = 0u64;

        for _ in 0..max_ticks {
            let delivered = self.tick();
            total_delivered += delivered as u64;

            if delivered == 0 && self.queue.is_empty() {
                idle_ticks += 1;
                if idle_ticks > 1 {
                    break;
                }
            } else {
                idle_ticks = 0;
            }
        }

        total_delivered
    }

    /// Return the current simulation time.
    pub fn now(&self) -> u64 {
        self.clock.now()
    }

    /// Return the trace log for determinism verification.
    pub fn trace(&self) -> &[(u64, ActorId, ActorId, String)] {
        &self.trace
    }

    /// Return how many messages are still pending.
    pub fn pending_count(&self) -> usize {
        self.queue.values().map(|v| v.len()).sum()
    }

    /// Return total messages delivered across the simulation.
    pub fn delivered_count(&self) -> u64 {
        self.delivered_count
    }

    /// Get a reference to a registered actor, downcast to a concrete type.
    pub fn actor<T: StateMachine + 'static>(&self, id: ActorId) -> Option<&T> {
        self.actors.get(&id).and_then(|a| {
            let any: &dyn std::any::Any = a.as_any();
            any.downcast_ref::<T>()
        })
    }
}
