use std::any::Any;

use crate::traits::message::{ActorId, Destination, Message};

/// Core abstraction: every component is a state machine that communicates
/// exclusively through the message bus.
///
/// No async. No shared mutable state. The bus controls all interleaving,
/// making execution fully deterministic given the same seed.
pub trait StateMachine: Any {
    /// Return this state machine's unique ID.
    fn id(&self) -> ActorId;

    /// Handle an incoming message from `from`.
    /// Returns outgoing messages to be enqueued on the bus.
    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>>;

    /// Periodic tick called by the bus at each time step.
    /// Returns outgoing messages (e.g., timeouts, retries).
    fn tick(&mut self, now: u64) -> Option<Vec<(Message, Destination)>>;

    /// Downcast support.
    fn as_any(&self) -> &dyn Any;
}
