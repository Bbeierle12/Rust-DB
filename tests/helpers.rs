use rust_dst_db::traits::message::{ActorId, Destination, Message};
use rust_dst_db::traits::state_machine::StateMachine;

/// A test actor that simply collects all messages it receives.
/// Used to observe outcomes in tests.
pub struct Collector {
    id: ActorId,
    pub received: Vec<Message>,
}

impl Collector {
    pub fn new(id: ActorId) -> Self {
        Self {
            id,
            received: Vec::new(),
        }
    }
}

impl StateMachine for Collector {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, _from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        self.received.push(msg);
        None
    }

    fn tick(&mut self, _now: u64) -> Option<Vec<(Message, Destination)>> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A simple echo actor that sends back whatever it receives.
pub struct Echo {
    id: ActorId,
}

impl Echo {
    pub fn new(id: ActorId) -> Self {
        Self { id }
    }
}

impl StateMachine for Echo {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        Some(vec![(msg, Destination { actor: from, delay: 0 })])
    }

    fn tick(&mut self, _now: u64) -> Option<Vec<(Message, Destination)>> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
