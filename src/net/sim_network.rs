use std::collections::{BTreeMap, BTreeSet};

use crate::config::DatabaseConfig;
use crate::sim::rng::SeededRng;
use crate::traits::message::{ActorId, Destination, Message};
use crate::traits::state_machine::StateMachine;

/// Simulated peer-to-peer network state machine.
///
/// Runs on the bus; enables partition testing, message reordering,
/// latency variance, and connection-level fault injection — all
/// deterministically given the same seed.
///
/// # Two-hop routing
///
/// `peers` maps peer node names to a destination ActorId. In a multi-node
/// simulation that ActorId is the peer's own `SimNetwork`; the peer's
/// `SimNetwork` then forwards `NetRecv` to its `app_actor`. In simple
/// single-SimNetwork tests the peer ActorId can point directly to a Collector.
pub struct SimNetwork {
    id: ActorId,
    /// This node's logical name.
    node_id: String,
    /// peer node name → actor to deliver NetRecv to (typically the peer SimNetwork).
    peers: BTreeMap<String, ActorId>,
    /// Local application actor that should receive incoming `NetRecv` messages.
    /// When `None`, `NetRecv` messages received from the bus are silently dropped.
    app_actor: Option<ActorId>,
    /// Partitioned pairs in canonical order: (min, max).
    partitions: BTreeSet<(String, String)>,
    base_latency: u64,
    latency_variance: u64,
    /// Independent RNG for jitter — separate from the bus's fault RNG.
    rng: SeededRng,
}

impl SimNetwork {
    /// Create a new `SimNetwork` using latency values from config.
    pub fn new(
        id: ActorId,
        node_id: impl Into<String>,
        rng_seed: u64,
        cfg: &DatabaseConfig,
    ) -> Self {
        Self {
            id,
            node_id: node_id.into(),
            peers: BTreeMap::new(),
            app_actor: None,
            partitions: BTreeSet::new(),
            base_latency: cfg.sim_network_latency_ticks,
            latency_variance: cfg.sim_network_latency_variance_ticks,
            rng: SeededRng::new(rng_seed),
        }
    }

    /// Set the local application actor that receives incoming `NetRecv` messages.
    pub fn set_app_actor(mut self, app: ActorId) -> Self {
        self.app_actor = Some(app);
        self
    }

    /// Register a peer by name and its destination ActorId on the bus.
    pub fn add_peer(&mut self, node_id: impl Into<String>, actor: ActorId) {
        self.peers.insert(node_id.into(), actor);
    }

    /// Return this node's logical ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Return whether the given pair is currently partitioned.
    pub fn is_partitioned(&self, a: &str, b: &str) -> bool {
        self.partitions.contains(&canonical_pair(a, b))
    }

    fn jitter(&mut self) -> u64 {
        if self.latency_variance == 0 {
            0
        } else {
            self.rng.next_range(self.latency_variance + 1)
        }
    }
}

/// Return the canonical (alphabetically ordered) partition key.
fn canonical_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

impl StateMachine for SimNetwork {
    fn id(&self) -> ActorId {
        self.id
    }

    fn receive(&mut self, from: ActorId, msg: Message) -> Option<Vec<(Message, Destination)>> {
        match msg {
            // ── Outbound: route data to a remote peer ─────────────────────
            Message::NetSend {
                conn_id,
                ref to_node,
                ref data,
            } => {
                // Check for active partition.
                if self.is_partitioned(&self.node_id.clone(), to_node) {
                    return Some(vec![(
                        Message::NetSendErr {
                            conn_id,
                            to_node: to_node.clone(),
                            reason: format!("partitioned: {} <-> {}", self.node_id, to_node),
                        },
                        Destination {
                            actor: from,
                            delay: 0,
                        },
                    )]);
                }

                // Look up the destination actor for this peer.
                let peer_actor = match self.peers.get(to_node) {
                    Some(&a) => a,
                    None => {
                        return Some(vec![(
                            Message::NetSendErr {
                                conn_id,
                                to_node: to_node.clone(),
                                reason: format!("unknown peer: {}", to_node),
                            },
                            Destination {
                                actor: from,
                                delay: 0,
                            },
                        )]);
                    }
                };

                let delay = self.base_latency + self.jitter();
                let from_node = self.node_id.clone();

                Some(vec![
                    // Forward NetRecv to the peer (with simulated latency).
                    (
                        Message::NetRecv {
                            conn_id,
                            from_node,
                            data: data.clone(),
                        },
                        Destination {
                            actor: peer_actor,
                            delay,
                        },
                    ),
                    // Ack to the local sender immediately.
                    (
                        Message::NetSendOk {
                            conn_id,
                            to_node: to_node.clone(),
                        },
                        Destination {
                            actor: from,
                            delay: 0,
                        },
                    ),
                ])
            }

            // ── Inbound: forward NetRecv to the local application ─────────
            Message::NetRecv { .. } => {
                if let Some(app) = self.app_actor {
                    Some(vec![(
                        msg,
                        Destination {
                            actor: app,
                            delay: 0,
                        },
                    )])
                } else {
                    // No app registered — message is delivered here (for tests
                    // that directly inspect SimNetwork state or use it as sink).
                    None
                }
            }

            // ── Connection lifecycle ───────────────────────────────────────
            Message::NetConnect { conn_id, ref node } => {
                let reply = if self.peers.contains_key(node.as_str()) {
                    Message::NetConnected {
                        conn_id,
                        node: node.clone(),
                    }
                } else {
                    Message::NetDisconnected {
                        conn_id,
                        node: node.clone(),
                    }
                };
                Some(vec![(
                    reply,
                    Destination {
                        actor: from,
                        delay: 0,
                    },
                )])
            }

            // ── Simulation control ─────────────────────────────────────────
            Message::NetPartition {
                ref node_a,
                ref node_b,
            } => {
                self.partitions.insert(canonical_pair(node_a, node_b));
                None
            }

            Message::NetHeal {
                ref node_a,
                ref node_b,
            } => {
                self.partitions.remove(&canonical_pair(node_a, node_b));
                None
            }

            _ => None,
        }
    }

    fn tick(&mut self, _now: u64) -> Option<Vec<(Message, Destination)>> {
        None
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
