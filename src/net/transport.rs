use std::io;

/// Abstraction over a point-to-point network transport.
///
/// The simulated path uses [`crate::net::sim_network::SimNetwork`] on the bus.
/// A production TCP implementation would live here.
pub trait NetworkTransport {
    /// Open a connection to the named node.
    fn connect(&mut self, node: &str) -> io::Result<()>;

    /// Close the connection to the named node.
    fn disconnect(&mut self, node: &str);

    /// Send bytes to the named node.
    fn send(&mut self, node: &str, data: &[u8]) -> io::Result<()>;

    /// Drain all received messages, returning (from_node, data) pairs.
    fn recv_all(&mut self) -> Vec<(String, Vec<u8>)>;
}
