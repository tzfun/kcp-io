//! Transport abstraction for KCP packet I/O.
//!
//! This module provides [`KcpTransport`], a trait that decouples the KCP protocol
//! engine from the underlying network transport. Users can implement custom
//! transports to:
//!
//! - Use non-UDP transport layers (e.g., WebSocket, Unix sockets)
//! - Encrypt/decrypt packets before/after transmission
//! - Filter, log, or modify packets in transit
//! - Implement custom congestion or QoS policies
//!
//! The default implementation is [`KcpUdpTransport`], which wraps a
//! `tokio::net::UdpSocket` for standard UDP-based KCP communication.
//!
//! # Example: Custom Transport with XOR Obfuscation
//!
//! ```no_run
//! use kcp_io::tokio_rt::{KcpTransport, KcpUdpTransport};
//! use std::io;
//! use std::net::SocketAddr;
//! use std::sync::Arc;
//! use tokio::net::UdpSocket;
//!
//! struct XorTransport {
//!     inner: KcpUdpTransport,
//!     key: u8,
//! }
//!
//! impl KcpTransport for XorTransport {
//!     fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> {
//!         self.inner.try_send(data, addr)
//!     }
//!
//!     fn process_outgoing(&self, data: &[u8], _addr: SocketAddr) -> Vec<u8> {
//!         data.iter().map(|b| b ^ self.key).collect()
//!     }
//!
//!     fn process_incoming(&self, data: &[u8], _addr: SocketAddr) -> Option<Vec<u8>> {
//!         Some(data.iter().map(|b| b ^ self.key).collect())
//!     }
//! }
//! ```

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

/// Trait for custom KCP transport implementations.
///
/// Implementors provide the actual packet I/O for a KCP session. The trait
/// separates three concerns:
///
/// 1. **Actual transmission** ([`try_send`](KcpTransport::try_send)) — the
///    synchronous send operation called from KCP's output callback.
/// 2. **Outgoing transformation** ([`process_outgoing`](KcpTransport::process_outgoing)) —
///    called before data is sent to the wire. Use for encryption, compression, etc.
/// 3. **Incoming transformation** ([`process_incoming`](KcpTransport::process_incoming)) —
///    called after data is received from the wire, before feeding to KCP.
///    Return `None` to silently drop the packet.
///
/// # Thread Safety
///
/// The trait requires `Send + Sync + 'static`. Implementations are typically
/// wrapped in `Arc<dyn KcpTransport>` and shared between the session, read half,
/// and write half.
///
/// # Performance
///
/// [`try_send`](KcpTransport::try_send) is called from KCP's synchronous output
/// callback context. It must not block — use non-blocking I/O (e.g.,
/// `UdpSocket::try_send_to`).
pub trait KcpTransport: Send + Sync + 'static {
    /// Synchronously send a raw KCP packet to the remote peer.
    ///
    /// This is called from KCP's synchronous output callback context during
    /// [`Kcp::update`](crate::core::Kcp::update) or
    /// [`Kcp::flush`](crate::core::Kcp::flush). It **must not block**.
    ///
    /// # Arguments
    ///
    /// * `data` — The raw KCP packet bytes (already processed by
    ///   [`process_outgoing`](KcpTransport::process_outgoing)).
    /// * `addr` — The remote peer's socket address.
    ///
    /// # Returns
    ///
    /// The number of bytes sent, or an I/O error. Non-fatal errors (e.g.,
    /// `WouldBlock`) should be treated as success, since KCP handles
    /// retransmission internally.
    fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize>;

    /// Transform outgoing bytes before transmission (KCP → wire).
    ///
    /// Called in the KCP output callback immediately before
    /// [`try_send`](KcpTransport::try_send). The returned `Vec<u8>` is what
    /// actually gets sent.
    ///
    /// The default implementation returns the data unchanged (identity transform).
    ///
    /// # Arguments
    ///
    /// * `data` — The raw bytes produced by the KCP engine.
    /// * `addr` — The remote peer's socket address.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kcp_io::tokio_rt::KcpTransport;
    /// # use std::io;
    /// # use std::net::SocketAddr;
    /// # use std::sync::Arc;
    /// # use tokio::net::UdpSocket;
    /// # struct MyTransport;
    /// # impl KcpTransport for MyTransport {
    /// # fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> { Ok(data.len()) }
    /// // Encrypt outgoing data with a simple XOR cipher
    /// fn process_outgoing(&self, data: &[u8], _addr: SocketAddr) -> Vec<u8> {
    ///     data.iter().map(|b| b ^ 0xAB).collect()
    /// }
    /// # }
    /// ```
    fn process_outgoing(&self, data: &[u8], _addr: SocketAddr) -> Vec<u8> {
        data.to_vec()
    }

    /// Transform incoming bytes before feeding to KCP (wire → KCP).
    ///
    /// Called immediately after receiving raw bytes from the transport, before
    /// they are passed to [`Kcp::input`](crate::core::Kcp::input).
    ///
    /// The default implementation returns the data unchanged (identity transform).
    ///
    /// Return `None` to silently drop the packet. This can be used for:
    /// - Filtering unwanted traffic
    /// - Dropping packets that fail authentication/decryption
    /// - Implementing custom access control
    ///
    /// # Arguments
    ///
    /// * `data` — The raw bytes received from the transport.
    /// * `addr` — The remote peer's socket address.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kcp_io::tokio_rt::KcpTransport;
    /// # use std::io;
    /// # use std::net::SocketAddr;
    /// # use std::sync::Arc;
    /// # use tokio::net::UdpSocket;
    /// # struct MyTransport;
    /// # impl KcpTransport for MyTransport {
    /// # fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> { Ok(data.len()) }
    /// // Decrypt incoming data, drop packets that fail decryption
    /// fn process_incoming(&self, data: &[u8], _addr: SocketAddr) -> Option<Vec<u8>> {
    ///     if data.is_empty() {
    ///         return None; // Drop empty packets
    ///     }
    ///     Some(data.iter().map(|b| b ^ 0xAB).collect())
    /// }
    /// # }
    /// ```
    fn process_incoming(&self, data: &[u8], _addr: SocketAddr) -> Option<Vec<u8>> {
        Some(data.to_vec())
    }
}

/// Default UDP-based transport for KCP communication.
///
/// Wraps a `tokio::net::UdpSocket` in an `Arc` and provides non-blocking
/// send via [`UdpSocket::try_send_to`].
///
/// This is the transport used by [`KcpStream::connect`](super::KcpStream::connect)
/// and [`KcpListener::bind`](super::KcpListener::bind) by default.
///
/// # Example
///
/// ```no_run
/// use kcp_io::tokio_rt::KcpUdpTransport;
/// use std::sync::Arc;
/// use tokio::net::UdpSocket;
///
/// # async fn example() -> std::io::Result<()> {
/// let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
/// let transport = KcpUdpTransport::new(socket);
/// # Ok(())
/// # }
/// ```
pub struct KcpUdpTransport {
    socket: Arc<UdpSocket>,
}

impl KcpUdpTransport {
    /// Creates a new UDP transport wrapping the given socket.
    pub fn new(socket: Arc<UdpSocket>) -> Self {
        Self { socket }
    }

    /// Returns a reference to the underlying UDP socket.
    pub fn socket(&self) -> &Arc<UdpSocket> {
        &self.socket
    }
}

impl KcpTransport for KcpUdpTransport {
    fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> {
        match self.socket.try_send_to(data, addr) {
            Ok(n) => Ok(n),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Non-blocking send would block — KCP will retransmit
                Ok(data.len())
            }
            // On Windows, ConnectionReset (error 10054 / WSAECONNRESET) can
            // occur when the remote peer has closed its UDP socket. Treated
            // as success since KCP handles retransmission or timeout.
            Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => Ok(data.len()),
            Err(e) => Err(e),
        }
    }
}
