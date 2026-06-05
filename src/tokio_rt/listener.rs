//! KCP server listener for accepting incoming connections.
//!
//! This module provides [`KcpListener`], which binds to a UDP socket and accepts
//! incoming KCP connections. It runs a background task that multiplexes incoming
//! UDP packets by `(SocketAddr, conv)`, routing them to the appropriate
//! [`KcpSession`](super::session::KcpSession) via `mpsc::channel`.
//!
//! # Example
//!
//! ```no_run
//! use kcp_io::tokio_rt::{KcpListener, KcpSessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut listener = KcpListener::bind("0.0.0.0:9090", KcpSessionConfig::fast()).await?;
//! println!("Listening on {}", listener.local_addr());
//!
//! loop {
//!     let (mut stream, addr) = listener.accept().await?;
//!     println!("New connection from {} (conv={})", addr, stream.conv());
//!     // Handle the stream...
//! }
//! # }
//! ```

use super::config::KcpSessionConfig;
use super::error::{KcpTokioError, KcpTokioResult};
use super::session::KcpSession;
use super::stream::KcpStream;
use super::transport::KcpUdpTransport;
use crate::core::Kcp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// Composite key for identifying KCP sessions: `(remote address, conversation ID)`.
type SessionKey = (SocketAddr, u32);

/// A KCP server listener that accepts incoming connections.
///
/// `KcpListener` binds to a local UDP address and runs a background task that:
/// 1. Receives all incoming UDP packets on the shared socket.
/// 2. Extracts the KCP conversation ID from each packet header.
/// 3. Routes packets to existing sessions via `mpsc::channel`, keyed by
///    `(SocketAddr, conv)`.
/// 4. Creates new [`KcpStream`]s for previously unseen `(addr, conv)` pairs
///    and makes them available via [`accept()`](KcpListener::accept).
///
/// # Architecture
///
/// ```text
/// UDP Socket (shared)
///     │
///     ▼
/// Background Task (tokio::spawn)
///     ├── Known session? → mpsc::Sender → KcpSession (Channel mode)
///     └── New session?   → Create KcpSession → incoming_tx → accept()
/// ```
pub struct KcpListener {
    /// Channel receiver for newly accepted connections.
    incoming_rx: mpsc::Receiver<(KcpStream, SocketAddr)>,
    /// The local address the listener is bound to.
    local_addr: SocketAddr,
}

impl KcpListener {
    /// Binds a KCP listener to the specified address.
    ///
    /// Starts a background task that receives UDP packets and routes them
    /// to the appropriate KCP sessions.
    ///
    /// # Arguments
    ///
    /// * `addr` — Local address to bind to (e.g., `"0.0.0.0:9090"`).
    /// * `config` — Session configuration applied to all accepted connections.
    ///
    /// # Errors
    ///
    /// Returns an error if the UDP socket cannot be bound.
    pub async fn bind<A: tokio::net::ToSocketAddrs>(
        addr: A,
        config: KcpSessionConfig,
    ) -> KcpTokioResult<Self> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        let socket = Arc::new(socket);
        let (incoming_tx, incoming_rx) = mpsc::channel(64);
        let accept_socket = socket.clone();
        tokio::spawn(async move {
            Self::accept_loop(accept_socket, config, incoming_tx).await;
        });
        Ok(Self {
            incoming_rx,
            local_addr,
        })
    }

    /// Accepts the next incoming KCP connection.
    ///
    /// Returns a tuple of `(KcpStream, SocketAddr)` where the `SocketAddr` is
    /// the remote peer's address. Blocks until a new connection arrives.
    ///
    /// # Errors
    ///
    /// Returns [`KcpTokioError::Closed`] if the listener has been dropped.
    pub async fn accept(&mut self) -> KcpTokioResult<(KcpStream, SocketAddr)> {
        self.incoming_rx.recv().await.ok_or(KcpTokioError::Closed)
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Background task that receives UDP packets and routes them to sessions.
    ///
    /// For each incoming packet:
    /// 1. Skip packets smaller than `IKCP_OVERHEAD` (not valid KCP packets).
    /// 2. Extract the conversation ID from the packet header.
    /// 3. If a session for `(addr, conv)` exists, forward the packet via its channel.
    /// 4. If no session exists, create a new one and send it via `incoming_tx`.
    async fn accept_loop(
        socket: Arc<UdpSocket>,
        config: KcpSessionConfig,
        incoming_tx: mpsc::Sender<(KcpStream, SocketAddr)>,
    ) {
        let mut recv_buf = vec![0u8; config.recv_buf_size];
        let mut sessions: HashMap<SessionKey, mpsc::Sender<Vec<u8>>> = HashMap::new();
        // Tracks recently closed sessions to avoid re-creating them from stale
        // retransmission packets. Maps session key to the time it was closed.
        let mut closed_sessions: HashMap<SessionKey, Instant> = HashMap::new();
        // How long to remember closed sessions (prevents stale packets from
        // creating ghost sessions). Should be longer than KCP's max retransmit window.
        let closed_session_ttl = std::time::Duration::from_secs(60);
        loop {
            let (n, addr) = match socket.recv_from(&mut recv_buf).await {
                Ok(result) => result,
                Err(e) => {
                    // On Windows, error 10054 (WSAECONNRESET / ConnectionReset) is
                    // returned when an ICMP "Port Unreachable" message is received.
                    // This is normal when a remote peer has closed its UDP socket.
                    // We silently ignore it and continue accepting new packets.
                    if e.kind() == std::io::ErrorKind::ConnectionReset {
                        log::debug!("KcpListener: peer connection reset (ignored)");
                        continue;
                    }
                    log::error!("KcpListener: UDP recv error: {}", e);
                    continue;
                }
            };
            let packet = recv_buf[..n].to_vec();
            // Skip packets that are too small to be valid KCP packets
            if n < crate::sys::IKCP_OVERHEAD {
                continue;
            }
            let conv = Kcp::get_conv(&packet);
            let key = (addr, conv);
            // Route to existing session
            if let Some(tx) = sessions.get(&key) {
                if tx.send(packet).await.is_err() {
                    // Session's receiver has been dropped, clean up and remember
                    // it as closed to prevent re-creation from stale packets.
                    log::debug!("KcpListener: session {:?} closed, removing", key);
                    sessions.remove(&key);
                    closed_sessions.insert(key, Instant::now());
                }
                continue;
            }
            // Skip packets from recently closed sessions (stale retransmissions).
            if let Some(closed_at) = closed_sessions.get(&key) {
                if closed_at.elapsed() < closed_session_ttl {
                    continue;
                }
                // TTL expired, allow new connections from this (addr, conv)
                closed_sessions.remove(&key);
            }
            // Periodically clean up expired entries from the closed sessions map
            if closed_sessions.len() > 100 {
                closed_sessions.retain(|_, closed_at| closed_at.elapsed() < closed_session_ttl);
            }
            // Create new session for unknown (addr, conv) pair
            let (pkt_tx, pkt_rx) = mpsc::channel::<Vec<u8>>(256);
            let transport = Arc::new(KcpUdpTransport::new(socket.clone()));
            let session = match KcpSession::new_with_channel(
                conv,
                socket.clone(),
                transport,
                addr,
                config.clone(),
                pkt_rx,
            ) {
                Ok(s) => s,
                Err(e) => {
                    log::error!("KcpListener: failed to create session: {}", e);
                    continue;
                }
            };
            let stream = KcpStream::from_session(session);
            // Feed the first packet to the new session's channel
            if pkt_tx.send(packet).await.is_err() {
                continue;
            }
            sessions.insert(key, pkt_tx);
            if incoming_tx.send((stream, addr)).await.is_err() {
                break;
            }
        }
    }
}
