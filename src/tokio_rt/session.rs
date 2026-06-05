//! KCP session management.
//!
//! This module provides [`KcpSession`], the internal state machine that manages
//! a single KCP connection. It bridges the [`Kcp`](crate::core::Kcp) engine with
//! a `tokio::net::UdpSocket` and handles:
//!
//! - Sending data through KCP with optional immediate flush
//! - Receiving data asynchronously (with `tokio::select!` for concurrent UDP recv + timer)
//! - Timeout detection
//! - Two receive modes: direct socket (client) or channel (server)

use super::config::KcpSessionConfig;
use super::error::{KcpTokioError, KcpTokioResult};
use super::transport::KcpTransport;
use crate::core::Kcp;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex};
use tokio::time;

/// Determines how the session receives raw UDP packets.
enum RecvMode {
    /// Client mode: reads directly from the owned `UdpSocket`.
    Socket,
    /// Server mode: receives packets via an `mpsc::channel` from the
    /// [`KcpListener`](super::KcpListener)'s background routing task.
    Channel(mpsc::Receiver<Vec<u8>>),
}

/// A KCP session that manages a single KCP connection over UDP.
///
/// `KcpSession` is the internal workhorse behind [`KcpStream`](super::KcpStream).
/// It owns a [`Kcp`](crate::core::Kcp) instance and a reference to the underlying
/// `UdpSocket`, driving the KCP state machine during send/recv operations.
///
/// # Receive Modes
///
/// - **Socket mode** (client): The session reads UDP packets directly from the socket.
/// - **Channel mode** (server): The session receives pre-routed packets from
///   the listener's background task via `mpsc::channel`.
pub struct KcpSession {
    /// The core KCP protocol engine.
    kcp: Kcp,
    /// Shared reference to the underlying UDP socket.
    socket: Arc<UdpSocket>,
    /// The transport layer for sending and byte transformations.
    transport: Arc<dyn KcpTransport>,
    /// The remote peer's address.
    remote_addr: SocketAddr,
    /// Session configuration.
    config: KcpSessionConfig,
    /// Reusable buffer for receiving UDP packets.
    udp_recv_buf: Vec<u8>,
    /// Timestamp when this session was created (used for KCP's monotonic clock).
    start_time: Instant,
    /// Timestamp of the last received data (used for timeout detection).
    last_recv_time: Instant,
    /// Whether this session has been closed.
    closed: bool,
    /// How this session receives raw UDP packets.
    recv_mode: RecvMode,
}

impl KcpSession {
    /// Creates a new KCP session in socket mode (for client connections).
    ///
    /// The session will read UDP packets directly from the given socket.
    ///
    /// # Arguments
    ///
    /// * `conv` — KCP conversation ID.
    /// * `socket` — Shared UDP socket for receiving.
    /// * `transport` — Transport implementation for sending and byte transformations.
    /// * `remote_addr` — The remote peer's address.
    /// * `config` — Session configuration.
    pub fn new(
        conv: u32,
        socket: Arc<UdpSocket>,
        transport: Arc<dyn KcpTransport>,
        remote_addr: SocketAddr,
        config: KcpSessionConfig,
    ) -> KcpTokioResult<Self> {
        Self::new_inner(
            conv,
            socket,
            transport,
            remote_addr,
            config,
            RecvMode::Socket,
        )
    }

    /// Creates a new KCP session in channel mode (for server-side connections).
    ///
    /// The session will receive pre-routed UDP packets via the given channel.
    pub(crate) fn new_with_channel(
        conv: u32,
        socket: Arc<UdpSocket>,
        transport: Arc<dyn KcpTransport>,
        remote_addr: SocketAddr,
        config: KcpSessionConfig,
        pkt_rx: mpsc::Receiver<Vec<u8>>,
    ) -> KcpTokioResult<Self> {
        Self::new_inner(
            conv,
            socket,
            transport,
            remote_addr,
            config,
            RecvMode::Channel(pkt_rx),
        )
    }

    /// Internal constructor shared by both socket and channel modes.
    fn new_inner(
        conv: u32,
        socket: Arc<UdpSocket>,
        transport: Arc<dyn KcpTransport>,
        remote_addr: SocketAddr,
        config: KcpSessionConfig,
        recv_mode: RecvMode,
    ) -> KcpTokioResult<Self> {
        let transport_for_kcp = transport.clone();
        let remote = remote_addr;
        // The output callback sends KCP packets via the transport.
        // The transport's try_send must be non-blocking (called from sync context).
        let kcp = Kcp::with_config(
            conv,
            &config.kcp_config,
            move |data: &[u8]| -> io::Result<usize> {
                // Apply outgoing byte transformation (e.g., encryption)
                let processed = transport_for_kcp.process_outgoing(data, remote);
                transport_for_kcp.try_send(&processed, remote)
            },
        )?;
        let now = Instant::now();
        Ok(Self {
            kcp,
            socket,
            transport,
            remote_addr,
            udp_recv_buf: vec![0u8; config.recv_buf_size],
            config,
            start_time: now,
            last_recv_time: now,
            closed: false,
            recv_mode,
        })
    }

    /// Returns the elapsed time in milliseconds since session creation.
    /// Used as KCP's monotonic clock source.
    fn current_ms(&self) -> u32 {
        self.start_time.elapsed().as_millis() as u32
    }

    /// Sends data through the KCP session.
    ///
    /// If `flush_write` is enabled in the config, the KCP engine is flushed
    /// immediately after sending to minimize latency.
    ///
    /// # Errors
    ///
    /// Returns [`KcpTokioError::Closed`] if the session is closed.
    pub fn send(&mut self, data: &[u8]) -> KcpTokioResult<usize> {
        if self.closed {
            return Err(KcpTokioError::Closed);
        }
        let n = self.kcp.send(data)?;
        if self.config.flush_write {
            self.kcp.flush();
        }
        Ok(n)
    }

    /// Attempts to receive data into the provided buffer without blocking.
    ///
    /// Returns immediately with available data or an error if no data is ready.
    pub fn try_recv(&mut self, buf: &mut [u8]) -> KcpTokioResult<usize> {
        if self.closed {
            return Err(KcpTokioError::Closed);
        }
        Ok(self.kcp.recv(buf)?)
    }

    /// Attempts to receive data without blocking, returning a dynamically-sized `Vec<u8>`.
    ///
    /// Internally calls `peeksize()` to determine the exact message size, then allocates
    /// a buffer of the correct size. This avoids the caller needing to guess the buffer size.
    pub fn try_recv_auto(&mut self) -> KcpTokioResult<Vec<u8>> {
        if self.closed {
            return Err(KcpTokioError::Closed);
        }
        let size = self.kcp.peeksize()?;
        let mut buf = vec![0u8; size];
        let n = self.kcp.recv(&mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }

    /// Feeds raw packet data into the KCP engine and updates the last-received timestamp.
    pub fn input(&mut self, data: &[u8]) -> KcpTokioResult<()> {
        self.last_recv_time = Instant::now();
        self.kcp.input(data)?;
        Ok(())
    }

    /// Drives the KCP state machine (retransmission, flushing, etc.).
    pub fn update(&mut self) {
        let current = self.current_ms();
        self.kcp.update(current);
    }

    /// Returns whether the session has timed out based on the configured timeout.
    pub fn is_timed_out(&self) -> bool {
        self.config
            .timeout
            .is_some_and(|t| self.last_recv_time.elapsed() > t)
    }

    /// Returns whether the session has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Closes the session. Subsequent send/recv operations will return [`KcpTokioError::Closed`].
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Returns the conversation ID.
    pub fn conv(&self) -> u32 {
        self.kcp.conv()
    }

    /// Returns the remote peer's address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Returns the number of packets waiting to be sent.
    #[allow(dead_code)]
    pub fn waitsnd(&self) -> u32 {
        self.kcp.waitsnd()
    }

    /// Returns a reference to the underlying UDP socket.
    pub fn socket(&self) -> &Arc<UdpSocket> {
        &self.socket
    }

    /// Returns a reference to the transport implementation.
    pub fn transport(&self) -> &Arc<dyn KcpTransport> {
        &self.transport
    }

    /// Returns a reference to the session configuration.
    pub fn config(&self) -> &KcpSessionConfig {
        &self.config
    }

    /// Extracts the channel receiver from the session (for split support).
    ///
    /// After calling this, the session's recv_mode becomes `Socket` and the
    /// original `recv()` method should no longer be called. The channel receiver
    /// is transferred to `OwnedReadHalf` for independent async reading.
    pub(crate) fn take_channel_receiver(&mut self) -> Option<mpsc::Receiver<Vec<u8>>> {
        match std::mem::replace(&mut self.recv_mode, RecvMode::Socket) {
            RecvMode::Channel(rx) => Some(rx),
            RecvMode::Socket => None,
        }
    }

    /// Receives data asynchronously, blocking until data is available.
    ///
    /// This method uses `tokio::select!` to concurrently:
    /// 1. Wait for incoming UDP packets (socket mode) or channel messages (server mode)
    /// 2. Drive the KCP update timer at the configured `flush_interval`
    ///
    /// # Errors
    ///
    /// - [`KcpTokioError::Closed`] — Session was closed.
    /// - [`KcpTokioError::Timeout`] — No data received within the timeout period.
    /// - [`KcpTokioError::Kcp`] — KCP engine error.
    /// - [`KcpTokioError::Io`] — UDP socket I/O error.
    pub async fn recv(&mut self, buf: &mut [u8]) -> KcpTokioResult<usize> {
        if self.closed {
            return Err(KcpTokioError::Closed);
        }
        loop {
            // Try to receive from KCP first (data may already be reassembled)
            match self.kcp.recv(buf) {
                Ok(n) => return Ok(n),
                Err(crate::core::KcpError::RecvWouldBlock) => {}
                Err(e) => return Err(e.into()),
            }
            self.wait_for_data().await?;
        }
    }

    /// Receives data asynchronously, returning a dynamically-sized `Vec<u8>`.
    ///
    /// Internally uses `peeksize()` to determine the exact message size and
    /// allocates a buffer of the correct size. The caller does not need to
    /// pre-allocate or guess the buffer size.
    ///
    /// # Errors
    ///
    /// Same as [`recv()`](KcpSession::recv).
    pub async fn recv_auto(&mut self) -> KcpTokioResult<Vec<u8>> {
        if self.closed {
            return Err(KcpTokioError::Closed);
        }
        loop {
            match self.try_recv_auto() {
                Ok(data) => return Ok(data),
                Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
                Err(e) => return Err(e),
            }
            self.wait_for_data().await?;
        }
    }

    /// Waits for incoming data or a timer tick, feeds it into KCP, and updates
    /// the state machine. Shared logic between `recv()` and `recv_auto()`.
    async fn wait_for_data(&mut self) -> KcpTokioResult<()> {
        // Check timeout
        if self.is_timed_out() {
            self.closed = true;
            return Err(KcpTokioError::Timeout);
        }
        let flush_interval = self.config.flush_interval;
        // Wait for new data or timer tick
        match &mut self.recv_mode {
            RecvMode::Socket => {
                tokio::select! {
                    result = self.socket.recv_from(&mut self.udp_recv_buf) => {
                        let (n, addr) = result?;
                        if addr == self.remote_addr {
                            self.last_recv_time = Instant::now();
                            // Apply incoming byte transformation (e.g., decryption)
                            if let Some(processed) = self.transport.process_incoming(&self.udp_recv_buf[..n], addr) {
                                self.kcp.input(&processed).ok();
                            }
                            // If None, packet is silently dropped
                        }
                    }
                    _ = time::sleep(flush_interval) => {}
                }
            }
            RecvMode::Channel(rx) => {
                tokio::select! {
                    pkt = rx.recv() => {
                        match pkt {
                            Some(data) => {
                                self.last_recv_time = Instant::now();
                                // Apply incoming byte transformation (e.g., decryption)
                                if let Some(processed) = self.transport.process_incoming(&data, self.remote_addr) {
                                    self.kcp.input(&processed).ok();
                                }
                                // If None, packet is silently dropped
                            }
                            None => { self.closed = true; return Err(KcpTokioError::Closed); }
                        }
                    }
                    _ = time::sleep(flush_interval) => {}
                }
            }
        }
        self.update();
        Ok(())
    }
}

/// A KCP session wrapped in `Arc<Mutex>` for shared access across tasks.
#[allow(dead_code)]
pub type SharedKcpSession = Arc<Mutex<KcpSession>>;
