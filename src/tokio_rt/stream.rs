//! Async KCP stream with `AsyncRead`/`AsyncWrite` support.
//!
//! This module provides [`KcpStream`], the primary user-facing type for async
//! KCP communication. It wraps a [`KcpSession`](super::session::KcpSession) and
//! provides both high-level methods ([`send_kcp`](KcpStream::send_kcp),
//! [`recv_kcp`](KcpStream::recv_kcp)) and Tokio trait implementations
//! ([`AsyncRead`], [`AsyncWrite`]).
//!
//! # Example
//!
//! ```no_run
//! use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
//! stream.send_kcp(b"hello").await?;
//!
//! let data = stream.recv_kcp().await?;
//! println!("Received: {:?}", data);
//! # Ok(())
//! # }
//! ```

use super::config::KcpSessionConfig;
use super::error::{KcpTokioError, KcpTokioResult};
use super::session::KcpSession;
use super::transport::{KcpTransport, KcpUdpTransport};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;

/// An async KCP stream for reliable UDP communication.
///
/// `KcpStream` is analogous to `TcpStream` but uses the KCP protocol over UDP.
/// It implements [`AsyncRead`] and [`AsyncWrite`] for compatibility with the
/// Tokio ecosystem (e.g., `tokio::io::copy`, `BufReader`, etc.).
///
/// # Connection
///
/// - **Client**: Use [`connect()`](KcpStream::connect) or
///   [`connect_with_conv()`](KcpStream::connect_with_conv) to connect to a server.
/// - **Server**: Obtained from [`KcpListener::accept()`](super::KcpListener::accept).
///
/// # Sending and Receiving
///
/// - [`send_kcp()`](KcpStream::send_kcp) / [`recv_kcp()`](KcpStream::recv_kcp) —
///   High-level KCP-aware methods.
/// - [`AsyncRead`] / [`AsyncWrite`] — Standard Tokio trait implementations for
///   interoperability with the async I/O ecosystem.
pub struct KcpStream {
    /// The underlying KCP session.
    session: KcpSession,
    /// Internal read buffer for `AsyncRead` implementation.
    read_buf: Vec<u8>,
    /// Current read position in the internal buffer.
    read_pos: usize,
    /// Number of valid bytes in the internal buffer.
    read_len: usize,
}

impl KcpStream {
    /// Connects to a remote KCP server with an auto-generated conversation ID.
    ///
    /// Binds a new UDP socket to an ephemeral local port and creates a KCP
    /// session to the specified remote address.
    ///
    /// # Arguments
    ///
    /// * `addr` — Remote server address (e.g., `"127.0.0.1:9090"`).
    /// * `config` — Session configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if address resolution, socket binding, or session creation fails.
    pub async fn connect<A: tokio::net::ToSocketAddrs>(
        addr: A,
        config: KcpSessionConfig,
    ) -> KcpTokioResult<Self> {
        Self::connect_with_conv(addr, config, rand_conv()).await
    }

    /// Connects to a remote KCP server with a specific conversation ID.
    ///
    /// Both endpoints must use the same `conv` for the session to work.
    ///
    /// # Arguments
    ///
    /// * `addr` — Remote server address.
    /// * `config` — Session configuration.
    /// * `conv` — KCP conversation ID.
    pub async fn connect_with_conv<A: tokio::net::ToSocketAddrs>(
        addr: A,
        config: KcpSessionConfig,
        conv: u32,
    ) -> KcpTokioResult<Self> {
        let remote_addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
            KcpTokioError::ConnectionFailed("could not resolve address".to_string())
        })?;
        let local_addr = if remote_addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = Arc::new(UdpSocket::bind(local_addr).await?);
        let transport = Arc::new(KcpUdpTransport::new(socket.clone()));
        let recv_buf_size = config.recv_buf_size;
        let session = KcpSession::new(conv, socket, transport, remote_addr, config)?;
        Ok(Self {
            session,
            read_buf: vec![0u8; recv_buf_size],
            read_pos: 0,
            read_len: 0,
        })
    }

    /// Connects to a remote KCP server with a custom transport implementation.
    ///
    /// This method allows you to inject a custom [`KcpTransport`] for sending
    /// and byte transformations (e.g., encryption, compression, custom I/O).
    /// You must provide both the transport and the UDP socket it uses, so that
    /// the same socket can be used for both sending and receiving.
    ///
    /// # Arguments
    ///
    /// * `addr` — Remote server address.
    /// * `config` — Session configuration.
    /// * `transport` — Custom transport implementation (should wrap `socket`).
    /// * `socket` — UDP socket for receiving (same socket used by `transport`).
    /// * `conv` — KCP conversation ID.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig, KcpUdpTransport};
    /// use std::sync::Arc;
    /// use tokio::net::UdpSocket;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    /// let transport = Arc::new(KcpUdpTransport::new(socket.clone()));
    /// let stream = KcpStream::connect_with_transport(
    ///     "127.0.0.1:9090",
    ///     KcpSessionConfig::fast(),
    ///     transport,
    ///     socket,
    ///     0x1234,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_transport<A: tokio::net::ToSocketAddrs>(
        addr: A,
        config: KcpSessionConfig,
        transport: Arc<dyn KcpTransport>,
        socket: Arc<UdpSocket>,
        conv: u32,
    ) -> KcpTokioResult<Self> {
        let remote_addr = tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
            KcpTokioError::ConnectionFailed("could not resolve address".to_string())
        })?;
        let recv_buf_size = config.recv_buf_size;
        let session = KcpSession::new(conv, socket, transport, remote_addr, config)?;
        Ok(Self {
            session,
            read_buf: vec![0u8; recv_buf_size],
            read_pos: 0,
            read_len: 0,
        })
    }

    /// Creates a `KcpStream` from an existing [`KcpSession`].
    ///
    /// Used internally by [`KcpListener`](super::KcpListener) to wrap accepted sessions.
    pub(crate) fn from_session(session: KcpSession) -> Self {
        let recv_buf_size = session.config().recv_buf_size;
        Self {
            session,
            read_buf: vec![0u8; recv_buf_size],
            read_pos: 0,
            read_len: 0,
        }
    }

    /// Returns the KCP conversation ID for this stream.
    pub fn conv(&self) -> u32 {
        self.session.conv()
    }

    /// Returns the remote peer's address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.session.remote_addr()
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.session.socket().local_addr()
    }

    /// Sends data reliably through the KCP protocol.
    ///
    /// The data is queued in the KCP engine and transmitted via the output callback.
    /// If `flush_write` is enabled in the config, data is flushed immediately.
    pub async fn send_kcp(&mut self, data: &[u8]) -> KcpTokioResult<usize> {
        self.session.send(data)
    }

    /// Receives data from the KCP protocol asynchronously, returning a `Vec<u8>`.
    ///
    /// The buffer is automatically sized to fit the incoming message — the caller
    /// does not need to pre-allocate or guess the buffer size.
    ///
    /// Blocks until data is available, the session times out, or the session is closed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
    /// let data = stream.recv_kcp().await?;
    /// println!("Received {} bytes: {:?}", data.len(), data);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recv_kcp(&mut self) -> KcpTokioResult<Vec<u8>> {
        self.session.recv_auto().await
    }

    /// Receives data from the KCP protocol into a caller-provided buffer.
    ///
    /// This is the original buffer-based receive method. Use [`recv_kcp()`](KcpStream::recv_kcp)
    /// for automatic buffer management.
    ///
    /// # Errors
    ///
    /// Returns [`KcpTokioError::Kcp(KcpError::RecvBufferTooSmall)`](crate::core::KcpError::RecvBufferTooSmall)
    /// if `buf` is smaller than the next message.
    pub async fn recv_kcp_buf(&mut self, buf: &mut [u8]) -> KcpTokioResult<usize> {
        self.session.recv(buf).await
    }

    /// Flushes all pending data in the KCP send queue.
    ///
    /// Forces the KCP engine to transmit any queued packets immediately via the
    /// output callback.
    pub fn flush(&mut self) {
        self.session.update();
    }

    /// Closes the KCP stream.
    ///
    /// After calling this method, subsequent [`send_kcp()`](KcpStream::send_kcp) and
    /// [`recv_kcp()`](KcpStream::recv_kcp) calls will return [`KcpTokioError::Closed`].
    ///
    /// > **Note:** KCP is a UDP-based protocol without a built-in connection teardown
    /// > handshake (unlike TCP's FIN/FIN-ACK). Calling `close()` only shuts down the
    /// > local side. The remote peer will detect the disconnection when its session
    /// > times out (controlled by [`KcpSessionConfig::timeout`]).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
    /// stream.send_kcp(b"goodbye").await?;
    /// stream.close();
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(&mut self) {
        self.session.close();
    }

    /// Returns whether the stream has been closed.
    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }
}

impl AsyncRead for KcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Return buffered data first
        if this.read_pos < this.read_len {
            let remaining = &this.read_buf[this.read_pos..this.read_len];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            this.read_pos += to_copy;
            return Poll::Ready(Ok(()));
        }
        // Reset buffer positions
        this.read_pos = 0;
        this.read_len = 0;
        // Try to receive from KCP (non-blocking)
        match this.session.try_recv(&mut this.read_buf) {
            Ok(n) => {
                let to_copy = n.min(buf.remaining());
                buf.put_slice(&this.read_buf[..to_copy]);
                if to_copy < n {
                    this.read_pos = to_copy;
                    this.read_len = n;
                }
                return Poll::Ready(Ok(()));
            }
            Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
            Err(KcpTokioError::Closed) => return Poll::Ready(Ok(())),
            Err(KcpTokioError::Timeout) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "KCP session timed out",
                )))
            }
            Err(KcpTokioError::Io(e)) => return Poll::Ready(Err(e)),
            Err(e) => return Poll::Ready(Err(io::Error::other(e.to_string()))),
        }
        // Poll the UDP socket for new data
        let socket = this.session.socket().clone();
        let mut udp_buf = [0u8; 65536];
        let mut read_buf = ReadBuf::new(&mut udp_buf);
        match socket.poll_recv_from(cx, &mut read_buf) {
            Poll::Ready(Ok(addr)) => {
                let n = read_buf.filled().len();
                if addr == this.session.remote_addr() {
                    // Apply incoming byte transformation before feeding to KCP
                    if let Some(processed) = this
                        .session
                        .transport()
                        .process_incoming(&udp_buf[..n], addr)
                    {
                        this.session.input(&processed).ok();
                    }
                }
                this.session.update();
                match this.session.try_recv(&mut this.read_buf) {
                    Ok(n) => {
                        let to_copy = n.min(buf.remaining());
                        buf.put_slice(&this.read_buf[..to_copy]);
                        if to_copy < n {
                            this.read_pos = to_copy;
                            this.read_len = n;
                        }
                        Poll::Ready(Ok(()))
                    }
                    Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(KcpTokioError::Closed) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(io::Error::other(e.to_string()))),
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                this.session.update();
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for KcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.session.send(buf) {
            Ok(n) => Poll::Ready(Ok(n)),
            Err(KcpTokioError::Closed) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "KCP session closed",
            ))),
            Err(KcpTokioError::Io(e)) => Poll::Ready(Err(e)),
            Err(e) => Poll::Ready(Err(io::Error::other(e.to_string()))),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.session.update();
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.session.close();
        Poll::Ready(Ok(()))
    }
}

/// Generates a pseudo-random conversation ID based on the current system time.
fn rand_conv() -> u32 {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    ((seed ^ (seed >> 16) ^ (seed >> 32)) & 0xFFFFFFFF) as u32
}

// ---------------------------------------------------------------------------
// Split support: OwnedReadHalf / OwnedWriteHalf
// ---------------------------------------------------------------------------

use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

/// Determines how the read half receives raw UDP packets after split.
enum RecvSource {
    /// Client mode: reads directly from the shared `UdpSocket`.
    Socket,
    /// Server mode: receives packets from `mpsc::channel`.
    Channel(tokio::sync::mpsc::Receiver<Vec<u8>>),
}

/// The read half of a [`KcpStream`], created by [`KcpStream::into_split()`].
///
/// `OwnedReadHalf` can receive data independently from `OwnedWriteHalf`,
/// allowing concurrent reading and writing from separate tasks.
///
/// The KCP session state is shared with the write half via `Arc<Mutex>`,
/// but the mutex is **never held across await points**, ensuring both halves
/// can operate concurrently without deadlocks.
///
/// # Example
///
/// ```no_run
/// use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
/// let (mut read_half, mut write_half) = stream.into_split();
///
/// // Spawn a task for reading
/// let reader = tokio::spawn(async move {
///     while let Ok(data) = read_half.recv_kcp().await {
///         println!("Received: {:?}", data);
///     }
/// });
///
/// // Write from the current task
/// write_half.send_kcp(b"hello").await?;
/// # Ok(())
/// # }
/// ```
pub struct OwnedReadHalf {
    session: Arc<TokioMutex<KcpSession>>,
    socket: Arc<UdpSocket>,
    transport: Arc<dyn KcpTransport>,
    remote_addr: SocketAddr,
    recv_source: RecvSource,
    udp_recv_buf: Vec<u8>,
    flush_interval: Duration,
    read_buf: Vec<u8>,
    read_pos: usize,
    read_len: usize,
}

/// The write half of a [`KcpStream`], created by [`KcpStream::into_split()`].
///
/// `OwnedWriteHalf` can send data independently from `OwnedReadHalf`,
/// allowing concurrent reading and writing from separate tasks.
///
/// # Example
///
/// ```no_run
/// use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
/// let (mut read_half, mut write_half) = stream.into_split();
///
/// write_half.send_kcp(b"hello").await?;
/// write_half.close().await;
/// # Ok(())
/// # }
/// ```
pub struct OwnedWriteHalf {
    session: Arc<TokioMutex<KcpSession>>,
}

impl KcpStream {
    /// Splits this `KcpStream` into a read half and a write half, allowing
    /// concurrent reading and writing from separate tasks.
    ///
    /// This is similar to [`TcpStream::into_split()`](tokio::net::TcpStream::into_split).
    /// The underlying KCP session state is shared via `Arc<Mutex>`, but the
    /// mutex is only held for brief synchronous operations (never across await
    /// points), so both halves can operate concurrently.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
    /// let (mut read_half, mut write_half) = stream.into_split();
    ///
    /// let reader = tokio::spawn(async move {
    ///     while let Ok(data) = read_half.recv_kcp().await {
    ///         println!("Received {} bytes", data.len());
    ///     }
    /// });
    ///
    /// write_half.send_kcp(b"hello").await?;
    /// write_half.send_kcp(b"world").await?;
    /// write_half.close().await;
    ///
    /// reader.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_split(mut self) -> (OwnedReadHalf, OwnedWriteHalf) {
        let socket = self.session.socket().clone();
        let transport = self.session.transport().clone();
        let remote_addr = self.session.remote_addr();
        let flush_interval = self.session.config().flush_interval;
        let recv_buf_size = self.session.config().recv_buf_size;

        let recv_source = match self.session.take_channel_receiver() {
            Some(rx) => RecvSource::Channel(rx),
            None => RecvSource::Socket,
        };

        let session = Arc::new(TokioMutex::new(self.session));

        let read_half = OwnedReadHalf {
            session: session.clone(),
            socket,
            transport,
            remote_addr,
            recv_source,
            udp_recv_buf: vec![0u8; recv_buf_size],
            flush_interval,
            read_buf: self.read_buf,
            read_pos: self.read_pos,
            read_len: self.read_len,
        };

        let write_half = OwnedWriteHalf { session };

        (read_half, write_half)
    }
}

// --- OwnedReadHalf impl ---

impl OwnedReadHalf {
    /// Receives data from the KCP protocol asynchronously, returning a `Vec<u8>`.
    ///
    /// The buffer is automatically sized to fit the incoming message — the caller
    /// does not need to pre-allocate or guess the buffer size.
    ///
    /// Blocks until data is available, the session times out, or the session is closed.
    /// The underlying mutex is only held briefly during synchronous KCP operations,
    /// never across await points.
    pub async fn recv_kcp(&mut self) -> KcpTokioResult<Vec<u8>> {
        loop {
            // 1. Briefly lock session to try recv
            {
                let mut session = self.session.lock().await;
                match session.try_recv_auto() {
                    Ok(data) => return Ok(data),
                    Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
                    Err(e) => return Err(e),
                }
                if session.is_timed_out() {
                    session.close();
                    return Err(KcpTokioError::Timeout);
                }
                if session.is_closed() {
                    return Err(KcpTokioError::Closed);
                }
            } // lock released

            // 2. Wait for I/O data WITHOUT holding the lock
            self.wait_for_data().await?;
        }
    }

    /// Receives data from the KCP protocol into a caller-provided buffer.
    ///
    /// This is the original buffer-based receive method. Use [`recv_kcp()`](OwnedReadHalf::recv_kcp)
    /// for automatic buffer management.
    ///
    /// # Errors
    ///
    /// Returns [`KcpTokioError::Kcp(KcpError::RecvBufferTooSmall)`](crate::core::KcpError::RecvBufferTooSmall)
    /// if `buf` is smaller than the next message.
    pub async fn recv_kcp_buf(&mut self, buf: &mut [u8]) -> KcpTokioResult<usize> {
        loop {
            // 1. Briefly lock session to try recv
            {
                let mut session = self.session.lock().await;
                match session.try_recv(buf) {
                    Ok(n) => return Ok(n),
                    Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
                    Err(e) => return Err(e),
                }
                if session.is_timed_out() {
                    session.close();
                    return Err(KcpTokioError::Timeout);
                }
                if session.is_closed() {
                    return Err(KcpTokioError::Closed);
                }
            } // lock released

            // 2. Wait for I/O data WITHOUT holding the lock
            self.wait_for_data().await?;
        }
    }

    /// Waits for incoming data, feeds it into KCP, and updates the state machine.
    /// Shared logic between `recv_kcp()` and `recv_kcp_buf()`.
    async fn wait_for_data(&mut self) -> KcpTokioResult<()> {
        let received_data = match &mut self.recv_source {
            RecvSource::Socket => {
                tokio::select! {
                    result = self.socket.recv_from(&mut self.udp_recv_buf) => {
                        match result {
                            Ok((n, addr)) if addr == self.remote_addr => {
                                // Apply incoming byte transformation
                                self.transport.process_incoming(&self.udp_recv_buf[..n], addr)
                            }
                            Ok(_) => None,
                            Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset => None,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    _ = tokio::time::sleep(self.flush_interval) => None,
                }
            }
            RecvSource::Channel(rx) => {
                tokio::select! {
                    pkt = rx.recv() => {
                        match pkt {
                            Some(data) => {
                                // Apply incoming byte transformation
                                self.transport.process_incoming(&data, self.remote_addr)
                            }
                            None => return Err(KcpTokioError::Closed),
                        }
                    }
                    _ = tokio::time::sleep(self.flush_interval) => None,
                }
            }
        };

        // 3. Briefly lock session to input data and update
        {
            let mut session = self.session.lock().await;
            if let Some(data) = received_data {
                session.input(&data).ok();
            }
            session.update();
        } // lock released
        Ok(())
    }

    /// Returns the KCP conversation ID.
    pub async fn conv(&self) -> u32 {
        self.session.lock().await.conv()
    }

    /// Returns the remote peer's address.
    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

impl AsyncRead for OwnedReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Return buffered data first
        if this.read_pos < this.read_len {
            let remaining = &this.read_buf[this.read_pos..this.read_len];
            let to_copy = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..to_copy]);
            this.read_pos += to_copy;
            return Poll::Ready(Ok(()));
        }
        this.read_pos = 0;
        this.read_len = 0;

        // Try to lock and recv (non-blocking lock attempt)
        let mut lock_fut = Box::pin(this.session.lock());
        match lock_fut.as_mut().poll(cx) {
            Poll::Ready(mut session) => {
                match session.try_recv(&mut this.read_buf) {
                    Ok(n) => {
                        let to_copy = n.min(buf.remaining());
                        buf.put_slice(&this.read_buf[..to_copy]);
                        if to_copy < n {
                            this.read_pos = to_copy;
                            this.read_len = n;
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
                    Err(KcpTokioError::Closed) => return Poll::Ready(Ok(())),
                    Err(KcpTokioError::Timeout) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "KCP session timed out",
                        )))
                    }
                    Err(e) => return Poll::Ready(Err(io::Error::other(e.to_string()))),
                }
                // Drive update while we have the lock
                session.update();
            }
            Poll::Pending => {}
        }

        // Poll the socket for new data
        let mut udp_buf = [0u8; 65536];
        let mut read_buf = ReadBuf::new(&mut udp_buf);
        match this.socket.poll_recv_from(cx, &mut read_buf) {
            Poll::Ready(Ok(addr)) => {
                let n = read_buf.filled().len();
                if addr == this.remote_addr {
                    // Apply incoming byte transformation before feeding to KCP
                    let processed = this.transport.process_incoming(&udp_buf[..n], addr);
                    // Lock briefly to input and try recv
                    let mut lock_fut = Box::pin(this.session.lock());
                    if let Poll::Ready(mut session) = lock_fut.as_mut().poll(cx) {
                        if let Some(data) = processed {
                            session.input(&data).ok();
                        }
                        session.update();
                        match session.try_recv(&mut this.read_buf) {
                            Ok(n) => {
                                let to_copy = n.min(buf.remaining());
                                buf.put_slice(&this.read_buf[..to_copy]);
                                if to_copy < n {
                                    this.read_pos = to_copy;
                                    this.read_len = n;
                                }
                                return Poll::Ready(Ok(()));
                            }
                            Err(KcpTokioError::Kcp(crate::core::KcpError::RecvWouldBlock)) => {}
                            Err(KcpTokioError::Closed) => return Poll::Ready(Ok(())),
                            Err(e) => return Poll::Ready(Err(io::Error::other(e.to_string()))),
                        }
                    }
                }
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => {}
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

// --- OwnedWriteHalf impl ---

impl OwnedWriteHalf {
    /// Sends data reliably through the KCP protocol.
    ///
    /// The underlying mutex is only held briefly during the synchronous
    /// KCP send operation.
    pub async fn send_kcp(&mut self, data: &[u8]) -> KcpTokioResult<usize> {
        let mut session = self.session.lock().await;
        session.send(data)
    }

    /// Flushes all pending data in the KCP send queue.
    pub async fn flush(&mut self) {
        let mut session = self.session.lock().await;
        session.update();
    }

    /// Closes the KCP stream.
    ///
    /// Both the read half and write half will stop working after this call.
    pub async fn close(&mut self) {
        let mut session = self.session.lock().await;
        session.close();
    }

    /// Returns whether the stream has been closed.
    pub async fn is_closed(&self) -> bool {
        self.session.lock().await.is_closed()
    }

    /// Returns the KCP conversation ID.
    pub async fn conv(&self) -> u32 {
        self.session.lock().await.conv()
    }
}

impl AsyncWrite for OwnedWriteHalf {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut lock_fut = Box::pin(this.session.lock());
        match lock_fut.as_mut().poll(cx) {
            Poll::Ready(mut session) => match session.send(buf) {
                Ok(n) => Poll::Ready(Ok(n)),
                Err(KcpTokioError::Closed) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "KCP session closed",
                ))),
                Err(KcpTokioError::Io(e)) => Poll::Ready(Err(e)),
                Err(e) => Poll::Ready(Err(io::Error::other(e.to_string()))),
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut lock_fut = Box::pin(this.session.lock());
        match lock_fut.as_mut().poll(cx) {
            Poll::Ready(mut session) => {
                session.update();
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut lock_fut = Box::pin(this.session.lock());
        match lock_fut.as_mut().poll(cx) {
            Poll::Ready(mut session) => {
                session.close();
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
