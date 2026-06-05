//! Async KCP protocol implementation based on [tokio](https://tokio.rs/).
//!
//! This module provides a fully asynchronous API for reliable UDP communication
//! using the KCP protocol, built on top of the [`core`](crate::core) module and
//! Tokio's async runtime.
//!
//! # Main Types
//!
//! - [`KcpStream`] — An async KCP connection that implements [`AsyncRead`](tokio::io::AsyncRead)
//!   and [`AsyncWrite`](tokio::io::AsyncWrite). Use it like a TCP stream but over UDP.
//! - [`KcpListener`] — A server-side listener that accepts incoming KCP connections
//!   on a shared UDP socket, multiplexing by `(SocketAddr, conv)`.
//! - [`KcpSessionConfig`] — Configuration combining KCP protocol parameters with
//!   runtime settings (flush interval, timeout, buffer sizes).
//!
//! # Architecture
//!
//! - **Client mode**: Each [`KcpStream`] owns its own `UdpSocket` and reads
//!   directly from it (`RecvMode::Socket`).
//! - **Server mode**: The [`KcpListener`] runs a background task on a shared
//!   `UdpSocket`, routing incoming packets by `(SocketAddr, conv)` to per-session
//!   `mpsc::channel`s (`RecvMode::Channel`).
//!
//! # Example
//!
//! ```no_run
//! use kcp_io::tokio_rt::{KcpStream, KcpListener, KcpSessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Client
//! let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
//! stream.send_kcp(b"hello").await?;
//!
//! // Server
//! let mut listener = KcpListener::bind("0.0.0.0:9090", KcpSessionConfig::fast()).await?;
//! let (mut stream, addr) = listener.accept().await?;
//! # Ok(())
//! # }
//! ```

mod config;
mod error;
mod listener;
mod session;
mod stream;
pub mod transport;

pub use config::KcpSessionConfig;
pub use error::{KcpTokioError, KcpTokioResult};
pub use listener::KcpListener;
pub use stream::{KcpStream, OwnedReadHalf, OwnedWriteHalf};
pub use transport::{KcpTransport, KcpUdpTransport};
