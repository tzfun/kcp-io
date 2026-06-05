//! # kcp-io
//!
//! A Rust wrapper for the [KCP](https://github.com/skywind3000/kcp) protocol,
//! providing FFI bindings, a safe API, and async tokio integration.
//!
//! ## Features
//!
//! - **`kcp-sys`** — Raw FFI bindings to the KCP C library
//! - **`kcp-core`** — Safe Rust wrapper (implies `kcp-sys`)
//! - **`kcp-tokio`** — Async tokio integration (implies `kcp-core`, enabled by default)
//!
//! ## Quick Start
//!
//! ```no_run
//! use kcp_io::tokio_rt::{KcpStream, KcpSessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut stream = KcpStream::connect("127.0.0.1:8080", KcpSessionConfig::fast()).await?;
//! stream.send_kcp(b"hello").await?;
//!
//! let data = stream.recv_kcp().await?;
//! println!("Received: {:?}", data);
//! # Ok(())
//! # }
//! ```

// FFI bindings — always compiled (C code is always linked)
#[cfg(feature = "kcp-sys")]
pub mod sys;

// Internal sys module (always available for core/tokio, but not public without feature)
#[cfg(all(not(feature = "kcp-sys"), feature = "kcp-core"))]
pub(crate) mod sys;

// Safe wrapper
#[cfg(feature = "kcp-core")]
pub mod core;

// Async tokio integration
#[cfg(feature = "kcp-tokio")]
pub mod tokio_rt;

// Convenience re-exports when kcp-tokio is enabled (default)
#[cfg(feature = "kcp-tokio")]
pub use tokio_rt::{
    KcpListener, KcpSessionConfig, KcpStream, KcpTokioError, KcpTokioResult, KcpTransport,
    KcpUdpTransport,
};

// Convenience re-exports when kcp-core is enabled
#[cfg(feature = "kcp-core")]
pub use core::{Kcp, KcpConfig, KcpError, KcpResult};
