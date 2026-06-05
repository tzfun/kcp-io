# Changelog

All notable changes to kcp-io are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [0.0.5] — 2026-06-05

### Added

- **`KcpTransport` trait**: New transport abstraction trait in `kcp_io::tokio_rt::transport`
  that decouples KCP packet I/O from the session layer. The trait provides:
  - `try_send()` — synchronous, non-blocking send (called from KCP's output callback)
  - `process_outgoing()` — byte transformation hook before sending (KCP → wire)
  - `process_incoming()` — byte transformation hook after receiving (wire → KCP)
  Default implementations are identity (no transformation).
- **`KcpUdpTransport`**: Default UDP-based transport implementation wrapping
  `Arc<UdpSocket>`. Used automatically by `KcpStream::connect()` and
  `KcpListener::bind()`.
- **`KcpStream::connect_with_transport()`**: New constructor that accepts a custom
  `Arc<dyn KcpTransport>` + `Arc<UdpSocket>` for advanced use cases (encryption,
  compression, custom I/O).
- **2 new integration tests**: `test_custom_transport_counting` and
  `test_custom_transport_kcp_udp` verify custom transport end-to-end.

### Changed

- **KCP output callback uses transport**: The synchronous output callback in
  `KcpSession` now routes through `transport.try_send()` and applies
  `transport.process_outgoing()` instead of directly calling
  `UdpSocket::try_send_to()`.
- **Incoming packets pass through `process_incoming`**: Both `KcpSession::wait_for_data()`
  and `OwnedReadHalf::wait_for_data()` now apply `transport.process_incoming()`
  before feeding data to `Kcp::input()`, in both Socket and Channel recv modes.
- **`KcpListener::accept_loop()`** now creates a per-session `KcpUdpTransport`
  and passes it to `KcpSession::new_with_channel()`.
- **`OwnedReadHalf`** now stores an `Arc<dyn KcpTransport>` for incoming
  byte processing in split mode.
- **Test suite reorganization**: Split the 800-line monolithic
  `tests/integration_tests.rs` into 6 focused files:
  - `tests/common/mod.rs` — shared helpers
  - `tests/basic.rs` — basic communication
  - `tests/recv_buffer.rs` — adaptive recv buffer
  - `tests/packet_loss.rs` — packet loss reliability
  - `tests/split.rs` — split stream
  - `tests/transport.rs` — custom transport
- **`KcpTransport` and `KcpUdpTransport`** are re-exported from both
  `kcp_io::tokio_rt` and the crate root `kcp_io`.

---

## [0.0.4] — 2026-03-24

### Changed

- **`recv_kcp()` returns `Vec<u8>` (breaking change)**: The primary receive
  method `recv_kcp()` on `KcpStream`, `OwnedReadHalf` now returns
  `KcpTokioResult<Vec<u8>>` with automatic buffer sizing via `peeksize()`.
  Callers no longer need to pre-allocate a buffer or guess the message size.
- **`recv_kcp_buf()` for buffer-based receive**: The original `&mut [u8]`
  signature is preserved as `recv_kcp_buf()` for callers who prefer manual
  buffer management or need zero-allocation receives.

### Added

- **`KcpSession::try_recv_auto()`**: Internal method that uses `peeksize()`
  to allocate an exact-sized buffer before calling `kcp.recv()`.
- **`KcpSession::recv_auto()`**: Async auto-sizing receive method.
- **`KcpSession::wait_for_data()`**: Shared helper extracting the
  `tokio::select!` I/O wait logic from `recv()` and `recv_auto()`.
- **`OwnedReadHalf::wait_for_data()`**: Shared helper for the split read
  half, extracting duplicated I/O wait logic.
- **5 new integration tests** for adaptive recv buffer:
  `test_recv_kcp_auto_small_message`, `test_recv_kcp_auto_large_message`,
  `test_recv_kcp_auto_varying_sizes`, `test_recv_kcp_auto_split_half`,
  `test_recv_kcp_buf_too_small_returns_error`.

---

## [0.0.3] — 2026-03-23

### Fixed

- **`recv()` error code `-3` misclassified**: KCP C library returns `-3`
  from `ikcp_recv` when the receive buffer is too small for the next message
  (`peeksize > buf.len()`). Previously this fell into the generic
  `RecvFailed(-3)` branch. Now both `-2` and `-3` are correctly mapped to
  `KcpError::RecvBufferTooSmall { need, got }`, providing actionable error
  messages instead of an opaque error code.

### Changed

- **Packet loss tests marked `#[ignore]`**: The three packet loss reliability
  tests are now skipped by default in `cargo test` and CI. They can be run
  locally with `cargo test -- --ignored` for manual verification.

### Added

- **Packet loss reliability tests**: Three new integration tests that verify
  KCP's ARQ retransmission guarantees under simulated packet loss conditions:
  - `test_reliability_under_10_percent_packet_loss` (20 messages, 10% loss)
  - `test_reliability_under_30_percent_packet_loss` (20 messages, 30% loss)
  - `test_reliability_under_50_percent_packet_loss` (10 messages, 50% loss)
  Tests use a **Lossy UDP Proxy** that sits between client and server,
  randomly dropping packets at the configured rate via xorshift64 PRNG.
  Proxy statistics (forwarded/dropped counts, actual loss rate) are printed
  after each test run.
- **CHANGELOG.md**: Added changelog tracking all changes since project
  creation, following [Keep a Changelog](https://keepachangelog.com/) format.
- **AI coding rules** (`.codemaker/rules/rules.mdc`): Comprehensive rules
  file for consistent AI-assisted code generation.

---

## [0.0.2] — 2026-03-23

### Added

- **Read/Write Split** (`into_split()`): `KcpStream` can now be split into
  `OwnedReadHalf` and `OwnedWriteHalf` for concurrent reading and writing
  from separate tasks, similar to `TcpStream::into_split()`. The shared
  session state uses `Arc<TokioMutex<KcpSession>>` and the mutex is never
  held across `.await` points to prevent deadlocks.
- **Explicit close API**: Added `close()` and `flush()` methods to
  `KcpStream`, `OwnedReadHalf`, and `OwnedWriteHalf`.
- **`is_closed()` method**: Check whether a stream or split half has been
  closed without performing I/O.
- **Ghost session prevention**: `KcpListener` now maintains a
  `closed_sessions` cache with a 60-second TTL. Stale retransmission packets
  from recently closed connections no longer create phantom sessions.
  Expired entries are cleaned up when the cache exceeds 100 entries.
- **Chinese README** (`README_ZH.md`): Full Chinese translation of the
  documentation with bidirectional language links at the top of both READMEs.
- **New integration tests** for split functionality:
  - `test_split_concurrent_read_write`
  - `test_split_separate_tasks`
  - `test_split_close_from_write_half`
- **AI coding rules**: Added `.codemaker/rules/rules.mdc` for consistent
  AI-assisted code generation.
- **`AsyncRead` / `AsyncWrite`** trait implementations for `OwnedReadHalf`
  and `OwnedWriteHalf`, enabling use with the broader Tokio I/O ecosystem.
- **`KcpSession::take_channel_receiver()`**: Internal method to transfer the
  `mpsc::Receiver` from a server-mode session to `OwnedReadHalf` during split.

### Fixed

- **Windows UDP Error 10054** (WSAECONNRESET): `recv_from` on Windows returns
  `ConnectionReset` when the remote peer's UDP port is unreachable (ICMP
  "Port Unreachable"). Both `KcpListener` and `KcpSession`'s output callback
  now catch this error, log at `debug` level, and continue instead of
  propagating it as a fatal error. This was causing infinite error loops in
  the echo server example on Windows.
- **Output callback resilience**: The KCP output callback (`try_send_to`) now
  silently treats `WouldBlock` and `ConnectionReset` as success
  (`Ok(data.len())`), since KCP handles retransmission internally.
- **Clippy warning**: Replaced `map_or(false, |t| ...)` with
  `is_some_and(|t| ...)` in `KcpSession::is_timed_out()` to satisfy the
  `unnecessary_map_or` lint.
- **Formatting**: Applied `cargo fmt` to fix minor formatting inconsistencies
  in `AsyncWrite` impl for `OwnedWriteHalf`.

### Changed

- **README structure**: Reorganized both English and Chinese READMEs to lead
  with "Quick Start" and "API Overview" sections, moving "Project Structure"
  to the bottom for better developer onboarding experience.
- **README architecture diagram** (Chinese): Fixed alignment of vertical
  lines and box characters in the ASCII architecture diagram.
- **Version bump**: `0.0.1` → `0.0.2`.

---

## [0.0.1] — 2026-03-20

### Added

- **Initial release** of kcp-io as a single-crate library with three feature
  flags: `kcp-sys`, `kcp-core`, and `kcp-tokio` (default).

#### `kcp-sys` — Raw FFI Bindings
- Compiled the original KCP C source (`kcp/ikcp.c`) via `build.rs` using the
  `cc` crate.
- Exposed all KCP C functions as `unsafe extern "C"` declarations:
  `ikcp_create`, `ikcp_release`, `ikcp_setoutput`, `ikcp_send`, `ikcp_recv`,
  `ikcp_input`, `ikcp_update`, `ikcp_check`, `ikcp_flush`, `ikcp_peeksize`,
  `ikcp_waitsnd`, `ikcp_setmtu`, `ikcp_wndsize`, `ikcp_nodelay`,
  `ikcp_getconv`, `ikcp_allocator`.
- Defined `#[repr(C)]` struct bindings: `IKCPCB`, `IKCPSEG`, `IQUEUEHEAD`.
- Defined callback type aliases: `ikcp_output_callback`,
  `ikcp_writelog_callback`.
- Defined constants: `IKCP_OVERHEAD` (24 bytes), `IKCP_LOG_*` bitmasks.

#### `kcp-core` — Safe Rust Wrapper
- `Kcp` struct: Safe wrapper around `IKCPCB` with `Drop` for automatic
  cleanup. Implements `Send` (not `Sync`). All unsafe FFI calls encapsulated.
- `KcpConfig`: Protocol configuration with presets `default()`, `fast()`,
  `normal()`.
- `KcpError` / `KcpResult<T>`: Error enum with variants for all KCP failure
  modes, derived via `thiserror`.
- Output callback bridging: C → Rust via `KcpContext` + `user` pointer.

#### `kcp-tokio` — Async Tokio Integration
- `KcpStream`: Async KCP connection implementing `AsyncRead` + `AsyncWrite`.
  Client-side `connect()` / `connect_with_conv()` methods.
  High-level `send_kcp()` / `recv_kcp()` methods.
- `KcpListener`: Server-side listener with background `tokio::spawn` task
  that multiplexes incoming UDP packets by `(SocketAddr, conv)` and routes
  them via `mpsc::channel`.
- `KcpSessionConfig`: Runtime configuration combining `KcpConfig` with
  session-level settings (flush interval, timeout, buffer size).
- `KcpTokioError` / `KcpTokioResult<T>`: Async-layer error enum wrapping
  `KcpError`, `io::Error`, plus `Timeout`, `Closed`, `ConnectionFailed`.
- `KcpSession`: Internal session state machine with dual receive modes
  (Socket for clients, Channel for servers) and `tokio::select!`-based I/O.

#### Documentation & Tooling
- Comprehensive rustdoc comments on all public types and methods, with
  `# Arguments`, `# Errors`, `# Example`, and `# Safety` sections.
- `README.md`: Project documentation with Quick Start, API Overview,
  Features, Architecture diagram, and Build/Test commands.
- `examples/echo_server.rs` and `examples/echo_client.rs`.
- Integration tests: `test_client_server_basic_communication`,
  `test_bidirectional_communication`, `test_large_data_transfer`,
  `test_multiple_messages`.
- Benchmark suite (`benches/throughput.rs`): KCP throughput, raw UDP
  baseline, TCP baseline, KCP latency (ping-pong).
- GitHub Actions CI: Multi-platform (Ubuntu/Windows/macOS), multi-toolchain
  (stable/1.85.0), clippy, rustfmt, doc build.
- MIT License.

#### Project Restructure (same release)
- Migrated from a multi-crate workspace (`kcp-sys`, `kcp-core`, `kcp-tokio`
  as separate crates) to a single crate with feature flags. This simplified
  dependency management and the publishing process.
- `kcp-sys/src/lib.rs` → `src/sys.rs`
- `kcp-core/src/` → `src/core/`
- `kcp-tokio/src/` → `src/tokio_rt/`
- Added convenience re-exports at crate root (`src/lib.rs`).
