# CLAUDE.md — kcp-io Project Reference

> Auto-generated from `.codemaker/rules/rules.mdc`, `PLAN.md`, and `CHANGELOG.md`.
> Last updated: 2026-06-05

## Project Identity

- **Name**: kcp-io
- **Type**: Rust networking library (single crate) wrapping the KCP ARQ reliable-UDP protocol
- **License**: MIT
- **MSRV**: 1.85.0 (edition 2021)
- **Repository**: https://github.com/tzfun/kcp-io
- **KCP upstream**: https://github.com/skywind3000/kcp

## Architecture: Three-Layer Feature-Flag Design

```
kcp-tokio (default feature)     ← Async Tokio integration (src/tokio_rt/)
  └── kcp-core                  ← Safe Rust wrapper (src/core/)
        └── kcp-sys             ← Raw FFI bindings (src/sys.rs, kcp/ikcp.c)
```

- **`kcp-sys`**: Raw `unsafe extern "C"` FFI declarations. Allowed `#[allow(non_upper_case_globals, non_camel_case_types, non_snake_case)]`. No logic.
- **`kcp-core`**: Safe Rust encapsulation of all `unsafe` FFI calls. `Kcp` struct wraps `*mut IKCPCB`, manages lifecycle via `Drop`. NO `unsafe` should leak beyond this layer.
- **`kcp-tokio`**: Pure safe Rust async code. MUST NOT contain any `unsafe` blocks. Uses `tokio::select!`, `Arc<TokioMutex<KcpSession>>`, `AsyncRead`/`AsyncWrite`.

The C code (`kcp/ikcp.c`) is compiled via `build.rs` using the `cc` crate.

## Source Layout

```
src/
├── lib.rs              # Crate root: feature-gated re-exports, doc-tests
├── sys.rs              # FFI bindings (feature: kcp-sys)
├── core/
│   ├── mod.rs          # Module re-exports
│   ├── kcp.rs          # Kcp struct — safe IKCPCB wrapper
│   ├── config.rs       # KcpConfig (presets: default/fast/normal)
│   └── error.rs        # KcpError enum + KcpResult<T>
└── tokio_rt/
    ├── mod.rs          # Module re-exports
    ├── stream.rs       # KcpStream (AsyncRead + AsyncWrite + send_kcp/recv_kcp)
    ├── listener.rs     # KcpListener (background task, mpsc routing, ghost prevention)
    ├── session.rs      # KcpSession (dual recv mode: Socket/Channel)
    ├── config.rs       # KcpSessionConfig (presets: default/fast/normal)
    └── error.rs        # KcpTokioError enum + KcpTokioResult<T>
```

## Coding Conventions (from codemaker rules)

### Naming
- Types/Structs/Enums/Traits: `PascalCase` (KcpStream, KcpTokioError)
- Functions/Methods/Variables: `snake_case` (recv_kcp, flush_interval)
- Constants/Statics: `UPPER_SNAKE_CASE` (IKCP_OVERHEAD)
- Module files: `snake_case` (tokio_rt, session.rs)

### Documentation
- Every `.rs` file starts with `//!` module-level rustdoc describing purpose, main types, usage example
- All public items have `///` rustdoc with: summary, `# Arguments`, `# Errors`, `# Example`, `# Safety` (for unsafe)
- Keep README.md (EN) and README_ZH.md (CN) in sync; bidirectional language links at top of both

### Quality Gates (MUST PASS)
- `cargo fmt` — default settings
- `cargo clippy --all-targets -- -D warnings` — zero warnings
- `cargo doc --no-deps` — zero warnings (RUSTDOCFLAGS=-D warnings)
- `cargo test` — all tests pass
- Prefer Clippy idioms: `is_some_and(...)` over `map_or(false, ...)`

## Key Design Patterns

### Async I/O Pattern
Use `tokio::select!` for concurrent I/O (UDP recv + timer simultaneously).
Use `Poll`-based impls for `AsyncRead`/`AsyncWrite`.
Always `tokio::time::sleep`, never `std::thread::sleep`.

### Concurrency — CRITICAL RULE
Shared session state: `Arc<tokio::sync::Mutex<KcpSession>>`
**NEVER hold a TokioMutex lock across an `.await` point.**
Acquire → sync ops → release → then await I/O.

### Error Handling
Two-layer error design with `thiserror`:
- `KcpError` (core) — protocol errors: CreateFailed, SendFailed, RecvWouldBlock, RecvBufferTooSmall, RecvFailed, InputFailed, etc.
- `KcpTokioError` (async) — wraps KcpError + io::Error + Timeout + Closed + ConnectionFailed
- Conversion: `KcpError` → `KcpTokioError` via `#[from]`
- In AsyncRead/AsyncWrite impls: convert to `io::Error` via `io::Error::other()` or specific ErrorKinds

### Windows Compatibility
- `recv_from` may return `ErrorKind::ConnectionReset` (WSAECONNRESET/10054) — catch, `log::debug!`, `continue`
- KCP output callback: `WouldBlock` and `ConnectionReset` treated as success (`Ok(data.len())`), KCP handles retransmission

### Ghost Session Prevention
KcpListener maintains `closed_sessions: HashMap<SessionKey, Instant>` with 60s TTL.
Clean up when >100 entries. Stale retransmission packets from closed connections won't create phantom sessions.

### KCP State Machine
- `Kcp::update(current_ms)` called periodically (driven by `flush_interval`)
- Monotonic clock: `Instant::now().elapsed().as_millis() as u32` from session `start_time`
- `flush_write`: when enabled, `kcp.flush()` immediately after `kcp.send()`

### Send/Recv Modes
- **Client (Socket)**: KcpSession reads directly from `Arc<UdpSocket>`
- **Server (Channel)**: KcpSession receives via `mpsc::Receiver<Vec<u8>>`
- `take_channel_receiver()` transfers channel to `OwnedReadHalf` during split

### Logging
- Use `log` crate only (`debug!`, `error!`, etc.)
- `debug!`: normal operational events (ignored ICMP, closed sessions)
- `error!`: unexpected failures (session creation failure, UDP recv error)
- NO `println!`/`eprintln!` in library code (examples only)

## Testing Conventions

### Integration Tests (`tests/integration_tests.rs`)
- Use `#[tokio::test]` for all async tests
- Bind to `127.0.0.1:0` (random ephemeral port)
- Wait 50ms after bind before connect (listener background task startup)
- Wrap server handles in `time::timeout(Duration::from_secs(5), handle)`
- Shared `test_config()` helper with 5s timeout
- Unique `conv` ID per test

### Benchmarks (`benches/throughput.rs`)
- Criterion framework with `async_tokio` feature
- Compare KCP vs raw UDP vs TCP baselines

## Dependencies Policy

| Dependency | Role | Constraint |
|------------|------|------------|
| `thiserror` | Error derive | Major v2 |
| `log` | Logging facade | 0.4 |
| `tokio` | Async runtime (optional) | v1, features: net,rt,time,sync,macros,io-util |
| `bytes` | Byte buffers (optional) | v1 |
| `cc` | C compiler (build-dep) | v1 |
| `env_logger` | Example/test logging (dev-dep) | 0.11 |
| `criterion` | Benchmarking (dev-dep) | 0.5 |

- Keep dependencies minimal; prefer std where possible
- Optional deps gated behind features with `dep:` syntax
- Pin major versions only

## Current State

- **Version**: 0.0.4 (released 2026-03-24)
- **All planned milestones (M0–M5) complete**
- **Latest feature**: Adaptive recv buffer — `recv_kcp()` returns `Vec<u8>` using `peeksize()` auto-sizing; `recv_kcp_buf()` for manual buffer management
- **Roadmap**: See `PLAN.md` for completed items; new features TBD

## Development Commands

| Command | Purpose |
|---------|---------|
| `cargo build` | Build library (compiles C code) |
| `cargo test` | Run all tests |
| `cargo test <name>` | Run single test |
| `cargo test -- --ignored` | Run ignored tests (packet loss) |
| `cargo clippy --all-targets -- -D warnings` | Lint check |
| `cargo fmt` | Format code |
| `cargo fmt -- --check` | Check formatting |
| `cargo bench` | Run benchmarks |
| `cargo doc --no-deps --open` | Generate docs |
| `cargo publish --dry-run` | Verify publish readiness |
