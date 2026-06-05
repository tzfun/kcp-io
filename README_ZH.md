# kcp-io

**[English](README.md) | [中文](README_ZH.md)**

[![CI](https://github.com/tzfun/kcp-io/actions/workflows/ci.yml/badge.svg)](https://github.com/tzfun/kcp-io/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/kcp-io.svg)](https://crates.io/crates/kcp-io)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

一个 Rust 版本的 [KCP](https://github.com/skywind3000/kcp) 协议封装库，提供对原始 C 实现的安全绑定，以及基于 [tokio](https://tokio.rs/) 的异步 UDP 可靠通信层。

## 什么是 KCP？

KCP 是一个快速可靠的 ARQ（自动重传请求）协议，与 TCP 相比可以**降低 30%–40% 的平均延迟**，代价是多消耗 10%–20% 的带宽。它运行在传输层（UDP）之上，提供：

- ✅ 可靠、有序的数据传输
- ✅ 可配置策略的自动重传
- ✅ 拥塞控制（可选）
- ✅ 流模式和消息模式
- ✅ 无内核依赖 — 完全在用户空间运行

## 功能特性

本 crate 通过 Cargo features 提供分层架构：

- **`kcp-sys`** — KCP C 库的原始 FFI 绑定（通过 `cc` crate 从源码编译）
- **`kcp-core`** — 安全的、符合 Rust 惯例的 KCP 封装，支持 `Send`（隐含 `kcp-sys`）
- **`kcp-tokio`** *（默认启用）* — 基于 tokio 的全异步 `KcpStream` + `KcpListener`，支持 `AsyncRead`/`AsyncWrite`（隐含 `kcp-core`）

Feature 依赖链：`kcp-tokio` → `kcp-core` → `kcp-sys`

## 快速开始

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
kcp-io = "0.1"
tokio = { version = "1", features = ["full"] }
```

### 回显服务器

```rust
use kcp_io::{KcpListener, KcpSessionConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut listener = KcpListener::bind("0.0.0.0:9090", KcpSessionConfig::fast()).await?;
    println!("监听地址：{}", listener.local_addr());

    loop {
        let (mut stream, addr) = listener.accept().await?;
        println!("[{}] 已连接 (conv={})", addr, stream.conv());

        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                match stream.recv_kcp(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        stream.send_kcp(&buf[..n]).await.ok();
                    }
                    Err(_) => break,
                }
            }
        });
    }
}
```

### 回显客户端

```rust
use kcp_io::{KcpStream, KcpSessionConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
    println!("已连接！conv={}", stream.conv());

    // 发送数据
    stream.send_kcp(b"Hello, KCP!").await?;

    // 接收回显
    let mut buf = [0u8; 4096];
    let n = stream.recv_kcp(&mut buf).await?;
    println!("回显：{}", String::from_utf8_lossy(&buf[..n]));

    Ok(())
}
```

### 使用 `AsyncRead` / `AsyncWrite`

`KcpStream` 实现了 Tokio 的 `AsyncRead` 和 `AsyncWrite` trait，可以与任何兼容 Tokio 的生态系统配合使用：

```rust
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kcp_io::{KcpStream, KcpSessionConfig};

async fn example() -> std::io::Result<()> {
    let mut stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // 通过 AsyncWrite 写入
    stream.write_all(b"hello via AsyncWrite").await?;

    // 通过 AsyncRead 读取
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    println!("收到：{}", String::from_utf8_lossy(&buf[..n]));

    Ok(())
}
```

### 读写分离 (`into_split`)

`KcpStream::into_split()` 将一个流拆分为独立的读半和写半，支持在不同 task 中并发读写 — 类似于 [`TcpStream::into_split()`](https://docs.rs/tokio/latest/tokio/net/struct.TcpStream.html#method.into_split)：

```rust
use kcp_io::{KcpStream, KcpSessionConfig};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let stream = KcpStream::connect("127.0.0.1:9090", KcpSessionConfig::fast()).await?;
    let (mut read_half, mut write_half) = stream.into_split();

    // 在独立 task 中读取
    let reader = tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match read_half.recv_kcp(&mut buf).await {
                Ok(n) => println!("收到：{}", String::from_utf8_lossy(&buf[..n])),
                Err(_) => break,
            }
        }
    });

    // 在当前 task 中写入
    write_half.send_kcp(b"hello").await?;
    write_half.send_kcp(b"world").await?;
    write_half.close().await;

    reader.await?;
    Ok(())
}
```

### 自定义传输层 (`KcpTransport`)

`KcpTransport` 是一个 trait，允许你在 KCP 数据包发送前和接收后拦截并转换字节流，甚至完全替换底层 I/O：

```rust
use kcp_io::{KcpStream, KcpSessionConfig, KcpTransport, KcpUdpTransport};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;

/// 一个对 KCP 数据包进行 XOR 混淆的传输层。
struct XorTransport {
    inner: KcpUdpTransport,
    key: u8,
}

impl KcpTransport for XorTransport {
    fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.inner.try_send(data, addr)
    }

    fn process_outgoing(&self, data: &[u8], _addr: SocketAddr) -> Vec<u8> {
        data.iter().map(|b| b ^ self.key).collect()
    }

    fn process_incoming(&self, data: &[u8], _addr: SocketAddr) -> Option<Vec<u8>> {
        Some(data.iter().map(|b| b ^ self.key).collect())
    }
}

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    let transport = Arc::new(XorTransport {
        inner: KcpUdpTransport::new(socket.clone()),
        key: 0x5A,
    });

    let mut stream = KcpStream::connect_with_transport(
        "127.0.0.1:9090",
        KcpSessionConfig::fast(),
        transport,
        socket,
        0x1234,
    ).await?;
    // 所有 KCP 数据包在发送前会被 XOR 混淆，
    // 接收后会被解混淆。
    stream.send_kcp(b"hello").await?;
    Ok(())
}
```

自定义传输层的使用场景：
- **加密**：在 `process_outgoing` / `process_incoming` 中加解密 KCP 数据包
- **压缩**：发送前压缩，接收后解压
- **日志/监控**：统计数据包数量、记录流量、测量吞吐量
- **替代 I/O**：将 UDP 替换为 WebSocket、Unix 套接字或自定义通道

### 仅使用 Core 层

如果你只需要安全的 KCP 封装，不需要异步支持：

```toml
[dependencies]
kcp-io = { version = "0.1", default-features = false, features = ["kcp-core"] }
```

```rust
use kcp_io::core::{Kcp, KcpConfig};
use std::io;

let mut kcp = Kcp::new(0x01, |data: &[u8]| -> io::Result<usize> {
    // 通过你自己的传输层发送数据
    Ok(data.len())
}).unwrap();

kcp.apply_config(&KcpConfig::fast()).unwrap();
kcp.send(b"Hello, KCP!").unwrap();
kcp.update(0);
```

## API 概览

### `tokio_rt` 模块（默认 feature: `kcp-tokio`）

| 类型 | 说明 |
|------|------|
| `KcpStream` | 异步 KCP 流 — 实现 `AsyncRead` + `AsyncWrite` |
| `KcpListener` | 在 UDP 套接字上接受传入的 KCP 连接 |
| `KcpSessionConfig` | 运行时配置（KCP 参数 + 会话设置） |
| `KcpTokioError` | 异步操作的错误类型 |
| `KcpTransport` | 自定义传输层 trait（发包 + 字节处理钩子） |
| `KcpUdpTransport` | 默认 UDP 传输层实现（封装 `Arc<UdpSocket>`） |

**`KcpStream` 方法：**

| 方法 | 说明 |
|------|------|
| `connect(addr, config)` | 连接到远程 KCP 服务器 |
| `connect_with_conv(addr, conv, config)` | 使用指定的会话 ID 连接 |
| `connect_with_transport(addr, config, transport, socket, conv)` | 使用自定义 [`KcpTransport`] 连接 |
| `send_kcp(data)` | 可靠地发送数据 |
| `recv_kcp(buf)` | 接收数据 |
| `into_split()` | 拆分为 `OwnedReadHalf` + `OwnedWriteHalf`，支持并发读写 |
| `close()` | 关闭流 |
| `flush()` | 刷新待发送的 KCP 数据 |
| `conv()` | 获取会话 ID |
| `remote_addr()` | 获取远程地址 |
| `local_addr()` | 获取本地地址 |

**`OwnedReadHalf` 方法**（通过 `into_split()` 获得）：

| 方法 | 说明 |
|------|------|
| `recv_kcp(buf)` | 接收数据（异步，锁不跨 await 点持有） |
| `conv()` | 获取会话 ID |
| `remote_addr()` | 获取远程地址 |
| `local_addr()` | 获取本地地址 |

**`OwnedWriteHalf` 方法**（通过 `into_split()` 获得）：

| 方法 | 说明 |
|------|------|
| `send_kcp(data)` | 可靠地发送数据 |
| `flush()` | 刷新待发送的 KCP 数据 |
| `close()` | 关闭流（两半都会受影响） |
| `is_closed()` | 检查流是否已关闭 |
| `conv()` | 获取会话 ID |

**`KcpListener` 方法：**

| 方法 | 说明 |
|------|------|
| `bind(addr, config)` | 绑定本地地址并开始监听 |
| `accept()` | 接受下一个传入连接 |
| `local_addr()` | 获取监听器的本地地址 |

### `core` 模块（feature: `kcp-core`）

| 类型 | 说明 |
|------|------|
| `Kcp` | C `IKCPCB` 结构体的安全封装 |
| `KcpConfig` | 协议配置（nodelay、interval、resend 等） |
| `KcpError` | KCP 操作的错误类型 |

### `sys` 模块（feature: `kcp-sys`）

所有 `ikcp_*` 函数的直接绑定。通常不需要直接使用。

## 配置

### 预设方案

| 预设 | nodelay | interval | resend | nc | snd_wnd | rcv_wnd | 适用场景 |
|------|---------|----------|--------|----|---------|---------|----------|
| `default()` | 关闭 | 100ms | 0 | 关闭 | 32 | 128 | 保守方案，低带宽消耗 |
| `normal()` | 开启 | 40ms | 2 | 关闭 | 64 | 128 | 延迟与带宽均衡 |
| `fast()` | 开启 | 10ms | 2 | 开启 | 128 | 128 | 最低延迟，带宽消耗较高 |

### 自定义配置

```rust
use kcp_io::tokio_rt::KcpSessionConfig;
use kcp_io::core::KcpConfig;
use std::time::Duration;

let config = KcpSessionConfig {
    kcp_config: KcpConfig {
        nodelay: true,
        interval: 20,
        resend: 2,
        nc: true,
        mtu: 1200,          // 较小的 MTU，适合受限网络
        snd_wnd: 256,        // 较大的窗口，适合高吞吐场景
        rcv_wnd: 256,
        stream_mode: true,   // 字节流模式（类似 TCP）
    },
    flush_interval: Duration::from_millis(20),
    timeout: Some(Duration::from_secs(30)),
    flush_write: true,       // 写入后立即刷新
    recv_buf_size: 65536,
};
```

### 流模式 vs 消息模式

| 特性 | 消息模式（默认） | 流模式 |
|------|-----------------|--------|
| 边界 | 保留消息边界 | 字节流（类似 TCP） |
| 大数据 | 受分片数量限制 | 无大小限制 |
| 小数据 | 每条消息独立成包 | 合并小消息 |
| 适用场景 | 游戏数据包、RPC | 文件传输、批量数据 |

## 架构

```
客户端                                   服务端
┌──────────────────┐                     ┌──────────────────┐
│   应用层         │                     │   应用层          │
│                  │                     │                  │
│  KcpStream       │                     │  KcpListener     │
│  ├─ send_kcp()   │                     │  ├─ bind()       │
│  ├─ recv_kcp()   │                     │  ├─ accept()     │
│  └─ AsyncR/W     │                     │  └─ KcpStream    │
├──────────────────┤                     ├──────────────────┤
│  KcpSession      │                     │  KcpSession      │
│  ├─ KCP 引擎     │                     │  ├─ KCP 引擎      │
│  ├─ 定时器       │                     │  ├─ mpsc 通道     │
│  └─ UdpSocket    │                     │  └─ (共享 UDP)   │
├──────────────────┤                     ├──────────────────┤
│  core::Kcp       │                     │  core::Kcp       │
│  └─ output 回调 ─┼── UDP 数据包 ──▶  ──┼─ input()         │
├──────────────────┤                     ├──────────────────┤
│  sys (FFI)       │                     │  sys (FFI)       │
│  └─ ikcp.c       │                     │  └─ ikcp.c       │
└──────────────────┘                     └──────────────────┘
         │                                        │
         └──────── UDP / 互联网 ──────────────────────┘
```

**核心设计决策：**
- **客户端会话**直接拥有自己的 UDP 套接字（`RecvMode::Socket`）
- **服务端会话**通过 `mpsc::channel` 从监听器的共享套接字接收数据包（`RecvMode::Channel`）
- `KcpListener` 运行一个后台 `tokio::spawn` 任务，按 `(SocketAddr, conv)` 将传入的 UDP 数据包多路复用到对应的会话通道
- `KcpSession` 在 send/recv 调用期间驱动 `ikcp_update()`

## 性能特征

| 方面 | 详情 |
|------|------|
| 延迟 | 在丢包网络上比 TCP 低 30–40% |
| 带宽 | 相比原始 UDP 约有 10–20% 的额外开销 |
| CPU | 极低 — KCP 是轻量级 C 代码 |
| 内存 | 每个会话一个 `Kcp` 实例（约 10 KB） |
| 并发 | 每个连接一个 `KcpSession`，由 tokio 运行时管理 |

## 开发

### 环境要求

- Rust 1.85+（2021 edition）
- C 编译器（Windows 上为 MSVC，Linux/macOS 上为 gcc/clang）— 用于编译 `ikcp.c`

### 构建

```bash
# 构建（默认 features: kcp-tokio）
cargo build

# 仅构建 core 功能
cargo build --no-default-features --features kcp-core

# Release 模式构建
cargo build --release
```

### 测试

```bash
# 运行所有测试（单元测试 + 集成测试 + 文档测试）
cargo test

# 仅运行集成测试
cargo test --test basic --test recv_buffer --test split --test transport

# 运行丢包测试（耗时较长，建议本地运行）
cargo test --test packet_loss -- --ignored

# 带日志运行
RUST_LOG=debug cargo test -- --nocapture
```

### 运行示例

```bash
# 终端 1：启动回显服务器
cargo run --example echo_server

# 终端 2：运行回显客户端
cargo run --example echo_client
```

### 项目结构

```
kcp-io/
├── Cargo.toml              # 单 crate 配置（含 feature 标志）
├── build.rs                # 通过 cc crate 编译 KCP C 代码
├── kcp/                    # KCP C 源码
│   ├── ikcp.c
│   ├── ikcp.h
│   └── wrapper.h
├── src/
│   ├── lib.rs              # 根模块：feature 条件导出 + 便捷 re-exports
│   ├── sys.rs              # 原始 FFI 绑定（feature: kcp-sys）
│   ├── core/               # 安全的 Rust API 封装（feature: kcp-core）
│   │   ├── mod.rs
│   │   ├── kcp.rs          # Kcp 结构体（IKCPCB 的安全封装）
│   │   ├── config.rs       # KcpConfig 配置预设
│   │   └── error.rs        # KcpError 错误枚举
│   └── tokio_rt/           # 异步 tokio 集成（feature: kcp-tokio）
│       ├── mod.rs
│       ├── stream.rs       # KcpStream（AsyncRead + AsyncWrite）
│       ├── listener.rs     # KcpListener（接受传入连接）
│       ├── session.rs      # KcpSession（内部状态机）
│       ├── transport.rs    # KcpTransport trait + KcpUdpTransport
│       ├── config.rs       # KcpSessionConfig 运行时配置
│       └── error.rs        # KcpTokioError 错误类型
├── tests/
│   ├── common/mod.rs       # 共享测试工具
│   ├── basic.rs            # 基础通信测试
│   ├── recv_buffer.rs      # 自适应接收缓冲区测试
│   ├── packet_loss.rs      # 丢包可靠性测试
│   ├── split.rs            # 分流读写测试
│   └── transport.rs        # 自定义传输层测试
├── benches/                # Criterion 基准测试
└── examples/               # 回显服务器和客户端示例
```

## 许可证

MIT 许可证。详见 [LICENSE](LICENSE) 文件。

## 致谢

- [KCP](https://github.com/skywind3000/kcp)，作者 skywind3000 — 原始 C 实现
- [tokio](https://tokio.rs/) — 驱动异步层的异步运行时