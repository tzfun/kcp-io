//! Integration tests for kcp-io.

use kcp_io::tokio_rt::{KcpListener, KcpSessionConfig, KcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
#[allow(unused_imports)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UdpSocket;
use tokio::sync::Barrier;
use tokio::time;

fn test_config() -> KcpSessionConfig {
    KcpSessionConfig {
        timeout: Some(Duration::from_secs(5)),
        ..KcpSessionConfig::fast()
    }
}

#[tokio::test]
async fn test_client_server_basic_communication() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .expect("Failed to bind listener");

    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, addr) = listener.accept().await.expect("Failed to accept");
        println!("Server: accepted connection from {}", addr);

        let data = stream.recv_kcp().await.expect("Server recv failed");
        stream.send_kcp(&data).await.expect("Server send failed");
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0x12345678)
        .await
        .expect("Client connect failed");

    let msg = b"Hello, KCP server!";
    client.send_kcp(msg).await.expect("Client send failed");

    let data = client.recv_kcp().await.expect("Client recv failed");

    assert_eq!(&data, msg);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

// ---------------------------------------------------------------------------
// Adaptive recv buffer tests — verify recv_kcp() auto-sizing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_recv_kcp_auto_small_message() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Client sends first to establish the session, then server echoes
        let data = stream.recv_kcp().await.unwrap();
        // Send back a tiny 5-byte message
        stream.send_kcp(b"hello").await.unwrap();
        assert_eq!(&data, b"ping");
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA001)
        .await
        .unwrap();

    // Initiate communication so the server session is active
    client.send_kcp(b"ping").await.unwrap();

    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data, b"hello");
    assert_eq!(data.len(), 5, "Auto buffer should be exactly 5 bytes");

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_recv_kcp_auto_large_message() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    // Create a 8000-byte payload — larger than a single MTU (1400)
    let large_msg: Vec<u8> = (0..8000u16).map(|i| (i % 256) as u8).collect();
    let expected = large_msg.clone();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Wait for client to initiate, then send the large payload
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&expected).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA002)
        .await
        .unwrap();

    client.send_kcp(b"go").await.unwrap();

    // recv_kcp() should auto-size to hold the entire 8000-byte message
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 8000, "Auto buffer should be exactly 8000 bytes");
    assert_eq!(data, large_msg);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_recv_kcp_auto_varying_sizes() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Echo-based: client sends a request, server replies with varying sizes
        for response in [
            b"A".as_slice(),
            &[0xBB; 100],
            &vec![0xCC; 5000],
            b"end".as_slice(),
        ] {
            let _ = stream.recv_kcp().await.unwrap(); // wait for client request
            stream.send_kcp(response).await.unwrap();
        }
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA003)
        .await
        .unwrap();

    // Each recv_kcp() should return exactly the right size
    client.send_kcp(b"req1").await.unwrap();
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data, b"A");

    client.send_kcp(b"req2").await.unwrap();
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 100);
    assert_eq!(data, vec![0xBB; 100]);

    client.send_kcp(b"req3").await.unwrap();
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 5000);
    assert_eq!(data, vec![0xCC; 5000]);

    client.send_kcp(b"req4").await.unwrap();
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 3);
    assert_eq!(data, b"end");

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_recv_kcp_auto_split_half() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Echo-based: wait for request, then reply with varying sizes
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&vec![0xDD; 3000]).await.unwrap();
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(b"tiny").await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let client = KcpStream::connect_with_conv(server_addr, config, 0xA004)
        .await
        .unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    // OwnedReadHalf::recv_kcp() should also auto-size correctly
    write_half.send_kcp(b"req1").await.unwrap();
    let data = read_half.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 3000);
    assert_eq!(data, vec![0xDD; 3000]);

    write_half.send_kcp(b"req2").await.unwrap();
    let data = read_half.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 4);
    assert_eq!(data, b"tiny");

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_recv_kcp_buf_too_small_returns_error() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Wait for client request, then send a 200-byte message
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&[0xEE; 200]).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA005)
        .await
        .unwrap();

    client.send_kcp(b"go").await.unwrap();

    // Using recv_kcp_buf with a buffer that's too small should fail
    let mut small_buf = [0u8; 10];
    let result = client.recv_kcp_buf(&mut small_buf).await;
    assert!(
        result.is_err(),
        "recv_kcp_buf should fail when buffer is smaller than message"
    );

    // But recv_kcp() (auto) should succeed on the same data
    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 200);
    assert_eq!(data, vec![0xEE; 200]);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

// ---------------------------------------------------------------------------
// Lossy UDP Proxy — simulates packet loss between client and server
// ---------------------------------------------------------------------------

/// Statistics collected by the lossy proxy.
struct ProxyStats {
    forwarded: AtomicU64,
    dropped: AtomicU64,
}

/// Starts a UDP proxy that forwards packets between `client_side` and `server_addr`
/// with a configurable packet loss rate.
///
/// Returns `(proxy_addr, stats)` where `proxy_addr` is the address the client
/// should connect to, and `stats` tracks forwarded/dropped packet counts.
///
/// The proxy runs as a background task and stops when the `UdpSocket` is dropped
/// or the task is aborted.
async fn start_lossy_proxy(
    server_addr: std::net::SocketAddr,
    loss_rate: f64,
) -> (std::net::SocketAddr, Arc<ProxyStats>) {
    let proxy_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_socket.local_addr().unwrap();
    let stats = Arc::new(ProxyStats {
        forwarded: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
    });
    let stats_clone = stats.clone();

    tokio::spawn(async move {
        let mut buf = [0u8; 65536];
        // Track the client's address (first packet from a non-server addr is the client)
        let mut client_addr: Option<std::net::SocketAddr> = None;
        // Simple pseudo-random state seeded from the current time
        let mut rng_state: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        loop {
            let (n, from_addr) = match proxy_socket.recv_from(&mut buf).await {
                Ok(v) => v,
                Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
                Err(_) => break,
            };

            // Determine forwarding direction
            let target = if from_addr == server_addr {
                // Packet from server → forward to client
                match client_addr {
                    Some(addr) => addr,
                    None => continue,
                }
            } else {
                // Packet from client → forward to server
                client_addr = Some(from_addr);
                server_addr
            };

            // xorshift64 PRNG for packet loss decision
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            let rand_val = (rng_state as f64) / (u64::MAX as f64);

            if rand_val < loss_rate {
                stats_clone.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }

            stats_clone.forwarded.fetch_add(1, Ordering::Relaxed);
            let _ = proxy_socket.send_to(&buf[..n], target).await;
        }
    });

    (proxy_addr, stats)
}

/// Helper: run a packet-loss reliability test with the given loss rate.
///
/// Sends `message_count` messages from client to server (echo), and verifies
/// that every single message is received correctly despite the simulated loss.
async fn run_packet_loss_test(loss_rate: f64, message_count: usize) {
    // Use a longer timeout to allow KCP retransmissions under heavy loss
    let mut config = KcpSessionConfig::fast();
    config.timeout = Some(Duration::from_secs(30));
    config.kcp_config.snd_wnd = 256;
    config.kcp_config.rcv_wnd = 256;

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    // Start a lossy proxy between client and server
    let (proxy_addr, stats) = start_lossy_proxy(server_addr, loss_rate).await;

    let expected_count = message_count;
    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        for _ in 0..expected_count {
            let data = stream.recv_kcp().await.unwrap();
            stream.send_kcp(&data).await.unwrap();
        }
    });

    time::sleep(Duration::from_millis(50)).await;

    // Client connects to the proxy, not directly to the server
    let mut client = KcpStream::connect_with_conv(proxy_addr, config, 0xDEAD)
        .await
        .unwrap();

    for i in 0..message_count {
        let msg = format!("loss-test-msg-{:04}", i);
        client.send_kcp(msg.as_bytes()).await.unwrap();

        let data = client.recv_kcp().await.unwrap();
        assert_eq!(
            &data,
            msg.as_bytes(),
            "Message {} corrupted or lost at {:.0}% loss rate",
            i,
            loss_rate * 100.0
        );
    }

    let forwarded = stats.forwarded.load(Ordering::Relaxed);
    let dropped = stats.dropped.load(Ordering::Relaxed);
    let total = forwarded + dropped;
    let actual_loss = if total > 0 {
        dropped as f64 / total as f64
    } else {
        0.0
    };
    println!(
        "Packet loss test ({:.0}% configured): {} messages OK | packets: {} forwarded, {} dropped ({:.1}% actual loss)",
        loss_rate * 100.0,
        message_count,
        forwarded,
        dropped,
        actual_loss * 100.0
    );

    let _ = time::timeout(Duration::from_secs(30), server_handle).await;
}

#[tokio::test]
#[ignore = "Long-running packet loss simulation; run locally with `cargo test -- --ignored`"]
async fn test_reliability_under_10_percent_packet_loss() {
    run_packet_loss_test(0.10, 20).await;
}

#[tokio::test]
#[ignore = "Long-running packet loss simulation; run locally with `cargo test -- --ignored`"]
async fn test_reliability_under_30_percent_packet_loss() {
    run_packet_loss_test(0.30, 20).await;
}

#[tokio::test]
#[ignore = "Long-running packet loss simulation; run locally with `cargo test -- --ignored`"]
async fn test_reliability_under_50_percent_packet_loss() {
    run_packet_loss_test(0.50, 10).await;
}

#[tokio::test]
async fn test_split_concurrent_read_write() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    // Server: split the stream and use read/write halves independently
    let server_handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut read_half, mut write_half) = stream.into_split();

        // Server sends a greeting immediately
        write_half.send_kcp(b"server-hello").await.unwrap();

        // Server echoes back whatever it receives
        let data = read_half.recv_kcp().await.unwrap();
        assert_eq!(&data, b"client-hello");

        // Send a second message
        write_half.send_kcp(b"server-ack").await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    // Client: split the stream and use read/write halves in separate tasks
    let client = KcpStream::connect_with_conv(server_addr, config, 0x400)
        .await
        .unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    // Client sends a greeting
    write_half.send_kcp(b"client-hello").await.unwrap();

    // Client receives server's greeting and ack
    let data = read_half.recv_kcp().await.unwrap();
    assert_eq!(&data, b"server-hello");

    let data = read_half.recv_kcp().await.unwrap();
    assert_eq!(&data, b"server-ack");

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_split_separate_tasks() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Echo 3 messages
        for _ in 0..3 {
            let data = stream.recv_kcp().await.unwrap();
            stream.send_kcp(&data).await.unwrap();
        }
    });

    time::sleep(Duration::from_millis(50)).await;

    let client = KcpStream::connect_with_conv(server_addr, config, 0x500)
        .await
        .unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    let barrier = Arc::new(Barrier::new(2));

    // Writer task: send 3 messages
    let write_barrier = barrier.clone();
    let writer = tokio::spawn(async move {
        let messages = [b"msg-1" as &[u8], b"msg-2", b"msg-3"];
        for msg in &messages {
            write_half.send_kcp(msg).await.unwrap();
            time::sleep(Duration::from_millis(20)).await;
        }
        write_barrier.wait().await;
    });

    // Reader task: receive 3 echoes
    let read_barrier = barrier.clone();
    let reader = tokio::spawn(async move {
        let expected = [b"msg-1" as &[u8], b"msg-2", b"msg-3"];
        for exp in &expected {
            let data = read_half.recv_kcp().await.unwrap();
            assert_eq!(&data, *exp);
        }
        read_barrier.wait().await;
    });

    let _ = time::timeout(Duration::from_secs(5), writer).await;
    let _ = time::timeout(Duration::from_secs(5), reader).await;
    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_split_close_from_write_half() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let _server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        // Just try to read until error
        let _ = stream.recv_kcp().await;
    });

    time::sleep(Duration::from_millis(50)).await;

    let client = KcpStream::connect_with_conv(server_addr, config, 0x600)
        .await
        .unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    // Send some data
    write_half.send_kcp(b"before-close").await.unwrap();

    // Close from write half
    write_half.close().await;

    // Both halves should now return Closed
    assert!(write_half.is_closed().await);
    assert!(write_half.send_kcp(b"after-close").await.is_err());
    assert!(read_half.recv_kcp().await.is_err());
}

#[tokio::test]
async fn test_bidirectional_communication() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _addr) = listener.accept().await.unwrap();
        stream.send_kcp(b"Hello from server").await.unwrap();

        let data = stream.recv_kcp().await.unwrap();
        assert_eq!(&data, b"Hello from client");
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0x100)
        .await
        .unwrap();

    client.send_kcp(b"Hello from client").await.unwrap();

    let data = client.recv_kcp().await.unwrap();
    assert_eq!(&data, b"Hello from server");

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_large_data_transfer() {
    let mut config = test_config();
    config.kcp_config.stream_mode = true;
    config.kcp_config.snd_wnd = 512;
    config.kcp_config.rcv_wnd = 512;

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let large_data: Vec<u8> = (0..2048u16).map(|i| (i % 256) as u8).collect();
    let expected = large_data.clone();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        let mut total_recv = Vec::new();
        while total_recv.len() < expected.len() {
            let data = stream.recv_kcp().await.unwrap();
            total_recv.extend_from_slice(&data);
        }
        assert_eq!(total_recv.len(), expected.len());
        assert_eq!(&total_recv, &expected);

        stream.send_kcp(&total_recv).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0x200)
        .await
        .unwrap();

    client.send_kcp(&large_data).await.unwrap();

    let mut total_recv = Vec::new();
    while total_recv.len() < large_data.len() {
        let data = client.recv_kcp().await.unwrap();
        total_recv.extend_from_slice(&data);
    }
    assert_eq!(total_recv.len(), large_data.len());
    assert_eq!(&total_recv, &large_data);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_multiple_messages() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        for _i in 0..3 {
            let data = stream.recv_kcp().await.unwrap();
            stream.send_kcp(&data).await.unwrap();
        }
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0x300)
        .await
        .unwrap();

    let messages = [
        b"First message" as &[u8],
        b"Second message",
        b"Third message",
    ];

    for msg in &messages {
        client.send_kcp(msg).await.unwrap();

        let data = client.recv_kcp().await.unwrap();
        assert_eq!(&data, *msg);
    }

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

// ---------------------------------------------------------------------------
// Custom transport tests — verify KcpTransport trait works end-to-end
// ---------------------------------------------------------------------------

use kcp_io::tokio_rt::{KcpTransport, KcpUdpTransport};
use std::io;
use std::net::SocketAddr;

/// A transport wrapper that counts packets sent/received without modifying data.
struct CountingTransport {
    inner: KcpUdpTransport,
    sent: AtomicUsize,
    recv: AtomicUsize,
}

impl CountingTransport {
    fn new(socket: Arc<UdpSocket>) -> Self {
        Self {
            inner: KcpUdpTransport::new(socket),
            sent: AtomicUsize::new(0),
            recv: AtomicUsize::new(0),
        }
    }
}

impl KcpTransport for CountingTransport {
    fn try_send(&self, data: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.inner.try_send(data, addr)
    }

    fn process_outgoing(&self, data: &[u8], _addr: SocketAddr) -> Vec<u8> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        data.to_vec()
    }

    fn process_incoming(&self, data: &[u8], _addr: SocketAddr) -> Option<Vec<u8>> {
        self.recv.fetch_add(1, Ordering::SeqCst);
        Some(data.to_vec())
    }
}

#[tokio::test]
async fn test_custom_transport_counting() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let data = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&data).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    // Create counting transport wrapping a UDP socket
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let transport = Arc::new(CountingTransport::new(socket.clone()));

    // Connect with custom transport
    let mut client =
        KcpStream::connect_with_transport(server_addr, config, transport.clone(), socket, 0xCAFE)
            .await
            .unwrap();

    let msg = b"Counting transport test!";
    client.send_kcp(msg).await.unwrap();

    let data = client.recv_kcp().await.unwrap();
    assert_eq!(&data, msg);

    // Verify the counting hooks were called
    assert!(
        transport.sent.load(Ordering::SeqCst) > 0,
        "process_outgoing should have been called at least once"
    );
    assert!(
        transport.recv.load(Ordering::SeqCst) > 0,
        "process_incoming should have been called at least once"
    );

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}

#[tokio::test]
async fn test_custom_transport_kcp_udp() {
    // Verify connect_with_transport works with plain KcpUdpTransport
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let data = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&data).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let transport = Arc::new(KcpUdpTransport::new(socket.clone()));

    let mut client =
        KcpStream::connect_with_transport(server_addr, config, transport, socket, 0xBEEF)
            .await
            .unwrap();

    let msg = b"KCP UDP transport test!";
    client.send_kcp(msg).await.unwrap();

    let data = client.recv_kcp().await.unwrap();
    assert_eq!(&data, msg);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}
