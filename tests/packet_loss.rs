//! Packet loss reliability tests.
//!
//! These tests verify KCP's ARQ retransmission guarantees under simulated
//! packet loss conditions. A lossy UDP proxy sits between client and server,
//! randomly dropping packets at a configured rate.
//!
//! These tests are marked `#[ignore]` because they are long-running and
//! unsuitable for CI. Run them locally with:
//!
//! ```text
//! cargo test -- --ignored
//! ```

use kcp_io::tokio_rt::{KcpListener, KcpSessionConfig, KcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time;

/// Statistics collected by the lossy proxy.
struct ProxyStats {
    forwarded: AtomicU64,
    dropped: AtomicU64,
}

/// Starts a UDP proxy that forwards packets between client and `server_addr`
/// with a configurable packet loss rate.
///
/// Returns `(proxy_addr, stats)` where `proxy_addr` is the address the client
/// should connect to.
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
        let mut client_addr: Option<std::net::SocketAddr> = None;
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
                match client_addr {
                    Some(addr) => addr,
                    None => continue,
                }
            } else {
                client_addr = Some(from_addr);
                server_addr
            };

            // xorshift64 PRNG
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

/// Run a packet-loss reliability test with the given loss rate.
async fn run_packet_loss_test(loss_rate: f64, message_count: usize) {
    let mut config = KcpSessionConfig::fast();
    config.timeout = Some(Duration::from_secs(30));
    config.kcp_config.snd_wnd = 256;
    config.kcp_config.rcv_wnd = 256;

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

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
        "Packet loss test ({:.0}% configured): {} messages OK | \
         packets: {} forwarded, {} dropped ({:.1}% actual loss)",
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
