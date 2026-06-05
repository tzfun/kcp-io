//! Custom transport tests.
//!
//! Tests for the `KcpTransport` trait and `KcpUdpTransport` default
//! implementation, including `KcpStream::connect_with_transport()`.

mod common;

use common::test_config;
use kcp_io::tokio_rt::{KcpListener, KcpStream, KcpTransport, KcpUdpTransport};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time;

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
