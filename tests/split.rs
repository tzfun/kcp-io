//! Split stream tests.
//!
//! Tests for `KcpStream::into_split()` — concurrent read/write
//! from separate tasks using `OwnedReadHalf` and `OwnedWriteHalf`.

mod common;

use common::test_config;
use kcp_io::tokio_rt::{KcpListener, KcpStream};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time;

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

    // Client: split the stream and use read/write halves
    let client = KcpStream::connect_with_conv(server_addr, config, 0x400)
        .await
        .unwrap();
    let (mut read_half, mut write_half) = client.into_split();

    write_half.send_kcp(b"client-hello").await.unwrap();

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
