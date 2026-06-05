//! Adaptive recv buffer tests.
//!
//! Tests for `recv_kcp()` auto-sizing (via `peeksize()`) and
//! `recv_kcp_buf()` manual buffer management.

mod common;

use common::test_config;
use kcp_io::tokio_rt::{KcpListener, KcpStream};
use std::time::Duration;
use tokio::time;

#[tokio::test]
async fn test_recv_kcp_auto_small_message() {
    let config = test_config();

    let mut listener = KcpListener::bind("127.0.0.1:0", config.clone())
        .await
        .unwrap();
    let server_addr = listener.local_addr();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let data = stream.recv_kcp().await.unwrap();
        stream.send_kcp(b"hello").await.unwrap();
        assert_eq!(&data, b"ping");
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA001)
        .await
        .unwrap();

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

    let large_msg: Vec<u8> = (0..8000u16).map(|i| (i % 256) as u8).collect();
    let expected = large_msg.clone();

    let server_handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&expected).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA002)
        .await
        .unwrap();

    client.send_kcp(b"go").await.unwrap();

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
        for response in [
            b"A".as_slice(),
            &[0xBB; 100],
            &vec![0xCC; 5000],
            b"end".as_slice(),
        ] {
            let _ = stream.recv_kcp().await.unwrap();
            stream.send_kcp(response).await.unwrap();
        }
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA003)
        .await
        .unwrap();

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
        let _ = stream.recv_kcp().await.unwrap();
        stream.send_kcp(&[0xEE; 200]).await.unwrap();
    });

    time::sleep(Duration::from_millis(50)).await;

    let mut client = KcpStream::connect_with_conv(server_addr, config, 0xA005)
        .await
        .unwrap();

    client.send_kcp(b"go").await.unwrap();

    let mut small_buf = [0u8; 10];
    let result = client.recv_kcp_buf(&mut small_buf).await;
    assert!(
        result.is_err(),
        "recv_kcp_buf should fail when buffer is smaller than message"
    );

    let data = client.recv_kcp().await.unwrap();
    assert_eq!(data.len(), 200);
    assert_eq!(data, vec![0xEE; 200]);

    let _ = time::timeout(Duration::from_secs(5), server_handle).await;
}
