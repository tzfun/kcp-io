//! Basic KCP communication tests.
//!
//! Tests fundamental send/receive patterns: echo, bidirectional,
//! multiple messages, and large data transfers.

mod common;

use common::test_config;
use kcp_io::tokio_rt::{KcpListener, KcpStream};
use std::time::Duration;
use tokio::time;

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
