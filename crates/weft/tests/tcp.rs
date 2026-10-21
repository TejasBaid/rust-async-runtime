//! TCP networking over the real reactor. All listeners bind to port 0.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

use weft::Runtime;
use weft::io::{AsyncReadExt, AsyncWriteExt};
use weft::net::{TcpListener, TcpStream};
use weft::time::timeout;

fn rt() -> Runtime {
    Runtime::builder().worker_threads(4).build().unwrap()
}

async fn echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    weft::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            weft::spawn(async move {
                let mut buf = vec![0u8; 16 * 1024];
                loop {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => {
                            if socket.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

#[test]
fn echo_round_trip() {
    let rt = rt();
    rt.block_on(async {
        let addr = echo_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        stream.write_all(b"hello weft").await.unwrap();
        let mut buf = [0u8; 10];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello weft");
    });
}

#[test]
fn large_transfer_exercises_partial_writes() {
    let rt = rt();
    rt.block_on(async {
        let addr = echo_server().await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let (mut rd, mut wr) = stream.into_split();
        let payload: Vec<u8> = (0..8 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let expected = payload.clone();
        let writer = weft::spawn(async move {
            wr.write_all(&payload).await.unwrap();
            wr.shutdown().await.unwrap();
        });
        let mut received = Vec::with_capacity(expected.len());
        rd.read_to_end(&mut received).await.unwrap();
        writer.await.unwrap();
        assert_eq!(received.len(), expected.len());
        assert!(received == expected);
    });
}

#[test]
fn many_concurrent_connections() {
    let rt = rt();
    rt.block_on(async {
        let addr = echo_server().await;
        let clients: Vec<_> = (0..200u32)
            .map(|i| {
                weft::spawn(async move {
                    let mut s = TcpStream::connect(addr).await.unwrap();
                    for round in 0..10u32 {
                        let msg = format!("client {i} round {round}");
                        s.write_all(msg.as_bytes()).await.unwrap();
                        let mut buf = vec![0u8; msg.len()];
                        s.read_exact(&mut buf).await.unwrap();
                        assert_eq!(buf, msg.as_bytes());
                    }
                })
            })
            .collect();
        for c in clients {
            timeout(Duration::from_secs(30), c).await.unwrap().unwrap();
        }
    });
}

#[test]
fn connect_refused() {
    let rt = rt();
    rt.block_on(async {
        // Bind then drop to get a port that is (very likely) closed.
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let err = TcpStream::connect(addr).await.unwrap_err();
        assert!(
            matches!(
                err.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotConnected
            ),
            "{err:?}"
        );
    });
}

#[test]
fn interop_with_blocking_std_client() {
    let rt = rt();
    let addr = rt.block_on(echo_server());
    let mut client = std::net::TcpStream::connect(addr).unwrap();
    client.write_all(b"ping").unwrap();
    let mut buf = [0u8; 4];
    client.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"ping");
}

#[test]
fn read_returns_zero_on_peer_close() {
    let rt = rt();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = weft::spawn(async move {
            let s = TcpStream::connect(addr).await.unwrap();
            drop(s);
        });
        let (mut server, peer) = listener.accept().await.unwrap();
        assert_eq!(peer.ip(), addr.ip());
        client.await.unwrap();
        let mut buf = [0u8; 8];
        let n = timeout(Duration::from_secs(5), server.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, 0);
    });
}

#[test]
fn concurrent_read_and_write_on_shared_stream() {
    let rt = rt();
    rt.block_on(async {
        let addr = echo_server().await;
        let stream = std::sync::Arc::new(TcpStream::connect(addr).await.unwrap());
        let writer = {
            let stream = stream.clone();
            weft::spawn(async move {
                let mut s = &*stream;
                for _ in 0..100 {
                    s.write_all(&[7u8; 1000]).await.unwrap();
                }
            })
        };
        let mut s = &*stream;
        let mut total = 0;
        let mut buf = [0u8; 4096];
        while total < 100_000 {
            let n = s.read(&mut buf).await.unwrap();
            assert!(buf[..n].iter().all(|&b| b == 7));
            total += n;
        }
        writer.await.unwrap();
    });
}

#[test]
fn listener_accepts_after_idle_period() {
    let rt = rt();
    rt.block_on(async {
        let addr = echo_server().await;
        weft::time::sleep(Duration::from_millis(50)).await;
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"x").await.unwrap();
        let mut b = [0u8; 1];
        s.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"x");
    });
}
