//! White-box runtime tests: resource accounting inside the driver.

use std::time::Duration;

use crate::Runtime;
use crate::net::{TcpListener, TcpStream};
use crate::time::{sleep, timeout};

#[test]
fn io_registrations_are_released() {
    let rt = Runtime::builder().worker_threads(2).build().unwrap();
    let driver = rt.handle().shared.driver.clone();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut conns = Vec::new();
        for _ in 0..10 {
            let client = TcpStream::connect(addr).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            conns.push((client, server));
        }
        assert_eq!(driver.num_sources(), 21);
        drop(conns);
        assert_eq!(driver.num_sources(), 1);
        drop(listener);
        assert_eq!(driver.num_sources(), 0);
    });
}

#[test]
fn timers_are_released_on_drop_and_fire() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let driver = rt.handle().shared.driver.clone();
    rt.block_on(async {
        for _ in 0..100 {
            let _ = timeout(Duration::from_millis(0), sleep(Duration::from_secs(60))).await;
        }
        assert_eq!(driver.num_timers(), 0, "cancelled timers removed");
        sleep(Duration::from_millis(5)).await;
        assert_eq!(driver.num_timers(), 0, "fired timers removed");
    });
}

#[test]
fn shutdown_errors_pending_io() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let listener = rt.block_on(async { TcpListener::bind("127.0.0.1:0").unwrap() });
    let driver = rt.handle().shared.driver.clone();
    drop(rt);
    assert!(driver.is_shutdown());
    assert_eq!(driver.num_sources(), 0);
    // The listener's registration is gone; dropping it must not panic.
    drop(listener);
}
