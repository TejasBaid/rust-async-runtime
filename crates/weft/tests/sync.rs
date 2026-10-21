//! Channel semantics.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use weft::Runtime;
use weft::sync::{mpsc, oneshot};
use weft::time::{sleep, timeout};

fn rt() -> Runtime {
    Runtime::builder().worker_threads(4).build().unwrap()
}

#[test]
fn oneshot_send_recv() {
    let rt = rt();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        weft::spawn(async move {
            sleep(Duration::from_millis(5)).await;
            tx.send(String::from("v")).unwrap();
        });
        assert_eq!(rx.await.unwrap(), "v");
    });
}

#[test]
fn oneshot_sender_dropped() {
    let rt = rt();
    rt.block_on(async {
        let (tx, rx) = oneshot::channel::<u8>();
        weft::spawn(async move { drop(tx) });
        assert!(rx.await.is_err());
    });
}

#[test]
fn oneshot_receiver_dropped_returns_value() {
    let (tx, rx) = oneshot::channel();
    drop(rx);
    assert!(tx.is_closed());
    assert_eq!(tx.send(5), Err(5));
}

#[test]
fn oneshot_try_recv() {
    let (tx, mut rx) = oneshot::channel();
    assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Empty));
    tx.send(1).unwrap();
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(oneshot::TryRecvError::Closed));
}

#[test]
fn oneshot_value_dropped_exactly_once() {
    struct D(Arc<AtomicUsize>);
    impl Drop for D {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    for i in 0..1000 {
        let (tx, rx) = oneshot::channel();
        let d = D(drops.clone());
        let t = std::thread::spawn(move || {
            let _ = tx.send(d);
        });
        if i % 2 == 0 {
            drop(rx);
        } else {
            std::thread::yield_now();
            drop(rx);
        }
        t.join().unwrap();
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1000);
}

#[test]
fn oneshot_cross_thread_ping_pong() {
    let rt = rt();
    rt.block_on(async {
        for i in 0..10_000u32 {
            let (tx, rx) = oneshot::channel();
            weft::spawn(async move { tx.send(i).unwrap() });
            assert_eq!(rx.await.unwrap(), i);
        }
    });
}

#[test]
fn mpsc_preserves_order_per_sender() {
    let rt = rt();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(8);
        weft::spawn(async move {
            for i in 0..1000 {
                tx.send(i).await.unwrap();
            }
        });
        for i in 0..1000 {
            assert_eq!(rx.recv().await, Some(i));
        }
        assert_eq!(rx.recv().await, None);
    });
}

#[test]
fn mpsc_many_producers() {
    let rt = rt();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(4);
        for p in 0..16u64 {
            let tx = tx.clone();
            weft::spawn(async move {
                for i in 0..500u64 {
                    tx.send(p * 1000 + i).await.unwrap();
                }
            });
        }
        drop(tx);
        let mut count = 0;
        let mut sum = 0u64;
        while let Some(v) = rx.recv().await {
            count += 1;
            sum += v;
        }
        assert_eq!(count, 16 * 500);
        let expected: u64 = (0..16u64)
            .map(|p| (0..500u64).map(|i| p * 1000 + i).sum::<u64>())
            .sum();
        assert_eq!(sum, expected);
    });
}

#[test]
fn mpsc_bounded_applies_backpressure() {
    let rt = rt();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(2);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        assert!(matches!(tx.try_send(3), Err(mpsc::TrySendError::Full(3))));
        // A third send blocks until the receiver makes room.
        assert!(
            timeout(Duration::from_millis(20), tx.send(3))
                .await
                .is_err()
        );
        assert_eq!(rx.recv().await, Some(1));
        timeout(Duration::from_secs(1), tx.send(3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    });
}

#[test]
fn mpsc_dropped_blocked_sender_forwards_capacity() {
    let rt = rt();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::channel(1);
        tx.send(0).await.unwrap();
        // Two blocked senders; the first gives up after being woken.
        let tx1 = tx.clone();
        let first =
            weft::spawn(async move { timeout(Duration::from_millis(50), tx1.send(1)).await });
        sleep(Duration::from_millis(5)).await;
        let tx2 = tx.clone();
        let second = weft::spawn(async move { tx2.send(2).await });
        sleep(Duration::from_millis(5)).await;
        first.abort();
        let _ = first.await;
        assert_eq!(rx.recv().await, Some(0));
        timeout(Duration::from_secs(1), second)
            .await
            .expect("second sender was stranded")
            .unwrap()
            .unwrap();
        assert_eq!(rx.recv().await, Some(2));
    });
}

#[test]
fn mpsc_receiver_drop_fails_senders() {
    let rt = rt();
    rt.block_on(async {
        let (tx, rx) = mpsc::channel(1);
        tx.send(1).await.unwrap();
        let blocked = weft::spawn({
            let tx = tx.clone();
            async move { tx.send(2).await }
        });
        sleep(Duration::from_millis(5)).await;
        drop(rx);
        assert!(blocked.await.unwrap().is_err());
        assert!(tx.is_closed());
        assert_eq!(tx.send(3).await, Err(mpsc::SendError(3)));
    });
}

#[test]
fn unbounded_channel() {
    let rt = rt();
    rt.block_on(async {
        let (tx, mut rx) = mpsc::unbounded_channel();
        for i in 0..10_000 {
            tx.send(i).unwrap();
        }
        drop(tx);
        let mut n = 0;
        while let Some(v) = rx.recv().await {
            assert_eq!(v, n);
            n += 1;
        }
        assert_eq!(n, 10_000);
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    });
}

#[test]
fn unbounded_send_from_std_thread() {
    let rt = rt();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let producer = std::thread::spawn(move || {
        for i in 0..1000 {
            tx.send(i).unwrap();
            if i % 100 == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    });
    let sum = rt.block_on(async move {
        let mut sum = 0;
        while let Some(v) = rx.recv().await {
            sum += v;
        }
        sum
    });
    producer.join().unwrap();
    assert_eq!(sum, (0..1000).sum::<i32>());
}
