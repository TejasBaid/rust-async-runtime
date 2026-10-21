//! Stress tests: many tasks and heavy cross-thread wake-up traffic.
//!
//! These run in debug mode as part of `cargo test`; their sizes are chosen to
//! finish in a few seconds while still exercising stealing, overflow to the
//! global queue, and the parking protocol under contention.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use weft::Runtime;
use weft::sync::{mpsc, oneshot};
use weft::task::yield_now;
use weft::time::timeout;

fn rt(workers: usize) -> Runtime {
    Runtime::builder().worker_threads(workers).build().unwrap()
}

/// Spawn far more tasks than fit in local queues, from inside a task (so the
/// local queue overflows into the injector).
#[test]
fn spawn_storm_from_worker() {
    let rt = rt(4);
    let done = Arc::new(AtomicUsize::new(0));
    let total = rt.block_on({
        let done = done.clone();
        async move {
            weft::spawn(async move {
                let handles: Vec<_> = (0..100_000)
                    .map(|_| {
                        let done = done.clone();
                        weft::spawn(async move {
                            done.fetch_add(1, Ordering::Relaxed);
                        })
                    })
                    .collect();
                let n = handles.len();
                for h in handles {
                    h.await.unwrap();
                }
                n
            })
            .await
            .unwrap()
        }
    });
    assert_eq!(total, 100_000);
    assert_eq!(done.load(Ordering::Relaxed), 100_000);
}

/// A ring of tasks passing a token: every hop is a cross-task wake-up, and
/// with several workers most hops cross threads.
#[test]
fn token_ring() {
    const TASKS: usize = 64;
    const LAPS: usize = 200;
    let rt = rt(4);
    rt.block_on(async {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..TASKS {
            let (tx, rx) = mpsc::channel::<usize>(1);
            senders.push(tx);
            receivers.push(rx);
        }
        receivers.rotate_left(1);
        let first = senders[0].clone();
        let mut handles = Vec::new();
        for (i, (tx, mut rx)) in senders.into_iter().zip(receivers).enumerate() {
            handles.push(weft::spawn(async move {
                let mut seen = 0;
                while let Some(hops) = rx.recv().await {
                    seen += 1;
                    if hops == TASKS * LAPS {
                        return seen;
                    }
                    if tx.send(hops + 1).await.is_err() {
                        return seen;
                    }
                    let _ = i;
                }
                seen
            }));
        }
        first.send(1).await.unwrap();
        drop(first);
        let h = handles.remove(TASKS - 1);
        timeout(Duration::from_secs(60), h).await.unwrap().unwrap();
        for h in handles {
            h.abort();
        }
    });
}

/// Wakers invoked from many plain OS threads at once.
#[test]
fn cross_thread_wakeups_from_os_threads() {
    let rt = rt(4);
    const N: usize = 2_000;
    let mut senders = Vec::with_capacity(N);
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let (tx, rx) = oneshot::channel::<usize>();
        senders.push(tx);
        handles.push(rt.spawn(async move { rx.await.unwrap() }));
    }
    // Let the tasks park first.
    std::thread::sleep(Duration::from_millis(20));
    let chunks: Vec<Vec<_>> = {
        let mut chunks: Vec<Vec<_>> = (0..8).map(|_| Vec::new()).collect();
        for (i, tx) in senders.into_iter().enumerate() {
            chunks[i % 8].push((i, tx));
        }
        chunks
    };
    let threads: Vec<_> = chunks
        .into_iter()
        .map(|chunk| {
            std::thread::spawn(move || {
                for (i, tx) in chunk {
                    tx.send(i).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let sum: usize = rt.block_on(async move {
        let mut sum = 0;
        for h in handles {
            sum += h.await.unwrap();
        }
        sum
    });
    assert_eq!(sum, (0..N).sum::<usize>());
}

/// Tasks that repeatedly yield and wake each other while new tasks keep
/// arriving from outside: shakes out lost wake-ups in the park/unpark path.
#[test]
fn bursty_external_spawns_never_stall() {
    let rt = rt(4);
    for burst in 0..200 {
        let n = 1 + burst % 17;
        let handles: Vec<_> = (0..n)
            .map(|i| {
                rt.spawn(async move {
                    for _ in 0..(i % 3) {
                        yield_now().await;
                    }
                    i
                })
            })
            .collect();
        let sum: usize = rt.block_on(async move {
            let mut sum = 0;
            for h in handles {
                sum += timeout(Duration::from_secs(10), h)
                    .await
                    .expect("task stalled")
                    .unwrap();
            }
            sum
        });
        assert_eq!(sum, (0..n).sum::<usize>());
        if burst % 50 == 0 {
            // Let every worker go idle so the next burst must wake them.
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Many short-lived runtimes: exercises startup/shutdown and task
/// cancellation paths repeatedly.
#[test]
fn runtime_churn() {
    for _ in 0..30 {
        let rt = rt(3);
        let (tx, rx) = oneshot::channel::<()>();
        rt.spawn(async move {
            let _ = rx.await;
        });
        rt.block_on(async {
            let hs: Vec<_> = (0..100).map(|i| weft::spawn(async move { i })).collect();
            for h in hs {
                h.await.unwrap();
            }
        });
        drop(rt);
        drop(tx);
    }
}
