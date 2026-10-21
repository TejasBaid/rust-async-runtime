//! Scheduler, task and waker behaviour on a real multi-threaded runtime.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use weft::Runtime;
use weft::sync::oneshot;
use weft::task::yield_now;

fn rt(workers: usize) -> Runtime {
    Runtime::builder().worker_threads(workers).build().unwrap()
}

#[test]
fn block_on_returns_value() {
    let rt = rt(2);
    assert_eq!(rt.block_on(async { 1 + 1 }), 2);
}

#[test]
fn block_on_non_send_future() {
    let rt = rt(1);
    let rc = std::rc::Rc::new(5);
    let out = rt.block_on(async move { *rc + 1 });
    assert_eq!(out, 6);
}

#[test]
fn spawn_and_join() {
    let rt = rt(4);
    let out = rt.block_on(async {
        let h = weft::spawn(async { "hi".to_string() });
        h.await.unwrap()
    });
    assert_eq!(out, "hi");
}

#[test]
fn spawn_from_runtime_handle_outside_context() {
    let rt = rt(2);
    let h = rt.spawn(async { 7 });
    assert_eq!(rt.block_on(h).unwrap(), 7);
}

#[test]
fn spawn_from_foreign_thread_via_handle() {
    let rt = rt(2);
    let handle = rt.handle().clone();
    let join = thread::spawn(move || handle.spawn(async { 11 }))
        .join()
        .unwrap();
    assert_eq!(rt.block_on(join).unwrap(), 11);
}

#[test]
fn nested_spawns() {
    let rt = rt(4);
    let out = rt.block_on(async {
        let h = weft::spawn(async {
            let inner = weft::spawn(async { weft::spawn(async { 3 }).await.unwrap() * 2 });
            inner.await.unwrap() + 1
        });
        h.await.unwrap()
    });
    assert_eq!(out, 7);
}

#[test]
fn many_tasks_all_complete() {
    let rt = rt(4);
    let counter = Arc::new(AtomicUsize::new(0));
    rt.block_on({
        let counter = counter.clone();
        async move {
            let handles: Vec<_> = (0..10_000)
                .map(|i| {
                    let counter = counter.clone();
                    weft::spawn(async move {
                        if i % 3 == 0 {
                            yield_now().await;
                        }
                        counter.fetch_add(1, Ordering::Relaxed);
                        i
                    })
                })
                .collect();
            let mut sum = 0u64;
            for h in handles {
                sum += h.await.unwrap() as u64;
            }
            assert_eq!(sum, (0..10_000u64).sum());
        }
    });
    assert_eq!(counter.load(Ordering::Relaxed), 10_000);
}

#[test]
fn work_is_spread_across_workers() {
    let rt = rt(4);
    let threads = Arc::new(Mutex::new(std::collections::HashSet::new()));
    rt.block_on({
        let threads = threads.clone();
        async move {
            let handles: Vec<_> = (0..64)
                .map(|_| {
                    let threads = threads.clone();
                    weft::spawn(async move {
                        // Busy work long enough that stealing kicks in.
                        let start = Instant::now();
                        while start.elapsed() < Duration::from_millis(5) {
                            std::hint::spin_loop();
                        }
                        threads.lock().unwrap().insert(thread::current().id());
                    })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
        }
    });
    let n = threads.lock().unwrap().len();
    assert!(n >= 2, "expected tasks on multiple workers, got {n}");
}

#[test]
fn task_panic_is_captured() {
    let rt = rt(2);
    rt.block_on(async {
        let h = weft::spawn(async {
            panic!("boom");
        });
        let err = h.await.unwrap_err();
        assert!(err.is_panic());
        assert!(err.to_string().contains("boom"));
        // The worker survives the panic.
        assert_eq!(weft::spawn(async { 1 }).await.unwrap(), 1);
    });
}

#[test]
fn abort_pending_task() {
    let rt = rt(2);
    rt.block_on(async {
        let (_tx, rx) = oneshot::channel::<()>();
        let h = weft::spawn(async move {
            let _ = rx.await;
            "never"
        });
        yield_now().await;
        h.abort();
        let err = h.await.unwrap_err();
        assert!(err.is_cancelled());
    });
}

#[test]
fn abort_drops_future_and_its_resources() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let rt = rt(2);
    let dropped = Arc::new(AtomicBool::new(false));
    rt.block_on({
        let dropped = dropped.clone();
        async move {
            let flag = DropFlag(dropped);
            let h = weft::spawn(async move {
                let _flag = flag;
                std::future::pending::<()>().await;
            });
            h.abort();
            assert!(h.await.unwrap_err().is_cancelled());
        }
    });
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn abort_after_completion_keeps_output() {
    let rt = rt(2);
    rt.block_on(async {
        let h = weft::spawn(async { 5 });
        while !h.is_finished() {
            yield_now().await;
        }
        h.abort();
        assert_eq!(h.await.unwrap(), 5);
    });
}

#[test]
fn detached_task_still_runs() {
    let rt = rt(2);
    let (tx, rx) = std::sync::mpsc::channel();
    rt.block_on(async move {
        drop(weft::spawn(async move {
            yield_now().await;
            tx.send(42).unwrap();
        }));
    });
    assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), 42);
}

#[test]
fn output_dropped_when_join_handle_dropped() {
    struct Out(Arc<AtomicUsize>);
    impl Drop for Out {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let rt = rt(2);
    let drops = Arc::new(AtomicUsize::new(0));
    rt.block_on({
        let drops = drops.clone();
        async move {
            for _ in 0..100 {
                let d = drops.clone();
                drop(weft::spawn(async move { Out(d) }));
                let d = drops.clone();
                let h = weft::spawn(async move { Out(d) });
                drop(h.await.unwrap());
            }
        }
    });
    drop(rt);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        200,
        "each output dropped exactly once"
    );
}

/// A future woken from a plain OS thread (not a worker): exercises the remote
/// scheduling path and the parked-worker wake-up protocol.
#[test]
fn wake_from_foreign_thread() {
    struct WaitFlag {
        flag: Arc<AtomicBool>,
        waker: Arc<Mutex<Option<Waker>>>,
    }
    impl Future for WaitFlag {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.flag.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    let rt = rt(3);
    for _ in 0..200 {
        let flag = Arc::new(AtomicBool::new(false));
        let waker = Arc::new(Mutex::new(None::<Waker>));
        let h = rt.spawn(WaitFlag {
            flag: flag.clone(),
            waker: waker.clone(),
        });
        let t = thread::spawn(move || {
            loop {
                if let Some(w) = waker.lock().unwrap().take() {
                    flag.store(true, Ordering::SeqCst);
                    w.wake();
                    return;
                }
                thread::yield_now();
            }
        });
        rt.block_on(h).unwrap();
        t.join().unwrap();
    }
}

#[test]
fn yield_now_lets_other_tasks_run() {
    // With one worker, two tasks that yield must interleave.
    let rt = rt(1);
    let log = Arc::new(Mutex::new(Vec::new()));
    rt.block_on({
        let log = log.clone();
        async move {
            let mut handles = Vec::new();
            for id in 0..2 {
                let log = log.clone();
                handles.push(weft::spawn(async move {
                    for step in 0..3 {
                        log.lock().unwrap().push((id, step));
                        yield_now().await;
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        }
    });
    let log = log.lock().unwrap();
    let ids: Vec<_> = log.iter().map(|(id, _)| *id).collect();
    assert!(
        ids.windows(2).any(|w| w[0] != w[1]),
        "tasks did not interleave: {ids:?}"
    );
}

#[test]
fn spawn_blocking_runs_off_worker() {
    let rt = rt(1);
    rt.block_on(async {
        let worker = weft::spawn(async { thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        let blocking = weft::spawn_blocking(|| {
            thread::sleep(Duration::from_millis(10));
            thread::current().name().map(str::to_owned)
        })
        .await
        .unwrap();
        assert_ne!(worker, blocking);
        assert!(blocking.unwrap().contains("blocking"));
    });
}

#[test]
fn blocking_tasks_do_not_stall_async_tasks() {
    let rt = rt(1);
    rt.block_on(async {
        let slow = weft::spawn_blocking(|| thread::sleep(Duration::from_millis(200)));
        let start = Instant::now();
        weft::spawn(async { 1 }).await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(150));
        slow.await.unwrap();
    });
}

#[test]
fn shutdown_cancels_pending_tasks() {
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let rt = rt(2);
    for _ in 0..50 {
        let d = DropCount(drops.clone());
        rt.spawn(async move {
            let _d = d;
            std::future::pending::<()>().await;
        });
    }
    // Also a task blocked on a timer and one on a socket.
    let d = DropCount(drops.clone());
    rt.spawn(async move {
        let _d = d;
        weft::time::sleep(Duration::from_secs(3600)).await;
    });
    rt.block_on(async { weft::time::sleep(Duration::from_millis(20)).await });
    assert_eq!(rt.handle().num_alive_tasks(), 51);
    drop(rt);
    assert_eq!(drops.load(Ordering::SeqCst), 51);
}

#[test]
fn join_handle_resolves_cancelled_after_shutdown() {
    let rt = rt(1);
    let h = rt.spawn(std::future::pending::<()>());
    drop(rt);
    let rt2 = Runtime::builder().worker_threads(1).build().unwrap();
    let err = rt2.block_on(h).unwrap_err();
    assert!(err.is_cancelled());
}

#[test]
#[should_panic(expected = "context of a weft runtime")]
fn spawn_outside_runtime_panics() {
    drop(weft::spawn(async {}));
}

#[test]
fn lifo_slot_can_be_disabled() {
    let rt = Runtime::builder()
        .worker_threads(2)
        .disable_lifo_slot()
        .build()
        .unwrap();
    let out = rt.block_on(async {
        let (tx, rx) = oneshot::channel();
        weft::spawn(async move { tx.send(3).unwrap() });
        rx.await.unwrap()
    });
    assert_eq!(out, 3);
}

#[test]
fn many_runtimes_can_coexist() {
    let rts: Vec<_> = (0..4).map(|_| rt(2)).collect();
    for (i, rt) in rts.iter().enumerate() {
        assert_eq!(
            rt.block_on(async move { weft::spawn(async move { i }).await.unwrap() }),
            i
        );
    }
}
