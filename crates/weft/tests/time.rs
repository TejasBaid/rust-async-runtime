//! Timer behaviour on a real runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use weft::Runtime;
use weft::time::{sleep, sleep_until, timeout};

fn rt() -> Runtime {
    Runtime::builder().worker_threads(2).build().unwrap()
}

#[test]
fn sleep_waits_at_least_duration() {
    let rt = rt();
    rt.block_on(async {
        for ms in [1u64, 5, 20, 50] {
            let start = Instant::now();
            sleep(Duration::from_millis(ms)).await;
            let elapsed = start.elapsed();
            assert!(
                elapsed >= Duration::from_millis(ms),
                "{ms}ms slept {elapsed:?}"
            );
            assert!(
                elapsed < Duration::from_millis(ms + 200),
                "{ms}ms slept {elapsed:?}"
            );
        }
    });
}

#[test]
fn zero_and_past_deadlines_complete_immediately() {
    let rt = rt();
    rt.block_on(async {
        let start = Instant::now();
        sleep(Duration::ZERO).await;
        sleep_until(Instant::now() - Duration::from_secs(1)).await;
        assert!(start.elapsed() < Duration::from_millis(50));
    });
}

#[test]
fn sleep_inside_spawned_tasks() {
    let rt = rt();
    rt.block_on(async {
        let start = Instant::now();
        let handles: Vec<_> = (0..100)
            .map(|i| {
                weft::spawn(async move {
                    sleep(Duration::from_millis(10 + (i % 10))).await;
                })
            })
            .collect();
        for h in handles {
            h.await.unwrap();
        }
        // All sleeps run concurrently.
        assert!(start.elapsed() < Duration::from_millis(500));
    });
}

#[test]
fn timers_fire_in_deadline_order() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    rt.block_on({
        let order = order.clone();
        async move {
            let handles: Vec<_> = [40u64, 10, 30, 20]
                .into_iter()
                .map(|ms| {
                    let order = order.clone();
                    weft::spawn(async move {
                        sleep(Duration::from_millis(ms)).await;
                        order.lock().unwrap().push(ms);
                    })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
        }
    });
    assert_eq!(*order.lock().unwrap(), vec![10, 20, 30, 40]);
}

#[test]
fn timeout_elapses() {
    let rt = rt();
    rt.block_on(async {
        let start = Instant::now();
        let res = timeout(Duration::from_millis(20), sleep(Duration::from_secs(60))).await;
        assert!(res.is_err());
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert!(start.elapsed() < Duration::from_secs(1));
    });
}

#[test]
fn timeout_passes_through_value() {
    let rt = rt();
    rt.block_on(async {
        let res = timeout(Duration::from_secs(5), async { 9 }).await;
        assert_eq!(res, Ok(9));
    });
}

#[test]
fn cancelled_timers_are_removed() {
    let rt = rt();
    rt.block_on(async {
        for _ in 0..1000 {
            let res = timeout(Duration::from_secs(3600), async { 1 }).await;
            assert_eq!(res, Ok(1));
            // Poll a long sleep once, then drop it.
            let _ = timeout(Duration::from_millis(0), sleep(Duration::from_secs(3600))).await;
        }
    });
}

#[test]
fn reset_moves_deadline() {
    let rt = rt();
    rt.block_on(async {
        let start = Instant::now();
        let mut s = sleep(Duration::from_secs(3600));
        // Register it, then pull the deadline in.
        let _ = timeout(Duration::from_millis(1), &mut s).await;
        s.reset(Instant::now() + Duration::from_millis(15));
        (&mut s).await;
        assert!(start.elapsed() >= Duration::from_millis(15));
        assert!(start.elapsed() < Duration::from_secs(2));
    });
}

/// An earlier timer registered while the driver is parked on a later one must
/// interrupt the park.
#[test]
fn earlier_timer_interrupts_parked_driver() {
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    rt.block_on(async {
        let long = weft::spawn(sleep(Duration::from_secs(10)));
        sleep(Duration::from_millis(30)).await; // driver now parked on the 10s timer
        let start = Instant::now();
        sleep(Duration::from_millis(10)).await;
        assert!(start.elapsed() < Duration::from_secs(2));
        long.abort();
    });
}

#[test]
fn many_concurrent_timers() {
    let rt = Runtime::builder().worker_threads(4).build().unwrap();
    let fired = Arc::new(AtomicUsize::new(0));
    rt.block_on({
        let fired = fired.clone();
        async move {
            let handles: Vec<_> = (0..20_000u64)
                .map(|i| {
                    let fired = fired.clone();
                    weft::spawn(async move {
                        sleep(Duration::from_millis(i % 100)).await;
                        fired.fetch_add(1, Ordering::Relaxed);
                    })
                })
                .collect();
            for h in handles {
                h.await.unwrap();
            }
        }
    });
    assert_eq!(fired.load(Ordering::Relaxed), 20_000);
}

#[test]
fn timers_fire_while_all_workers_busy() {
    // A CPU-bound task that keeps yielding must not prevent timers from
    // firing: busy workers poll the driver every `event_interval` ticks.
    let rt = Runtime::builder().worker_threads(1).build().unwrap();
    rt.block_on(async {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let spinner = weft::spawn({
            let stop = stop.clone();
            async move {
                while !stop.load(Ordering::Relaxed) {
                    weft::task::yield_now().await;
                }
            }
        });
        let timer = weft::spawn(async {
            let start = Instant::now();
            sleep(Duration::from_millis(20)).await;
            start.elapsed()
        });
        let elapsed = timer.await.unwrap();
        stop.store(true, Ordering::Relaxed);
        spinner.await.unwrap();
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    });
}
