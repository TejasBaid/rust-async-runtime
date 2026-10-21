//! Deterministic tests of the task state machine, reference counting and
//! waker behaviour, using a mock scheduler that just records `Notified`s.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use super::*;

#[derive(Clone, Default)]
struct Mock(Arc<Mutex<VecDeque<Notified<Mock>>>>);

impl Schedule for Mock {
    fn schedule(&self, task: Notified<Self>) {
        self.0.lock().unwrap().push_back(task);
    }
    fn release(&self, _task: &Task<Self>) -> Option<Task<Self>> {
        None
    }
}

impl Mock {
    fn pop(&self) -> Option<Notified<Mock>> {
        self.0.lock().unwrap().pop_front()
    }
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    /// Number of live tasks still holding a clone of the scheduler.
    fn live_tasks(&self) -> usize {
        Arc::strong_count(&self.0) - 1
    }
}

#[derive(Default)]
struct CountingWaker(AtomicUsize);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWaker>, Waker) {
    let c = Arc::new(CountingWaker::default());
    (c.clone(), Waker::from(c))
}

fn poll_join<T>(join: &mut JoinHandle<T>, waker: &Waker) -> Poll<Result<T, JoinError>> {
    Pin::new(join).poll(&mut Context::from_waker(waker))
}

/// A future that returns `Pending` until `ready` is set, stashing its waker.
#[derive(Clone, Default)]
struct Gate {
    ready: Arc<Mutex<bool>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl Gate {
    fn open(&self) {
        *self.ready.lock().unwrap() = true;
        if let Some(w) = self.waker.lock().unwrap().take() {
            w.wake();
        }
    }
}

impl Future for Gate {
    type Output = &'static str;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
        if *self.ready.lock().unwrap() {
            Poll::Ready("open")
        } else {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[test]
fn run_to_completion_and_join() {
    let sched = Mock::default();
    let (task, notified, mut join) = new_task(async { 42 }, sched.clone(), Id::next());
    let (count, waker) = counting_waker();
    assert!(poll_join(&mut join, &waker).is_pending());
    notified.run();
    assert_eq!(count.0.load(Ordering::SeqCst), 1, "join waker woken");
    assert!(join.is_finished());
    assert!(matches!(poll_join(&mut join, &waker), Poll::Ready(Ok(42))));
    drop(join);
    drop(task);
    assert_eq!(sched.live_tasks(), 0, "task deallocated");
}

#[test]
fn wake_from_waker_reschedules_once() {
    let sched = Mock::default();
    let gate = Gate::default();
    let (task, notified, mut join) = new_task(gate.clone(), sched.clone(), Id::next());
    notified.run();
    assert_eq!(sched.len(), 0);
    // Two wakes while idle produce one queue entry.
    let waker = gate.waker.lock().unwrap().clone().unwrap();
    waker.wake_by_ref();
    waker.wake_by_ref();
    assert_eq!(sched.len(), 1);
    drop(waker);
    gate.open();
    assert_eq!(sched.len(), 1, "already notified");
    sched.pop().unwrap().run();
    let (_, w) = counting_waker();
    assert!(matches!(poll_join(&mut join, &w), Poll::Ready(Ok("open"))));
    drop((task, join));
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn self_wake_during_poll_requeues() {
    let sched = Mock::default();
    let (task, notified, join) = new_task(
        async {
            yield_now().await;
            1
        },
        sched.clone(),
        Id::next(),
    );
    notified.run();
    assert_eq!(sched.len(), 1, "yielded task is re-queued");
    sched.pop().unwrap().run();
    assert!(join.is_finished());
    drop((task, join));
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn waker_clones_keep_task_alive() {
    let sched = Mock::default();
    let gate = Gate::default();
    let (task, notified, join) = new_task(gate.clone(), sched.clone(), Id::next());
    notified.run();
    let clones: Vec<Waker> = (0..10)
        .map(|_| gate.waker.lock().unwrap().clone().unwrap())
        .collect();
    drop((task, join));
    *gate.waker.lock().unwrap() = None;
    assert_eq!(sched.live_tasks(), 1, "wakers hold references");
    // Waking by value from the last references schedules the task, and the
    // Notified keeps it alive.
    for w in clones {
        w.wake();
    }
    assert_eq!(sched.len(), 1);
    assert_eq!(sched.live_tasks(), 1);
    gate.open();
    sched.pop().unwrap().run();
    assert_eq!(sched.live_tasks(), 0, "freed after completion");
}

#[test]
fn wake_after_completion_is_noop() {
    let sched = Mock::default();
    let gate = Gate::default();
    let (task, notified, join) = new_task(gate.clone(), sched.clone(), Id::next());
    notified.run();
    let waker = gate.waker.lock().unwrap().clone().unwrap();
    gate.open();
    sched.pop().unwrap().run();
    assert!(join.is_finished());
    waker.wake_by_ref();
    let by_val = waker.clone();
    by_val.wake();
    assert_eq!(sched.len(), 0);
    drop((waker, task, join));
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn join_waker_is_replaced_when_polled_from_another_task() {
    let sched = Mock::default();
    let gate = Gate::default();
    let (task, notified, mut join) = new_task(gate.clone(), sched.clone(), Id::next());
    notified.run();
    let (c1, w1) = counting_waker();
    let (c2, w2) = counting_waker();
    assert!(poll_join(&mut join, &w1).is_pending());
    assert!(poll_join(&mut join, &w1).is_pending(), "same waker: no-op");
    assert!(poll_join(&mut join, &w2).is_pending());
    gate.open();
    sched.pop().unwrap().run();
    assert_eq!(c1.0.load(Ordering::SeqCst), 0);
    assert_eq!(c2.0.load(Ordering::SeqCst), 1);
    drop((task, join));
}

struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn output_dropped_by_task_when_join_handle_gone() {
    let sched = Mock::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let d = drops.clone();
    let (task, notified, join) = new_task(async move { DropCounter(d) }, sched.clone(), Id::next());
    drop(join);
    notified.run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(task);
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn output_dropped_by_join_handle_after_completion() {
    let sched = Mock::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let d = drops.clone();
    let (task, notified, join) = new_task(async move { DropCounter(d) }, sched.clone(), Id::next());
    notified.run();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "output kept for the handle"
    );
    drop(join);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(task);
}

#[test]
fn abort_idle_task_cancels_and_drops_future() {
    let sched = Mock::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let d = DropCounter(drops.clone());
    let (task, notified, mut join) = new_task(
        async move {
            let _d = d;
            std::future::pending::<()>().await
        },
        sched.clone(),
        Id::next(),
    );
    notified.run();
    join.abort();
    assert_eq!(sched.len(), 1, "abort schedules the task");
    sched.pop().unwrap().run();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (_, w) = counting_waker();
    match poll_join(&mut join, &w) {
        Poll::Ready(Err(e)) => assert!(e.is_cancelled()),
        _ => panic!("expected cancellation"),
    }
    drop((task, join));
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn abort_before_first_poll() {
    let sched = Mock::default();
    let (task, notified, mut join) = new_task(async { 1 }, sched.clone(), Id::next());
    join.abort();
    assert_eq!(sched.len(), 0, "already notified, no second entry");
    notified.run();
    let (_, w) = counting_waker();
    assert!(matches!(poll_join(&mut join, &w), Poll::Ready(Err(e)) if e.is_cancelled()));
    drop((task, join));
}

#[test]
fn panic_is_reported_through_join_handle() {
    let sched = Mock::default();
    let (task, notified, mut join) = new_task(
        async {
            panic!("kaboom");
        },
        sched.clone(),
        Id::next(),
    );
    notified.run();
    let (_, w) = counting_waker();
    match poll_join(&mut join, &w) {
        Poll::Ready(Err(e)) => {
            assert!(e.is_panic());
            assert!(e.to_string().contains("kaboom"));
        }
        _ => panic!("expected panic error"),
    }
    drop((task, join));
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn panicking_output_destructor_is_contained() {
    struct Bomb;
    impl Drop for Bomb {
        fn drop(&mut self) {
            panic!("drop bomb");
        }
    }
    let sched = Mock::default();
    let (task, notified, join) = new_task(async { Bomb }, sched.clone(), Id::next());
    drop(join);
    notified.run(); // must not unwind
    drop(task);
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn shutdown_cancels_idle_task() {
    let sched = Mock::default();
    let gate = Gate::default();
    let (task, notified, mut join) = new_task(gate.clone(), sched.clone(), Id::next());
    notified.run();
    task.shutdown();
    let (_, w) = counting_waker();
    assert!(matches!(poll_join(&mut join, &w), Poll::Ready(Err(e)) if e.is_cancelled()));
    *gate.waker.lock().unwrap() = None;
    drop(join);
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn queued_notified_after_shutdown_is_discarded() {
    let sched = Mock::default();
    let (task, notified, join) = new_task(async { 1 }, sched.clone(), Id::next());
    task.shutdown();
    // The stale queue entry must not poll the (already dropped) future.
    notified.run();
    assert!(join.is_finished());
    drop(join);
    assert_eq!(sched.live_tasks(), 0);
}

#[test]
fn ids_are_unique() {
    let sched = Mock::default();
    let (t1, n1, j1) = new_task(async {}, sched.clone(), Id::next());
    let (t2, n2, j2) = new_task(async {}, sched.clone(), Id::next());
    assert_ne!(j1.id(), j2.id());
    assert_eq!(n1.id(), j1.id());
    assert_eq!(t2.id(), j2.id());
    drop((t1, t2, n1, n2, j1, j2));
    assert_eq!(sched.live_tasks(), 0);
}
