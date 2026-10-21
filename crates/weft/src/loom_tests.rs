//! Exhaustive concurrency tests using [loom](https://docs.rs/loom).
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test -p weft --lib --release loom_
//! ```
//!
//! loom explores every legal interleaving (and every permitted weak-memory
//! reordering) of the threads in each model, and fails on data races detected
//! through its `UnsafeCell`, on deadlocks (lost wake-ups), and on leaks.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

mod queue {
    use super::*;
    use crate::runtime::scheduler::queue::{self, Local, Overflow};

    struct Inject(Mutex<Vec<usize>>);

    impl Overflow<usize> for Inject {
        fn push(&self, task: usize) {
            self.0.lock().unwrap().push(task);
        }
        fn push_batch(&self, tasks: impl Iterator<Item = usize>) {
            self.0.lock().unwrap().extend(tasks);
        }
    }

    fn drain(local: &mut Local<usize>) -> usize {
        let mut n = 0;
        while local.pop().is_some() {
            n += 1;
        }
        n
    }

    /// Owner pushes/pops while a stealer steals: no task is lost or
    /// duplicated.
    #[test]
    fn loom_queue_steal_while_popping() {
        loom::model(|| {
            let (steal, mut local) = queue::local::<usize>();
            let inject = Inject(Mutex::new(Vec::new()));

            let th = thread::spawn(move || {
                let (_, mut stealer_local) = queue::local::<usize>();
                let mut n = 0;
                if steal.steal_into(&mut stealer_local).is_some() {
                    n += 1;
                }
                n + drain(&mut stealer_local)
            });

            let mut n = 0;
            for i in 0..3 {
                local.push_back_or_overflow(i, &inject);
            }
            if local.pop().is_some() {
                n += 1;
            }
            local.push_back_or_overflow(3, &inject);
            n += drain(&mut local);
            n += th.join().unwrap();
            n += inject.0.lock().unwrap().len();
            assert_eq!(n, 4);
        });
    }

    /// Overflow (push to a full queue) racing with a steal.
    #[test]
    fn loom_queue_overflow_vs_steal() {
        loom::model(|| {
            let (steal, mut local) = queue::local::<usize>();
            let inject = Arc::new(Inject(Mutex::new(Vec::new())));
            for i in 0..queue::LOCAL_QUEUE_CAPACITY {
                local.push_back_or_overflow(i, &*inject);
            }

            let th = thread::spawn(move || {
                let (_, mut mine) = queue::local::<usize>();
                let mut n = 0;
                if steal.steal_into(&mut mine).is_some() {
                    n += 1;
                }
                n + drain(&mut mine)
            });

            local.push_back_or_overflow(99, &*inject);
            let mut n = drain(&mut local);
            n += th.join().unwrap();
            n += inject.0.lock().unwrap().len();
            assert_eq!(n, queue::LOCAL_QUEUE_CAPACITY + 1);
        });
    }

    /// Two stealers racing on the same queue.
    #[test]
    fn loom_queue_concurrent_stealers() {
        loom::model(|| {
            let (steal, mut local) = queue::local::<usize>();
            let inject = Inject(Mutex::new(Vec::new()));
            for i in 0..4 {
                local.push_back_or_overflow(i, &inject);
            }
            let s2 = steal.clone();
            let t1 = thread::spawn(move || {
                let (_, mut mine) = queue::local::<usize>();
                steal.steal_into(&mut mine).map_or(0, |_| 1) + drain(&mut mine)
            });
            let t2 = thread::spawn(move || {
                let (_, mut mine) = queue::local::<usize>();
                s2.steal_into(&mut mine).map_or(0, |_| 1) + drain(&mut mine)
            });
            let mut n = drain(&mut local);
            n += t1.join().unwrap() + t2.join().unwrap();
            assert_eq!(n, 4);
        });
    }
}

mod atomic_waker {
    use super::*;
    use crate::sync::AtomicWaker;

    /// A wake racing with a registration is never lost.
    #[test]
    fn loom_atomic_waker_no_lost_wakeup() {
        loom::model(|| {
            let waker = Arc::new(AtomicWaker::new());
            let flag = Arc::new(AtomicBool::new(false));

            let th = thread::spawn({
                let waker = waker.clone();
                let flag = flag.clone();
                move || {
                    flag.store(true, Ordering::SeqCst);
                    waker.wake();
                }
            });

            loom::future::block_on(poll_fn(|cx| {
                if flag.load(Ordering::SeqCst) {
                    return Poll::Ready(());
                }
                waker.register(cx.waker());
                if flag.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }));
            th.join().unwrap();
        });
    }
}

mod oneshot {
    use super::*;
    use crate::sync::oneshot;

    #[test]
    fn loom_oneshot_send_recv() {
        loom::model(|| {
            let (tx, rx) = oneshot::channel::<usize>();
            let th = thread::spawn(move || tx.send(7).unwrap());
            assert_eq!(loom::future::block_on(rx), Ok(7));
            th.join().unwrap();
        });
    }

    #[test]
    fn loom_oneshot_sender_drop_wakes_receiver() {
        loom::model(|| {
            let (tx, rx) = oneshot::channel::<usize>();
            let th = thread::spawn(move || drop(tx));
            assert!(loom::future::block_on(rx).is_err());
            th.join().unwrap();
        });
    }

    /// Sending races with the receiver being dropped: the value must be
    /// dropped exactly once, by whichever side loses the race.
    #[test]
    fn loom_oneshot_send_vs_receiver_drop() {
        loom::model(|| {
            let drops = Arc::new(AtomicUsize::new(0));
            struct D(Arc<AtomicUsize>);
            impl Drop for D {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let (tx, rx) = oneshot::channel::<D>();
            let d = D(drops.clone());
            let th = thread::spawn(move || {
                let _ = tx.send(d);
            });
            drop(rx);
            th.join().unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        });
    }
}

mod task {
    use super::*;
    use crate::task::{Id, JoinHandle, Notified, Schedule, Task, new_task};
    use std::collections::VecDeque;

    #[derive(Clone)]
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
        fn new() -> Mock {
            Mock(Arc::new(Mutex::new(VecDeque::new())))
        }
        fn run_all(&self) {
            loop {
                let task = self.0.lock().unwrap().pop_front();
                match task {
                    Some(t) => t.run(),
                    None => return,
                }
            }
        }
    }

    fn join<T>(handle: JoinHandle<T>) -> Result<T, crate::task::JoinError> {
        loom::future::block_on(handle)
    }

    /// The JoinHandle is awaited on one thread while the task completes on
    /// another: the join waker protocol must neither lose the wake-up nor
    /// race on the output cell.
    #[test]
    fn loom_task_join_vs_complete() {
        loom::model(|| {
            let sched = Mock::new();
            let (task, notified, handle) = new_task(async { 5usize }, sched.clone(), Id::next());
            let th = thread::spawn(move || {
                notified.run();
                drop(task);
            });
            assert_eq!(join(handle).unwrap(), 5);
            th.join().unwrap();
        });
    }

    /// A future that yields once, woken from a separate thread while it may
    /// still be running: NOTIFIED must coalesce wake-ups so the task is
    /// queued at most once and completes exactly once.
    #[test]
    fn loom_task_remote_wake_while_running() {
        struct WaitFlag {
            flag: Arc<AtomicBool>,
            slot: Arc<Mutex<Option<std::task::Waker>>>,
        }
        impl Future for WaitFlag {
            type Output = usize;
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
                *self.slot.lock().unwrap() = Some(cx.waker().clone());
                if self.flag.load(Ordering::SeqCst) {
                    Poll::Ready(1)
                } else {
                    Poll::Pending
                }
            }
        }

        loom::model(|| {
            let sched = Mock::new();
            let flag = Arc::new(AtomicBool::new(false));
            let slot = Arc::new(Mutex::new(None::<std::task::Waker>));
            let fut = WaitFlag {
                flag: flag.clone(),
                slot: slot.clone(),
            };
            let (task, notified, handle) = new_task(fut, sched.clone(), Id::next());
            notified.run();

            let waker_thread = thread::spawn(move || {
                flag.store(true, Ordering::SeqCst);
                let waker = slot.lock().unwrap().take();
                if let Some(w) = waker {
                    w.wake();
                }
            });
            let runner = {
                let sched = sched.clone();
                thread::spawn(move || sched.run_all())
            };
            waker_thread.join().unwrap();
            runner.join().unwrap();
            // Anything scheduled after the runner finished.
            sched.run_all();
            drop(task);
            assert_eq!(join(handle).unwrap(), 1);
        });
    }

    /// Abort races with the task being polled.
    #[test]
    fn loom_task_abort_vs_run() {
        loom::model(|| {
            let sched = Mock::new();
            let (task, notified, handle) = new_task(
                async {
                    crate::task::yield_now().await;
                    3usize
                },
                sched.clone(),
                Id::next(),
            );
            let runner = {
                let sched = sched.clone();
                thread::spawn(move || {
                    notified.run();
                    sched.run_all();
                })
            };
            handle.abort();
            runner.join().unwrap();
            sched.run_all();
            drop(task);
            match join(handle) {
                Ok(v) => assert_eq!(v, 3),
                Err(e) => assert!(e.is_cancelled()),
            }
        });
    }
}
