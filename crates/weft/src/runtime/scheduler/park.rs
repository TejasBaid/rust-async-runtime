//! Worker parking.
//!
//! An idle worker sleeps in one of two ways:
//!
//! * If it can grab the I/O driver (a `try_lock`), it blocks in the OS
//!   poller (`kevent`/`epoll_wait`) with a timeout equal to the next timer
//!   deadline, so I/O readiness and timers wake it directly.
//! * Otherwise another worker is already driving I/O, and it blocks on a
//!   condition variable.
//!
//! `unpark` must know which of the two to interrupt, hence the small state
//! machine below (a classic "parker" with a NOTIFIED token so that an unpark
//! that races ahead of the park is never lost).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, TryLockError};

use crate::driver::{self, Driver};

const EMPTY: usize = 0;
const PARKED_CONDVAR: usize = 1;
const PARKED_DRIVER: usize = 2;
const NOTIFIED: usize = 3;

pub(crate) struct Parker {
    state: AtomicUsize,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl Parker {
    pub(crate) fn new() -> Parker {
        Parker {
            state: AtomicUsize::new(EMPTY),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    fn consume_notification(&self) -> bool {
        self.state
            .compare_exchange(NOTIFIED, EMPTY, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Blocks until `unpark` is called (or, when driving I/O, until any I/O
    /// or timer event — callers must tolerate such wake-ups).
    pub(crate) fn park(&self, driver_lock: &Mutex<Driver>, handle: &driver::Handle) {
        if self.consume_notification() {
            return;
        }
        match driver_lock.try_lock() {
            Ok(mut driver) => self.park_driver(&mut driver, handle),
            // The driver holds no invariants that a panic could break (its
            // only state is a reusable event buffer), so recover from poison.
            Err(TryLockError::Poisoned(poisoned)) => {
                self.park_driver(&mut poisoned.into_inner(), handle)
            }
            Err(TryLockError::WouldBlock) => self.park_condvar(),
        }
    }

    fn park_driver(&self, driver: &mut Driver, handle: &driver::Handle) {
        match self
            .state
            .compare_exchange(EMPTY, PARKED_DRIVER, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {}
            Err(NOTIFIED) => {
                self.state.store(EMPTY, Ordering::SeqCst);
                return;
            }
            Err(actual) => panic!("inconsistent park state: {actual}"),
        }
        driver.park(handle, None);
        match self.state.swap(EMPTY, Ordering::SeqCst) {
            NOTIFIED | PARKED_DRIVER => {}
            actual => panic!("inconsistent park_driver state: {actual}"),
        }
    }

    fn park_condvar(&self) {
        let mut guard = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
        match self
            .state
            .compare_exchange(EMPTY, PARKED_CONDVAR, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {}
            Err(NOTIFIED) => {
                self.state.store(EMPTY, Ordering::SeqCst);
                return;
            }
            Err(actual) => panic!("inconsistent park state: {actual}"),
        }
        loop {
            guard = self.condvar.wait(guard).unwrap_or_else(|e| e.into_inner());
            if self.consume_notification() {
                return;
            }
            // Spurious wake-up: keep waiting.
        }
    }

    pub(crate) fn unpark(&self, handle: &driver::Handle) {
        match self.state.swap(NOTIFIED, Ordering::SeqCst) {
            EMPTY | NOTIFIED => {}
            PARKED_CONDVAR => {
                // Taking the lock orders us after the parker's check of the
                // state, so the notification cannot slip in before `wait`.
                drop(self.mutex.lock().unwrap_or_else(|e| e.into_inner()));
                self.condvar.notify_one();
            }
            PARKED_DRIVER => handle.unpark(),
            actual => panic!("inconsistent unpark state: {actual}"),
        }
    }
}
