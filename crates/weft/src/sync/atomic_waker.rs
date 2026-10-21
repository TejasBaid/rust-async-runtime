//! A lock-free single-slot waker cell.
//!
//! This is the classic algorithm from `futures-util`: a small state machine
//! (`WAITING`, `REGISTERING`, `WAKING`) serialises the consumer's `register`
//! against any number of concurrent `wake` calls without a mutex, and never
//! loses a wake-up: if a wake races with a registration, the registering
//! thread performs the wake itself.

use std::fmt;
use std::task::Waker;

use crate::loom::cell::UnsafeCell;
use crate::loom::sync::atomic::{AtomicUsize, Ordering};

const WAITING: usize = 0;
const REGISTERING: usize = 0b01;
const WAKING: usize = 0b10;

/// A slot holding at most one [`Waker`], updatable by one task and wakeable
/// from any thread.
pub struct AtomicWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: access to `waker` is serialised by the `state` protocol: only the
// thread that moves the state from WAITING to REGISTERING (or that sets WAKING
// while WAITING) touches the cell.
unsafe impl Send for AtomicWaker {}
// SAFETY: as above.
unsafe impl Sync for AtomicWaker {}

impl Default for AtomicWaker {
    fn default() -> Self {
        AtomicWaker::new()
    }
}

impl AtomicWaker {
    /// Creates an empty slot.
    pub fn new() -> AtomicWaker {
        AtomicWaker {
            state: AtomicUsize::new(WAITING),
            waker: UnsafeCell::new(None),
        }
    }

    /// Stores `waker`, replacing any previous one. Must not be called
    /// concurrently with itself (single consumer).
    pub fn register(&self, waker: &Waker) {
        match self
            .state
            .compare_exchange(WAITING, REGISTERING, Ordering::Acquire, Ordering::Acquire)
            .unwrap_or_else(|actual| actual)
        {
            WAITING => {
                self.waker.with_mut(|slot| {
                    // SAFETY: we hold the REGISTERING "lock".
                    let slot = unsafe { &mut *slot };
                    match slot {
                        Some(old) if old.will_wake(waker) => {}
                        _ => *slot = Some(waker.clone()),
                    }
                });
                // Release the lock. Failure means a `wake` arrived meanwhile
                // (state is REGISTERING | WAKING) and left the waking to us.
                if let Err(actual) = self.state.compare_exchange(
                    REGISTERING,
                    WAITING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    debug_assert_eq!(actual, REGISTERING | WAKING);
                    // SAFETY: still holding the lock (REGISTERING is set).
                    let waker = self.waker.with_mut(|slot| unsafe { (*slot).take() });
                    self.state.swap(WAITING, Ordering::AcqRel);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
            WAKING => {
                // A concurrent `wake` is running; it may have already taken
                // the old waker, so wake the new one directly.
                waker.wake_by_ref();
            }
            state => {
                // Concurrent `register` calls: a usage error, but harmless.
                debug_assert!(state == REGISTERING || state == REGISTERING | WAKING);
            }
        }
    }

    /// Removes and returns the stored waker, if any.
    pub fn take(&self) -> Option<Waker> {
        match self.state.fetch_or(WAKING, Ordering::AcqRel) {
            WAITING => {
                // SAFETY: we moved WAITING -> WAKING, which grants exclusive
                // access to the slot until WAKING is cleared.
                let waker = self.waker.with_mut(|slot| unsafe { (*slot).take() });
                self.state.fetch_and(!WAKING, Ordering::Release);
                waker
            }
            _ => {
                // Either a registration is in progress (it will see WAKING and
                // wake) or another `take` is running.
                None
            }
        }
    }

    /// Wakes the stored waker, if any.
    pub fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }
}

impl fmt::Debug for AtomicWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AtomicWaker")
    }
}
