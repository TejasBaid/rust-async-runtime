//! Synchronisation primitives that switch to [loom](https://docs.rs/loom)'s
//! model-checked versions when compiled with `RUSTFLAGS="--cfg loom"`.
//!
//! Only the lock-free core of the runtime (task state machine, run queue,
//! atomic waker, oneshot channel) goes through this shim; everything else
//! uses `std` directly. That keeps loom's state space small enough to explore
//! exhaustively.

#[cfg(loom)]
pub(crate) use loom::{cell, sync};

#[cfg(not(loom))]
pub(crate) mod sync {
    pub(crate) use std::sync::Arc;

    pub(crate) mod atomic {
        pub(crate) use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
    }
}

#[cfg(not(loom))]
pub(crate) mod cell {
    /// `std::cell::UnsafeCell` with loom's closure-based access API, so the
    /// same code compiles against both.
    #[derive(Debug)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        pub(crate) const fn new(data: T) -> UnsafeCell<T> {
            UnsafeCell(std::cell::UnsafeCell::new(data))
        }

        #[inline(always)]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline(always)]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}
