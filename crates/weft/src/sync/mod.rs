//! Asynchronous synchronisation primitives: channels and an atomic waker
//! slot.

mod atomic_waker;
pub mod mpsc;
pub mod oneshot;

pub use atomic_waker::AtomicWaker;
