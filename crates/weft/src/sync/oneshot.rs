//! A single-value, single-use channel.
//!
//! Lock-free: one atomic state word and an [`AtomicWaker`] for the receiver.
//! The sender writes the value into the shared cell *before* publishing
//! `VALUE_SENT`; whichever side observes the other's flag second is
//! responsible for the value (the receiver reads it, or the sender takes it
//! back if the receiver is already gone).
//!
//! ```
//! use weft::sync::oneshot;
//! let rt = weft::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let (tx, rx) = oneshot::channel();
//!     weft::spawn(async move { tx.send("done").unwrap() });
//!     assert_eq!(rx.await.unwrap(), "done");
//! });
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::AtomicWaker;
use crate::loom::cell::UnsafeCell;
use crate::loom::sync::Arc;
use crate::loom::sync::atomic::{AtomicUsize, Ordering};

const VALUE_SENT: usize = 0b001;
const TX_DROPPED: usize = 0b010;
const RX_CLOSED: usize = 0b100;

struct Inner<T> {
    state: AtomicUsize,
    value: UnsafeCell<Option<T>>,
    rx_waker: AtomicWaker,
}

// SAFETY: the value is moved between threads (requires `T: Send`); accesses
// to the cell are serialised by the state protocol described above.
unsafe impl<T: Send> Send for Inner<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Inner<T> {}

/// Sends the value. Created by [`channel`].
pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

/// Receives the value; a future resolving to `Result<T, RecvError>`.
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

/// The sender was dropped without sending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError(());

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("oneshot sender dropped without sending")
    }
}

impl std::error::Error for RecvError {}

/// Error from [`Receiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// No value has been sent yet.
    Empty,
    /// The sender was dropped without sending.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("channel empty"),
            TryRecvError::Closed => f.write_str("channel closed"),
        }
    }
}

impl std::error::Error for TryRecvError {}

/// Creates a oneshot channel.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(0),
        value: UnsafeCell::new(None),
        rx_waker: AtomicWaker::new(),
    });
    (
        Sender {
            inner: Some(inner.clone()),
        },
        Receiver { inner: Some(inner) },
    )
}

impl<T> Sender<T> {
    /// Sends `value`. Returns it back if the receiver has been dropped.
    pub fn send(mut self, value: T) -> Result<(), T> {
        let inner = self.inner.take().expect("sender used twice");
        if inner.state.load(Ordering::Acquire) & RX_CLOSED != 0 {
            return Err(value);
        }
        // SAFETY: before VALUE_SENT is published only the sender touches the
        // cell, and it is published exactly once.
        inner.value.with_mut(|slot| unsafe { *slot = Some(value) });
        let prev = inner.state.fetch_or(VALUE_SENT, Ordering::AcqRel);
        if prev & RX_CLOSED != 0 {
            // The receiver closed before seeing VALUE_SENT, so it will never
            // read the cell: take the value back.
            // SAFETY: the receiver only reads the cell after observing
            // VALUE_SENT, and it set RX_CLOSED before we published it.
            let value = inner.value.with_mut(|slot| unsafe { (*slot).take() });
            return Err(value.expect("value present"));
        }
        inner.rx_waker.wake();
        Ok(())
    }

    /// Returns `true` if the receiver has been dropped.
    pub fn is_closed(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|i| i.state.load(Ordering::Acquire) & RX_CLOSED != 0)
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.state.fetch_or(TX_DROPPED, Ordering::AcqRel);
            inner.rx_waker.wake();
        }
    }
}

impl<T> Receiver<T> {
    fn take_value(&mut self, state: usize) -> Option<Result<T, RecvError>> {
        let inner = self.inner.as_ref()?;
        if state & VALUE_SENT != 0 {
            // SAFETY: VALUE_SENT was observed with Acquire ordering, so the
            // sender's write happened-before and it will not touch the cell
            // again. We are the only receiver.
            let value = inner.value.with_mut(|slot| unsafe { (*slot).take() });
            self.inner = None;
            return Some(value.ok_or(RecvError(())));
        }
        if state & TX_DROPPED != 0 {
            self.inner = None;
            return Some(Err(RecvError(())));
        }
        None
    }

    /// Attempts to receive without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let Some(inner) = self.inner.as_ref() else {
            return Err(TryRecvError::Closed);
        };
        let state = inner.state.load(Ordering::Acquire);
        match self.take_value(state) {
            Some(Ok(v)) => Ok(v),
            Some(Err(_)) => Err(TryRecvError::Closed),
            None => Err(TryRecvError::Empty),
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(inner) = self.inner.as_ref() else {
            panic!("oneshot::Receiver polled after completion");
        };
        let state = inner.state.load(Ordering::Acquire);
        if let Some(res) = self.take_value(state) {
            return Poll::Ready(res);
        }
        let inner = self.inner.as_ref().expect("checked above");
        inner.rx_waker.register(cx.waker());
        // Re-check: the sender may have completed before our registration.
        let state = inner.state.load(Ordering::Acquire);
        match self.take_value(state) {
            Some(res) => Poll::Ready(res),
            None => Poll::Pending,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            let prev = inner.state.fetch_or(RX_CLOSED, Ordering::AcqRel);
            if prev & VALUE_SENT != 0 {
                // SAFETY: the sender published the value and will not touch
                // the cell again; drop it here rather than leaking it until
                // the Arc goes away.
                drop(inner.value.with_mut(|slot| unsafe { (*slot).take() }));
            }
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("oneshot::Sender")
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("oneshot::Receiver").finish_non_exhaustive()
    }
}
