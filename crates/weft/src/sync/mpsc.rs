//! Multi-producer, single-consumer channels, bounded and unbounded.
//!
//! The channel state lives behind one `Mutex`: a `VecDeque` of messages, the
//! receiver's waker and a FIFO of blocked senders. Critical sections are a
//! handful of instructions and wakers are always invoked *after* the lock is
//! released, so the lock is held only briefly even under contention. This
//! trades the peak throughput of a lock-free linked-block queue (as in Tokio)
//! for an implementation whose correctness is easy to audit; see the README
//! for measured numbers.
//!
//! Back-pressure: a bounded sender that finds the queue full parks in the
//! senders FIFO. Each `recv` that frees a slot wakes exactly one blocked
//! sender. If a woken sender's future is dropped before it sends, its wake-up
//! is forwarded to the next blocked sender, so capacity is never stranded.
//!
//! ```
//! use weft::sync::mpsc;
//! let rt = weft::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let (tx, mut rx) = mpsc::channel(16);
//!     for i in 0..4 {
//!         let tx = tx.clone();
//!         weft::spawn(async move { tx.send(i).await.unwrap() });
//!     }
//!     drop(tx);
//!     let mut got = Vec::new();
//!     while let Some(v) = rx.recv().await {
//!         got.push(v);
//!     }
//!     got.sort();
//!     assert_eq!(got, vec![0, 1, 2, 3]);
//! });
//! ```

use std::collections::VecDeque;
use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

/// Error returned when the receiver has been dropped; contains the message.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SendError(..)")
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("channel closed")
    }
}

impl<T> std::error::Error for SendError<T> {}

/// Error returned by [`Sender::try_send`].
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum TrySendError<T> {
    /// The channel is at capacity.
    Full(T),
    /// The receiver has been dropped.
    Closed(T),
}

impl<T> fmt::Debug for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("Full(..)"),
            TrySendError::Closed(_) => f.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => f.write_str("channel full"),
            TrySendError::Closed(_) => f.write_str("channel closed"),
        }
    }
}

impl<T> std::error::Error for TrySendError<T> {}

/// Error returned by `try_recv`.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TryRecvError {
    /// No message is available right now.
    Empty,
    /// All senders are gone and the queue is drained.
    Disconnected,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("channel empty"),
            TryRecvError::Disconnected => f.write_str("channel disconnected"),
        }
    }
}

impl std::error::Error for TryRecvError {}

struct Chan<T> {
    state: Mutex<State<T>>,
}

struct State<T> {
    queue: VecDeque<T>,
    /// `None` for unbounded channels.
    capacity: Option<usize>,
    rx_waker: Option<Waker>,
    rx_closed: bool,
    num_senders: usize,
    /// Senders waiting for capacity, in arrival order.
    send_waiters: VecDeque<(u64, Waker)>,
    next_waiter_id: u64,
}

impl<T> State<T> {
    /// Removes a blocked sender's entry, returning its waker so the caller
    /// can drop it after releasing the lock.
    fn remove_waiter(&mut self, id: u64) -> Option<Waker> {
        let pos = self.send_waiters.iter().position(|(w, _)| *w == id)?;
        self.send_waiters.remove(pos).map(|(_, w)| w)
    }

    fn has_capacity(&self) -> bool {
        self.capacity.is_none_or(|cap| self.queue.len() < cap)
    }
}

impl<T> Chan<T> {
    fn new(capacity: Option<usize>) -> Arc<Chan<T>> {
        Arc::new(Chan {
            state: Mutex::new(State {
                queue: VecDeque::with_capacity(capacity.unwrap_or(0).min(1024)),
                capacity,
                rx_waker: None,
                rx_closed: false,
                num_senders: 1,
                send_waiters: VecDeque::new(),
                next_waiter_id: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Pushes if there is room. On success returns the receiver waker to wake
    /// (outside the lock).
    fn try_push(&self, value: T) -> Result<Option<Waker>, TrySendError<T>> {
        let mut state = self.lock();
        if state.rx_closed {
            return Err(TrySendError::Closed(value));
        }
        if !state.has_capacity() {
            return Err(TrySendError::Full(value));
        }
        state.queue.push_back(value);
        Ok(state.rx_waker.take())
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut state = self.lock();
        if let Some(value) = state.queue.pop_front() {
            let sender = state.send_waiters.pop_front().map(|(_, w)| w);
            drop(state);
            if let Some(w) = sender {
                w.wake();
            }
            return Poll::Ready(Some(value));
        }
        if state.num_senders == 0 {
            return Poll::Ready(None);
        }
        let old = match &state.rx_waker {
            Some(w) if w.will_wake(cx.waker()) => None,
            _ => state.rx_waker.replace(cx.waker().clone()),
        };
        // Dropping a waker can drop a task that owns this channel's other
        // half, so never do it under the channel lock.
        drop(state);
        drop(old);
        Poll::Pending
    }

    fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = self.lock();
        match state.queue.pop_front() {
            Some(value) => {
                let sender = state.send_waiters.pop_front().map(|(_, w)| w);
                drop(state);
                if let Some(w) = sender {
                    w.wake();
                }
                Ok(value)
            }
            None if state.num_senders == 0 => Err(TryRecvError::Disconnected),
            None => Err(TryRecvError::Empty),
        }
    }

    fn close_rx(&self) {
        let (queued, waiters) = {
            let mut state = self.lock();
            state.rx_closed = true;
            (
                std::mem::take(&mut state.queue),
                std::mem::take(&mut state.send_waiters),
            )
        };
        // Drop messages and wake blocked senders outside the lock (message
        // destructors may themselves use channels).
        drop(queued);
        for (_, waker) in waiters {
            waker.wake();
        }
    }

    fn add_sender(&self) {
        self.lock().num_senders += 1;
    }

    fn drop_sender(&self) {
        let waker = {
            let mut state = self.lock();
            state.num_senders -= 1;
            if state.num_senders == 0 {
                state.rx_waker.take()
            } else {
                None
            }
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    fn is_closed(&self) -> bool {
        self.lock().rx_closed
    }
}

// ===== bounded =====

/// Creates a bounded channel holding at most `capacity` messages.
///
/// # Panics
/// Panics if `capacity` is zero.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "mpsc channel capacity must be positive");
    let chan = Chan::new(Some(capacity));
    (Sender { chan: chan.clone() }, Receiver { chan })
}

/// The sending half of a bounded channel. Cloneable.
pub struct Sender<T> {
    chan: Arc<Chan<T>>,
}

/// The receiving half of a bounded channel.
pub struct Receiver<T> {
    chan: Arc<Chan<T>>,
}

impl<T> Sender<T> {
    /// Sends a message, waiting for capacity if the channel is full.
    pub fn send(&self, value: T) -> SendFut<'_, T> {
        SendFut {
            chan: &self.chan,
            value: Some(value),
            waiter: None,
        }
    }

    /// Sends without waiting.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if let Some(w) = self.chan.try_push(value)? {
            w.wake();
        }
        Ok(())
    }

    /// Returns `true` if the receiver has been dropped or closed.
    pub fn is_closed(&self) -> bool {
        self.chan.is_closed()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.chan.add_sender();
        Sender {
            chan: self.chan.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.chan.drop_sender();
    }
}

/// Future returned by [`Sender::send`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct SendFut<'a, T> {
    chan: &'a Chan<T>,
    value: Option<T>,
    /// Our id in the senders FIFO while registered.
    waiter: Option<u64>,
}

impl<T> Unpin for SendFut<'_, T> {}

impl<T> Future for SendFut<'_, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let value = this.value.take().expect("SendFut polled after completion");
        let mut state = this.chan.lock();
        if state.rx_closed {
            drop(state);
            this.waiter = None;
            return Poll::Ready(Err(SendError(value)));
        }
        if state.has_capacity() {
            state.queue.push_back(value);
            let own = this.waiter.take().and_then(|id| state.remove_waiter(id));
            let rx = state.rx_waker.take();
            drop(state);
            drop(own);
            if let Some(w) = rx {
                w.wake();
            }
            return Poll::Ready(Ok(()));
        }
        // Full: (re-)register in the senders FIFO, keeping our place if we
        // are already queued.
        let entry = this
            .waiter
            .and_then(|id| state.send_waiters.iter_mut().find(|(w, _)| *w == id));
        let old = match entry {
            Some((_, waker)) if waker.will_wake(cx.waker()) => None,
            Some((_, waker)) => Some(std::mem::replace(waker, cx.waker().clone())),
            None => {
                let id = state.next_waiter_id;
                state.next_waiter_id += 1;
                state.send_waiters.push_back((id, cx.waker().clone()));
                this.waiter = Some(id);
                None
            }
        };
        drop(state);
        drop(old);
        this.value = Some(value);
        Poll::Pending
    }
}

impl<T> Drop for SendFut<'_, T> {
    fn drop(&mut self) {
        let Some(id) = self.waiter else {
            return;
        };
        let mut state = self.chan.lock();
        let own = state.remove_waiter(id);
        let was_queued = own.is_some();
        // We were woken (removed from the FIFO) but never used the freed
        // slot: pass the wake-up on.
        let forward = if !was_queued && state.has_capacity() {
            state.send_waiters.pop_front().map(|(_, w)| w)
        } else {
            None
        };
        drop(state);
        drop(own);
        if let Some(w) = forward {
            w.wake();
        }
    }
}

impl<T> Receiver<T> {
    /// Receives the next message, or `None` once all senders are gone and the
    /// queue is empty.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.chan.poll_recv(cx)).await
    }

    /// Polls for the next message.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.chan.poll_recv(cx)
    }

    /// Receives without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.chan.try_recv()
    }

    /// Closes the channel: further sends fail; queued messages are dropped.
    pub fn close(&mut self) {
        self.chan.close_rx();
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.chan.close_rx();
    }
}

// ===== unbounded =====

/// Creates an unbounded channel. Sending never waits.
pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let chan = Chan::new(None);
    (
        UnboundedSender { chan: chan.clone() },
        UnboundedReceiver { chan },
    )
}

/// The sending half of an unbounded channel. Cloneable.
pub struct UnboundedSender<T> {
    chan: Arc<Chan<T>>,
}

/// The receiving half of an unbounded channel.
pub struct UnboundedReceiver<T> {
    chan: Arc<Chan<T>>,
}

impl<T> UnboundedSender<T> {
    /// Sends a message.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.chan.try_push(value) {
            Ok(waker) => {
                if let Some(w) = waker {
                    w.wake();
                }
                Ok(())
            }
            Err(TrySendError::Closed(v)) | Err(TrySendError::Full(v)) => Err(SendError(v)),
        }
    }

    /// Returns `true` if the receiver has been dropped or closed.
    pub fn is_closed(&self) -> bool {
        self.chan.is_closed()
    }
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.chan.add_sender();
        UnboundedSender {
            chan: self.chan.clone(),
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        self.chan.drop_sender();
    }
}

impl<T> UnboundedReceiver<T> {
    /// Receives the next message, or `None` once all senders are gone and the
    /// queue is empty.
    pub async fn recv(&mut self) -> Option<T> {
        poll_fn(|cx| self.chan.poll_recv(cx)).await
    }

    /// Polls for the next message.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.chan.poll_recv(cx)
    }

    /// Receives without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.chan.try_recv()
    }

    /// Closes the channel.
    pub fn close(&mut self) {
        self.chan.close_rx();
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.chan.close_rx();
    }
}

macro_rules! debug_impl {
    ($($ty:ident),*) => {$(
        impl<T> fmt::Debug for $ty<T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($ty)).finish_non_exhaustive()
            }
        }
    )*};
}

debug_impl!(Sender, Receiver, UnboundedSender, UnboundedReceiver);

impl<T> fmt::Debug for SendFut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFut").finish_non_exhaustive()
    }
}
