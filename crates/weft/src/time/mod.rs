//! Timers: [`sleep`], [`sleep_until`] and [`timeout`].
//!
//! Timers are stored in a hierarchical timing wheel (`time/wheel.rs`) owned by the
//! driver, with 1 ms resolution. Deadlines are rounded *up* to the next tick,
//! so a timer never fires early. The worker that drives I/O uses the wheel's
//! next expiration as its poll timeout, so timers cost no extra threads.

pub(crate) mod wheel;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

pub use std::time::Instant;

use crate::driver;
use crate::runtime::context;
use wheel::TimerKey;

/// Waits until `duration` has elapsed.
///
/// # Panics
/// Polling the returned future panics outside a weft runtime.
///
/// ```
/// use std::time::{Duration, Instant};
/// let rt = weft::Runtime::new().unwrap();
/// rt.block_on(async {
///     let start = Instant::now();
///     weft::time::sleep(Duration::from_millis(20)).await;
///     assert!(start.elapsed() >= Duration::from_millis(20));
/// });
/// ```
pub fn sleep(duration: Duration) -> Sleep {
    let deadline = Instant::now()
        .checked_add(duration)
        .unwrap_or_else(far_future);
    sleep_until(deadline)
}

/// Waits until `deadline` is reached.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep {
        deadline,
        handle: None,
        key: None,
        waker: None,
    }
}

fn far_future() -> Instant {
    // Roughly 30 years; the wheel clamps anything beyond ~2 years anyway and
    // re-arms when the clamped deadline is reached.
    Instant::now() + Duration::from_secs(86_400 * 365 * 30)
}

/// Future returned by [`sleep`] and [`sleep_until`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Sleep {
    deadline: Instant,
    handle: Option<driver::Handle>,
    key: Option<TimerKey>,
    /// The waker currently stored in the wheel. Re-polling from the same task
    /// (the common case) can then skip the wheel's lock entirely.
    waker: Option<Waker>,
}

impl Sleep {
    /// The instant at which this future completes.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns `true` if the deadline has been reached.
    pub fn is_elapsed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Changes the deadline, re-arming the timer. Useful for idle timeouts
    /// that are pushed back on every activity.
    pub fn reset(&mut self, deadline: Instant) {
        self.cancel();
        self.deadline = deadline;
    }

    fn cancel(&mut self) {
        self.waker = None;
        if let (Some(key), Some(handle)) = (self.key.take(), &self.handle) {
            handle.remove_timer(key);
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        if Instant::now() >= this.deadline {
            this.cancel();
            return Poll::Ready(());
        }
        if this.key.is_some() && this.waker.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
            // Already registered with an equivalent waker: nothing to do.
            return Poll::Pending;
        }
        let handle = this.handle.get_or_insert_with(context::driver_handle);
        if let Some(key) = this.key {
            if handle.update_timer(key, cx.waker()) {
                this.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            // Fired, but the clock disagrees (cannot happen with monotonic
            // rounding, handled defensively): re-arm below.
            this.key = None;
        }
        match handle.insert_timer(this.deadline, cx.waker().clone()) {
            Ok(key) => {
                this.key = Some(key);
                this.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            // The wheel has already advanced past the deadline, which implies
            // the deadline has passed.
            Err(_) => Poll::Ready(()),
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl fmt::Debug for Sleep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sleep")
            .field("deadline", &self.deadline)
            .field("registered", &self.key.is_some())
            .finish()
    }
}

/// Error returned by [`timeout`] when the deadline elapses first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed(());

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for std::io::Error {
    fn from(_: Elapsed) -> std::io::Error {
        std::io::ErrorKind::TimedOut.into()
    }
}

/// Requires `future` to complete within `duration`.
///
/// The inner future is polled first, so a future that is ready at the same
/// time as the deadline wins.
///
/// ```
/// use std::time::Duration;
/// let rt = weft::Runtime::new().unwrap();
/// rt.block_on(async {
///     let slow = weft::time::sleep(Duration::from_secs(10));
///     let res = weft::time::timeout(Duration::from_millis(10), slow).await;
///     assert!(res.is_err());
/// });
/// ```
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep(duration),
    }
}

/// Like [`timeout`], with an absolute deadline.
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout {
        future,
        sleep: sleep_until(deadline),
    }
}

/// Future returned by [`timeout`].
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Timeout<F> {
    future: F,
    sleep: Sleep,
}

impl<F> Timeout<F> {
    /// Consumes the timeout, returning the inner future.
    pub fn into_inner(self) -> F {
        self.future
    }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: `future` is structurally pinned: it is never moved out of a
        // pinned `Timeout` (no `Unpin`-independent accessors hand out `&mut F`,
        // and `into_inner` takes `self` by value, which requires `Unpin` once
        // pinned). `sleep` is `Unpin` and treated as unpinned.
        let this = unsafe { self.get_unchecked_mut() };
        // SAFETY: see above.
        let future = unsafe { Pin::new_unchecked(&mut this.future) };
        if let Poll::Ready(out) = future.poll(cx) {
            return Poll::Ready(Ok(out));
        }
        match Pin::new(&mut this.sleep).poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Elapsed(()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<F> fmt::Debug for Timeout<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Timeout")
            .field("deadline", &self.sleep.deadline)
            .finish_non_exhaustive()
    }
}
