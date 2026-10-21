//! # weft
//!
//! A multi-threaded, work-stealing async runtime written from scratch on top
//! of raw `kqueue`/`epoll` system calls.
//!
//! * [`runtime`]: the scheduler (per-worker lock-free run queues with
//!   steal-half, a global injector, a LIFO slot) and `block_on`.
//! * [`task`]: single-allocation tasks with a lock-free state machine,
//!   [`JoinHandle`]s, cancellation and [`spawn_blocking`].
//! * [`net`] / [`io`]: non-blocking TCP and poll-based read/write traits.
//! * [`time`]: `sleep`/`timeout` backed by a hierarchical timing wheel.
//! * [`sync`]: oneshot and mpsc channels.
//! * [`sys`]: the `Poller` abstraction over kqueue and epoll.
//!
//! ```
//! use std::time::Duration;
//!
//! let rt = weft::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let (tx, rx) = weft::sync::oneshot::channel();
//!     weft::spawn(async move {
//!         weft::time::sleep(Duration::from_millis(5)).await;
//!         tx.send("hello").unwrap();
//!     });
//!     assert_eq!(rx.await.unwrap(), "hello");
//! });
//! ```

#![warn(missing_docs, missing_debug_implementations)]

pub mod io;
pub mod net;
pub mod runtime;
pub mod sync;
pub mod sys;
pub mod task;
pub mod time;

mod driver;
mod loom;
mod util;

#[cfg(all(test, loom))]
mod loom_tests;

pub use runtime::{Builder, Handle, Runtime};
pub use task::{JoinHandle, spawn, spawn_blocking};
