//! Asynchronous TCP networking.
//!
//! Sockets are put in non-blocking mode and registered with the driver once,
//! in edge-triggered mode, for both directions. Each operation is attempted
//! optimistically; only when the kernel reports `EWOULDBLOCK` does the task
//! park until the driver observes a readiness edge.

mod tcp;

pub use tcp::{OwnedReadHalf, OwnedWriteHalf, TcpListener, TcpStream};
