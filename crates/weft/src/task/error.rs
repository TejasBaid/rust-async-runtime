use std::any::Any;
use std::fmt;

use super::Id;

/// Error returned by awaiting a [`JoinHandle`](super::JoinHandle) whose task
/// did not run to completion.
pub struct JoinError {
    id: Id,
    repr: Repr,
}

enum Repr {
    Cancelled,
    Panic(Box<dyn Any + Send + 'static>),
}

impl JoinError {
    pub(crate) fn cancelled(id: Id) -> JoinError {
        JoinError {
            id,
            repr: Repr::Cancelled,
        }
    }

    pub(crate) fn panic(id: Id, payload: Box<dyn Any + Send + 'static>) -> JoinError {
        JoinError {
            id,
            repr: Repr::Panic(payload),
        }
    }

    /// The task was cancelled (aborted or the runtime shut down).
    pub fn is_cancelled(&self) -> bool {
        matches!(self.repr, Repr::Cancelled)
    }

    /// The task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self.repr, Repr::Panic(_))
    }

    /// Returns the panic payload. Panics if the task was cancelled instead.
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        match self.repr {
            Repr::Panic(p) => p,
            Repr::Cancelled => panic!("`JoinError::into_panic` called on a cancelled task"),
        }
    }

    /// The id of the task that failed.
    pub fn id(&self) -> Id {
        self.id
    }

    fn panic_message(&self) -> Option<&str> {
        match &self.repr {
            Repr::Panic(p) => p
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| p.downcast_ref::<String>().map(String::as_str)),
            Repr::Cancelled => None,
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => write!(f, "task {} was cancelled", self.id),
            Repr::Panic(_) => match self.panic_message() {
                Some(msg) => write!(f, "task {} panicked with message {msg:?}", self.id),
                None => write!(f, "task {} panicked", self.id),
            },
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            Repr::Cancelled => write!(f, "JoinError::Cancelled({:?})", self.id),
            Repr::Panic(_) => write!(
                f,
                "JoinError::Panic({:?}, {:?})",
                self.id,
                self.panic_message()
            ),
        }
    }
}

impl std::error::Error for JoinError {}

impl From<JoinError> for std::io::Error {
    fn from(err: JoinError) -> std::io::Error {
        std::io::Error::other(err.to_string())
    }
}
