//! A tiny xorshift PRNG used to randomise steal order.

use std::cell::Cell;

/// xorshift64* generator. Not cryptographic; only used to spread work-stealing
/// victims so that idle workers don't all hammer the same queue.
#[derive(Debug)]
pub(crate) struct FastRand {
    state: Cell<u64>,
}

impl FastRand {
    pub(crate) fn new(seed: u64) -> FastRand {
        // The state must never be zero.
        FastRand {
            state: Cell::new(seed | 1),
        }
    }

    pub(crate) fn next_u64(&self) -> u64 {
        let mut x = self.state.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Returns a value in `0..n` (n > 0) using Lemire's multiply-shift.
    pub(crate) fn below(&self, n: usize) -> usize {
        ((self.next_u64() >> 32).wrapping_mul(n as u64) >> 32) as usize
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::FastRand;

    #[test]
    fn below_is_in_range_and_covers_all_values() {
        let rng = FastRand::new(42);
        let mut seen = [false; 7];
        for _ in 0..10_000 {
            let v = rng.below(7);
            assert!(v < 7);
            seen[v] = true;
        }
        assert!(seen.iter().all(|&s| s));
    }
}
