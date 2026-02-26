use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub(crate) struct ResourceManager {
    budget_bytes: u64,
    used_bytes: AtomicU64,
}

impl ResourceManager {
    pub(crate) fn new(budget_bytes: u64, initial_used_bytes: u64) -> Self {
        Self {
            budget_bytes,
            used_bytes: AtomicU64::new(initial_used_bytes),
        }
    }

    pub(crate) fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn try_reserve(&self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        if self.budget_bytes == 0 {
            let _ = self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
            return true;
        }

        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.budget_bytes {
                return false;
            }
            match self.used_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

pub(crate) fn estimated_vector_bytes(dimension: usize) -> u64 {
    (dimension as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
}
