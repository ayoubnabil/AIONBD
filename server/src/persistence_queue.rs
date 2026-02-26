use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

use aionbd_core::WalRecord;
use tokio::sync::{oneshot, Mutex};

#[derive(Clone, Copy, Debug)]
pub(crate) struct WalAppendState {
    pub(crate) wal_size_bytes: u64,
    pub(crate) wal_tail_open: bool,
}

pub(crate) type WalAppendResult = Result<WalAppendState, String>;

pub(crate) struct PendingWalWrite {
    pub(crate) record: WalRecord,
    pub(crate) response: oneshot::Sender<WalAppendResult>,
}

#[derive(Default)]
struct WalGroupQueueState {
    processing: bool,
    queue: VecDeque<PendingWalWrite>,
}

#[derive(Default)]
pub(crate) struct WalGroupQueue {
    state: Mutex<WalGroupQueueState>,
    pending: AtomicUsize,
}

impl WalGroupQueue {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn enqueue(
        &self,
        record: WalRecord,
    ) -> (bool, oneshot::Receiver<WalAppendResult>) {
        let (response_tx, response_rx) = oneshot::channel();
        let mut state = self.state.lock().await;
        state.queue.push_back(PendingWalWrite {
            record,
            response: response_tx,
        });
        let _ = self.pending.fetch_add(1, Ordering::Relaxed);
        if state.processing {
            return (false, response_rx);
        }
        state.processing = true;
        (true, response_rx)
    }

    pub(crate) async fn take_batch_or_release_leader(
        &self,
        max_batch: usize,
    ) -> Vec<PendingWalWrite> {
        let mut state = self.state.lock().await;
        if state.queue.is_empty() {
            state.processing = false;
            return Vec::new();
        }

        let mut batch = Vec::with_capacity(max_batch.max(1));
        for _ in 0..max_batch.max(1) {
            if let Some(item) = state.queue.pop_front() {
                batch.push(item);
            } else {
                break;
            }
        }
        let drained = batch.len();
        if drained > 0 {
            let _ = self
                .pending
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(drained))
                });
        }
        batch
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::WalGroupQueue;
    use aionbd_core::WalRecord;

    fn upsert_record(id: u64) -> WalRecord {
        WalRecord::UpsertPoint {
            collection: "demo".to_string(),
            id,
            values: vec![1.0, 2.0],
            payload: None,
        }
    }

    #[tokio::test]
    async fn queue_assigns_single_leader_until_released() {
        let queue = WalGroupQueue::new();
        let (leader1, _) = queue.enqueue(upsert_record(1)).await;
        assert!(leader1);

        let (leader2, _) = queue.enqueue(upsert_record(2)).await;
        assert!(!leader2);

        let first_batch = queue.take_batch_or_release_leader(8).await;
        assert_eq!(first_batch.len(), 2);
        assert!(queue.take_batch_or_release_leader(8).await.is_empty());

        let (leader3, _) = queue.enqueue(upsert_record(3)).await;
        assert!(leader3);
    }
}
