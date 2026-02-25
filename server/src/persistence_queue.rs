use std::collections::VecDeque;

use aionbd_core::WalRecord;
use tokio::sync::{oneshot, Mutex};

pub(crate) type WalAppendResult = Result<(), String>;

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
        batch
    }

    pub(crate) async fn pending_len(&self) -> usize {
        self.state.lock().await.queue.len()
    }

    pub(crate) fn pending_len_blocking(&self) -> usize {
        self.state.blocking_lock().queue.len()
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
