use std::sync::atomic::Ordering;
use std::sync::Arc;

use aionbd_core::{
    append_wal_record_with_sync, append_wal_records_with_sync, checkpoint_wal_with_policy,
    CheckpointPolicy, PersistOutcome, PersistenceError, WalRecord,
};
use tokio::task;

use crate::errors::ApiError;
use crate::index_manager::{clear_l2_build_tracking, remove_l2_index_entry};
use crate::persistence_queue::{PendingWalWrite, WalAppendResult};
use crate::state::AppState;

mod settings;
use settings::{
    batch_should_sync, next_wal_sync_sequence, next_wal_sync_sequence_batch_start,
    should_sync_this_write,
};
pub(crate) use settings::{
    configured_async_checkpoints, configured_checkpoint_compact_after,
    configured_wal_group_commit_max_batch, configured_wal_sync_every_n_writes,
};

pub(crate) async fn persist_change_if_enabled(
    state: &AppState,
    record: &WalRecord,
) -> Result<(), ApiError> {
    if !state.config.persistence_enabled {
        invalidate_cached_l2_index(state, record);
        return Ok(());
    }

    if let Err(error) = append_wal_record_with_config(state, record.clone()).await {
        state.storage_available.store(false, Ordering::Relaxed);
        tracing::error!(error = %error, "failed to append wal record");
        return Err(ApiError::internal("failed to persist state"));
    }
    invalidate_cached_l2_index(state, record);

    let writes_since_start = state
        .metrics
        .persistence_writes
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    if is_checkpoint_due(writes_since_start, state.config.checkpoint_interval) {
        if configured_async_checkpoints() {
            if !schedule_checkpoint_if_needed(state) {
                let _ = state
                    .metrics
                    .persistence_checkpoint_schedule_skips_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        } else {
            run_checkpoint(state.clone()).await;
        }
    }
    Ok(())
}

async fn run_serialized_persistence_io<T>(
    state: &AppState,
    operation: impl FnOnce() -> Result<T, PersistenceError> + Send + 'static,
) -> Result<T, PersistenceError>
where
    T: Send + 'static,
{
    let _guard = Arc::clone(&state.persistence_io_serial)
        .acquire_owned()
        .await
        .map_err(|_| {
            PersistenceError::InvalidData("persistence io serial semaphore closed".to_string())
        })?;
    run_persistence_io(operation).await
}

async fn append_wal_record_with_config(state: &AppState, record: WalRecord) -> Result<(), String> {
    let sync_on_write = state.config.wal_sync_on_write;
    let sync_every_n_writes = configured_wal_sync_every_n_writes();
    let max_batch = configured_wal_group_commit_max_batch();

    if max_batch <= 1 {
        let wal_path = state.config.wal_path.clone();
        return run_serialized_persistence_io(state, move || {
            append_wal_record_with_sync(
                &wal_path,
                &record,
                should_sync_this_write(
                    sync_on_write,
                    sync_every_n_writes,
                    next_wal_sync_sequence(),
                ),
            )
        })
        .await
        .map_err(|error| error.to_string());
    }

    append_wal_record_grouped(state, record, sync_on_write, sync_every_n_writes, max_batch).await
}

async fn append_wal_record_grouped(
    state: &AppState,
    record: WalRecord,
    sync_on_write: bool,
    sync_every_n_writes: u64,
    max_batch: usize,
) -> Result<(), String> {
    let (is_leader, response) = state.wal_group_queue.enqueue(record).await;
    if is_leader {
        run_wal_group_leader(state, sync_on_write, sync_every_n_writes, max_batch).await;
    }
    response
        .await
        .map_err(|_| "wal group commit response channel closed unexpectedly".to_string())?
}

async fn run_wal_group_leader(
    state: &AppState,
    sync_on_write: bool,
    sync_every_n_writes: u64,
    max_batch: usize,
) {
    loop {
        let pending = state
            .wal_group_queue
            .take_batch_or_release_leader(max_batch)
            .await;
        if pending.is_empty() {
            break;
        }

        let records_count = pending.len() as u64;
        let (records, responses) = split_pending_writes(pending);
        let seq_start = next_wal_sync_sequence_batch_start(records_count);
        let should_sync =
            batch_should_sync(sync_on_write, sync_every_n_writes, seq_start, records_count);
        let wal_path = state.config.wal_path.clone();
        let result: WalAppendResult = run_serialized_persistence_io(state, move || {
            append_wal_records_with_sync(&wal_path, &records, should_sync)
        })
        .await
        .map_err(|error| error.to_string());

        let _ = state
            .metrics
            .persistence_wal_group_commits_total
            .fetch_add(1, Ordering::Relaxed);
        let _ = state
            .metrics
            .persistence_wal_grouped_records_total
            .fetch_add(records_count, Ordering::Relaxed);

        for response in responses {
            let _ = response.send(result.clone());
        }
    }
}

fn split_pending_writes(
    pending: Vec<PendingWalWrite>,
) -> (
    Vec<WalRecord>,
    Vec<tokio::sync::oneshot::Sender<WalAppendResult>>,
) {
    let mut records = Vec::with_capacity(pending.len());
    let mut responses = Vec::with_capacity(pending.len());
    for item in pending {
        records.push(item.record);
        responses.push(item.response);
    }
    (records, responses)
}

fn invalidate_cached_l2_index(state: &AppState, record: &WalRecord) {
    match record {
        WalRecord::CreateCollection { name, .. } | WalRecord::DeleteCollection { name } => {
            remove_l2_index_entry(state, name);
            clear_l2_build_tracking(state, name);
        }
        WalRecord::UpsertPoint { collection, .. } | WalRecord::DeletePoint { collection, .. } => {
            remove_l2_index_entry(state, collection);
        }
    }
}

fn schedule_checkpoint_if_needed(state: &AppState) -> bool {
    if state
        .persistence_checkpoint_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }

    let state = state.clone();
    tokio::spawn(async move {
        run_checkpoint(state.clone()).await;
        state
            .persistence_checkpoint_in_flight
            .store(false, Ordering::Release);
    });
    true
}

async fn run_checkpoint(state: AppState) {
    let snapshot_path = state.config.snapshot_path.clone();
    let wal_path = state.config.wal_path.clone();
    let checkpoint_policy = CheckpointPolicy {
        incremental_compact_after: configured_checkpoint_compact_after(),
    };
    match run_serialized_persistence_io(&state, move || {
        checkpoint_wal_with_policy(&snapshot_path, &wal_path, checkpoint_policy)
            .map(|_| PersistOutcome::Checkpointed)
            .or_else(|error| {
                Ok(PersistOutcome::WalOnly {
                    reason: error.to_string(),
                })
            })
    })
    .await
    {
        Ok(PersistOutcome::Checkpointed) => {
            state.storage_available.store(true, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_success_total
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(PersistOutcome::WalOnly { reason }) => {
            state.storage_available.store(false, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_degraded_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%reason, "snapshot checkpoint skipped, relying on wal replay");
        }
        Err(error) => {
            state.storage_available.store(false, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_error_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(%error, "failed to persist state");
        }
    }
}

async fn run_persistence_io<T>(
    operation: impl FnOnce() -> Result<T, PersistenceError> + Send + 'static,
) -> Result<T, PersistenceError>
where
    T: Send + 'static,
{
    task::spawn_blocking(operation).await.map_err(|error| {
        PersistenceError::InvalidData(format!("persistence background task failed: {error}"))
    })?
}

fn is_checkpoint_due(writes_since_start: u64, checkpoint_interval: usize) -> bool {
    writes_since_start.checked_rem(checkpoint_interval as u64) == Some(0)
}
