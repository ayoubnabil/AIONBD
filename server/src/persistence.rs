use std::sync::atomic::Ordering;
use std::sync::Arc;

use aionbd_core::{
    append_wal_record_with_sync, checkpoint_wal_with_policy, CheckpointPolicy, PersistOutcome,
    PersistenceError, WalRecord,
};
use tokio::task;

use crate::errors::ApiError;
use crate::index_manager::{clear_l2_build_tracking, remove_l2_index_entry};
use crate::state::AppState;

const CHECKPOINT_COMPACT_AFTER: usize = 64;

pub(crate) async fn persist_change_if_enabled(
    state: &AppState,
    record: &WalRecord,
) -> Result<(), ApiError> {
    if !state.config.persistence_enabled {
        invalidate_cached_l2_index(state, record);
        return Ok(());
    }

    let wal_path = state.config.wal_path.clone();
    let wal_record = record.clone();
    let sync_on_write = state.config.wal_sync_on_write;
    if let Err(error) = run_serialized_persistence_io(state, move || {
        append_wal_record_with_sync(&wal_path, &wal_record, sync_on_write)
    })
    .await
    {
        state.storage_available.store(false, Ordering::Relaxed);
        tracing::error!(%error, "failed to append wal record");
        return Err(ApiError::internal("failed to persist state"));
    }
    invalidate_cached_l2_index(state, record);

    let writes_since_start = state
        .metrics
        .persistence_writes
        .fetch_add(1, Ordering::Relaxed)
        + 1;
    if !is_checkpoint_due(writes_since_start, state.config.checkpoint_interval) {
        return Ok(());
    }

    let snapshot_path = state.config.snapshot_path.clone();
    let wal_path = state.config.wal_path.clone();
    let checkpoint_policy = CheckpointPolicy {
        incremental_compact_after: CHECKPOINT_COMPACT_AFTER,
    };
    match run_serialized_persistence_io(state, move || {
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
            Ok(())
        }
        Ok(PersistOutcome::WalOnly { reason }) => {
            state.storage_available.store(false, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_degraded_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%reason, "snapshot checkpoint skipped, relying on wal replay");
            Ok(())
        }
        Err(error) => {
            state.storage_available.store(false, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_error_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::error!(%error, "failed to persist state");
            Err(ApiError::internal("failed to persist state"))
        }
    }
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
