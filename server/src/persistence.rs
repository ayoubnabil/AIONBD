use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use aionbd_core::{
    append_wal_record_with_sync, checkpoint_wal_with_policy, CheckpointPolicy, PersistOutcome,
    PersistenceError, WalRecord,
};
use tokio::task;

use crate::errors::ApiError;
use crate::index_manager::{clear_l2_build_tracking, remove_l2_index_entry};
use crate::state::AppState;

const CHECKPOINT_COMPACT_AFTER: usize = 64;
const ASYNC_CHECKPOINTS_DEFAULT: bool = false;
const WAL_SYNC_EVERY_N_DEFAULT: u64 = 0;

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
    let sync_every_n_writes = configured_wal_sync_every_n_writes();
    if let Err(error) = run_serialized_persistence_io(state, move || {
        append_wal_record_with_sync(
            &wal_path,
            &wal_record,
            should_sync_this_write(sync_on_write, sync_every_n_writes, next_wal_sync_sequence()),
        )
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
    if is_checkpoint_due(writes_since_start, state.config.checkpoint_interval) {
        if configured_async_checkpoints() {
            schedule_checkpoint_if_needed(state);
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

fn schedule_checkpoint_if_needed(state: &AppState) {
    if state
        .persistence_checkpoint_in_flight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return;
    }

    let state = state.clone();
    tokio::spawn(async move {
        run_checkpoint(state.clone()).await;
        state
            .persistence_checkpoint_in_flight
            .store(false, Ordering::Release);
    });
}

fn configured_async_checkpoints() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_ASYNC_CHECKPOINTS") else {
            return ASYNC_CHECKPOINTS_DEFAULT;
        };
        match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => {
                tracing::warn!(
                    %raw,
                    default = ASYNC_CHECKPOINTS_DEFAULT,
                    "invalid AIONBD_ASYNC_CHECKPOINTS; using default"
                );
                ASYNC_CHECKPOINTS_DEFAULT
            }
        }
    })
}

pub(crate) fn configured_wal_sync_every_n_writes() -> u64 {
    static EVERY_N_WRITES: OnceLock<u64> = OnceLock::new();
    *EVERY_N_WRITES.get_or_init(|| {
        let Ok(raw) = std::env::var("AIONBD_WAL_SYNC_EVERY_N_WRITES") else {
            return WAL_SYNC_EVERY_N_DEFAULT;
        };
        match raw.parse::<u64>() {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(
                    %raw,
                    %error,
                    default = WAL_SYNC_EVERY_N_DEFAULT,
                    "invalid AIONBD_WAL_SYNC_EVERY_N_WRITES; using default"
                );
                WAL_SYNC_EVERY_N_DEFAULT
            }
        }
    })
}

fn next_wal_sync_sequence() -> u64 {
    static WAL_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    WAL_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
}

fn should_sync_this_write(sync_on_write: bool, sync_every_n_writes: u64, write_seq: u64) -> bool {
    if sync_on_write {
        return true;
    }
    if sync_every_n_writes == 0 {
        return false;
    }
    write_seq.checked_rem(sync_every_n_writes) == Some(0)
}

async fn run_checkpoint(state: AppState) {
    let snapshot_path = state.config.snapshot_path.clone();
    let wal_path = state.config.wal_path.clone();
    let checkpoint_policy = CheckpointPolicy {
        incremental_compact_after: CHECKPOINT_COMPACT_AFTER,
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

#[cfg(test)]
mod tests {
    use super::should_sync_this_write;

    #[test]
    fn wal_sync_policy_prefers_explicit_sync_on_write() {
        assert!(should_sync_this_write(true, 0, 1));
        assert!(should_sync_this_write(true, 32, 7));
    }

    #[test]
    fn wal_sync_policy_supports_periodic_sync_when_disabled() {
        assert!(!should_sync_this_write(false, 0, 1));
        assert!(!should_sync_this_write(false, 3, 1));
        assert!(!should_sync_this_write(false, 3, 2));
        assert!(should_sync_this_write(false, 3, 3));
        assert!(should_sync_this_write(false, 3, 6));
    }
}
