use std::sync::atomic::Ordering;
use std::sync::Arc;

use aionbd_core::{
    append_wal_record_with_sync_info, append_wal_records_with_sync_info,
    checkpoint_wal_with_policy, CheckpointPolicy, PersistOutcome, PersistenceError, WalRecord,
};
use tokio::task;
use tokio::time::Duration;

use crate::errors::ApiError;
use crate::index_manager::{clear_l2_build_tracking, remove_l2_index_entry};
use crate::persistence_backlog;
use crate::persistence_queue::{PendingWalWrite, WalAppendResult, WalAppendState};
use crate::state::AppState;

mod settings;
use settings::{
    batch_should_sync, next_wal_sync_sequence, next_wal_sync_sequence_batch_start,
    record_successful_sync, should_sync_this_write, sync_due_by_interval, unix_seconds_now,
};

pub(crate) async fn persist_change_if_enabled(
    state: &AppState,
    record: &WalRecord,
) -> Result<(), ApiError> {
    if !state.config.persistence_enabled {
        invalidate_cached_l2_index(state, record);
        return Ok(());
    }

    let wal_append = append_wal_record_with_config(state, record.clone()).await;
    let wal_state = match wal_append {
        Ok(state) => state,
        Err(error) => {
            state.storage_available.store(false, Ordering::Relaxed);
            tracing::error!(error = %error, "failed to append wal record");
            return Err(ApiError::internal("failed to persist state"));
        }
    };
    persistence_backlog::apply_wal_state(state, wal_state.wal_size_bytes, wal_state.wal_tail_open);
    invalidate_cached_l2_index(state, record);
    on_records_persisted(state, 1).await;
    Ok(())
}

pub(crate) async fn persist_change_owned_if_enabled(
    state: &AppState,
    record: WalRecord,
) -> Result<WalRecord, ApiError> {
    let mut records = persist_changes_if_enabled(state, vec![record]).await?;
    match records.pop() {
        Some(record) => Ok(record),
        None => Err(ApiError::internal("failed to persist state")),
    }
}

pub(crate) async fn persist_changes_if_enabled(
    state: &AppState,
    records: Vec<WalRecord>,
) -> Result<Vec<WalRecord>, ApiError> {
    if records.is_empty() {
        return Ok(records);
    }
    if !state.config.persistence_enabled {
        for record in &records {
            invalidate_cached_l2_index(state, record);
        }
        return Ok(records);
    }

    let wal_append = append_wal_records_with_config(state, records).await;
    let (wal_state, records) = match wal_append {
        Ok(result) => result,
        Err(error) => {
            state.storage_available.store(false, Ordering::Relaxed);
            tracing::error!(error = %error, "failed to append wal records");
            return Err(ApiError::internal("failed to persist state"));
        }
    };
    persistence_backlog::apply_wal_state(state, wal_state.wal_size_bytes, wal_state.wal_tail_open);
    for record in &records {
        invalidate_cached_l2_index(state, record);
    }
    on_records_persisted(state, records.len().min(u64::MAX as usize) as u64).await;
    Ok(records)
}

async fn on_records_persisted(state: &AppState, writes_added: u64) {
    let writes_before = state
        .metrics
        .persistence_writes
        .fetch_add(writes_added, Ordering::Relaxed);
    if !checkpoint_due_in_range(
        writes_before,
        writes_added,
        state.config.checkpoint_interval,
    ) {
        return;
    }
    if state.config.async_checkpoints {
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

async fn append_wal_record_with_config(
    state: &AppState,
    record: WalRecord,
) -> Result<WalAppendState, String> {
    let sync_on_write = state.config.wal_sync_on_write;
    let sync_every_n_writes = state.config.wal_sync_every_n_writes;
    let sync_interval_seconds = state.config.wal_sync_interval_seconds;
    let max_batch = state.config.wal_group_commit_max_batch;
    let flush_delay_ms = state.config.wal_group_commit_flush_delay_ms;

    if max_batch <= 1 {
        let wal_path = state.config.wal_path.clone();
        let write_seq = next_wal_sync_sequence();
        let now_seconds = unix_seconds_now();
        let should_sync = should_sync_this_write(sync_on_write, sync_every_n_writes, write_seq)
            || sync_due_by_interval(sync_interval_seconds, now_seconds);
        return run_serialized_persistence_io(state, move || {
            let wal_info = append_wal_record_with_sync_info(&wal_path, &record, should_sync)?;
            if should_sync {
                record_successful_sync(now_seconds);
            }
            Ok(WalAppendState {
                wal_size_bytes: wal_info.wal_size_bytes,
                wal_tail_open: wal_info.wal_tail_open,
            })
        })
        .await
        .map_err(|error| error.to_string());
    }

    append_wal_record_grouped(
        state,
        record,
        sync_on_write,
        sync_every_n_writes,
        sync_interval_seconds,
        max_batch,
        flush_delay_ms,
    )
    .await
}

async fn append_wal_records_with_config(
    state: &AppState,
    records: Vec<WalRecord>,
) -> Result<(WalAppendState, Vec<WalRecord>), String> {
    let sync_on_write = state.config.wal_sync_on_write;
    let sync_every_n_writes = state.config.wal_sync_every_n_writes;
    let sync_interval_seconds = state.config.wal_sync_interval_seconds;
    let records_count = records.len().min(u64::MAX as usize) as u64;
    let seq_start = next_wal_sync_sequence_batch_start(records_count);
    let now_seconds = unix_seconds_now();
    let should_sync =
        batch_should_sync(sync_on_write, sync_every_n_writes, seq_start, records_count)
            || sync_due_by_interval(sync_interval_seconds, now_seconds);
    let wal_path = state.config.wal_path.clone();

    run_serialized_persistence_io(state, move || {
        let wal_info = append_wal_records_with_sync_info(&wal_path, &records, should_sync)?;
        if should_sync {
            record_successful_sync(now_seconds);
        }
        Ok((
            WalAppendState {
                wal_size_bytes: wal_info.wal_size_bytes,
                wal_tail_open: wal_info.wal_tail_open,
            },
            records,
        ))
    })
    .await
    .map_err(|error| error.to_string())
}

async fn append_wal_record_grouped(
    state: &AppState,
    record: WalRecord,
    sync_on_write: bool,
    sync_every_n_writes: u64,
    sync_interval_seconds: u64,
    max_batch: usize,
    flush_delay_ms: u64,
) -> Result<WalAppendState, String> {
    let (is_leader, response) = state.wal_group_queue.enqueue(record).await;
    if is_leader {
        run_wal_group_leader(
            state,
            sync_on_write,
            sync_every_n_writes,
            sync_interval_seconds,
            max_batch,
            flush_delay_ms,
        )
        .await;
    }
    response
        .await
        .map_err(|_| "wal group commit response channel closed unexpectedly".to_string())?
}

async fn run_wal_group_leader(
    state: &AppState,
    sync_on_write: bool,
    sync_every_n_writes: u64,
    sync_interval_seconds: u64,
    max_batch: usize,
    flush_delay_ms: u64,
) {
    loop {
        if flush_delay_ms > 0 {
            let pending_len = state.wal_group_queue.pending_len();
            if pending_len > 0 && pending_len < max_batch {
                tokio::time::sleep(Duration::from_millis(flush_delay_ms)).await;
            }
        }
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
        let now_seconds = unix_seconds_now();
        let should_sync =
            batch_should_sync(sync_on_write, sync_every_n_writes, seq_start, records_count)
                || sync_due_by_interval(sync_interval_seconds, now_seconds);
        let wal_path = state.config.wal_path.clone();
        let result: WalAppendResult = run_serialized_persistence_io(state, move || {
            let wal_info = append_wal_records_with_sync_info(&wal_path, &records, should_sync)?;
            if should_sync {
                record_successful_sync(now_seconds);
            }
            Ok(WalAppendState {
                wal_size_bytes: wal_info.wal_size_bytes,
                wal_tail_open: wal_info.wal_tail_open,
            })
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
        incremental_compact_after: state.config.checkpoint_compact_after,
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
            persistence_backlog::refresh_full_scan(&state);
            state.storage_available.store(true, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_success_total
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(PersistOutcome::WalOnly { reason }) => {
            persistence_backlog::refresh_full_scan(&state);
            state.storage_available.store(false, Ordering::Relaxed);
            let _ = state
                .metrics
                .persistence_checkpoint_degraded_total
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%reason, "snapshot checkpoint skipped, relying on wal replay");
        }
        Err(error) => {
            persistence_backlog::refresh_full_scan(&state);
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

fn checkpoint_due_in_range(
    writes_before: u64,
    writes_added: u64,
    checkpoint_interval: usize,
) -> bool {
    if writes_added == 0 {
        return false;
    }
    let interval = checkpoint_interval as u64;
    let writes_after = writes_before.saturating_add(writes_added);
    (writes_before / interval) != (writes_after / interval)
}
