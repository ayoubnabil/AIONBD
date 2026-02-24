use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use aionbd_core::{append_wal_record, checkpoint_snapshot, Collection, PersistOutcome, WalRecord};

use crate::errors::ApiError;
use crate::state::AppState;

pub(crate) fn persist_change_if_enabled(
    state: &AppState,
    collections: &BTreeMap<String, Collection>,
    record: &WalRecord,
) -> Result<(), ApiError> {
    if !state.config.persistence_enabled {
        return Ok(());
    }

    if let Err(error) = append_wal_record(&state.config.wal_path, record) {
        tracing::error!(%error, "failed to append wal record");
        return Err(ApiError::internal("failed to persist state"));
    }

    let writes_since_start = state.persistence_writes.fetch_add(1, Ordering::Relaxed) + 1;
    if !writes_since_start.is_multiple_of(state.config.checkpoint_interval as u64) {
        return Ok(());
    }

    match checkpoint_snapshot(
        &state.config.snapshot_path,
        &state.config.wal_path,
        collections,
    ) {
        Ok(PersistOutcome::Checkpointed) => Ok(()),
        Ok(PersistOutcome::WalOnly { reason }) => {
            tracing::warn!(%reason, "snapshot checkpoint skipped, relying on wal replay");
            Ok(())
        }
        Err(error) => {
            tracing::error!(%error, "failed to persist state");
            Err(ApiError::internal("failed to persist state"))
        }
    }
}
