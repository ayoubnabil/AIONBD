use std::collections::BTreeMap;

use aionbd_core::{persist_change, Collection, PersistOutcome, WalRecord};

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

    match persist_change(
        &state.config.snapshot_path,
        &state.config.wal_path,
        collections,
        record,
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
