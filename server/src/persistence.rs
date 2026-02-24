use std::collections::BTreeMap;

use aionbd_core::{persist_change, Collection, WalRecord};

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

    persist_change(
        &state.config.snapshot_path,
        &state.config.wal_path,
        collections,
        record,
    )
    .map_err(|error| {
        tracing::error!(%error, "failed to persist state");
        ApiError::internal("failed to persist state")
    })
}
