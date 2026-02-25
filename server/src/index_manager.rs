use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use aionbd_core::Collection;

use crate::ivf_index::IvfIndex;
use crate::state::AppState;

const L2_BUILD_COOLDOWN_MS: u64 = 1_000;

pub(crate) fn record_l2_lookup_hit(state: &AppState) {
    let _ = state
        .metrics
        .l2_index_cache_lookups
        .fetch_add(1, Ordering::Relaxed);
    let _ = state
        .metrics
        .l2_index_cache_hits
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_l2_lookup_miss(state: &AppState) {
    let _ = state
        .metrics
        .l2_index_cache_lookups
        .fetch_add(1, Ordering::Relaxed);
    let _ = state
        .metrics
        .l2_index_cache_misses
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn l2_cache_hit_ratio(state: &AppState) -> f64 {
    let lookups = state.metrics.l2_index_cache_lookups.load(Ordering::Relaxed);
    if lookups == 0 {
        return 0.0;
    }
    let hits = state.metrics.l2_index_cache_hits.load(Ordering::Relaxed);
    hits as f64 / lookups as f64
}

pub(crate) fn l2_build_in_flight(state: &AppState) -> usize {
    state
        .l2_index_building
        .read()
        .map(|building| building.len())
        .unwrap_or(0)
}

pub(crate) fn remove_l2_index_entry(state: &AppState, collection_name: &str) {
    if let Ok(mut cache) = state.l2_indexes.write() {
        let _ = cache.remove(collection_name);
    }
}

pub(crate) fn clear_l2_build_tracking(state: &AppState, collection_name: &str) {
    if let Ok(mut started) = state.l2_index_last_started_ms.lock() {
        let _ = started.remove(collection_name);
    }
}

pub(crate) fn schedule_l2_build_if_needed(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
) {
    if collection.len() < IvfIndex::min_indexed_points() {
        return;
    }

    let collection_name = collection_name.to_string();
    if is_l2_build_in_flight(state, &collection_name) {
        return;
    }
    if should_throttle_build(state, &collection_name) {
        let _ = state
            .metrics
            .l2_index_build_cooldown_skips
            .fetch_add(1, Ordering::Relaxed);
        return;
    }
    if !mark_l2_build_in_flight(state, &collection_name) {
        return;
    }

    let _ = state
        .metrics
        .l2_index_build_requests
        .fetch_add(1, Ordering::Relaxed);
    let state = state.clone();
    tokio::spawn(async move {
        let collection_name_for_build = collection_name.clone();
        let state_for_build = state.clone();

        let build_result = tokio::task::spawn_blocking(move || {
            let handle = state_for_build
                .collections
                .read()
                .ok()
                .and_then(|collections| collections.get(&collection_name_for_build).cloned());
            let Some(handle) = handle else {
                return Ok::<Option<IvfIndex>, String>(None);
            };

            let (dimension, len, mutation_version, snapshot_points) = {
                let collection = handle
                    .read()
                    .map_err(|_| "collection lock poisoned during index build".to_string())?;
                if collection.len() < IvfIndex::min_indexed_points() {
                    return Ok(None);
                }
                let snapshot_points: Vec<(u64, Vec<f32>)> = collection
                    .iter_points()
                    .map(|(id, values)| (id, values.to_vec()))
                    .collect();
                (
                    collection.dimension(),
                    collection.len(),
                    collection.mutation_version(),
                    snapshot_points,
                )
            };

            Ok(IvfIndex::build_from_snapshot(
                dimension,
                len,
                mutation_version,
                &snapshot_points,
            ))
        })
        .await;

        if let Ok(mut building) = state.l2_index_building.write() {
            let _ = building.remove(&collection_name);
        }

        match build_result {
            Ok(Ok(Some(index))) => {
                if let Ok(mut cache) = state.l2_indexes.write() {
                    cache.insert(collection_name.clone(), index);
                }
                let _ = state
                    .metrics
                    .l2_index_build_successes
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => {
                let _ = state
                    .metrics
                    .l2_index_build_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(collection = %collection_name, %error, "l2 index build failed");
            }
            Err(error) => {
                let _ = state
                    .metrics
                    .l2_index_build_failures
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(collection = %collection_name, %error, "l2 index build task failed");
            }
        }
    });
}

fn is_l2_build_in_flight(state: &AppState, collection_name: &str) -> bool {
    state
        .l2_index_building
        .read()
        .map(|building| building.contains(collection_name))
        .unwrap_or(false)
}

fn mark_l2_build_in_flight(state: &AppState, collection_name: &str) -> bool {
    let Ok(mut building) = state.l2_index_building.write() else {
        return false;
    };
    if building.contains(collection_name) {
        return false;
    }
    building.insert(collection_name.to_string());
    true
}

fn should_throttle_build(state: &AppState, collection_name: &str) -> bool {
    if L2_BUILD_COOLDOWN_MS == 0 {
        return false;
    }
    let now_ms = now_millis();
    let Ok(mut started) = state.l2_index_last_started_ms.lock() else {
        return false;
    };
    let throttled = started
        .get(collection_name)
        .is_some_and(|last_ms| now_ms.saturating_sub(*last_ms) < L2_BUILD_COOLDOWN_MS);
    if !throttled {
        started.insert(collection_name.to_string(), now_ms);
    }
    throttled
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

pub(crate) fn warmup_l2_indexes(state: &AppState) {
    let handles: Vec<(String, crate::state::CollectionHandle)> = state
        .collections
        .read()
        .map(|collections| {
            collections
                .iter()
                .map(|(name, handle)| (name.clone(), handle.clone()))
                .collect()
        })
        .unwrap_or_default();

    for (name, handle) in handles {
        let Ok(collection) = handle.read() else {
            continue;
        };
        schedule_l2_build_if_needed(state, &name, &collection);
    }
}
