use std::sync::atomic::Ordering;
use std::sync::OnceLock;

use aionbd_core::{Collection, VectorValidationOptions};

use crate::errors::ApiError;
use crate::index_manager::schedule_l2_build_if_needed;
use crate::ivf_index::IvfIndex;
use crate::models::{Metric, PointPayload, SearchFilter, SearchHit, SearchMode};
use crate::state::AppState;

use super::engine::{normalize_target_recall, SearchSelection};
use super::filter::validate_filter;
use super::scoring::{score_points, ScoreSource};

#[path = "engine_batch_exact.rs"]
mod exact_l2_batch;

use exact_l2_batch::{score_exact_l2_batch_small_topk, should_use_exact_batch_fast_path};

const AUTO_BATCH_EXACT_PREFER_MIN_QUERIES: usize = 16;
static AUTO_BATCH_EXACT_PREFER_MIN_QUERIES_CACHE: OnceLock<usize> = OnceLock::new();

fn auto_batch_exact_prefer_min_queries() -> usize {
    *AUTO_BATCH_EXACT_PREFER_MIN_QUERIES_CACHE.get_or_init(|| {
        std::env::var("AIONBD_AUTO_BATCH_EXACT_PREFER_MIN_QUERIES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(AUTO_BATCH_EXACT_PREFER_MIN_QUERIES)
    })
}

pub(crate) struct SearchBatchPlan<'a> {
    pub(crate) queries: &'a [Vec<f32>],
    pub(crate) metric: Metric,
    pub(crate) include_payload: bool,
    pub(crate) limit: usize,
    pub(crate) mode: SearchMode,
    pub(crate) target_recall: Option<f32>,
    pub(crate) filter: Option<&'a SearchFilter>,
}

#[derive(Debug)]
enum BatchCandidateStrategy {
    ExactScan { ivf_fallback: bool },
    Ivf { index: std::sync::Arc<IvfIndex> },
}

pub(crate) fn select_top_k_batch(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    plan: SearchBatchPlan<'_>,
) -> Result<Vec<SearchSelection>, ApiError> {
    validate_search_batch_inputs(collection, &plan)?;
    let query_count = plan.queries.len();
    let query_count_u64 = query_count_as_u64(query_count);
    let _ = state
        .metrics
        .search_queries_total
        .fetch_add(query_count_u64, Ordering::Relaxed);

    if let Some(filter) = plan.filter {
        validate_filter(filter)?;
    }

    let keep = plan.limit.min(collection.len());
    if keep == 0 {
        return Ok((0..query_count)
            .map(|_| SearchSelection {
                mode: SearchMode::Exact,
                recall_at_k: Some(1.0),
                hits: Vec::new(),
            })
            .collect());
    }

    let prefer_exact_batch_auto = matches!(plan.mode, SearchMode::Auto)
        && query_count >= auto_batch_exact_prefer_min_queries()
        && should_use_exact_batch_fast_path(plan.metric, plan.filter, keep, query_count);
    if prefer_exact_batch_auto {
        let hits = score_exact_l2_batch_small_topk(collection, plan.queries, keep);
        let hits = hydrate_batch_hits_payloads(
            collection,
            hits,
            plan.include_payload && collection.has_payload_points(),
        )?;
        return Ok(hits
            .into_iter()
            .map(|hits| SearchSelection {
                mode: SearchMode::Exact,
                recall_at_k: Some(1.0),
                hits,
            })
            .collect());
    }

    let options = VectorValidationOptions {
        strict_finite: collection.strict_finite(),
        zero_norm_epsilon: f32::EPSILON,
    };
    let target_recall = normalize_target_recall(plan.target_recall)?;
    let strategy = select_batch_candidate_strategy(
        state,
        collection_name,
        collection,
        plan.mode,
        plan.metric,
        query_count_u64,
    )?;

    match strategy {
        BatchCandidateStrategy::ExactScan { ivf_fallback } => {
            if ivf_fallback {
                let _ = state
                    .metrics
                    .search_ivf_fallback_exact_total
                    .fetch_add(query_count_u64, Ordering::Relaxed);
            }
            if should_use_exact_batch_fast_path(plan.metric, plan.filter, keep, plan.queries.len())
            {
                let hits = score_exact_l2_batch_small_topk(collection, plan.queries, keep);
                let hits = hydrate_batch_hits_payloads(
                    collection,
                    hits,
                    plan.include_payload && collection.has_payload_points(),
                )?;
                return Ok(hits
                    .into_iter()
                    .map(|hits| SearchSelection {
                        mode: SearchMode::Exact,
                        recall_at_k: Some(1.0),
                        hits,
                    })
                    .collect());
            }
            let mut selections = Vec::with_capacity(query_count);
            for query in plan.queries {
                selections.push(SearchSelection {
                    mode: SearchMode::Exact,
                    recall_at_k: Some(1.0),
                    hits: score_points(
                        collection,
                        query,
                        plan.metric,
                        plan.include_payload,
                        keep,
                        options,
                        plan.filter,
                        ScoreSource::All,
                    )?,
                });
            }
            Ok(selections)
        }
        BatchCandidateStrategy::Ivf { index } => {
            let _ = state
                .metrics
                .search_ivf_queries_total
                .fetch_add(query_count_u64, Ordering::Relaxed);

            let mut selections = Vec::with_capacity(query_count);
            for query in plan.queries {
                let centroid_ids =
                    index.centroid_ids_with_target_recall(query, keep, target_recall);
                let candidate_count = index.candidate_slot_count(&centroid_ids);
                let hits = score_points(
                    collection,
                    query,
                    plan.metric,
                    plan.include_payload,
                    keep,
                    options,
                    plan.filter,
                    ScoreSource::IvfCentroids {
                        index: index.clone(),
                        centroids: centroid_ids,
                        candidate_count,
                    },
                )?;
                selections.push(SearchSelection {
                    mode: SearchMode::Ivf,
                    recall_at_k: None,
                    hits,
                });
            }
            Ok(selections)
        }
    }
}

fn hydrate_batch_hits_payloads(
    collection: &Collection,
    mut batch_hits: Vec<Vec<SearchHit>>,
    include_payload: bool,
) -> Result<Vec<Vec<SearchHit>>, ApiError> {
    if !include_payload {
        return Ok(batch_hits);
    }

    for hits in &mut batch_hits {
        for hit in hits {
            let payload = collection
                .get_payload(hit.id)
                .ok_or_else(|| ApiError::internal("point id index is inconsistent"))?;
            hit.payload = response_payload(payload);
        }
    }

    Ok(batch_hits)
}

fn response_payload(payload: &PointPayload) -> Option<PointPayload> {
    if payload.is_empty() {
        None
    } else {
        Some(payload.clone())
    }
}

fn query_count_as_u64(query_count: usize) -> u64 {
    query_count.min(u64::MAX as usize) as u64
}

fn validate_search_batch_inputs(
    collection: &Collection,
    plan: &SearchBatchPlan<'_>,
) -> Result<(), ApiError> {
    if collection.is_empty() {
        return Err(ApiError::invalid_argument("collection contains no points"));
    }
    for query in plan.queries {
        if query.len() != collection.dimension() {
            return Err(ApiError::invalid_argument(format!(
                "query dimension {} does not match collection dimension {}",
                query.len(),
                collection.dimension()
            )));
        }
    }
    Ok(())
}

fn select_batch_candidate_strategy(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    mode: SearchMode,
    metric: Metric,
    query_count_u64: u64,
) -> Result<BatchCandidateStrategy, ApiError> {
    match mode {
        SearchMode::Exact => Ok(BatchCandidateStrategy::ExactScan {
            ivf_fallback: false,
        }),
        SearchMode::Ivf | SearchMode::Auto => {
            if !matches!(metric, Metric::L2) && mode == SearchMode::Auto {
                return Ok(BatchCandidateStrategy::ExactScan {
                    ivf_fallback: false,
                });
            }
            if collection.len() < IvfIndex::min_indexed_points() {
                if mode == SearchMode::Ivf {
                    return Err(ApiError::invalid_argument(format!(
                        "mode 'ivf' requires at least {} points",
                        IvfIndex::min_indexed_points()
                    )));
                }
                return Ok(BatchCandidateStrategy::ExactScan {
                    ivf_fallback: false,
                });
            }

            if let Some(index) = state
                .l2_indexes
                .get(collection_name)
                .map(|entry| std::sync::Arc::clone(entry.value()))
            {
                if index.is_compatible(collection) {
                    let _ = state
                        .metrics
                        .l2_index_cache_lookups
                        .fetch_add(query_count_u64, Ordering::Relaxed);
                    let _ = state
                        .metrics
                        .l2_index_cache_hits
                        .fetch_add(query_count_u64, Ordering::Relaxed);
                    return Ok(BatchCandidateStrategy::Ivf { index });
                }
            }

            let _ = state
                .metrics
                .l2_index_cache_lookups
                .fetch_add(query_count_u64, Ordering::Relaxed);
            let _ = state
                .metrics
                .l2_index_cache_misses
                .fetch_add(query_count_u64, Ordering::Relaxed);
            schedule_l2_build_if_needed(state, collection_name, collection);
            Ok(BatchCandidateStrategy::ExactScan {
                ivf_fallback: matches!(mode, SearchMode::Ivf),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aionbd_core::{CollectionConfig, MetadataValue};

    use super::exact_l2_batch::should_use_exact_batch_fast_path;
    use super::*;

    #[test]
    fn exact_batch_fast_path_allows_payload_collections() {
        let config = CollectionConfig::new(4, true).expect("valid config");
        let mut collection = Collection::new("bench", config).expect("collection");
        let mut payload = BTreeMap::new();
        payload.insert(
            "tenant".to_string(),
            MetadataValue::String("edge".to_string()),
        );
        let _ = collection
            .upsert_point_with_payload(1, vec![1.0, 2.0, 3.0, 4.0], payload)
            .expect("upsert");

        assert!(should_use_exact_batch_fast_path(Metric::L2, None, 10, 2));
        assert!(!should_use_exact_batch_fast_path(Metric::L2, None, 10, 1));
    }

    #[test]
    fn attach_payloads_for_small_topk_batch_hits_preserves_payload_data() {
        let config = CollectionConfig::new(4, true).expect("valid config");
        let mut collection = Collection::new("bench", config).expect("collection");
        let mut payload = BTreeMap::new();
        payload.insert(
            "tenant".to_string(),
            MetadataValue::String("edge".to_string()),
        );
        let _ = collection
            .upsert_point_with_payload(7, vec![1.0, 2.0, 3.0, 4.0], payload)
            .expect("upsert");

        let batch_hits = vec![vec![SearchHit {
            id: 7,
            value: 0.0,
            payload: None,
        }]];

        let hydrated =
            hydrate_batch_hits_payloads(&collection, batch_hits, true).expect("payload attach");
        assert_eq!(
            hydrated[0][0]
                .payload
                .as_ref()
                .and_then(|payload| payload.get("tenant")),
            Some(&MetadataValue::String("edge".to_string()))
        );
    }
}
