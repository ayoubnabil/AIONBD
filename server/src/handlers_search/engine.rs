use aionbd_core::{Collection, VectorValidationOptions};

use crate::errors::ApiError;
use crate::index_manager::{
    record_l2_lookup_hit, record_l2_lookup_miss, schedule_l2_build_if_needed,
};
use crate::ivf_index::IvfIndex;
use crate::models::{Metric, SearchFilter, SearchHit, SearchMode};
use crate::state::AppState;

use super::filter::validate_filter;
use super::scoring::{score_points, ScoreSource};

pub(crate) struct SearchPlan<'a> {
    pub(crate) query: &'a [f32],
    pub(crate) metric: Metric,
    pub(crate) limit: usize,
    pub(crate) mode: SearchMode,
    pub(crate) target_recall: Option<f32>,
    pub(crate) filter: Option<&'a SearchFilter>,
}

#[derive(Debug)]
pub(crate) struct SearchSelection {
    pub(crate) mode: SearchMode,
    pub(crate) recall_at_k: Option<f32>,
    pub(crate) hits: Vec<SearchHit>,
}

pub(crate) fn select_top_k(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    plan: SearchPlan<'_>,
) -> Result<SearchSelection, ApiError> {
    validate_search_inputs(collection, &plan)?;

    if let Some(filter) = plan.filter {
        validate_filter(filter)?;
    }

    let keep = plan.limit.min(collection.len());
    if keep == 0 {
        return Ok(SearchSelection {
            mode: SearchMode::Exact,
            recall_at_k: Some(1.0),
            hits: Vec::new(),
        });
    }

    let options = VectorValidationOptions {
        strict_finite: collection.strict_finite(),
        zero_norm_epsilon: f32::EPSILON,
    };
    let target_recall = normalize_target_recall(plan.target_recall)?;

    match select_candidate_strategy(
        state,
        collection_name,
        collection,
        &plan,
        keep,
        target_recall,
    )? {
        CandidateStrategy::ExactScan => Ok(SearchSelection {
            mode: SearchMode::Exact,
            recall_at_k: Some(1.0),
            hits: score_points(
                collection,
                plan.query,
                plan.metric,
                keep,
                options,
                plan.filter,
                ScoreSource::All,
            )?,
        }),
        CandidateStrategy::Ivf(candidate_ids) => {
            let approx_hits = score_points(
                collection,
                plan.query,
                plan.metric,
                keep,
                options,
                plan.filter,
                ScoreSource::CandidateIds(candidate_ids),
            )?;

            Ok(SearchSelection {
                mode: SearchMode::Ivf,
                recall_at_k: None,
                hits: approx_hits,
            })
        }
    }
}

fn validate_search_inputs(collection: &Collection, plan: &SearchPlan<'_>) -> Result<(), ApiError> {
    if collection.is_empty() {
        return Err(ApiError::invalid_argument("collection contains no points"));
    }
    if plan.query.len() != collection.dimension() {
        return Err(ApiError::invalid_argument(format!(
            "query dimension {} does not match collection dimension {}",
            plan.query.len(),
            collection.dimension()
        )));
    }
    Ok(())
}

fn normalize_target_recall(target_recall: Option<f32>) -> Result<Option<f32>, ApiError> {
    let Some(value) = target_recall else {
        return Ok(None);
    };
    if value <= 0.0 || value > 1.0 || !value.is_finite() {
        return Err(ApiError::invalid_argument(
            "target_recall must be within (0.0, 1.0]",
        ));
    }
    Ok(Some(value))
}

#[derive(Debug)]
enum CandidateStrategy {
    ExactScan,
    Ivf(Vec<u64>),
}

fn select_candidate_strategy(
    state: &AppState,
    collection_name: &str,
    collection: &Collection,
    plan: &SearchPlan<'_>,
    keep: usize,
    target_recall: Option<f32>,
) -> Result<CandidateStrategy, ApiError> {
    match plan.mode {
        SearchMode::Exact => Ok(CandidateStrategy::ExactScan),
        SearchMode::Ivf | SearchMode::Auto => {
            if !matches!(plan.metric, Metric::L2) && plan.mode == SearchMode::Auto {
                return Ok(CandidateStrategy::ExactScan);
            }
            if collection.len() < IvfIndex::min_indexed_points() {
                if plan.mode == SearchMode::Ivf {
                    return Err(ApiError::invalid_argument(format!(
                        "mode 'ivf' requires at least {} points",
                        IvfIndex::min_indexed_points()
                    )));
                }
                return Ok(CandidateStrategy::ExactScan);
            }

            if let Some(index) = state
                .l2_indexes
                .read()
                .map_err(|_| ApiError::internal("l2 index cache lock poisoned"))?
                .get(collection_name)
                .cloned()
            {
                if index.is_compatible(collection) {
                    record_l2_lookup_hit(state);
                    return Ok(CandidateStrategy::Ivf(
                        index.candidate_ids_with_target_recall(plan.query, keep, target_recall),
                    ));
                }
            }

            record_l2_lookup_miss(state);
            schedule_l2_build_if_needed(state, collection_name, collection);
            Ok(CandidateStrategy::ExactScan)
        }
    }
}
