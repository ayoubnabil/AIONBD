use aionbd_core::MetadataValue;

use crate::errors::ApiError;
use crate::models::{
    FilterClause, FilterMatchClause, FilterRangeClause, PointPayload, SearchFilter,
};

pub(crate) fn validate_filter(filter: &SearchFilter) -> Result<(), ApiError> {
    #[cfg(not(feature = "exp_filter_must_not"))]
    if !filter.must_not.is_empty() {
        return Err(ApiError::invalid_argument(
            "filter.must_not requires build feature exp_filter_must_not",
        ));
    }

    for clause in filter.must.iter().chain(filter.should.iter()) {
        validate_clause(clause)?;
    }

    #[cfg(feature = "exp_filter_must_not")]
    for clause in &filter.must_not {
        validate_clause(clause)?;
    }

    if let Some(required) = filter.minimum_should_match {
        if required > filter.should.len() {
            return Err(ApiError::invalid_argument(
                "minimum_should_match must be <= number of should clauses",
            ));
        }
    }

    Ok(())
}

pub(crate) fn matches_filter_strict(payload: &PointPayload, filter: &SearchFilter) -> bool {
    for clause in &filter.must {
        if !matches_clause(payload, clause) {
            return false;
        }
    }

    #[cfg(feature = "exp_filter_must_not")]
    for clause in &filter.must_not {
        if matches_clause(payload, clause) {
            return false;
        }
    }

    if filter.should.is_empty() {
        return true;
    }

    let required = filter.minimum_should_match.unwrap_or(1);
    if required == 0 {
        return true;
    }
    if required > filter.should.len() {
        return false;
    }

    let mut matched = 0usize;
    let mut remaining = filter.should.len();
    for clause in &filter.should {
        if matches_clause(payload, clause) {
            matched = matched.saturating_add(1);
            if matched >= required {
                return true;
            }
        }
        remaining = remaining.saturating_sub(1);
        if matched.saturating_add(remaining) < required {
            return false;
        }
    }

    false
}

fn validate_clause(clause: &FilterClause) -> Result<(), ApiError> {
    match clause {
        FilterClause::Match(FilterMatchClause { field, .. }) => {
            if field.trim().is_empty() {
                return Err(ApiError::invalid_argument(
                    "filter field names must not be empty",
                ));
            }
        }
        FilterClause::Range(FilterRangeClause {
            field,
            gt,
            gte,
            lt,
            lte,
        }) => {
            if field.trim().is_empty() {
                return Err(ApiError::invalid_argument(
                    "filter field names must not be empty",
                ));
            }
            if gt.is_none() && gte.is_none() && lt.is_none() && lte.is_none() {
                return Err(ApiError::invalid_argument(
                    "range filter requires at least one bound",
                ));
            }
            let lower = gte.or(*gt);
            let upper = lte.or(*lt);
            if let (Some(lower), Some(upper)) = (lower, upper) {
                if lower > upper {
                    return Err(ApiError::invalid_argument(
                        "range filter lower bound must be <= upper bound",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn matches_clause(payload: &PointPayload, clause: &FilterClause) -> bool {
    match clause {
        FilterClause::Match(FilterMatchClause { field, value }) => payload
            .get(field)
            .is_some_and(|actual| metadata_values_match(actual, value)),
        FilterClause::Range(FilterRangeClause {
            field,
            gt,
            gte,
            lt,
            lte,
        }) => {
            let Some(actual) = payload.get(field).and_then(MetadataValue::as_f64) else {
                return false;
            };

            if gt.is_some_and(|bound| actual <= bound) {
                return false;
            }
            if gte.is_some_and(|bound| actual < bound) {
                return false;
            }
            if lt.is_some_and(|bound| actual >= bound) {
                return false;
            }
            if lte.is_some_and(|bound| actual > bound) {
                return false;
            }
            true
        }
    }
}

fn metadata_values_match(left: &MetadataValue, right: &MetadataValue) -> bool {
    match (left.as_f64(), right.as_f64()) {
        (Some(left_num), Some(right_num)) => approx_equal_f64(left_num, right_num),
        _ => left == right,
    }
}

fn approx_equal_f64(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= f64::EPSILON * scale * 8.0
}
