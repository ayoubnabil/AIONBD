use aionbd_core::MetadataValue;

use crate::errors::ApiError;
use crate::models::{
    FilterClause, FilterMatchClause, FilterRangeClause, PointPayload, SearchFilter,
};

pub(crate) fn validate_filter(filter: &SearchFilter) -> Result<(), ApiError> {
    for clause in filter.must.iter().chain(filter.should.iter()) {
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

pub(crate) fn matches_filter(
    payload: &PointPayload,
    filter: Option<&SearchFilter>,
) -> Result<bool, ApiError> {
    let Some(filter) = filter else {
        return Ok(true);
    };

    for clause in &filter.must {
        if !matches_clause(payload, clause) {
            return Ok(false);
        }
    }

    if filter.should.is_empty() {
        return Ok(true);
    }

    let required = filter.minimum_should_match.unwrap_or(1);
    let matched = filter
        .should
        .iter()
        .filter(|clause| matches_clause(payload, clause))
        .count();
    Ok(matched >= required)
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
        (Some(left_num), Some(right_num)) => left_num == right_num,
        _ => left == right,
    }
}
