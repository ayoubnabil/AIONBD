use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::ops::Bound::{Excluded, Unbounded};

use serde::{Deserialize, Serialize};

pub type PointId = u64;
pub type MetadataPayload = BTreeMap<String, MetadataValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataValue {
    String(String),
    Integer(i64),
    Float(f64),
    Bool(bool),
}

impl MetadataValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Integer(value) => Some(*value as f64),
            Self::Float(value) => Some(*value),
            Self::String(_) | Self::Bool(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PointRecord {
    pub values: Vec<f32>,
    #[serde(default)]
    pub payload: MetadataPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionConfig {
    pub dimension: usize,
    pub strict_finite: bool,
}

impl CollectionConfig {
    pub fn new(dimension: usize, strict_finite: bool) -> Result<Self, CollectionError> {
        if dimension == 0 {
            return Err(CollectionError::InvalidConfig(
                "dimension must be > 0".to_string(),
            ));
        }

        Ok(Self {
            dimension,
            strict_finite,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionError {
    InvalidConfig(String),
    InvalidName,
    InvalidDimension { expected: usize, got: usize },
    NonFiniteValue { index: usize },
    InvalidPayloadKey,
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid config: {message}"),
            Self::InvalidName => write!(f, "collection name must not be empty"),
            Self::InvalidDimension { expected, got } => {
                write!(
                    f,
                    "invalid vector dimension: expected {expected}, got {got}"
                )
            }
            Self::NonFiniteValue { index } => {
                write!(f, "vector contains non-finite value at index {index}")
            }
            Self::InvalidPayloadKey => write!(f, "payload keys must not be empty"),
        }
    }
}

impl Error for CollectionError {}

#[derive(Debug, Clone)]
pub struct Collection {
    name: String,
    config: CollectionConfig,
    points: BTreeMap<PointId, PointRecord>,
    mutation_version: u64,
}

impl Collection {
    pub fn new(name: impl Into<String>, config: CollectionConfig) -> Result<Self, CollectionError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(CollectionError::InvalidName);
        }

        Ok(Self {
            name,
            config,
            points: BTreeMap::new(),
            mutation_version: 0,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    pub fn strict_finite(&self) -> bool {
        self.config.strict_finite
    }

    pub fn len(&self) -> usize {
        self.points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    pub fn mutation_version(&self) -> u64 {
        self.mutation_version
    }

    pub fn upsert_point(&mut self, id: PointId, values: Vec<f32>) -> Result<bool, CollectionError> {
        self.upsert_point_with_payload(id, values, MetadataPayload::new())
    }

    pub fn upsert_point_with_payload(
        &mut self,
        id: PointId,
        values: Vec<f32>,
        payload: MetadataPayload,
    ) -> Result<bool, CollectionError> {
        self.validate_vector(&values)?;
        self.validate_payload(&payload)?;
        let was_missing = self
            .points
            .insert(id, PointRecord { values, payload })
            .is_none();
        self.bump_mutation_version();
        Ok(was_missing)
    }

    pub fn get_point(&self, id: PointId) -> Option<&[f32]> {
        self.points.get(&id).map(|record| record.values.as_slice())
    }

    pub fn get_payload(&self, id: PointId) -> Option<&MetadataPayload> {
        self.points.get(&id).map(|record| &record.payload)
    }

    pub fn get_point_record(&self, id: PointId) -> Option<(&[f32], &MetadataPayload)> {
        self.points
            .get(&id)
            .map(|record| (record.values.as_slice(), &record.payload))
    }

    pub fn remove_point(&mut self, id: PointId) -> Option<Vec<f32>> {
        self.remove_point_record(id).map(|record| record.values)
    }

    pub fn remove_point_record(&mut self, id: PointId) -> Option<PointRecord> {
        let removed = self.points.remove(&id);
        if removed.is_some() {
            self.bump_mutation_version();
        }
        removed
    }

    pub fn point_ids(&self) -> Vec<PointId> {
        self.points.keys().copied().collect()
    }

    pub fn point_ids_page(&self, offset: usize, limit: usize) -> Vec<PointId> {
        self.points
            .keys()
            .skip(offset)
            .take(limit)
            .copied()
            .collect()
    }

    pub fn point_ids_page_after(
        &self,
        after_id: Option<PointId>,
        limit: usize,
    ) -> (Vec<PointId>, Option<PointId>) {
        if limit == 0 {
            return (Vec::new(), None);
        }

        let mut ids: Vec<PointId> = match after_id {
            Some(after_id) => self
                .points
                .range((Excluded(after_id), Unbounded))
                .map(|(id, _)| *id)
                .take(limit.saturating_add(1))
                .collect(),
            None => self
                .points
                .keys()
                .copied()
                .take(limit.saturating_add(1))
                .collect(),
        };

        let has_more = ids.len() > limit;
        if has_more {
            ids.truncate(limit);
        }

        let next_after_id = if has_more { ids.last().copied() } else { None };
        (ids, next_after_id)
    }

    pub fn iter_points(&self) -> impl Iterator<Item = (PointId, &[f32])> + '_ {
        self.points
            .iter()
            .map(|(id, record)| (*id, record.values.as_slice()))
    }

    pub fn iter_points_with_payload(
        &self,
    ) -> impl Iterator<Item = (PointId, &[f32], &MetadataPayload)> + '_ {
        self.points
            .iter()
            .map(|(id, record)| (*id, record.values.as_slice(), &record.payload))
    }

    fn validate_vector(&self, values: &[f32]) -> Result<(), CollectionError> {
        if values.len() != self.config.dimension {
            return Err(CollectionError::InvalidDimension {
                expected: self.config.dimension,
                got: values.len(),
            });
        }

        if self.config.strict_finite {
            if let Some(index) = values.iter().position(|value| !value.is_finite()) {
                return Err(CollectionError::NonFiniteValue { index });
            }
        }

        Ok(())
    }

    fn validate_payload(&self, payload: &MetadataPayload) -> Result<(), CollectionError> {
        if payload.keys().any(|key| key.trim().is_empty()) {
            return Err(CollectionError::InvalidPayloadKey);
        }
        Ok(())
    }

    fn bump_mutation_version(&mut self) {
        self.mutation_version = self.mutation_version.saturating_add(1);
    }
}

#[cfg(test)]
mod tests;
