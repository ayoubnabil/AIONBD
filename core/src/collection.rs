use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

pub type PointId = u64;

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
        }
    }
}

impl Error for CollectionError {}

#[derive(Debug, Clone)]
pub struct Collection {
    name: String,
    config: CollectionConfig,
    points: BTreeMap<PointId, Vec<f32>>,
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
        self.validate_vector(&values)?;
        let was_missing = self.points.insert(id, values).is_none();
        self.mutation_version = self.mutation_version.wrapping_add(1);
        Ok(was_missing)
    }

    pub fn get_point(&self, id: PointId) -> Option<&[f32]> {
        self.points.get(&id).map(Vec::as_slice)
    }

    pub fn remove_point(&mut self, id: PointId) -> Option<Vec<f32>> {
        let removed = self.points.remove(&id);
        if removed.is_some() {
            self.mutation_version = self.mutation_version.wrapping_add(1);
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

    pub fn iter_points(&self) -> impl Iterator<Item = (PointId, &[f32])> + '_ {
        self.points
            .iter()
            .map(|(id, values)| (*id, values.as_slice()))
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
}

#[cfg(test)]
mod tests;
