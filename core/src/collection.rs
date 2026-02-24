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

    pub fn upsert_point(&mut self, id: PointId, values: Vec<f32>) -> Result<bool, CollectionError> {
        self.validate_vector(&values)?;
        let was_missing = self.points.insert(id, values).is_none();
        Ok(was_missing)
    }

    pub fn get_point(&self, id: PointId) -> Option<&[f32]> {
        self.points.get(&id).map(Vec::as_slice)
    }

    pub fn remove_point(&mut self, id: PointId) -> Option<Vec<f32>> {
        self.points.remove(&id)
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
mod tests {
    use super::*;

    fn new_collection(strict_finite: bool) -> Collection {
        let config = CollectionConfig::new(3, strict_finite).expect("config must be valid");
        Collection::new("demo", config).expect("collection must be valid")
    }

    #[test]
    fn rejects_invalid_config() {
        let error = CollectionConfig::new(0, true).expect_err("must fail");
        assert!(matches!(error, CollectionError::InvalidConfig(_)));
    }

    #[test]
    fn rejects_empty_name() {
        let config = CollectionConfig::new(3, true).expect("config must be valid");
        let error = Collection::new("   ", config).expect_err("must fail");
        assert!(matches!(error, CollectionError::InvalidName));
    }

    #[test]
    fn insert_get_update_and_remove_point() {
        let mut collection = new_collection(true);

        let inserted = collection
            .upsert_point(10, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        assert!(inserted);
        assert_eq!(collection.len(), 1);
        assert_eq!(collection.get_point(10), Some(&[1.0, 2.0, 3.0][..]));

        let inserted = collection
            .upsert_point(10, vec![9.0, 8.0, 7.0])
            .expect("must succeed");
        assert!(!inserted);
        assert_eq!(collection.get_point(10), Some(&[9.0, 8.0, 7.0][..]));

        let removed = collection.remove_point(10).expect("point must exist");
        assert_eq!(removed, vec![9.0, 8.0, 7.0]);
        assert!(collection.is_empty());
    }

    #[test]
    fn rejects_dimension_mismatch() {
        let mut collection = new_collection(true);
        let error = collection
            .upsert_point(1, vec![1.0, 2.0])
            .expect_err("must fail");

        assert!(matches!(
            error,
            CollectionError::InvalidDimension {
                expected: 3,
                got: 2
            }
        ));
    }

    #[test]
    fn strict_mode_rejects_non_finite() {
        let mut collection = new_collection(true);
        let error = collection
            .upsert_point(1, vec![1.0, f32::NAN, 3.0])
            .expect_err("must fail");

        assert!(matches!(
            error,
            CollectionError::NonFiniteValue { index: 1 }
        ));
    }

    #[test]
    fn permissive_mode_accepts_non_finite() {
        let mut collection = new_collection(false);
        collection
            .upsert_point(1, vec![1.0, f32::NAN, 3.0])
            .expect("must succeed");

        let stored = collection.get_point(1).expect("point must exist");
        assert!(stored[1].is_nan());
    }

    #[test]
    fn ids_are_sorted() {
        let mut collection = new_collection(true);
        collection
            .upsert_point(50, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        collection
            .upsert_point(10, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        collection
            .upsert_point(30, vec![1.0, 2.0, 3.0])
            .expect("must succeed");

        assert_eq!(collection.point_ids(), vec![10, 30, 50]);
    }

    #[test]
    fn point_ids_page_respects_offset_and_limit() {
        let mut collection = new_collection(true);
        collection
            .upsert_point(10, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        collection
            .upsert_point(30, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        collection
            .upsert_point(50, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
        collection
            .upsert_point(70, vec![1.0, 2.0, 3.0])
            .expect("must succeed");

        assert_eq!(collection.point_ids_page(0, 2), vec![10, 30]);
        assert_eq!(collection.point_ids_page(1, 2), vec![30, 50]);
        assert_eq!(collection.point_ids_page(3, 10), vec![70]);
        assert!(collection.point_ids_page(10, 2).is_empty());
        assert!(collection.point_ids_page(0, 0).is_empty());
    }

    #[test]
    fn iter_points_is_sorted_and_contains_payloads() {
        let mut collection = new_collection(true);
        collection
            .upsert_point(50, vec![5.0, 6.0, 7.0])
            .expect("must succeed");
        collection
            .upsert_point(10, vec![1.0, 2.0, 3.0])
            .expect("must succeed");

        let points: Vec<(PointId, Vec<f32>)> = collection
            .iter_points()
            .map(|(id, values)| (id, values.to_vec()))
            .collect();
        assert_eq!(
            points,
            vec![(10, vec![1.0, 2.0, 3.0]), (50, vec![5.0, 6.0, 7.0])]
        );
    }
}
