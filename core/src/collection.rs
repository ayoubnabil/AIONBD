use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

pub type PointId = u64;
pub type MetadataPayload = BTreeMap<String, MetadataValue>;

fn empty_payload() -> &'static MetadataPayload {
    static EMPTY: OnceLock<MetadataPayload> = OnceLock::new();
    EMPTY.get_or_init(MetadataPayload::new)
}

const SLOT_COMPACT_MIN_FREE: usize = 256;

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
    pub values: Box<[f32]>,
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
    point_slots: HashMap<PointId, usize>,
    ordered_ids: BTreeSet<PointId>,
    slot_ids: Vec<PointId>,
    slot_occupied: Vec<bool>,
    slot_payloads: Vec<Option<MetadataPayload>>,
    slot_values: Vec<f32>,
    free_slots: Vec<usize>,
    payload_points: usize,
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
            point_slots: HashMap::new(),
            ordered_ids: BTreeSet::new(),
            slot_ids: Vec::new(),
            slot_occupied: Vec::new(),
            slot_payloads: Vec::new(),
            slot_values: Vec::new(),
            free_slots: Vec::new(),
            payload_points: 0,
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
        self.point_slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.point_slots.is_empty()
    }

    pub fn mutation_version(&self) -> u64 {
        self.mutation_version
    }

    pub fn has_payload_points(&self) -> bool {
        self.payload_points > 0
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
        Ok(self.upsert_point_with_payload_unchecked(id, values, payload))
    }

    /// Inserts or replaces a point without runtime validation.
    ///
    /// Callers must ensure the vector matches the configured dimension, any
    /// required finiteness constraints are met, and payload keys are valid.
    pub fn upsert_point_with_payload_unchecked(
        &mut self,
        id: PointId,
        values: Vec<f32>,
        payload: MetadataPayload,
    ) -> bool {
        debug_assert_eq!(values.len(), self.config.dimension);
        debug_assert!(
            !self.config.strict_finite || values.iter().all(|value| value.is_finite()),
            "unchecked upsert received non-finite value in strict collection"
        );
        debug_assert!(
            !payload.keys().any(|key| key.trim().is_empty()),
            "unchecked upsert received invalid payload key"
        );
        self.upsert_point_with_payload_impl(id, values, payload)
    }

    fn upsert_point_with_payload_impl(
        &mut self,
        id: PointId,
        values: Vec<f32>,
        payload: MetadataPayload,
    ) -> bool {
        let has_payload = !payload.is_empty();

        if let Some(&slot) = self.point_slots.get(&id) {
            if self.slot_payloads[slot].is_some() {
                self.payload_points = self.payload_points.saturating_sub(1);
            }
            if has_payload {
                self.payload_points = self.payload_points.saturating_add(1);
            }
            self.slot_payloads[slot] = has_payload.then_some(payload);
            let start = slot * self.config.dimension;
            let end = start + self.config.dimension;
            self.slot_values[start..end].copy_from_slice(&values);
            self.bump_mutation_version();
            return false;
        }

        let stored_payload = has_payload.then_some(payload);
        let slot = if let Some(slot) = self.free_slots.pop() {
            self.slot_ids[slot] = id;
            self.slot_occupied[slot] = true;
            self.slot_payloads[slot] = stored_payload;
            let start = slot * self.config.dimension;
            let end = start + self.config.dimension;
            self.slot_values[start..end].copy_from_slice(&values);
            slot
        } else {
            let slot = self.slot_ids.len();
            self.slot_ids.push(id);
            self.slot_occupied.push(true);
            self.slot_payloads.push(stored_payload);
            self.slot_values.extend_from_slice(&values);
            slot
        };

        self.point_slots.insert(id, slot);
        self.ordered_ids.insert(id);
        if has_payload {
            self.payload_points = self.payload_points.saturating_add(1);
        }
        self.bump_mutation_version();
        true
    }

    pub fn get_point(&self, id: PointId) -> Option<&[f32]> {
        let slot = *self.point_slots.get(&id)?;
        let start = slot * self.config.dimension;
        let end = start + self.config.dimension;
        Some(&self.slot_values[start..end])
    }

    pub fn get_payload(&self, id: PointId) -> Option<&MetadataPayload> {
        let slot = *self.point_slots.get(&id)?;
        Some(self.slot_payload_ref(slot))
    }

    pub fn get_point_record(&self, id: PointId) -> Option<(&[f32], &MetadataPayload)> {
        let slot = *self.point_slots.get(&id)?;
        let start = slot * self.config.dimension;
        let end = start + self.config.dimension;
        Some((&self.slot_values[start..end], self.slot_payload_ref(slot)))
    }

    pub fn remove_point(&mut self, id: PointId) -> Option<Vec<f32>> {
        self.remove_point_record(id)
            .map(|record| record.values.into_vec())
    }

    pub fn delete_point(&mut self, id: PointId) -> bool {
        let Some(slot) = self.detach_slot(id) else {
            return false;
        };
        if self.slot_payloads[slot].take().is_some() {
            self.payload_points = self.payload_points.saturating_sub(1);
        }
        self.compact_slots_if_needed();
        self.bump_mutation_version();
        true
    }

    pub fn remove_point_record(&mut self, id: PointId) -> Option<PointRecord> {
        let slot = self.detach_slot(id)?;

        let start = slot * self.config.dimension;
        let end = start + self.config.dimension;
        let values = self.slot_values[start..end].to_vec().into_boxed_slice();
        let payload = self.slot_payloads[slot].take().unwrap_or_default();
        if !payload.is_empty() {
            self.payload_points = self.payload_points.saturating_sub(1);
        }
        self.compact_slots_if_needed();
        self.bump_mutation_version();
        Some(PointRecord { values, payload })
    }

    pub fn point_ids(&self) -> Vec<PointId> {
        self.ordered_ids.iter().copied().collect()
    }

    pub fn point_ids_page(&self, offset: usize, limit: usize) -> Vec<PointId> {
        self.ordered_ids
            .iter()
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
                .ordered_ids
                .range((Excluded(after_id), Unbounded))
                .copied()
                .take(limit.saturating_add(1))
                .collect(),
            None => self
                .ordered_ids
                .iter()
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
        self.ordered_ids.iter().map(|id| {
            let slot = *self
                .point_slots
                .get(id)
                .expect("collection ordered id index must reference an existing point");
            let start = slot * self.config.dimension;
            let end = start + self.config.dimension;
            (*id, &self.slot_values[start..end])
        })
    }

    pub fn iter_points_with_payload(
        &self,
    ) -> impl Iterator<Item = (PointId, &[f32], &MetadataPayload)> + '_ {
        self.ordered_ids.iter().map(|id| {
            let slot = *self
                .point_slots
                .get(id)
                .expect("collection ordered id index must reference an existing point");
            let start = slot * self.config.dimension;
            let end = start + self.config.dimension;
            (
                *id,
                &self.slot_values[start..end],
                self.slot_payload_ref(slot),
            )
        })
    }

    pub fn iter_points_unordered(&self) -> impl Iterator<Item = (PointId, &[f32])> + '_ {
        (0..self.slot_count()).filter_map(|slot| self.point_at_slot(slot))
    }

    pub fn iter_points_with_payload_unordered(
        &self,
    ) -> impl Iterator<Item = (PointId, &[f32], &MetadataPayload)> + '_ {
        (0..self.slot_count()).filter_map(|slot| self.point_with_payload_at_slot(slot))
    }

    pub fn slot_count(&self) -> usize {
        self.slot_ids.len()
    }

    pub fn slots_dense(&self) -> bool {
        self.free_slots.is_empty() && self.slot_ids.len() == self.point_slots.len()
    }

    pub fn point_at_dense_slot(&self, slot: usize) -> (PointId, &[f32]) {
        debug_assert!(self.slots_dense());
        debug_assert!(slot < self.slot_ids.len());

        let id = self.slot_ids[slot];
        let start = slot * self.config.dimension;
        let end = start + self.config.dimension;
        (id, &self.slot_values[start..end])
    }

    pub fn point_with_payload_at_dense_slot(
        &self,
        slot: usize,
    ) -> (PointId, &[f32], &MetadataPayload) {
        let (id, values) = self.point_at_dense_slot(slot);
        (id, values, self.slot_payload_ref(slot))
    }

    pub fn point_at_slot(&self, slot: usize) -> Option<(PointId, &[f32])> {
        if slot >= self.slot_ids.len() || !self.slot_occupied[slot] {
            return None;
        }

        let id = self.slot_ids[slot];
        let start = slot * self.config.dimension;
        let end = start + self.config.dimension;
        Some((id, &self.slot_values[start..end]))
    }

    pub fn point_with_payload_at_slot(
        &self,
        slot: usize,
    ) -> Option<(PointId, &[f32], &MetadataPayload)> {
        let (id, values) = self.point_at_slot(slot)?;
        Some((id, values, self.slot_payload_ref(slot)))
    }

    fn slot_payload_ref(&self, slot: usize) -> &MetadataPayload {
        match self.slot_payloads[slot].as_ref() {
            Some(payload) => payload,
            None => empty_payload(),
        }
    }

    fn detach_slot(&mut self, id: PointId) -> Option<usize> {
        let slot = self.point_slots.remove(&id)?;
        self.ordered_ids.remove(&id);
        self.slot_occupied[slot] = false;
        self.free_slots.push(slot);
        Some(slot)
    }

    fn compact_slots_if_needed(&mut self) {
        let free = self.free_slots.len();
        let total = self.slot_ids.len();
        if free < SLOT_COMPACT_MIN_FREE || total == 0 || free.saturating_mul(2) < total {
            return;
        }

        let dimension = self.config.dimension;
        let live = self.point_slots.len();
        let mut new_point_slots: HashMap<PointId, usize> = HashMap::with_capacity(live);
        let mut new_slot_ids: Vec<PointId> = Vec::with_capacity(live);
        let mut new_slot_occupied: Vec<bool> = Vec::with_capacity(live);
        let mut new_slot_payloads: Vec<Option<MetadataPayload>> = Vec::with_capacity(live);
        let mut new_slot_values: Vec<f32> = Vec::with_capacity(live.saturating_mul(dimension));

        for slot in 0..total {
            if !self.slot_occupied[slot] {
                continue;
            }

            let id = self.slot_ids[slot];
            let new_slot = new_slot_ids.len();
            let start = slot * dimension;
            let end = start + dimension;
            new_point_slots.insert(id, new_slot);
            new_slot_ids.push(id);
            new_slot_occupied.push(true);
            new_slot_payloads.push(self.slot_payloads[slot].take());
            new_slot_values.extend_from_slice(&self.slot_values[start..end]);
        }

        self.point_slots = new_point_slots;
        self.slot_ids = new_slot_ids;
        self.slot_occupied = new_slot_occupied;
        self.slot_payloads = new_slot_payloads;
        self.slot_values = new_slot_values;
        self.free_slots.clear();
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
