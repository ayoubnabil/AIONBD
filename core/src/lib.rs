#![forbid(unsafe_code)]
//! Core primitives for AIONBD.
//!
//! This crate intentionally starts with deterministic, well-tested vector math
//! helpers that are reused by the server and benchmark crates.

pub mod collection;
pub mod persistence;
pub mod vector;

pub use collection::{
    Collection, CollectionConfig, CollectionError, MetadataPayload, MetadataValue, PointId,
    PointRecord,
};
pub use persistence::{
    append_wal_record, append_wal_record_with_sync, append_wal_record_with_sync_info,
    append_wal_records_with_sync, append_wal_records_with_sync_info, checkpoint_snapshot,
    checkpoint_snapshot_with_policy, checkpoint_wal, checkpoint_wal_with_policy,
    incremental_snapshot_dir, load_collections, persist_change, CheckpointPolicy, PersistOutcome,
    PersistenceError, WalAppendInfo, WalRecord,
};
pub use vector::{
    cosine_similarity, cosine_similarity_unchecked, cosine_similarity_with_options, dot_product,
    dot_product_unchecked, dot_product_with_options, l2_distance, l2_distance_with_options,
    l2_squared_unchecked, l2_squared_with_options, PreparedCosineQuery, PreparedDotQuery,
    PreparedL2Query, VectorError, VectorSide, VectorValidationOptions,
};
