#![forbid(unsafe_code)]
//! Core primitives for AIONBD.
//!
//! This crate intentionally starts with deterministic, well-tested vector math
//! helpers that are reused by the server and benchmark crates.

pub mod vector;

pub use vector::{
    cosine_similarity, cosine_similarity_with_options, dot_product, dot_product_with_options,
    l2_distance, l2_distance_with_options, VectorError, VectorSide, VectorValidationOptions,
};
