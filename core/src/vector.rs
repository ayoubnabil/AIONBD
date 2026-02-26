use std::error::Error;
use std::fmt;
use wide::f32x8;

/// Identifies which input vector triggered a validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSide {
    Left,
    Right,
}

/// Runtime validation options for vector operations.
#[derive(Debug, Clone, Copy)]
pub struct VectorValidationOptions {
    /// When true, NaN and +/-Inf values are rejected.
    pub strict_finite: bool,
    /// Squared norm threshold under which cosine is treated as zero-norm.
    pub zero_norm_epsilon: f32,
}

impl Default for VectorValidationOptions {
    fn default() -> Self {
        Self {
            strict_finite: true,
            zero_norm_epsilon: f32::EPSILON,
        }
    }
}

impl VectorValidationOptions {
    /// Returns strict production-safe defaults.
    pub const fn strict() -> Self {
        Self {
            strict_finite: true,
            zero_norm_epsilon: f32::EPSILON,
        }
    }

    /// Returns permissive options that allow non-finite values.
    pub const fn permissive() -> Self {
        Self {
            strict_finite: false,
            zero_norm_epsilon: f32::EPSILON,
        }
    }
}

/// Error type for vector operations.
#[derive(Debug, Clone, PartialEq)]
pub enum VectorError {
    /// Returned when vectors do not share the same dimension.
    DimensionMismatch { left: usize, right: usize },
    /// Returned when one or both vectors are empty.
    EmptyVector,
    /// Returned when cosine similarity is requested for near-zero norms.
    ZeroNorm { epsilon: f32 },
    /// Returned when strict mode rejects NaN or Infinity values.
    NonFinite {
        side: VectorSide,
        index: usize,
        value: f32,
    },
}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionMismatch { left, right } => {
                write!(f, "dimension mismatch: left={}, right={}", left, right)
            }
            Self::EmptyVector => write!(f, "vector is empty"),
            Self::ZeroNorm { epsilon } => {
                write!(
                    f,
                    "cosine similarity undefined for near-zero norm (epsilon={epsilon})"
                )
            }
            Self::NonFinite { side, index, value } => {
                let side = match side {
                    VectorSide::Left => "left",
                    VectorSide::Right => "right",
                };
                write!(
                    f,
                    "non-finite value in {side} vector at index {index}: {value}"
                )
            }
        }
    }
}

impl Error for VectorError {}

fn validate_vectors(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<(), VectorError> {
    if left.is_empty() || right.is_empty() {
        return Err(VectorError::EmptyVector);
    }
    if left.len() != right.len() {
        return Err(VectorError::DimensionMismatch {
            left: left.len(),
            right: right.len(),
        });
    }

    if options.strict_finite {
        if let Some((index, value)) = left
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(VectorError::NonFinite {
                side: VectorSide::Left,
                index,
                value,
            });
        }
        if let Some((index, value)) = right
            .iter()
            .copied()
            .enumerate()
            .find(|(_, value)| !value.is_finite())
        {
            return Err(VectorError::NonFinite {
                side: VectorSide::Right,
                index,
                value,
            });
        }
    }

    Ok(())
}

/// Computes the dot product between two vectors using strict validation.
pub fn dot_product(left: &[f32], right: &[f32]) -> Result<f32, VectorError> {
    dot_product_with_options(left, right, VectorValidationOptions::strict())
}

/// Computes the dot product between two vectors with custom validation options.
pub fn dot_product_with_options(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    validate_vectors(left, right, options)?;
    Ok(dot_product_unchecked(left, right))
}

/// Computes the Euclidean (L2) distance using strict validation.
pub fn l2_distance(left: &[f32], right: &[f32]) -> Result<f32, VectorError> {
    l2_distance_with_options(left, right, VectorValidationOptions::strict())
}

/// Computes the Euclidean (L2) distance with custom validation options.
pub fn l2_distance_with_options(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    let squared_sum = l2_squared_with_options(left, right, options)?;
    Ok(squared_sum.sqrt())
}

/// Computes the squared Euclidean (L2) distance with custom validation options.
pub fn l2_squared_with_options(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    validate_vectors(left, right, options)?;
    Ok(simd_l2_squared(left, right))
}

/// Computes the squared Euclidean (L2) distance without runtime validation.
///
/// Callers must ensure both vectors are non-empty, have the same length, and
/// satisfy any finiteness constraints required by their execution path.
pub fn l2_squared_unchecked(left: &[f32], right: &[f32]) -> f32 {
    debug_assert!(!left.is_empty());
    debug_assert_eq!(left.len(), right.len());
    simd_l2_squared(left, right)
}

/// Computes the dot product without runtime validation.
///
/// Callers must ensure both vectors are non-empty, have the same length, and
/// satisfy any finiteness constraints required by their execution path.
pub fn dot_product_unchecked(left: &[f32], right: &[f32]) -> f32 {
    debug_assert!(!left.is_empty());
    debug_assert_eq!(left.len(), right.len());
    simd_dot(left, right)
}

/// Computes cosine similarity using strict validation.
pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32, VectorError> {
    cosine_similarity_with_options(left, right, VectorValidationOptions::strict())
}

/// Computes cosine similarity with custom validation options. The implementation
/// uses a single pass to accumulate dot and squared norms.
pub fn cosine_similarity_with_options(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    validate_vectors(left, right, options)?;
    cosine_similarity_unchecked(left, right, options.zero_norm_epsilon).ok_or_else(|| {
        VectorError::ZeroNorm {
            epsilon: options.zero_norm_epsilon.max(0.0),
        }
    })
}

/// Computes cosine similarity without runtime validation.
///
/// Returns `None` when one of the vector norms is less than or equal to
/// `zero_norm_epsilon.max(0.0)`.
pub fn cosine_similarity_unchecked(
    left: &[f32],
    right: &[f32],
    zero_norm_epsilon: f32,
) -> Option<f32> {
    debug_assert!(!left.is_empty());
    debug_assert_eq!(left.len(), right.len());

    let (dot, left_sq_sum, right_sq_sum) = simd_dot_and_norms(left, right);
    let epsilon = zero_norm_epsilon.max(0.0);
    if left_sq_sum <= epsilon || right_sq_sum <= epsilon {
        return None;
    }

    Some(dot / (left_sq_sum.sqrt() * right_sq_sum.sqrt()))
}

const SIMD_WIDTH: usize = 8;

/// Pre-packed query representation for repeated L2 squared computations.
#[derive(Debug, Clone)]
pub struct PreparedL2Query {
    len: usize,
    simd_query: Vec<f32x8>,
    tail: [f32; SIMD_WIDTH],
    tail_len: usize,
}

impl PreparedL2Query {
    /// Builds a reusable SIMD-packed query.
    pub fn new(query: &[f32]) -> Self {
        let (simd_query, tail, tail_len) = prepare_simd_query(query);

        Self {
            len: query.len(),
            simd_query,
            tail,
            tail_len,
        }
    }

    /// Computes squared L2 distance against a same-length vector.
    pub fn l2_squared(&self, right: &[f32]) -> f32 {
        debug_assert_eq!(right.len(), self.len);
        let simd_chunks = self.simd_query.len();
        let simd_prefix_len = simd_chunks * SIMD_WIDTH;
        let right_prefix = &right[..simd_prefix_len];

        let mut simd_sum0 = f32x8::ZERO;
        let mut simd_sum1 = f32x8::ZERO;
        let mut chunk_idx = 0usize;
        let mut offset = 0usize;

        while chunk_idx + 1 < simd_chunks {
            let right_v0 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            let delta0 = self.simd_query[chunk_idx] - right_v0;
            simd_sum0 += delta0 * delta0;
            offset += SIMD_WIDTH;

            let right_v1 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            let delta1 = self.simd_query[chunk_idx + 1] - right_v1;
            simd_sum1 += delta1 * delta1;
            offset += SIMD_WIDTH;

            chunk_idx += 2;
        }
        if chunk_idx < simd_chunks {
            let right_v = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            let delta = self.simd_query[chunk_idx] - right_v;
            simd_sum0 += delta * delta;
        }

        let mut scalar_sum = 0.0f32;
        let right_tail = &right[simd_prefix_len..];
        for (index, right_value) in right_tail.iter().enumerate().take(self.tail_len) {
            let delta = self.tail[index] - *right_value;
            scalar_sum += delta * delta;
        }

        (simd_sum0 + simd_sum1).reduce_add() + scalar_sum
    }
}

/// Pre-packed query representation for repeated dot-product computations.
#[derive(Debug, Clone)]
pub struct PreparedDotQuery {
    len: usize,
    simd_query: Vec<f32x8>,
    tail: [f32; SIMD_WIDTH],
    tail_len: usize,
}

impl PreparedDotQuery {
    /// Builds a reusable SIMD-packed query.
    pub fn new(query: &[f32]) -> Self {
        let (simd_query, tail, tail_len) = prepare_simd_query(query);
        Self {
            len: query.len(),
            simd_query,
            tail,
            tail_len,
        }
    }

    /// Computes dot product against a same-length vector.
    pub fn dot_unchecked(&self, right: &[f32]) -> f32 {
        debug_assert_eq!(right.len(), self.len);
        let simd_chunks = self.simd_query.len();
        let simd_prefix_len = simd_chunks * SIMD_WIDTH;
        let right_prefix = &right[..simd_prefix_len];

        let mut simd_sum0 = f32x8::ZERO;
        let mut simd_sum1 = f32x8::ZERO;
        let mut chunk_idx = 0usize;
        let mut offset = 0usize;

        while chunk_idx + 1 < simd_chunks {
            let right_v0 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            simd_sum0 += self.simd_query[chunk_idx] * right_v0;
            offset += SIMD_WIDTH;

            let right_v1 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            simd_sum1 += self.simd_query[chunk_idx + 1] * right_v1;
            offset += SIMD_WIDTH;

            chunk_idx += 2;
        }
        if chunk_idx < simd_chunks {
            let right_v = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            simd_sum0 += self.simd_query[chunk_idx] * right_v;
        }

        let mut scalar_sum = 0.0f32;
        let right_tail = &right[simd_prefix_len..];
        for (index, right_value) in right_tail.iter().enumerate().take(self.tail_len) {
            scalar_sum += self.tail[index] * *right_value;
        }

        (simd_sum0 + simd_sum1).reduce_add() + scalar_sum
    }

    fn dot_and_right_sq_sum_unchecked(&self, right: &[f32]) -> (f32, f32) {
        debug_assert_eq!(right.len(), self.len);
        let simd_chunks = self.simd_query.len();
        let simd_prefix_len = simd_chunks * SIMD_WIDTH;
        let right_prefix = &right[..simd_prefix_len];

        let mut dot_sum0 = f32x8::ZERO;
        let mut dot_sum1 = f32x8::ZERO;
        let mut right_sq_sum0 = f32x8::ZERO;
        let mut right_sq_sum1 = f32x8::ZERO;
        let mut chunk_idx = 0usize;
        let mut offset = 0usize;

        while chunk_idx + 1 < simd_chunks {
            let right_v0 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            dot_sum0 += self.simd_query[chunk_idx] * right_v0;
            right_sq_sum0 += right_v0 * right_v0;
            offset += SIMD_WIDTH;

            let right_v1 = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            dot_sum1 += self.simd_query[chunk_idx + 1] * right_v1;
            right_sq_sum1 += right_v1 * right_v1;
            offset += SIMD_WIDTH;

            chunk_idx += 2;
        }
        if chunk_idx < simd_chunks {
            let right_v = load_f32x8(&right_prefix[offset..offset + SIMD_WIDTH]);
            dot_sum0 += self.simd_query[chunk_idx] * right_v;
            right_sq_sum0 += right_v * right_v;
        }

        let mut dot_scalar = 0.0f32;
        let mut right_sq_scalar = 0.0f32;
        let right_tail = &right[simd_prefix_len..];
        for (index, right_value) in right_tail.iter().enumerate().take(self.tail_len) {
            dot_scalar += self.tail[index] * *right_value;
            right_sq_scalar += right_value * right_value;
        }

        (
            (dot_sum0 + dot_sum1).reduce_add() + dot_scalar,
            (right_sq_sum0 + right_sq_sum1).reduce_add() + right_sq_scalar,
        )
    }
}

/// Pre-packed query representation for repeated cosine computations.
#[derive(Debug, Clone)]
pub struct PreparedCosineQuery {
    dot_query: PreparedDotQuery,
    query_sq_sum: f32,
}

impl PreparedCosineQuery {
    /// Builds a reusable SIMD-packed query.
    pub fn new(query: &[f32]) -> Self {
        let dot_query = PreparedDotQuery::new(query);
        let query_sq_sum = dot_query.dot_unchecked(query);
        Self {
            dot_query,
            query_sq_sum,
        }
    }

    /// Returns the squared L2 norm of the prepared query vector.
    pub fn query_sq_sum(&self) -> f32 {
        self.query_sq_sum
    }

    /// Computes cosine similarity against a same-length vector.
    ///
    /// Returns `None` when one of the vector norms is less than or equal to
    /// `zero_norm_epsilon.max(0.0)`.
    pub fn cosine_unchecked(&self, right: &[f32], zero_norm_epsilon: f32) -> Option<f32> {
        let epsilon = zero_norm_epsilon.max(0.0);
        let (dot, right_sq_sum) = self.dot_query.dot_and_right_sq_sum_unchecked(right);
        if self.query_sq_sum <= epsilon || right_sq_sum <= epsilon {
            return None;
        }
        Some(dot / (self.query_sq_sum.sqrt() * right_sq_sum.sqrt()))
    }
}

fn prepare_simd_query(query: &[f32]) -> (Vec<f32x8>, [f32; SIMD_WIDTH], usize) {
    let mut chunks = query.chunks_exact(SIMD_WIDTH);
    let simd_query = chunks.by_ref().map(load_f32x8).collect();
    let remainder = chunks.remainder();

    let mut tail = [0.0f32; SIMD_WIDTH];
    tail[..remainder.len()].copy_from_slice(remainder);
    (simd_query, tail, remainder.len())
}

fn load_f32x8(values: &[f32]) -> f32x8 {
    debug_assert_eq!(values.len(), SIMD_WIDTH);
    f32x8::from([
        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7],
    ])
}

fn simd_scan(
    left: &[f32],
    right: &[f32],
    mut simd_step: impl FnMut(f32x8, f32x8),
    mut scalar_step: impl FnMut(f32, f32),
) {
    let mut left_chunks = left.chunks_exact(SIMD_WIDTH);
    let mut right_chunks = right.chunks_exact(SIMD_WIDTH);

    for (left_chunk, right_chunk) in left_chunks.by_ref().zip(right_chunks.by_ref()) {
        simd_step(load_f32x8(left_chunk), load_f32x8(right_chunk));
    }

    for (&left_value, &right_value) in left_chunks.remainder().iter().zip(right_chunks.remainder())
    {
        scalar_step(left_value, right_value);
    }
}

fn simd_dot(left: &[f32], right: &[f32]) -> f32 {
    let mut simd_sum = f32x8::ZERO;
    let mut scalar_sum = 0.0;

    simd_scan(
        left,
        right,
        |left_v, right_v| {
            simd_sum += left_v * right_v;
        },
        |left_value, right_value| {
            scalar_sum += left_value * right_value;
        },
    );

    simd_sum.reduce_add() + scalar_sum
}

fn simd_l2_squared(left: &[f32], right: &[f32]) -> f32 {
    let mut simd_sum = f32x8::ZERO;
    let mut scalar_sum = 0.0;

    simd_scan(
        left,
        right,
        |left_v, right_v| {
            let delta = left_v - right_v;
            simd_sum += delta * delta;
        },
        |left_value, right_value| {
            let delta = left_value - right_value;
            scalar_sum += delta * delta;
        },
    );

    simd_sum.reduce_add() + scalar_sum
}

fn simd_dot_and_norms(left: &[f32], right: &[f32]) -> (f32, f32, f32) {
    let mut dot_sum = f32x8::ZERO;
    let mut left_sq_sum = f32x8::ZERO;
    let mut right_sq_sum = f32x8::ZERO;
    let mut dot_scalar = 0.0;
    let mut left_sq_scalar = 0.0;
    let mut right_sq_scalar = 0.0;

    simd_scan(
        left,
        right,
        |left_v, right_v| {
            dot_sum += left_v * right_v;
            left_sq_sum += left_v * left_v;
            right_sq_sum += right_v * right_v;
        },
        |left_value, right_value| {
            dot_scalar += left_value * right_value;
            left_sq_scalar += left_value * left_value;
            right_sq_scalar += right_value * right_value;
        },
    );

    (
        dot_sum.reduce_add() + dot_scalar,
        left_sq_sum.reduce_add() + left_sq_scalar,
        right_sq_sum.reduce_add() + right_sq_scalar,
    )
}

#[cfg(test)]
mod tests;
