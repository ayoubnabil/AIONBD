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
    Ok(simd_dot(left, right))
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

    let (dot, left_sq_sum, right_sq_sum) = simd_dot_and_norms(left, right);

    let epsilon = options.zero_norm_epsilon.max(0.0);
    if left_sq_sum <= epsilon || right_sq_sum <= epsilon {
        return Err(VectorError::ZeroNorm { epsilon });
    }

    Ok(dot / (left_sq_sum.sqrt() * right_sq_sum.sqrt()))
}

const SIMD_WIDTH: usize = 8;

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
