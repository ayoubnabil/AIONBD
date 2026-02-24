use std::error::Error;
use std::fmt;

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
    Ok(left.iter().zip(right).map(|(l, r)| l * r).sum())
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
    validate_vectors(left, right, options)?;
    let squared_sum: f32 = left
        .iter()
        .zip(right)
        .map(|(l, r)| {
            let delta = l - r;
            delta * delta
        })
        .sum();
    Ok(squared_sum.sqrt())
}

/// Computes cosine similarity using strict validation.
pub fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32, VectorError> {
    cosine_similarity_with_options(left, right, VectorValidationOptions::strict())
}

/// Computes cosine similarity with custom validation options.
///
/// The implementation uses a single pass to accumulate dot and squared norms.
pub fn cosine_similarity_with_options(
    left: &[f32],
    right: &[f32],
    options: VectorValidationOptions,
) -> Result<f32, VectorError> {
    validate_vectors(left, right, options)?;

    let mut dot = 0.0f32;
    let mut left_sq_sum = 0.0f32;
    let mut right_sq_sum = 0.0f32;

    for (left_value, right_value) in left.iter().zip(right) {
        dot += left_value * right_value;
        left_sq_sum += left_value * left_value;
        right_sq_sum += right_value * right_value;
    }

    let epsilon = options.zero_norm_epsilon.max(0.0);
    if left_sq_sum <= epsilon || right_sq_sum <= epsilon {
        return Err(VectorError::ZeroNorm { epsilon });
    }

    Ok(dot / (left_sq_sum.sqrt() * right_sq_sum.sqrt()))
}

#[cfg(test)]
mod tests;
