use super::*;

const EPSILON: f32 = 1e-5;

fn approx_eq(left: f32, right: f32) {
    assert!((left - right).abs() < EPSILON, "expected {left} ~= {right}");
}

fn approx_eq_tol(left: f32, right: f32, epsilon: f32) {
    assert!((left - right).abs() < epsilon, "expected {left} ~= {right}");
}

fn deterministic_vector(seed: usize, len: usize) -> Vec<f32> {
    (0..len)
        .map(|index| {
            let mixed = seed
                .wrapping_mul(1_103_515_245)
                .wrapping_add(index.wrapping_mul(12_345))
                .wrapping_add(97);
            let base = (mixed % 10_000) as f32 / 5_000.0;
            base - 1.0
        })
        .collect()
}

#[test]
fn dot_product_works() {
    let left = [1.0, 2.0, 3.0];
    let right = [4.0, 5.0, 6.0];
    let score = dot_product(&left, &right).expect("dot product should succeed");
    approx_eq(score, 32.0);
    let unchecked = dot_product_unchecked(&left, &right);
    approx_eq(unchecked, score);
}

#[test]
fn l2_distance_works() {
    let left = [1.0, 2.0, 3.0];
    let right = [1.0, 2.0, 6.0];
    let distance = l2_distance(&left, &right).expect("l2 distance should succeed");
    approx_eq(distance, 3.0);
}

#[test]
fn l2_squared_works_without_sqrt_roundtrip() {
    let left = [1.0, 2.0, 3.0];
    let right = [1.0, 2.0, 6.0];
    let squared = l2_squared_with_options(&left, &right, VectorValidationOptions::strict())
        .expect("l2 squared should succeed");
    approx_eq(squared, 9.0);
}

#[test]
fn cosine_similarity_works() {
    let left = [1.0, 0.0];
    let right = [0.0, 1.0];
    let value = cosine_similarity(&left, &right).expect("cosine should succeed");
    approx_eq(value, 0.0);
    let unchecked =
        cosine_similarity_unchecked(&left, &right, f32::EPSILON).expect("must be defined");
    approx_eq(unchecked, value);
}

#[test]
fn cosine_unchecked_returns_none_for_zero_norm() {
    let left = [0.0, 0.0];
    let right = [1.0, 2.0];
    assert!(cosine_similarity_unchecked(&left, &right, f32::EPSILON).is_none());
}

#[test]
fn prepared_dot_matches_dot_product() {
    for len in [1usize, 2, 3, 7, 8, 9, 16, 17, 64, 65, 127, 128, 129] {
        let query = deterministic_vector(17, len);
        let candidate = deterministic_vector(31, len);
        let prepared = PreparedDotQuery::new(&query);
        let prepared_value = prepared.dot_unchecked(&candidate);
        let reference = dot_product(&query, &candidate).expect("dot must succeed");
        approx_eq_tol(prepared_value, reference, 1e-3);
    }
}

#[test]
fn prepared_cosine_matches_cosine_similarity() {
    for len in [1usize, 2, 3, 7, 8, 9, 16, 17, 64, 65, 127, 128, 129] {
        let query = deterministic_vector(23, len);
        let candidate = deterministic_vector(47, len);
        let prepared = PreparedCosineQuery::new(&query);
        let prepared_value = prepared
            .cosine_unchecked(&candidate, f32::EPSILON)
            .expect("cosine should be defined");
        let reference = cosine_similarity(&query, &candidate).expect("cosine must succeed");
        approx_eq_tol(prepared_value, reference, 1e-4);
    }
}

#[test]
fn prepared_cosine_returns_none_for_zero_norm() {
    let query = [0.0f32, 0.0];
    let candidate = [1.0f32, 2.0];
    let prepared = PreparedCosineQuery::new(&query);
    assert!(prepared
        .cosine_unchecked(&candidate, f32::EPSILON)
        .is_none());
}

#[test]
fn errors_on_dimension_mismatch() {
    let error = dot_product(&[1.0, 2.0], &[1.0]).expect_err("must fail");
    assert!(matches!(
        error,
        VectorError::DimensionMismatch { left: 2, right: 1 }
    ));
}

#[test]
fn errors_on_empty_vectors() {
    let error = l2_distance(&[], &[]).expect_err("must fail");
    assert!(matches!(error, VectorError::EmptyVector));
}

#[test]
fn cosine_errors_on_zero_norm() {
    let error = cosine_similarity(&[0.0, 0.0], &[1.0, 2.0]).expect_err("must fail");
    assert!(matches!(error, VectorError::ZeroNorm { .. }));
}

#[test]
fn strict_mode_rejects_nan() {
    let error = dot_product_with_options(
        &[f32::NAN, 1.0],
        &[1.0, 1.0],
        VectorValidationOptions::strict(),
    )
    .expect_err("must fail");
    assert!(matches!(
        error,
        VectorError::NonFinite {
            side: VectorSide::Left,
            index: 0,
            ..
        }
    ));
}

#[test]
fn strict_mode_rejects_inf() {
    let error = dot_product_with_options(
        &[1.0, 1.0],
        &[f32::INFINITY, 1.0],
        VectorValidationOptions::strict(),
    )
    .expect_err("must fail");
    assert!(matches!(
        error,
        VectorError::NonFinite {
            side: VectorSide::Right,
            index: 0,
            ..
        }
    ));
}

#[test]
fn permissive_mode_allows_non_finite() {
    let value = dot_product_with_options(
        &[f32::NAN, 1.0],
        &[1.0, 1.0],
        VectorValidationOptions::permissive(),
    )
    .expect("must not fail in permissive mode");
    assert!(value.is_nan());
}

#[test]
fn l2_identity_and_symmetry() {
    let a = [1.5, -2.0, 4.25];
    let b = [-5.0, 3.0, 0.125];
    let aa = l2_distance(&a, &a).expect("must succeed");
    let ab = l2_distance(&a, &b).expect("must succeed");
    let ba = l2_distance(&b, &a).expect("must succeed");
    approx_eq(aa, 0.0);
    approx_eq(ab, ba);
}

#[test]
fn large_dimension_smoke() {
    let left = vec![1.0f32; 4096];
    let right = vec![2.0f32; 4096];
    let value = dot_product(&left, &right).expect("must succeed");
    approx_eq(value, 8192.0);
}

#[test]
fn simd_paths_match_scalar_reference_across_varied_dimensions() {
    for len in [1usize, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 127, 128, 129] {
        let left = deterministic_vector(11, len);
        let right = deterministic_vector(29, len);

        let dot = dot_product(&left, &right).expect("dot must succeed");
        let dot_reference: f32 = left.iter().zip(&right).map(|(l, r)| l * r).sum();
        approx_eq_tol(dot, dot_reference, 1e-3);

        let l2 = l2_distance(&left, &right).expect("l2 must succeed");
        let l2_reference = left
            .iter()
            .zip(&right)
            .map(|(l, r)| {
                let delta = l - r;
                delta * delta
            })
            .sum::<f32>()
            .sqrt();
        approx_eq_tol(l2, l2_reference, 1e-4);

        let cosine = cosine_similarity(&left, &right).expect("cosine must succeed");
        let dot_ref: f32 = left.iter().zip(&right).map(|(l, r)| l * r).sum();
        let left_norm: f32 = left.iter().map(|value| value * value).sum::<f32>().sqrt();
        let right_norm: f32 = right.iter().map(|value| value * value).sum::<f32>().sqrt();
        let cosine_reference = dot_ref / (left_norm * right_norm);
        approx_eq_tol(cosine, cosine_reference, 1e-4);
    }
}
