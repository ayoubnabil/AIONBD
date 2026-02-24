use super::*;

const EPSILON: f32 = 1e-5;

fn approx_eq(left: f32, right: f32) {
    assert!((left - right).abs() < EPSILON, "expected {left} ~= {right}");
}

#[test]
fn dot_product_works() {
    let left = [1.0, 2.0, 3.0];
    let right = [4.0, 5.0, 6.0];
    let score = dot_product(&left, &right).expect("dot product should succeed");
    approx_eq(score, 32.0);
}

#[test]
fn l2_distance_works() {
    let left = [1.0, 2.0, 3.0];
    let right = [1.0, 2.0, 6.0];
    let distance = l2_distance(&left, &right).expect("l2 distance should succeed");
    approx_eq(distance, 3.0);
}

#[test]
fn cosine_similarity_works() {
    let left = [1.0, 0.0];
    let right = [0.0, 1.0];
    let value = cosine_similarity(&left, &right).expect("cosine should succeed");
    approx_eq(value, 0.0);
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
