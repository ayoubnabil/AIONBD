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
fn point_ids_page_after_respects_cursor_and_limit() {
    let mut collection = new_collection(true);
    for id in [10_u64, 30, 50, 70] {
        collection
            .upsert_point(id, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
    }

    let (first_page, first_next) = collection.point_ids_page_after(None, 2);
    assert_eq!(first_page, vec![10, 30]);
    assert_eq!(first_next, Some(30));

    let (second_page, second_next) = collection.point_ids_page_after(first_next, 2);
    assert_eq!(second_page, vec![50, 70]);
    assert_eq!(second_next, None);
}

#[test]
fn point_ids_page_after_handles_missing_cursor_and_zero_limit() {
    let mut collection = new_collection(true);
    for id in [10_u64, 20, 30] {
        collection
            .upsert_point(id, vec![1.0, 2.0, 3.0])
            .expect("must succeed");
    }

    let (empty_page, empty_next) = collection.point_ids_page_after(Some(999), 2);
    assert!(empty_page.is_empty());
    assert_eq!(empty_next, None);

    let (zero_page, zero_next) = collection.point_ids_page_after(None, 0);
    assert!(zero_page.is_empty());
    assert_eq!(zero_next, None);
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

#[test]
fn mutation_version_only_changes_on_mutations() {
    let mut collection = new_collection(true);
    assert_eq!(collection.mutation_version(), 0);

    collection
        .upsert_point(1, vec![1.0, 2.0, 3.0])
        .expect("must succeed");
    let after_insert = collection.mutation_version();
    assert_eq!(after_insert, 1);

    let _ = collection.get_point(1);
    assert_eq!(collection.mutation_version(), after_insert);

    let _ = collection.remove_point(9999);
    assert_eq!(collection.mutation_version(), after_insert);

    let _ = collection.remove_point(1);
    assert_eq!(collection.mutation_version(), after_insert + 1);
}

#[test]
fn mutation_version_saturates_instead_of_wrapping() {
    let mut collection = new_collection(true);
    collection.mutation_version = u64::MAX - 1;

    collection
        .upsert_point(1, vec![1.0, 2.0, 3.0])
        .expect("must succeed");
    assert_eq!(collection.mutation_version(), u64::MAX);

    let _ = collection.remove_point(1);
    assert_eq!(collection.mutation_version(), u64::MAX);
}

#[test]
fn supports_payload_and_rejects_empty_payload_key() {
    let mut collection = new_collection(true);
    let mut payload = std::collections::BTreeMap::new();
    payload.insert(
        "tenant".to_string(),
        crate::MetadataValue::String("edge".to_string()),
    );

    let inserted = collection
        .upsert_point_with_payload(7, vec![1.0, 2.0, 3.0], payload.clone())
        .expect("upsert with payload should succeed");
    assert!(inserted);

    assert_eq!(collection.get_payload(7), Some(&payload));

    let mut invalid_payload = std::collections::BTreeMap::new();
    invalid_payload.insert(" ".to_string(), crate::MetadataValue::Bool(true));
    let error = collection
        .upsert_point_with_payload(8, vec![1.0, 2.0, 3.0], invalid_payload)
        .expect_err("payload with blank key must fail");
    assert!(matches!(error, CollectionError::InvalidPayloadKey));
}
