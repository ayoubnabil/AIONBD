use aionbd_core::CollectionConfig;

use super::*;

#[test]
fn index_becomes_incompatible_for_same_len_updates() {
    let mut collection = Collection::new(
        "demo",
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..MIN_INDEXED_POINTS as u64 {
        collection
            .upsert_point(id, vec![id as f32, 0.0])
            .expect("upsert should succeed");
    }

    let index = IvfIndex::build(&collection).expect("index should build");
    assert!(index.is_compatible(&collection));

    collection
        .upsert_point(1, vec![1234.0, 0.0])
        .expect("update should succeed");
    assert!(!index.is_compatible(&collection));
}

#[test]
fn index_becomes_incompatible_when_collection_len_changes() {
    let mut collection = Collection::new(
        "demo",
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..MIN_INDEXED_POINTS as u64 {
        collection
            .upsert_point(id, vec![id as f32, 0.0])
            .expect("upsert should succeed");
    }

    let index = IvfIndex::build(&collection).expect("index should build");
    collection
        .upsert_point(MIN_INDEXED_POINTS as u64 + 1, vec![0.0, 0.0])
        .expect("insert should succeed");
    assert!(!index.is_compatible(&collection));
}

#[test]
fn candidate_slots_reduce_search_space() {
    let mut collection = Collection::new(
        "demo",
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..MIN_INDEXED_POINTS as u64 {
        let cluster_shift = if id < (MIN_INDEXED_POINTS / 2) as u64 {
            0.0
        } else {
            1_000.0
        };
        collection
            .upsert_point(id, vec![cluster_shift + (id % 10) as f32, 0.0])
            .expect("upsert should succeed");
    }

    let index = IvfIndex::build(&collection).expect("index should build");
    let candidate_slots = index.candidate_slots_with_target_recall(&[1_005.0, 0.0], 10, None);
    assert!(!candidate_slots.is_empty());
    assert!(candidate_slots.len() < collection.len());
}

#[test]
fn higher_recall_target_expands_candidate_pool() {
    let mut collection = Collection::new(
        "demo",
        CollectionConfig::new(2, true).expect("config should be valid"),
    )
    .expect("collection should be valid");
    for id in 0..MIN_INDEXED_POINTS as u64 {
        let cluster = (id % 32) as f32;
        collection
            .upsert_point(id, vec![cluster, (id % 7) as f32])
            .expect("upsert should succeed");
    }

    let index = IvfIndex::build(&collection).expect("index should build");
    let low = index.candidate_slots_with_target_recall(&[3.0, 1.0], 20, Some(0.2));
    let high = index.candidate_slots_with_target_recall(&[3.0, 1.0], 20, Some(1.0));
    assert!(!low.is_empty());
    assert!(high.len() >= low.len());
    assert_eq!(high.len(), collection.len());
}
