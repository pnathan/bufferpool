use bufferpool::collection::{Collection, DataSource};
use bufferpool::framepool::{FramePool, MemPool};
use std::sync::Arc;

#[test]
fn test_collection_initialization_and_len() {
    let data1: Vec<Arc<String>> = (0..10)
        .map(|i| Arc::new(format!("data1_{}", i)))
        .collect();
    let data2: Vec<Arc<String>> = (0..5)
        .map(|i| Arc::new(format!("data2_{}", i)))
        .collect();

    let source1 = DataSource::new(data1, 3);
    let source2 = DataSource::new(data2, 2);

    let mut pool = MemPool::new();
    let collection = Collection::new(vec![source1, source2], &mut pool, 2).unwrap();

    assert_eq!(collection.len(), 15);
}

#[test]
fn test_collection_get() {
    let data1: Vec<Arc<String>> = (0..10)
        .map(|i| Arc::new(format!("data1_{}", i)))
        .collect();
    let data2: Vec<Arc<String>> = (0..5)
        .map(|i| Arc::new(format!("data2_{}", i)))
        .collect();

    let source1 = DataSource::new(data1, 3); // 4 pages
    let source2 = DataSource::new(data2, 2); // 3 pages

    let mut pool = MemPool::new();
    let collection = Collection::new(vec![source1, source2], &mut pool, 2).unwrap();

    // Test indexing within the first data source
    assert_eq!(*collection.get(0).unwrap(), "data1_0");
    assert_eq!(*collection.get(3).unwrap(), "data1_3"); // Crosses a page boundary
    assert_eq!(*collection.get(9).unwrap(), "data1_9");

    // Test indexing into the second data source
    assert_eq!(*collection.get(10).unwrap(), "data2_0");
    assert_eq!(*collection.get(12).unwrap(), "data2_2"); // Crosses a page boundary
    assert_eq!(*collection.get(14).unwrap(), "data2_4");

    // Test out of bounds
    assert!(collection.get(15).is_none());
}

#[test]
fn test_collection_iteration() {
    let data1: Vec<Arc<String>> = (0..10)
        .map(|i| Arc::new(format!("data1_{}", i)))
        .collect();
    let data2: Vec<Arc<String>> = (0..5)
        .map(|i| Arc::new(format!("data2_{}", i)))
        .collect();

    let source1 = DataSource::new(data1.clone(), 3);
    let source2 = DataSource::new(data2.clone(), 2);

    let mut pool = MemPool::new();
    // Use a small buffer pool to force eviction
    let collection = Collection::new(vec![source1, source2], &mut pool, 2).unwrap();

    let mut expected_data = Vec::new();
    expected_data.extend(data1);
    expected_data.extend(data2);

    let collected_data: Vec<Arc<String>> = collection.iter().collect();

    assert_eq!(collected_data.len(), 15);
    assert_eq!(collected_data, expected_data);
}

#[test]
fn test_collection_into_iter() {
    let data1: Vec<Arc<String>> = (0..8)
        .map(|i| Arc::new(format!("d1_{}", i)))
        .collect();
    let data2: Vec<Arc<String>> = (0..6)
        .map(|i| Arc::new(format!("d2_{}", i)))
        .collect();

    let source1 = DataSource::new(data1.clone(), 4);
    let source2 = DataSource::new(data2.clone(), 3);

    let mut pool = MemPool::new();
    let collection = Collection::new(vec![source1, source2], &mut pool, 2).unwrap();

    let mut expected_data = Vec::new();
    expected_data.extend(data1);
    expected_data.extend(data2);

    // Use `into_iter()` which is called by `for ... in &collection`
    let mut collected_data = vec![];
    for item in &collection {
        collected_data.push(item);
    }

    assert_eq!(collected_data, expected_data);
}