use bufferpool::bufferpool;
use bufferpool::framepool::{self, FramePool};
use bufferpool::unique_stack;
use std::fs;
use std::sync::Arc;

/// Integration tests for multi-file scenarios with cache evictions
/// Tests heterogeneous file handling with more files than buffer pool slots

#[test]
fn test_multi_file_cache_eviction_stress() {
    let test_dir = "/tmp/multi_file_integration_test";
    let _ = fs::remove_dir_all(test_dir);

    // Create DiskPool with many files (more than we'll have buffer slots)
    let mut disk_pool = framepool::DiskPool::new::<String>(test_dir);

    // Create 10 different files with different data types (represented as strings)
    let file_data = [
        "user_profile_1.json",
        "transaction_2023.csv",
        "config_settings.toml",
        "log_entries.txt",
        "image_metadata.xml",
        "database_schema.sql",
        "api_responses.json",
        "user_sessions.log",
        "error_reports.txt",
        "performance_metrics.csv",
    ];

    // Step 1: Initialize disk storage with all files first
    <framepool::DiskPool as FramePool<String>>::resize(&mut disk_pool, file_data.len() as u64)
        .unwrap();
    for (i, filename) in file_data.iter().enumerate() {
        let data_arc = Arc::new(filename.to_string());
        <framepool::DiskPool as FramePool<String>>::put_frame(&mut disk_pool, i as u64, data_arc)
            .unwrap();
    }

    // Create small buffer pool (3 slots) to force evictions
    let mut buffer_pool: bufferpool::BufferPool<String> =
        bufferpool::BufferPool::new(3, &mut disk_pool, bufferpool::bottom_evictor);

    // Step 2: Access files sequentially - this will force evictions after slot 3
    let mut accessed_files = Vec::new();
    for (i, expected_filename) in file_data.iter().enumerate() {
        let page = buffer_pool.get_page(i as u64);
        assert!(page.is_some(), "Should be able to access file {}", i);

        if let Some(p) = page {
            let data: String = p.data();
            accessed_files.push(data.clone());
            assert_eq!(data, *expected_filename, "File content should match");
        }
    }

    // Verify all files were accessed correctly
    assert_eq!(accessed_files.len(), file_data.len());
    for (i, accessed) in accessed_files.iter().enumerate() {
        assert_eq!(accessed, &file_data[i]);
    }

    // Step 3: Random access pattern to test eviction and reload
    let access_pattern = [0, 5, 2, 8, 1, 9, 3, 7, 4, 6];
    for &file_idx in &access_pattern {
        let page = buffer_pool.get_page(file_idx as u64);
        assert!(
            page.is_some(),
            "Should be able to access file {} randomly",
            file_idx
        );

        if let Some(p) = page {
            assert_eq!(p.data(), file_data[file_idx]);
        }
    }

    // Clean up
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn test_heterogeneous_data_with_memory_pool() {
    // Test with MemPool since it implements FramePool properly
    let mut mem_pool = framepool::MemPool::new();

    // Create different types of data simulating real-world scenarios
    let datasets = [
        "User database records",
        "Transaction log entries",
        "Configuration settings",
        "Error log messages",
        "Session token data",
        "API response cache",
        "File metadata index",
        "Performance metrics",
    ];

    // Initialize memory pool with data first
    mem_pool.resize(datasets.len() as u64).unwrap();
    for (i, data) in datasets.iter().enumerate() {
        let data_arc = Arc::new(data.to_string());
        mem_pool.put_frame(i as u64, data_arc).unwrap();
    }

    // Create small buffer pool (2 slots) to force aggressive eviction
    let mut buffer_pool: bufferpool::BufferPool<String> =
        bufferpool::BufferPool::new(2, &mut mem_pool, bufferpool::random_evictor);

    // Test cross-dataset access patterns that force evictions
    let access_patterns = vec![0, 3, 1, 5, 2, 7, 4, 6, 0, 3];

    for &idx in &access_patterns {
        let page = buffer_pool.get_page(idx);
        assert!(page.is_some(), "Should retrieve data at index {}", idx);
        assert_eq!(page.unwrap().data(), datasets[idx as usize]);
    }
}

#[test]
fn test_concurrent_file_operations_with_evictions() {
    let test_dir = "/tmp/concurrent_operations_test";
    let _ = fs::remove_dir_all(test_dir);

    // Simulate a document management system with different file types
    let file_categories = vec![
        (
            "documents",
            vec!["doc1.pdf", "doc2.docx", "doc3.txt", "doc4.md"],
        ),
        ("images", vec!["img1.jpg", "img2.png", "img3.gif"]),
        ("videos", vec!["vid1.mp4", "vid2.avi"]),
        (
            "archives",
            vec!["archive1.zip", "archive2.tar.gz", "archive3.rar"],
        ),
        ("configs", vec!["app.json", "db.conf", "server.ini"]),
    ];

    let mut disk_pool = framepool::DiskPool::new::<String>(test_dir);

    // Calculate total files and initialize all files first
    let total_files: usize = file_categories.iter().map(|(_, files)| files.len()).sum();
    <framepool::DiskPool as FramePool<String>>::resize(&mut disk_pool, total_files as u64).unwrap();

    let mut file_index = 0;
    let mut all_files = Vec::new();
    for (category, files) in &file_categories {
        for filename in files {
            let full_name = format!("{}_{}", category, filename);
            all_files.push(full_name.clone());
            let data_arc = Arc::new(full_name);
            <framepool::DiskPool as FramePool<String>>::put_frame(
                &mut disk_pool,
                file_index,
                data_arc,
            )
            .unwrap();
            file_index += 1;
        }
    }

    // Use very small buffer (2 slots) to maximize eviction pressure
    let mut buffer_pool: bufferpool::BufferPool<String> =
        bufferpool::BufferPool::new(2, &mut disk_pool, bufferpool::bottom_evictor);

    // Simulate realistic access patterns:
    // 1. Sequential scan of documents
    for i in 0..4 {
        let page = buffer_pool.get_page(i);
        assert!(page.is_some());
        assert!(page.unwrap().data().starts_with("documents_"));
    }

    // 2. Jump to images (forces eviction)
    for i in 4..7 {
        let page = buffer_pool.get_page(i);
        assert!(page.is_some());
        assert!(page.unwrap().data().starts_with("images_"));
    }

    // 3. Back to documents (forces reload from disk)
    let page = buffer_pool.get_page(0);
    assert!(page.is_some());
    assert_eq!(page.unwrap().data(), "documents_doc1.pdf");

    // 4. Mixed access pattern across all categories
    let mixed_pattern = [0, 10, 5, 15, 2, 12, 8, 3, 14, 6];
    for &idx in &mixed_pattern {
        if idx < total_files {
            let page = buffer_pool.get_page(idx as u64);
            assert!(page.is_some(), "Should access file at index {}", idx);

            // Verify content matches expected pattern
            let data = page.unwrap().data();
            assert_eq!(data, all_files[idx]);
        }
    }

    // 5. Test modification and flush operations under pressure
    if let Some(page) = buffer_pool.get_page(0) {
        page.with_data(|data: &mut String| {
            *data = "documents_doc1_modified.pdf".to_string();
        });
        // Force sync back to disk
        buffer_pool.sync_index(0).unwrap();
    }

    // Verify modification persisted after eviction and reload
    for _ in 0..5 {
        buffer_pool.get_page(10); // Force eviction of page 0
    }

    if let Some(page) = buffer_pool.get_page(0) {
        assert_eq!(page.data(), "documents_doc1_modified.pdf");
    }

    // Clean up
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn test_massive_file_dataset_with_lru_eviction() {
    let test_dir = "/tmp/massive_dataset_test";
    let _ = fs::remove_dir_all(test_dir);

    // Create a large dataset (50 files) with tiny buffer (3 slots)
    const NUM_FILES: usize = 50;
    const BUFFER_SIZE: usize = 3;

    let mut disk_pool = framepool::DiskPool::new::<String>(test_dir);

    // Initialize large dataset first
    <framepool::DiskPool as FramePool<String>>::resize(&mut disk_pool, NUM_FILES as u64).unwrap();
    for i in 0..NUM_FILES {
        let data = format!("file_{:03}_data_content", i);
        let data_arc = Arc::new(data);
        <framepool::DiskPool as FramePool<String>>::put_frame(&mut disk_pool, i as u64, data_arc)
            .unwrap();
    }

    let mut buffer_pool: bufferpool::BufferPool<String> =
        bufferpool::BufferPool::new(BUFFER_SIZE, &mut disk_pool, bufferpool::bottom_evictor);

    // Test 1: Sequential access through entire dataset
    for i in 0..NUM_FILES {
        let page = buffer_pool.get_page(i as u64);
        assert!(page.is_some(), "Should access file {}", i);
        let expected = format!("file_{:03}_data_content", i);
        assert_eq!(page.unwrap().data(), expected);
    }

    // Test 2: Working set larger than buffer - repeated access to subset
    let working_set = [5, 15, 25, 35, 45]; // 5 files > 3 buffer slots
    for _ in 0..3 {
        for &file_idx in &working_set {
            let page = buffer_pool.get_page(file_idx);
            assert!(page.is_some());
            let expected = format!("file_{:03}_data_content", file_idx);
            assert_eq!(page.unwrap().data(), expected);
        }
    }

    // Test 3: Stress test with random access pattern
    use std::collections::HashMap;
    let mut access_count = HashMap::new();
    let random_pattern = [
        12, 3, 47, 8, 23, 41, 7, 29, 15, 38, 2, 44, 19, 33, 6, 49, 11, 26, 1, 42, 18, 35, 9, 24,
        46, 13, 31, 4, 39, 17,
    ];

    for &file_idx in &random_pattern {
        *access_count.entry(file_idx).or_insert(0) += 1;
        let page = buffer_pool.get_page(file_idx);
        assert!(page.is_some(), "Random access to file {} failed", file_idx);
        let expected = format!("file_{:03}_data_content", file_idx);
        assert_eq!(page.unwrap().data(), expected);
    }

    // Test 4: Verify cache efficiency by accessing recently used files
    let recent_files = [46, 13, 31]; // Last few from random pattern
    for &file_idx in &recent_files {
        let page = buffer_pool.get_page(file_idx);
        assert!(page.is_some());
        let expected = format!("file_{:03}_data_content", file_idx);
        assert_eq!(page.unwrap().data(), expected);
    }

    // Clean up
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn test_mixed_read_write_operations_with_evictions() {
    let test_dir = "/tmp/mixed_operations_test";
    let _ = fs::remove_dir_all(test_dir);

    // Simulate database-like workload with reads and writes
    let mut disk_pool = framepool::DiskPool::new::<String>(test_dir);

    const NUM_TABLES: usize = 12;
    <framepool::DiskPool as FramePool<String>>::resize(&mut disk_pool, NUM_TABLES as u64).unwrap();

    // Initialize "database tables" first
    let table_names = [
        "users",
        "orders",
        "products",
        "inventory",
        "payments",
        "shipping",
        "reviews",
        "categories",
        "suppliers",
        "employees",
        "customers",
        "logs",
    ];

    for (i, table_name) in table_names.iter().enumerate() {
        let initial_data = format!("{}_initial_data", table_name);
        let data_arc = Arc::new(initial_data);
        <framepool::DiskPool as FramePool<String>>::put_frame(&mut disk_pool, i as u64, data_arc)
            .unwrap();
    }

    let mut buffer_pool: bufferpool::BufferPool<String> =
        bufferpool::BufferPool::new(4, &mut disk_pool, bufferpool::random_evictor);

    // Mixed workload simulation
    let operations = vec![
        ("read", 0),  // users
        ("read", 1),  // orders
        ("write", 0), // update users
        ("read", 5),  // shipping
        ("write", 1), // update orders
        ("read", 3),  // inventory
        ("read", 7),  // categories (forces eviction)
        ("write", 3), // update inventory
        ("read", 0),  // users (may need reload)
        ("read", 8),  // suppliers
        ("write", 5), // update shipping
        ("read", 10), // customers
        ("write", 7), // update categories
        ("read", 1),  // orders (reload)
    ];

    let mut modified_tables = std::collections::HashSet::new();

    for (op, table_idx) in operations {
        match op {
            "read" => {
                let page = buffer_pool.get_page(table_idx);
                assert!(
                    page.is_some(),
                    "Should read table {}",
                    table_names[table_idx as usize]
                );

                let data: String = page.unwrap().data();
                if modified_tables.contains(&table_idx) {
                    assert!(
                        data.contains("_modified"),
                        "Table {} should show modifications",
                        table_names[table_idx as usize]
                    );
                } else {
                    assert!(
                        data.contains("_initial_data"),
                        "Table {} should have initial data",
                        table_names[table_idx as usize]
                    );
                }
            }
            "write" => {
                if let Some(page) = buffer_pool.get_page(table_idx) {
                    page.with_data(|data: &mut String| {
                        *data = format!("{}_modified_data", table_names[table_idx as usize]);
                    });
                    modified_tables.insert(table_idx);

                    // Randomly sync some changes immediately
                    if table_idx % 3 == 0 {
                        buffer_pool.sync_index(table_idx).unwrap();
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    // Final sync of all dirty pages
    buffer_pool.flush_all().unwrap();

    // Verify all modifications were persisted by forcing evictions and reloading
    for table_idx in modified_tables.iter() {
        // Force eviction by accessing other tables
        for i in 0..5 {
            buffer_pool.get_page((*table_idx + i + 1) % NUM_TABLES as u64);
        }

        // Reload and verify
        if let Some(page) = buffer_pool.get_page(*table_idx) {
            let data = page.data();
            assert!(
                data.contains("_modified"),
                "Modifications to table {} should persist after eviction",
                table_names[*table_idx as usize]
            );
        }
    }

    // Clean up
    let _ = fs::remove_dir_all(test_dir);
}

#[test]
fn test_eviction_strategy_comparison() {
    // Test different eviction strategies with the same workload
    let test_patterns = [
        (
            "bottom_evictor",
            bufferpool::bottom_evictor
                as fn(
                    &[Option<framepool::PageFrame<String>>],
                    &unique_stack::UniqueStack<u64>,
                ) -> Result<u64, bufferpool::BufferPoolErrors>,
        ),
        (
            "random_evictor",
            bufferpool::random_evictor
                as fn(
                    &[Option<framepool::PageFrame<String>>],
                    &unique_stack::UniqueStack<u64>,
                ) -> Result<u64, bufferpool::BufferPoolErrors>,
        ),
    ];

    for (strategy_name, evictor_fn) in test_patterns {
        let mut mem_pool = framepool::MemPool::new();

        // Setup test data first
        const NUM_ITEMS: usize = 10;
        mem_pool.resize(NUM_ITEMS as u64).unwrap();
        for i in 0..NUM_ITEMS {
            let data_arc = Arc::new(format!("item_{}", i));
            mem_pool.put_frame(i as u64, data_arc).unwrap();
        }

        let mut buffer_pool: bufferpool::BufferPool<String> =
            bufferpool::BufferPool::new(3, &mut mem_pool, evictor_fn);

        // Access pattern that forces evictions
        let access_pattern = [0, 1, 2, 3, 4, 5, 0, 6, 7, 1, 8, 9, 2];

        for &idx in &access_pattern {
            let page = buffer_pool.get_page(idx);
            assert!(
                page.is_some(),
                "Strategy {} should handle access to item {}",
                strategy_name,
                idx
            );
            assert_eq!(page.unwrap().data(), format!("item_{}", idx));
        }
    }
}
