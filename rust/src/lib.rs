//! # BufferPool - High-Performance Memory Management with Cache Eviction
//!
//! A Rust implementation of a buffer pool system with pluggable eviction strategies,
//! supporting both in-memory and persistent storage backends.
//!
//! ## Features
//!
//! - **Flexible Storage Backends**: Memory-based (`MemPool`) and disk-based (`DiskPool`) frame pools
//! - **Pluggable Eviction Strategies**: Bottom eviction and random eviction algorithms
//! - **Copy-on-Write Semantics**: Efficient data sharing with Arc-based memory management
//! - **Thread Safety**: Safe concurrent access with proper synchronization
//! - **Comprehensive Testing**: Integration tests with forced cache evictions and benchmarking
//!
//! ## Basic Usage
//!
//! ```rust
//! use std::sync::Arc;
//! use bufferpool::bufferpool::BufferPool;
//! use bufferpool::framepool::{MemPool, FramePool};
//!
//! // Create a memory-based frame pool
//! let mut frame_pool = MemPool::new();
//! frame_pool.resize(100).unwrap(); // Allocate space for 100 items
//!
//! // Initialize with some data
//! for i in 0..10 {
//!     let data = Arc::new(format!("Item {}", i));
//!     frame_pool.put_frame(i, data).unwrap();
//! }
//!
//! // Create a buffer pool with 3 slots using bottom eviction strategy
//! let mut buffer_pool = BufferPool::new(
//!     3,
//!     &mut frame_pool,
//!     bufferpool::bufferpool::bottom_evictor
//! );
//!
//! // Access pages - first 3 will be cached, 4th will cause eviction
//! for i in 0..5 {
//!     if let Some(page) = buffer_pool.get_page(i) {
//!         println!("Page {}: {}", i, page.data());
//!     }
//! }
//!
//! // Modify data with copy-on-write semantics
//! if let Some(page) = buffer_pool.get_page(0) {
//!     page.with_data(|data: &mut String| {
//!         *data = "Modified Item 0".to_string();
//!     });
//!     // Mark as dirty and sync back to frame pool
//!     buffer_pool.sync_index(0).unwrap();
//! }
//! ```
//!
//! ## Iterator Support
//!
//! BufferPool implements iterator support for seamless data traversal with transparent caching:
//!
//! ```rust
//! use std::sync::Arc;
//! use bufferpool::bufferpool::BufferPool;
//! use bufferpool::framepool::{MemPool, FramePool};
//!
//! // Create and populate a frame pool
//! let mut frame_pool = MemPool::new();
//! frame_pool.resize(10).unwrap();
//!
//! for i in 0..10 {
//!     let data = Arc::new(format!("Data item {}", i));
//!     frame_pool.put_frame(i, data).unwrap();
//! }
//!
//! // Create a small buffer pool to demonstrate caching
//! let mut buffer_pool = BufferPool::new(
//!     3, // Only 3 slots in cache
//!     &mut frame_pool,
//!     bufferpool::bufferpool::bottom_evictor
//! );
//!
//! // Iterate over all data - caching and eviction happens transparently
//! for data in &mut buffer_pool {
//!     println!("Item: {}", data);
//! }
//!
//! // Or collect into a Vec
//! let mut buffer_pool2 = BufferPool::new(3, &mut frame_pool, bufferpool::bufferpool::bottom_evictor);
//! let all_data: Vec<String> = (&mut buffer_pool2).into_iter().collect();
//! assert_eq!(all_data.len(), 10);
//! ```
//!
//! The iterator yields the actual data `T` from each frame, not the frames themselves.
//! The BufferPool handles all caching, loading, and eviction transparently during iteration.
//!
//! ## Advanced Usage with Disk Storage
//!
//! ```rust
//! use std::sync::Arc;
//! use bufferpool::bufferpool::BufferPool;
//! use bufferpool::framepool::{DiskPool, FramePool};
//!
//! // Create a disk-based frame pool
//! let mut disk_pool = DiskPool::new::<String>("/tmp/buffer_test");
//! <DiskPool as FramePool<String>>::resize(&mut disk_pool, 1000).unwrap();
//!
//! // Store data that will persist to disk
//! for i in 0..50 {
//!     let data = Arc::new(format!("Persistent data {}", i));
//!     <DiskPool as FramePool<String>>::put_frame(&mut disk_pool, i, data).unwrap();
//! }
//!
//! // Create buffer pool with random eviction strategy
//! let mut buffer_pool: BufferPool<String> = BufferPool::new(
//!     5,
//!     &mut disk_pool,
//!     bufferpool::bufferpool::random_evictor
//! );
//!
//! // Access patterns that exceed buffer capacity
//! let access_pattern = [0, 10, 20, 30, 40, 5, 15, 25, 35, 45];
//! for &idx in &access_pattern {
//!     if let Some(page) = buffer_pool.get_page(idx) {
//!         println!("Accessed: {}", page.data());
//!     }
//! }
//!
//! // Flush all dirty pages back to storage
//! buffer_pool.flush_all().unwrap();
//! ```
//!
//! ## Eviction Strategies
//!
//! The buffer pool supports different eviction strategies:
//!
//! - **`bottom_evictor`**: Evicts the page at the bottom of the internal stack
//! - **`random_evictor`**: Randomly selects a page for eviction
//!
//! Custom eviction strategies can be implemented by providing a function with the signature:
//! ```rust
//! fn custom_evictor<T>(
//!     slots: &[Option<bufferpool::framepool::PageFrame<T>>],
//!     lru_stack: &bufferpool::unique_stack::UniqueStack<u64>
//! ) -> Result<u64, bufferpool::bufferpool::BufferPoolErrors>
//! where T: Clone
//! {
//!     // Your eviction logic here
//!     Ok(0) // Return index of slot to evict
//! }
//! ```
//!
//! ## Performance Analysis
//!
//! The crate includes comprehensive benchmarking tools:
//!
//! ```bash
//! # Run standalone performance analysis
//! cargo run --bin benchmark_runner
//!
//! # Run criterion benchmarks
//! cargo bench
//! ```
//!
//! ## Integration Testing
//!
//! Run multi-file integration tests that exceed buffer capacity:
//! ```bash
//! cargo test --test multi_file_integration_test
//! ```

pub mod bufferpool;
pub mod framepool;
pub mod unique_stack;
