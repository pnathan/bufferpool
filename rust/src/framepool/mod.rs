use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

struct InnerFrame<T> {
    data: Arc<T>,
    pins: u32,
    dirty: bool,
}

// A frame is a container for data to be written.
pub struct PageFrame<T> {
    mutex: Mutex<InnerFrame<T>>,
}

impl<T> PageFrame<T> {
    pub fn new(data: T) -> Self {
        PageFrame {
            mutex: Mutex::new(InnerFrame {
                data: Arc::new(data),
                pins: 0,
                dirty: false,
            }),
        }
    }

    pub fn new_with_arc(data: Arc<T>) -> Self {
        PageFrame {
            mutex: Mutex::new(InnerFrame {
                data,
                pins: 0,
                dirty: false,
            }),
        }
    }

    pub fn pin(&self) {
        let mut inner = self.mutex.lock().unwrap();
        inner.pins += 1;
    }

    pub fn unpin(&self) {
        let mut inner = self.mutex.lock().unwrap();
        inner.pins -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        let inner = self.mutex.lock().unwrap();
        inner.pins > 0
    }

    pub fn is_dirty(&self) -> bool {
        let inner = self.mutex.lock().unwrap();
        inner.dirty
    }

    pub fn set_dirty(&self, dirty: bool) {
        let mut inner = self.mutex.lock().unwrap();
        inner.dirty = dirty;
    }

    pub fn data(&self) -> T
    where
        T: Clone,
    {
        let inner = self.mutex.lock().unwrap();
        (*inner.data).clone()
    }

    pub fn put(&self, data: T) {
        let mut inner = self.mutex.lock().unwrap();
        inner.data = Arc::new(data);
    }

    // with_data uses copy-on-write semantics for efficient modification
    pub fn with_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
        T: Clone,
    {
        let mut inner = self.mutex.lock().unwrap();
        // Use Arc::make_mut for copy-on-write - only clones if there are other references
        let mut_data = Arc::make_mut(&mut inner.data);
        let result = f(mut_data);
        inner.dirty = true;
        result
    }

    // For read-only access (most common in read-heavy workloads) - zero-copy
    pub fn read_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let inner = self.mutex.lock().unwrap();
        f(&inner.data)
    }

    // Get a clone of the Arc<T> for sharing with the backing store
    pub fn get_data_arc(&self) -> Arc<T> {
        let inner = self.mutex.lock().unwrap();
        Arc::clone(&inner.data)
    }
}

// A FramePool is a pool of, obviously, frames of <T>.
// A frame can be nominally considered to be a "block" of data.
// From a distance, it might be said that a T is really a "Vec<U>", with an upper abstraction, a "slab",
// simply providing an interface that is vec'y.
pub trait FramePool<T>
where
    T: Clone,
{
    fn get_frame_ref(&mut self, idx: u64) -> Result<Arc<T>, String>;
    fn put_frame(&mut self, idx: u64, data: Arc<T>) -> Result<(), String>;
    fn resize(&mut self, count: u64) -> Result<(), String>;
    // internally known size of the pool.
    fn size(&self) -> u64;
    // assess_size retrieves the real-world data size of the pool and updates it
    fn assess_size(&mut self) -> Result<u64, String>;
}

// Storage backend abstraction for different storage systems
pub trait StorageBackend<T>
where
    T: Clone,
{
    fn read(&mut self, key: &str) -> Result<Arc<T>, String>;
    fn write(&mut self, key: &str, data: Arc<T>) -> Result<(), String>;
    fn exists(&self, key: &str) -> bool;
    fn delete(&mut self, key: &str) -> Result<(), String>;
    fn list_keys(&self) -> Result<Vec<String>, String>;
}

// File-based storage backend implementation
pub struct FileBackend {
    base_path: PathBuf,
}

impl FileBackend {
    pub fn new(base_path: &str) -> Self {
        FileBackend {
            base_path: PathBuf::from(base_path),
        }
    }

    fn ensure_directory(&self) -> Result<(), String> {
        if !self.base_path.exists() {
            fs::create_dir_all(&self.base_path)
                .map_err(|e| format!("Failed to create directory: {e}"))?;
        }
        Ok(())
    }

    fn get_file_path(&self, key: &str) -> PathBuf {
        self.base_path.join(format!("{key}.json"))
    }

    // Ergonomic helper methods that don't require explicit type annotations
    pub fn read_data<T>(&mut self, key: &str) -> Result<Arc<T>, String>
    where
        T: Clone + for<'de> Deserialize<'de> + Serialize,
    {
        <Self as StorageBackend<T>>::read(self, key)
    }

    pub fn write_data<T>(&mut self, key: &str, data: Arc<T>) -> Result<(), String>
    where
        T: Clone + for<'de> Deserialize<'de> + Serialize,
    {
        <Self as StorageBackend<T>>::write(self, key, data)
    }

    pub fn data_exists<T>(&self, key: &str) -> bool
    where
        T: Clone + for<'de> Deserialize<'de> + Serialize,
    {
        <Self as StorageBackend<T>>::exists(self, key)
    }

    pub fn delete_data<T>(&mut self, key: &str) -> Result<(), String>
    where
        T: Clone + for<'de> Deserialize<'de> + Serialize,
    {
        <Self as StorageBackend<T>>::delete(self, key)
    }

    pub fn list_data_keys<T>(&self) -> Result<Vec<String>, String>
    where
        T: Clone + for<'de> Deserialize<'de> + Serialize,
    {
        <Self as StorageBackend<T>>::list_keys(self)
    }
}

impl<T> StorageBackend<T> for FileBackend
where
    T: Clone + for<'de> Deserialize<'de> + Serialize,
{
    fn read(&mut self, key: &str) -> Result<Arc<T>, String> {
        self.ensure_directory()?;
        let file_path = self.get_file_path(key);

        let content = fs::read_to_string(&file_path)
            .map_err(|e| format!("Failed to read file {}: {}", file_path.display(), e))?;

        let data: T = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to deserialize data: {e}"))?;

        Ok(Arc::new(data))
    }

    fn write(&mut self, key: &str, data: Arc<T>) -> Result<(), String> {
        self.ensure_directory()?;
        let file_path = self.get_file_path(key);

        let content = serde_json::to_string_pretty(&*data)
            .map_err(|e| format!("Failed to serialize data: {e}"))?;

        fs::write(&file_path, content)
            .map_err(|e| format!("Failed to write file {}: {}", file_path.display(), e))?;

        Ok(())
    }

    fn exists(&self, key: &str) -> bool {
        self.get_file_path(key).exists()
    }

    fn delete(&mut self, key: &str) -> Result<(), String> {
        let file_path = self.get_file_path(key);
        if file_path.exists() {
            fs::remove_file(&file_path)
                .map_err(|e| format!("Failed to delete file {}: {}", file_path.display(), e))?;
        }
        Ok(())
    }

    fn list_keys(&self) -> Result<Vec<String>, String> {
        if !self.base_path.exists() {
            return Ok(Vec::new());
        }

        let entries = fs::read_dir(&self.base_path)
            .map_err(|e| format!("Failed to read directory: {e}"))?;

        let keys = entries
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let filename = entry.file_name();
                filename
                    .to_str()
                    .and_then(|s| s.strip_suffix(".json"))
                    .map(|s| s.to_string())
            })
            .collect();

        Ok(keys)
    }
}

// Implement MemPool, a memory-only FramePool implementation
pub struct MemPool<T> {
    pool: HashMap<u64, Option<PageFrame<T>>>,
}

impl<T> MemPool<T> {
    pub fn new() -> Self {
        MemPool {
            pool: HashMap::new(),
        }
    }
}

impl<T> Default for MemPool<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> FramePool<T> for MemPool<T>
where
    T: Clone,
{
    fn get_frame_ref(&mut self, id: u64) -> Result<Arc<T>, String> {
        match self.pool.get(&id) {
            Some(Some(frame)) => Ok(Arc::clone(&frame.mutex.lock().unwrap().data)),
            Some(None) => Err("Frame slot exists but is empty".to_string()),
            None => Err("No such frame".to_string()),
        }
    }

    fn put_frame(&mut self, idx: u64, data: Arc<T>) -> Result<(), String> {
        let frame = PageFrame::new_with_arc(data);
        self.pool.insert(idx, Some(frame));
        Ok(())
    }

    fn resize(&mut self, count: u64) -> Result<(), String> {
        let old_sz = self.size();
        // from i from 0 to count, insert a None into the pool at pageid = prior_size + i
        for i in 0..count {
            self.pool.insert(old_sz + i, None);
        }
        Ok(())
    }

    fn size(&self) -> u64 {
        self.pool.len() as u64
    }

    fn assess_size(&mut self) -> Result<u64, String> {
        Ok(self.size())
    }
}

pub struct DiskPool {
    initialized: bool,
    dirname: PathBuf,
    size: u64,
}

impl DiskPool {
    pub fn new<T>(dirname: &str) -> Self {
        DiskPool {
            initialized: false,
            dirname: PathBuf::from(dirname),
            size: 0,
        }
    }

    // initialize the pool, if it hasn't been already.
    // this will create the path
    fn initialize(&mut self) -> Result<(), String> {
        if self.initialized {
            return Ok(());
        }
        fs::create_dir_all(&self.dirname).map_err(|_| "Error creating directory".to_string())?;
        self.initialized = true;
        Ok(())
    }

    fn page_path(&self, pageid: u64) -> PathBuf {
        let path = self.dirname.clone();
        path.join(format!("page_{pageid}"))
    }
}

impl<T> FramePool<T> for DiskPool
where
    T: for<'de> Deserialize<'de> + Serialize + Clone,
{
    fn get_frame_ref(&mut self, id: u64) -> Result<Arc<T>, String> {
        self.initialize()?;

        let result: T = fs::read_to_string(self.page_path(id))
            .map_err(|_| "Error reading file".to_string())
            .and_then(|s| {
                serde_json::from_str(&s).map_err(|_| "Error deserializing".to_string())
            })?;

        Ok(Arc::new(result))
    }

    fn put_frame(&mut self, idx: u64, data: Arc<T>) -> Result<(), String> {
        self.initialize()?;

        serde_json::to_string(&*data)
            .map_err(|_| "Error serializing".to_string())
            .and_then(|s| {
                fs::write(self.page_path(idx), s)
                    .map_err(|x| format!("Error writing file: ${x:?}"))
            })
    }

    fn resize(&mut self, count: u64) -> Result<(), String> {
        self.initialize()?;
        let old_sz = <DiskPool as FramePool<T>>::size(self);
        // from i from 0 to count, insert a None into the pool at pageid = prior_size + i
        for i in 0..count {
            let path = self.page_path(old_sz + i);
            let b = path.exists();
            if !b {
                match fs::write(path, "{}") {
                    Ok(_) => (),
                    Err(e) => return Err(format!("Error writing file: {e:?}")),
                }
            }
        }
        self.size = old_sz + count;
        Ok(())
    }

    fn size(&self) -> u64 {
        self.size
    }

    // assess the size of the pool, by counting the number of files in the directory
    fn assess_size(&mut self) -> Result<u64, String> {
        self.initialize()?;

        let count = fs::read_dir(&self.dirname)
            .map_err(|e| format!("Failed to read directory: {e}"))?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("page_"))
            })
            .count() as u64;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;

    #[test]
    fn test_page_frame_new() {
        let frame = PageFrame::new(42);
        assert_eq!(frame.data(), 42);
        assert!(!frame.is_pinned());
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_page_frame_pin_unpin() {
        let frame = PageFrame::new(42);
        assert!(!frame.is_pinned());

        frame.pin();
        assert!(frame.is_pinned());

        frame.pin(); // Pin twice
        assert!(frame.is_pinned());

        frame.unpin();
        assert!(frame.is_pinned()); // Still pinned (count = 1)

        frame.unpin();
        assert!(!frame.is_pinned()); // Now unpinned
    }

    #[test]
    fn test_page_frame_dirty_flag() {
        let frame = PageFrame::new(42);
        assert!(!frame.is_dirty());

        frame.set_dirty(true);
        assert!(frame.is_dirty());

        frame.set_dirty(false);
        assert!(!frame.is_dirty());
    }

    #[test]
    fn test_page_frame_put() {
        let frame = PageFrame::new(42);
        assert_eq!(frame.data(), 42);

        frame.put(100);
        assert_eq!(frame.data(), 100);
    }

    #[test]
    fn test_page_frame_with_data() {
        let frame = PageFrame::new(vec![1, 2, 3]);
        assert!(!frame.is_dirty());

        frame.with_data(|v| {
            v.push(4);
        });

        assert_eq!(frame.data(), vec![1, 2, 3, 4]);
        assert!(frame.is_dirty()); // Should be marked dirty after modification
    }

    #[test]
    fn test_mempool_new() {
        let pool: MemPool<i32> = MemPool::new();
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_mempool_default() {
        let pool: MemPool<i32> = MemPool::default();
        assert_eq!(pool.size(), 0);
    }

    #[test]
    fn test_mempool_read_write() {
        let mut pool = MemPool::new();
        let data_arc = Arc::new(vec![1, 2, 3]);
        pool.put_frame(0, Arc::clone(&data_arc)).unwrap();

        let retrieved_arc = pool.get_frame_ref(0).unwrap();
        assert_eq!(*retrieved_arc, vec![1, 2, 3]);
    }

    #[test]
    fn test_mempool_read_nonexistent() {
        let mut pool: MemPool<i32> = MemPool::new();
        let result = pool.get_frame_ref(0);
        match result {
            Err(e) => assert_eq!(e, "No such frame"),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_mempool_resize() {
        let mut pool: MemPool<i32> = MemPool::new();
        assert_eq!(pool.size(), 0);

        pool.resize(5).unwrap();
        assert_eq!(pool.size(), 5);

        pool.resize(3).unwrap();
        assert_eq!(pool.size(), 8); // 5 + 3
    }

    #[test]
    fn test_mempool_assess_size() {
        let mut pool: MemPool<i32> = MemPool::new();
        pool.resize(10).unwrap();

        let size = pool.assess_size().unwrap();
        assert_eq!(size, 10);
    }

    #[test]
    fn test_mempool_overwrite() {
        let mut pool = MemPool::new();

        let data1 = Arc::new(100);
        pool.put_frame(0, data1).unwrap();

        let data2 = Arc::new(200);
        pool.put_frame(0, data2).unwrap();

        let retrieved_arc = pool.get_frame_ref(0).unwrap();
        assert_eq!(*retrieved_arc, 200);
    }

    #[test]
    fn test_diskpool_new() {
        let pool = DiskPool::new::<i32>("/tmp/test_diskpool_new");
        assert_eq!(pool.size, 0);

        // Clean up
        let _ = fs::remove_dir_all("/tmp/test_diskpool_new");
    }

    #[test]
    fn test_diskpool_read_write() {
        let test_dir = "/tmp/test_diskpool_rw";
        let _ = fs::remove_dir_all(test_dir);

        let mut pool = DiskPool::new::<Vec<i32>>(test_dir);
        let data_arc = Arc::new(vec![1, 2, 3]);
        <DiskPool as FramePool<Vec<i32>>>::put_frame(&mut pool, 0, data_arc).unwrap();

        let retrieved_arc = <DiskPool as FramePool<Vec<i32>>>::get_frame_ref(&mut pool, 0).unwrap();
        assert_eq!(*retrieved_arc, vec![1, 2, 3]);

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_diskpool_read_nonexistent() {
        let test_dir = "/tmp/test_diskpool_nonexist";
        let _ = fs::remove_dir_all(test_dir);

        let mut pool = DiskPool::new::<i32>(test_dir);
        <DiskPool as FramePool<i32>>::resize(&mut pool, 1).unwrap(); // Create directory

        let result = <DiskPool as FramePool<i32>>::get_frame_ref(&mut pool, 5);
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_diskpool_resize() {
        let test_dir = "/tmp/test_diskpool_resize";
        let _ = fs::remove_dir_all(test_dir);

        let mut pool = DiskPool::new::<i32>(test_dir);
        assert_eq!(pool.size, 0);

        <DiskPool as FramePool<i32>>::resize(&mut pool, 3).unwrap();
        assert_eq!(pool.size, 3);

        // Check files were created
        assert!(Path::new(&format!("{test_dir}/page_0")).exists());
        assert!(Path::new(&format!("{test_dir}/page_1")).exists());
        assert!(Path::new(&format!("{test_dir}/page_2")).exists());

        <DiskPool as FramePool<i32>>::resize(&mut pool, 2).unwrap();
        assert_eq!(pool.size, 5); // 3 + 2

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_diskpool_assess_size() {
        let test_dir = "/tmp/test_diskpool_assess";
        let _ = fs::remove_dir_all(test_dir);

        let mut pool = DiskPool::new::<i32>(test_dir);
        <DiskPool as FramePool<i32>>::resize(&mut pool, 5).unwrap();

        let size = <DiskPool as FramePool<i32>>::assess_size(&mut pool).unwrap();
        assert_eq!(size, 5);

        // Manually create another page file
        fs::write(format!("{test_dir}/page_10"), "{}").unwrap();

        let size = <DiskPool as FramePool<i32>>::assess_size(&mut pool).unwrap();
        assert_eq!(size, 6); // Should count the manually created file

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_diskpool_page_path() {
        let pool = DiskPool::new::<u8>("/tmp/x");
        let path = pool.page_path(0);
        assert_eq!(path, PathBuf::from("/tmp/x/page_0"));

        let path = pool.page_path(42);
        assert_eq!(path, PathBuf::from("/tmp/x/page_42"));
    }

    #[test]
    fn test_diskpool_persistence() {
        let test_dir = "/tmp/test_diskpool_persist";
        let _ = fs::remove_dir_all(test_dir);

        // Write data
        {
            let mut pool = DiskPool::new::<String>(test_dir);
            let data_arc = Arc::new("Hello, World!".to_string());
            <DiskPool as FramePool<String>>::put_frame(&mut pool, 0, data_arc).unwrap();
        }

        // Read data in new pool instance
        {
            let mut pool = DiskPool::new::<String>(test_dir);
            let retrieved_arc =
                <DiskPool as FramePool<String>>::get_frame_ref(&mut pool, 0).unwrap();
            assert_eq!(*retrieved_arc, "Hello, World!");
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_page_frame_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let frame = Arc::new(PageFrame::new(0));
        let mut handles = vec![];

        for i in 0..10 {
            let frame_clone = Arc::clone(&frame);
            let handle = thread::spawn(move || {
                frame_clone.pin();
                frame_clone.put(i);
                frame_clone.unpin();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Frame should not be pinned after all threads finish
        assert!(!frame.is_pinned());
    }

    #[test]
    fn test_storage_backend_file_operations() {
        let test_dir = "/tmp/test_storage_backend";
        let _ = fs::remove_dir_all(test_dir);

        let mut backend = FileBackend::new(test_dir);

        // Test write operation
        let data = vec![1, 2, 3, 4, 5];
        let data_arc = Arc::new(data.clone());
        backend.write_data("test_key", data_arc).unwrap();

        // Test exists
        assert!(backend.data_exists::<Vec<i32>>("test_key"));
        assert!(!backend.data_exists::<Vec<i32>>("nonexistent_key"));

        // Test read operation
        let read_arc: Arc<Vec<i32>> = backend.read_data("test_key").unwrap();
        assert_eq!(*read_arc, data);

        // Test list_keys
        let keys = backend.list_data_keys::<Vec<i32>>().unwrap();
        assert_eq!(keys, vec!["test_key"]);

        // Test delete operation
        backend.delete_data::<Vec<i32>>("test_key").unwrap();
        assert!(!backend.data_exists::<Vec<i32>>("test_key"));

        let keys_after_delete = backend.list_data_keys::<Vec<i32>>().unwrap();
        assert!(keys_after_delete.is_empty());

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_storage_backend_multiple_files() {
        let test_dir = "/tmp/test_storage_multi";
        let _ = fs::remove_dir_all(test_dir);

        let mut backend = FileBackend::new(test_dir);

        // Write multiple files
        for i in 0..5 {
            let data = format!("data_{i}");
            let data_arc = Arc::new(data);
            backend.write_data(&format!("key_{i}"), data_arc).unwrap();
        }

        // Test list_keys returns all keys
        let mut keys = backend.list_data_keys::<String>().unwrap();
        keys.sort();
        let expected_keys: Vec<String> = (0..5).map(|i| format!("key_{i}")).collect();
        assert_eq!(keys, expected_keys);

        // Test reading all files
        for i in 0..5 {
            let data_arc: Arc<String> = backend.read_data(&format!("key_{i}")).unwrap();
            assert_eq!(*data_arc, format!("data_{i}"));
        }

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_storage_backend_read_nonexistent() {
        let test_dir = "/tmp/test_storage_nonexist";
        let _ = fs::remove_dir_all(test_dir);

        let mut backend = FileBackend::new(test_dir);

        let result: Result<Arc<String>, String> = backend.read_data("nonexistent_key");
        assert!(result.is_err());

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_storage_backend_delete_nonexistent() {
        let test_dir = "/tmp/test_storage_delete_nonexist";
        let _ = fs::remove_dir_all(test_dir);

        let mut backend = FileBackend::new(test_dir);

        // Deleting nonexistent file should not error
        let result = backend.delete_data::<String>("nonexistent_key");
        assert!(result.is_ok());

        // Clean up
        let _ = fs::remove_dir_all(test_dir);
    }

    #[test]
    fn test_storage_backend_empty_directory() {
        let test_dir = "/tmp/test_storage_empty";
        let _ = fs::remove_dir_all(test_dir);

        let backend = FileBackend::new(test_dir);

        // list_keys on nonexistent directory should return empty vec
        let keys = backend.list_data_keys::<String>().unwrap();
        assert!(keys.is_empty());

        // exists on nonexistent directory should return false
        assert!(!backend.data_exists::<String>("any_key"));

        // Clean up not needed as directory was never created
    }

    #[test]
    fn test_page_frame_read_data() {
        let frame = PageFrame::new(vec![1, 2, 3, 4]);

        let result = frame.read_data(|data| data.iter().sum::<i32>());

        assert_eq!(result, 10);
        assert!(!frame.is_dirty()); // read_data should not mark as dirty
    }

    #[test]
    fn test_page_frame_copy_on_write() {
        let original_data = vec![1, 2, 3];
        let frame = PageFrame::new(original_data.clone());

        // Get Arc reference to track sharing
        let arc_before = frame.get_data_arc();
        assert_eq!(Arc::strong_count(&arc_before), 2); // frame + our reference

        // Modify with with_data - should trigger copy-on-write
        frame.with_data(|data| {
            data.push(4);
        });

        // Original arc should still have the old data (copy-on-write worked)
        assert_eq!(*arc_before, vec![1, 2, 3]);

        // Frame should have the new data
        assert_eq!(frame.data(), vec![1, 2, 3, 4]);
        assert!(frame.is_dirty()); // Should be marked dirty after modification
    }

    #[test]
    fn test_page_frame_new_with_arc() {
        let data = vec![10, 20, 30];
        let data_arc = Arc::new(data.clone());
        let frame = PageFrame::new_with_arc(Arc::clone(&data_arc));

        assert_eq!(frame.data(), data);
        assert!(!frame.is_dirty());
        assert!(!frame.is_pinned());

        // Both the original arc and the frame should reference the same data
        assert_eq!(Arc::strong_count(&data_arc), 2);
    }

    #[test]
    fn test_diskpool_size_access() {
        let temp_dir = "/tmp/test_diskpool_size";
        let _ = fs::remove_dir_all(temp_dir);

        let pool = DiskPool::new::<String>(temp_dir);
        // Test size method - we can access it through the struct field
        assert_eq!(pool.size, 0);

        // Clean up
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_filebackend_get_file_path() {
        let test_dir = "/tmp/test_filebackend_path";
        let backend = FileBackend::new(test_dir);

        // Test get_file_path method
        let path = backend.get_file_path("test_key");
        let expected = format!("{test_dir}/test_key.json");
        assert_eq!(path.to_str().unwrap(), expected);
    }

    #[test]
    fn test_page_frame_get_data_arc() {
        let frame = PageFrame::new(vec![42, 43, 44]);
        let arc = frame.get_data_arc();
        assert_eq!(*arc, vec![42, 43, 44]);
    }
}
