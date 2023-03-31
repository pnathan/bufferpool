use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

// the Innerframe contains the data for a Frame's mutex.
#[derive(Serialize, Deserialize)]
struct InnerFrame<T>
where
    T: Clone,
{
    data: T,
    pins: u32,
    dirty: bool,
}

// A frame is a container for data to be written.
pub struct PageFrame<T>
where
    T: Clone,
{
    mutex: Mutex<InnerFrame<T>>,
}

impl<T> PageFrame<T>
where
    T: Clone,
{
    pub fn new(data: T) -> Self {
        PageFrame {
            mutex: Mutex::new(InnerFrame {
                data: data,
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

    pub fn data(&self) -> T {
        let inner = self.mutex.lock().unwrap();
        let d = inner.data.clone();
        d
    }

    pub fn put(&self, data: T) {
        let mut inner = self.mutex.lock().unwrap();
        inner.data = data;
    }

    // with_data is the designated function that allows you to modify the data in the frame.
    pub fn with_data<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut inner = self.mutex.lock().unwrap();
        f(&mut inner.data);
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
    fn read_frame(&mut self, idx: u64) -> Result<PageFrame<T>, String>;
    fn write_frame(&mut self, idx: u64, data: Box<PageFrame<T>>) -> Result<(), String>;
    fn resize(&mut self, count: u64) -> Result<(), String>;
    // internally known size of the pool.
    fn size(&self) -> u64;
    // assess_size retrieves the real-world data size of the pool and updates it
    fn assess_size(&mut self) -> Result<u64, String>;
}

// Implement MemPool, a memory-only FramePool implementation
pub struct MemPool<T>
where
    T: Clone,
{
    pool: HashMap<u64, Option<PageFrame<T>>>,
}

impl<'a, T> MemPool<T>
where
    T: Clone,
{
    pub fn new() -> Self {
        MemPool {
            pool: HashMap::new(),
        }
    }
}

impl<T> FramePool<T> for MemPool<T>
where
    T: Clone,
{
    fn read_frame(&mut self, id: u64) -> Result<PageFrame<T>, String> {
        let element = match self.pool.get(&id) {
            Some(t) => t,
            None => &None,
        };
        match element {
            Some(t) => {
                let data = t.clone();
                // haxxx.
                Ok(PageFrame::new(data.data()))
            }
            None => Err("No such frame".to_string()),
        }
    }

    fn write_frame(&mut self, idx: u64, data: Box<PageFrame<T>>) -> Result<(), String> {
        self.pool.insert(idx, Some(*data));
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

struct DiskPool {
    initialized: bool,
    dirname: PathBuf,
    size: u64,
}

impl DiskPool {
    fn new<T>(dirname: &str) -> Self {
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
        path.join(format!("page_{}", pageid))
    }
}

impl<'a, T> FramePool<T> for DiskPool
where
    T: Clone + for<'b> Deserialize<'b> + Serialize,
{
    fn read_frame(&mut self, id: u64) -> Result<PageFrame<T>, String> {
        if let Err(e) = self.initialize() {
            return Err(e);
        }

        let result: T = fs::read_to_string(self.page_path(id))
            .map_err(|_| "Error reading file".to_string())
            .and_then(|s| {
                serde_json::from_str(&s).map_err(|_| "Error deserializing".to_string())
            })?;

        Ok(PageFrame::new(result))
    }

    fn write_frame(&mut self, idx: u64, data: Box<PageFrame<T>>) -> Result<(), String> {
        if let Err(e) = self.initialize() {
            return Err(e);
        }
        let d = data.data();
        serde_json::to_string(&d)
            .map_err(|_| "Error serializing".to_string())
            .and_then(|s| {
                fs::write(self.page_path(idx), s)
                    .map_err(|x| format!("Error writing file: ${:?}", x))
            })
    }

    fn resize(&mut self, count: u64) -> Result<(), String> {
        if let Err(e) = self.initialize() {
            return Err(e);
        }
        let old_sz = <DiskPool as FramePool<T>>::size(self);
        // from i from 0 to count, insert a None into the pool at pageid = prior_size + i
        for i in 0..count {
            let path = self.page_path(old_sz + i);
            let b = path.exists();
            if !b {
                match fs::write(path, "{}") {
                    Ok(_) => (),
                    Err(e) => return Err(format!("Error writing file: {:?}", e)),
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
        if let Err(e) = self.initialize() {
            return Err(e);
        }

        let paths = fs::read_dir(self.dirname.clone()).unwrap();
        let mut count = 0;
        for p in paths {
            if let Ok(p) = p {
                if let Some(s) = p.path().to_str() {
                    if s.starts_with("page_") {
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_mempool_happy() {
        let mut pool = MemPool::new();
        let frame = PageFrame::new(vec![1, 2, 3]);
        pool.write_frame(0, Box::new(frame)).unwrap();
        let frame = pool.read_frame(0).unwrap();
        assert_eq!(frame.data(), vec![1, 2, 3]);
    }
    #[test]
    fn test_diskpool_happy() {
        let mut pool = DiskPool::new::<i32>("/tmp/x");
        let frame: PageFrame<Vec<i32>> = PageFrame::new(vec![1, 2, 3]);
        match pool.write_frame(0, Box::new(frame)) {
            Ok(_) => (),
            Err(e) => {
                panic!("{}", e)
            }
        }
        let frame: PageFrame<Vec<i32>> = pool.read_frame(0).unwrap();
        assert_eq!(frame.data(), vec![1, 2, 3]);
        assert_ne!(frame.data(), vec![1, 2, 4]);
    }
    // Test page_path for DiskPool
    #[test]
    fn test_page_path() {
        let pool = DiskPool::new::<u8>("/tmp/x");
        let path = pool.page_path(0);
        assert_eq!(path, PathBuf::from("/tmp/x/page_0"));
    }
}
