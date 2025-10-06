use crate::bufferpool::{bottom_evictor, BufferPool};
use crate::framepool::FramePool;
use std::cell::RefCell;
use std::sync::Arc;

/// A DataSource encapsulates a vector of data items and a stride.
/// The stride determines how many items are packed into a single page
/// in the buffer pool.
pub struct DataSource<T>
where
    T: Clone,
{
    pub data: Vec<Arc<T>>,
    pub stride: usize,
}

impl<T> DataSource<T>
where
    T: Clone,
{
    /// Creates a new DataSource.
    pub fn new(data: Vec<Arc<T>>, stride: usize) -> Self {
        DataSource { data, stride }
    }
}

#[derive(Debug)]
struct SourceInfo {
    start_page: u64,
    stride: usize,
    item_count: usize,
    page_count: u64,
}

/// A Collection provides a unified, iterable, and indexable view over
/// multiple data sources, backed by a buffer pool for efficient caching.
pub struct Collection<'a, T>
where
    T: Clone,
{
    buffer_pool: RefCell<BufferPool<'a, Vec<Arc<T>>>>,
    source_info: Vec<SourceInfo>,
    total_items: usize,
}

impl<'a, T> Collection<'a, T>
where
    T: Clone,
{
    /// Creates a new Collection.
    ///
    /// It takes a vector of `DataSource`s, populates the provided `FramePool`,
    /// and sets up a `BufferPool` to manage access.
    pub fn new(
        sources: Vec<DataSource<T>>,
        pool: &'a mut dyn FramePool<Vec<Arc<T>>>,
        buffer_pool_size: u64,
    ) -> Result<Self, String> {
        let mut source_info = Vec::new();
        let mut total_items = 0;
        let mut page_counter = 0;

        for source in sources {
            let item_count = source.data.len();
            let stride = source.stride;
            if stride == 0 {
                // A stride of 0 is invalid.
                continue;
            }
            let page_count = ((item_count + stride - 1) / stride) as u64;

            let info = SourceInfo {
                start_page: page_counter,
                stride,
                item_count,
                page_count,
            };
            source_info.push(info);

            for (i, chunk) in source.data.chunks(stride).enumerate() {
                let current_page_id = page_counter + i as u64;
                pool.put_frame(current_page_id, Arc::new(chunk.to_vec()))?;
            }

            page_counter += page_count;
            total_items += item_count;
        }

        pool.resize(page_counter)?;

        let buffer_pool = BufferPool::new(buffer_pool_size as usize, pool, bottom_evictor);

        Ok(Self {
            buffer_pool: RefCell::new(buffer_pool),
            source_info,
            total_items,
        })
    }

    /// Returns the total number of items in the collection.
    pub fn len(&self) -> usize {
        self.total_items
    }

    /// Retrieves an item by its global index.
    ///
    /// This method provides indexed access to the collection's data. It handles
    /// mapping the global index to the correct page and offset, and uses the
    /// buffer pool to fetch the page if it's not already in memory.
    pub fn get(&self, index: usize) -> Option<Arc<T>> {
        if index >= self.total_items {
            return None;
        }

        let mut items_seen = 0;
        for info in &self.source_info {
            if index < items_seen + info.item_count {
                let local_index = index - items_seen;
                let page_offset = local_index / info.stride;
                let item_offset = local_index % info.stride;
                let page_id = info.start_page + page_offset as u64;

                let mut bp = self.buffer_pool.borrow_mut();
                if let Some(page_frame) = bp.get_page(page_id) {
                    let page_data = page_frame.data(); // Arc<Vec<Arc<T>>>
                    return Some(page_data[item_offset].clone());
                } else {
                    return None;
                }
            }
            items_seen += info.item_count;
        }

        None
    }

    /// Returns an iterator over the items in the collection.
    pub fn iter(&self) -> CollectionIterator<'_, 'a, T> {
        CollectionIterator {
            collection: self,
            current_index: 0,
        }
    }
}

/// An iterator over the items in a `Collection`.
pub struct CollectionIterator<'iter, 'collection, T>
where
    T: Clone,
{
    collection: &'iter Collection<'collection, T>,
    current_index: usize,
}

impl<'iter, 'collection, T> Iterator for CollectionIterator<'iter, 'collection, T>
where
    T: Clone,
{
    type Item = Arc<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index < self.collection.len() {
            let item = self.collection.get(self.current_index);
            self.current_index += 1;
            item
        } else {
            None
        }
    }
}

/// Allows `for item in &collection` syntax.
impl<'a, 'b, T> IntoIterator for &'b Collection<'a, T>
where
    T: Clone,
{
    type Item = Arc<T>;
    type IntoIter = CollectionIterator<'b, 'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}