use rand;
use rand::{Rng, thread_rng};
use std::collections::HashMap;

use crate::{framepool, pageframe};
use crate::unique_stack;

type BufferPoolId = u64;
type FramePoolId = u64;

type EvictorFn<T> = fn(
    &Vec<Option<pageframe::PageFrame<T>>>,
    &unique_stack::UniqueStack<BufferPoolId>,
) -> Result<BufferPoolId, BufferPoolErrors>;

#[derive(Debug)]
pub enum BufferPoolErrors {
    NoEvictablePage,
    NoPageAvailable,
}

impl std::fmt::Display for BufferPoolErrors {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.write_str(match self {
            Self::NoEvictablePage => "no evictable pages",
            Self::NoPageAvailable => "no available pages",
        })
    }
}

impl std::error::Error for BufferPoolErrors {}

fn random_evictor<T>(
    pages: &Vec<Option<pageframe::PageFrame<T>>>,
    _: &unique_stack::UniqueStack<BufferPoolId>,
) -> Result<BufferPoolId, BufferPoolErrors>
where
    T: Clone,
{
    let mut rng = thread_rng();
    let len = pages.len();
    let mut trials = 0;
    loop {
        let n: usize = rng.gen_range(0..len);
        match &pages[n as usize] {
            None => continue,
            Some(page) => {
                if page.is_pinned() {
                    trials += 1;
                    if trials > len {
                        return Err(BufferPoolErrors::NoEvictablePage);
                    }
                    continue;
                } else {
                    return Ok(n as BufferPoolId);
                }
            }
        }
    }
}

fn bottom_evictor<T>(
    pages: &Vec<Option<pageframe::PageFrame<T>>>,
    lru: &unique_stack::UniqueStack<BufferPoolId>,
) -> Result<BufferPoolId, BufferPoolErrors>
where
    T: Clone,
{
    for i in lru.order() {
        match &pages[i as usize] {
            None => continue,
            Some(page) => {
                if page.is_pinned() {
                    continue;
                } else {
                    return Ok(i as BufferPoolId);
                }
            }
        }
    }
    return Err(BufferPoolErrors::NoEvictablePage);
}

pub struct BufferPool<'a, T>
where
    T: Clone,
{
    // number of pages this bufferpool holds
    size: usize,
    // the pages that are loaded
    // None indicates an unloaded page.
    // BufferPoolIDs index into trhis.
    pages: Vec<Option<pageframe::PageFrame<T>>>,

    // maps bufferpool ids to framepool ids
    buf2frame: HashMap<BufferPoolId, FramePoolId>,
    // maps framepool ids to bufferpool ids
    frame2buf: HashMap<FramePoolId, BufferPoolId>,
    // for removing the least used page
    lru: unique_stack::UniqueStack<BufferPoolId>,

    evictor: EvictorFn<T>,
    // the framepool that this bufferpool uses
    // FramePoolIds index into this.
    frame_pool: &'a mut dyn framepool::FramePool<T>,
}

impl<'a, T> BufferPool<'a, T>
where
    T: Clone,
{
    pub fn new(
        size: usize,
        pool: &'a mut dyn framepool::FramePool<T>,
        evictor: EvictorFn<T>,
    ) -> Self {
        let mut alloced_pages = Vec::new();
        for i in 0..size {
            alloced_pages.push(None);
        }
        BufferPool {
            size: size,
            pages: alloced_pages,
            buf2frame: HashMap::new(),
            frame2buf: HashMap::new(),
            lru: unique_stack::UniqueStack::new(),
            evictor: evictor,
            frame_pool: pool,
        }
    }

    // ensure_allocation ensures that values up to the given idx in the backing Frame Pool store are allocated.
    fn ensure_allocation(&mut self, idx: FramePoolId) -> Result<(), String> {
        self.frame_pool.resize(idx)
    }

    pub fn sync_index(&mut self, frame_idx: FramePoolId) -> Result<(), String> {
        if !self.frame2buf.contains_key(&frame_idx) {
            return Ok(());
        }
        let buf_idx = self.frame2buf[&frame_idx];
        let page = self.pages[buf_idx as usize]
            .as_ref()
            .ok_or("unable to access index".to_string())?;
        if page.is_dirty() {
            let x = pageframe::PageFrame::new(page.data());
            self.frame_pool.write_frame(frame_idx, Box::new(x))?
        }
        Ok(())
    }

    // put_page writes data to the given index
    pub fn put_page(&mut self, frame_idx: FramePoolId, data: T) -> Result<(), BufferPoolErrors> {
        let mut page = self
            .get_page(frame_idx)
            .ok_or(BufferPoolErrors::NoPageAvailable)?;
        page.with_data(|d: &mut T| *d = data);
        Ok(())
    }

    // get_page returns a reference to the page at the given underlying index.
    pub fn get_page(&mut self, frame_idx: FramePoolId) -> Option<&pageframe::PageFrame<T>> {
        // If this is beyond the size of the backing frame, then we can't get the page.
        if frame_idx > self.frame_pool.size() {
            return None;
        }

        if !self.frame2buf.contains_key(&frame_idx) {
            // Then we don't have the page loaded.
            if self.frame2buf.len() == self.size {
                // Precondition of this block: the BufferPool is full.

                // Then we are full and must evict the least recently used page.
                let victim_idx = (self.evictor)(&self.pages, &self.lru).ok()?; // Select a bufferID to remove.

                let victim_page = self.pages[victim_idx as usize].as_ref().unwrap();
                if victim_page.is_dirty() {
                    // Flush the page to the pool

                    // something says that the reference and cloning logic here is junk.
                    let d = self.pages[victim_idx as usize].as_ref()?;
                    let x = pageframe::PageFrame::new(d.data());
                    self.frame_pool.write_frame(victim_idx, Box::new(x)).ok()?;
                }
                // Precondition: the page is not dirty, or we have flushed it.

                self.pages[victim_idx as usize] = None;
                self.buf2frame.remove(&victim_idx);
                self.frame2buf.remove(&frame_idx);
                self.lru.delete(victim_idx);

                // Postcondition of this block: the block is not full, we have 1 slot open.
            }

            // Precondition: We are not full, which is a None element in the self.pages vec.

            let target_idx = self.pages.iter().position(|x| x.is_none())? as BufferPoolId;

            let new_frame = self.frame_pool.read_frame(frame_idx).ok()?;

            self.pages[target_idx as usize] = Some(new_frame);
            self.buf2frame.insert(target_idx, frame_idx);
            self.frame2buf.insert(frame_idx, target_idx);
        }

        match self.frame2buf.get(&frame_idx) {
            None => None, // this should be an assert tbh.
            Some(buffer_id) => {
                let b: u64 = buffer_id.clone();
                self.lru.push(b);
                self.pages[b as usize].as_ref()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framepool;
    use crate::framepool::MemPool;
    use crate::unique_stack;

    #[test]
    fn test_new() {
        let mut pool = MemPool::<u8>::new();
        let mut bp = BufferPool::<u8>::new(10, &mut pool, bottom_evictor);
        assert_eq!(bp.size, 10);
        assert_eq!(bp.pages.len(), 10);
        assert_eq!(bp.buf2frame.len(), 0);
        assert_eq!(bp.frame2buf.len(), 0);
        assert_eq!(bp.lru.len(), 0);
    }

    // This tests the bottom_evictor function
    #[test]
    fn test_bottom_evictor() {
        let mut mem_pool = MemPool::<u8>::new();
        let mut bp = BufferPool::<u8>::new(10, &mut mem_pool, bottom_evictor);

        bp.put_page(0, 0).unwrap();
        bp.get_page(0);

        let evicted = bottom_evictor::<u8>(&bp.pages, &bp.lru);
    }
}
