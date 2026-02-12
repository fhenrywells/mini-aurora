use std::collections::HashMap;

use mini_aurora_common::{Lsn, Page, PageId};

/// Local buffer pool on the compute side.
///
/// Caches pages read from storage at specific read-point LSNs. In Aurora's
/// architecture, the compute node maintains a buffer cache to avoid repeated
/// round-trips to storage for hot pages.
pub struct BufferPool {
    pages: HashMap<PageId, CachedPage>,
    capacity: usize,
}

struct CachedPage {
    page: Page,
    read_point: Lsn,
    access_count: u64,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            pages: HashMap::new(),
            capacity,
        }
    }

    /// Get a page from the buffer pool if it exists and its read_point is ≥ the requested LSN.
    pub fn get(&mut self, page_id: PageId, min_lsn: Lsn) -> Option<&Page> {
        if let Some(entry) = self.pages.get_mut(&page_id) {
            if entry.read_point >= min_lsn {
                entry.access_count += 1;
                return Some(&entry.page);
            }
        }
        None
    }

    /// Insert or replace a page in the buffer pool.
    pub fn insert(&mut self, page_id: PageId, read_point: Lsn, page: Page) {
        // Simple eviction: if at capacity, remove the least accessed entry
        if self.pages.len() >= self.capacity && !self.pages.contains_key(&page_id) {
            if let Some((&evict_id, _)) = self
                .pages
                .iter()
                .min_by_key(|(_, v)| v.access_count)
            {
                self.pages.remove(&evict_id);
            }
        }

        self.pages.insert(
            page_id,
            CachedPage {
                page,
                read_point,
                access_count: 1,
            },
        );
    }

    /// Invalidate a page (e.g., after writing to it).
    pub fn invalidate(&mut self, page_id: PageId) {
        self.pages.remove(&page_id);
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_aurora_common::empty_page;

    #[test]
    fn test_miss_on_empty() {
        let mut pool = BufferPool::new(10);
        assert!(pool.get(1, 1).is_none());
    }

    #[test]
    fn test_hit() {
        let mut pool = BufferPool::new(10);
        let mut page = empty_page();
        page[0] = 0xAB;
        pool.insert(1, 5, page);

        let result = pool.get(1, 5);
        assert!(result.is_some());
        assert_eq!(result.unwrap()[0], 0xAB);
    }

    #[test]
    fn test_stale_read_point_misses() {
        let mut pool = BufferPool::new(10);
        pool.insert(1, 5, empty_page());

        // Requesting a newer read_point than cached → miss
        assert!(pool.get(1, 10).is_none());
        // Requesting an older or equal read_point → hit
        assert!(pool.get(1, 3).is_some());
        assert!(pool.get(1, 5).is_some());
    }

    #[test]
    fn test_invalidate() {
        let mut pool = BufferPool::new(10);
        pool.insert(1, 5, empty_page());
        assert!(pool.get(1, 5).is_some());

        pool.invalidate(1);
        assert!(pool.get(1, 5).is_none());
    }

    #[test]
    fn test_eviction() {
        let mut pool = BufferPool::new(2);
        pool.insert(1, 1, empty_page());
        pool.insert(2, 1, empty_page());

        // Access page 2 more to make page 1 the eviction target
        pool.get(2, 1);
        pool.get(2, 1);

        pool.insert(3, 1, empty_page()); // should evict page 1

        assert!(pool.get(1, 1).is_none());
        assert!(pool.get(2, 1).is_some());
        assert!(pool.get(3, 1).is_some());
    }
}
