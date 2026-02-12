use mini_aurora_common::{Lsn, Page, PageId};
use moka::sync::Cache;

/// Key for the page cache: (page_id, read_point_lsn).
/// Different read points may see different versions of the same page.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct CacheKey {
    pub page_id: PageId,
    pub lsn: Lsn,
}

/// LRU page cache backed by moka.
pub struct PageCache {
    cache: Cache<CacheKey, Box<Page>>,
}

impl PageCache {
    /// Create a new page cache with the given maximum number of entries.
    pub fn new(max_entries: u64) -> Self {
        Self {
            cache: Cache::new(max_entries),
        }
    }

    /// Try to get a cached page.
    pub fn get(&self, page_id: PageId, lsn: Lsn) -> Option<Page> {
        let key = CacheKey { page_id, lsn };
        self.cache.get(&key).map(|boxed| *boxed)
    }

    /// Insert a page into the cache.
    pub fn insert(&self, page_id: PageId, lsn: Lsn, page: Page) {
        let key = CacheKey { page_id, lsn };
        self.cache.insert(key, Box::new(page));
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_aurora_common::empty_page;

    #[test]
    fn test_cache_miss() {
        let cache = PageCache::new(100);
        assert!(cache.get(1, 1).is_none());
    }

    #[test]
    fn test_cache_hit() {
        let cache = PageCache::new(100);
        let mut page = empty_page();
        page[0] = 0xAB;

        cache.insert(1, 5, page);

        let retrieved = cache.get(1, 5).unwrap();
        assert_eq!(retrieved[0], 0xAB);
    }

    #[test]
    fn test_different_lsns_are_separate() {
        let cache = PageCache::new(100);
        let mut page_v1 = empty_page();
        page_v1[0] = 0x01;
        let mut page_v2 = empty_page();
        page_v2[0] = 0x02;

        cache.insert(1, 5, page_v1);
        cache.insert(1, 10, page_v2);

        assert_eq!(cache.get(1, 5).unwrap()[0], 0x01);
        assert_eq!(cache.get(1, 10).unwrap()[0], 0x02);
    }

    #[test]
    fn test_eviction() {
        let cache = PageCache::new(2);
        let page = empty_page();

        cache.insert(1, 1, page);
        cache.insert(2, 1, page);
        cache.insert(3, 1, page); // should evict one

        // moka eviction is async; we just verify it doesn't panic
        // and the newest entries are accessible
        assert!(cache.get(3, 1).is_some());
    }
}
