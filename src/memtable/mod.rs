use crate::bloom_filter::BloomFilter;
use skiplist::SkipList;

mod arena_allocator;
mod skiplist;

#[cfg(test)]
mod tests;

/// Write buffer for the LSM-Tree. All puts and removes land here before being flushed to disk.
///
/// Keys are sorted inside the SkipList so the flush path can emit entries in sorted order,
/// which is required for the SSTable format (binary-searchable blocks).
///
/// The Bloom Filter is kept in sync with every insert so that `get()` can answer "definitely
/// not present" in O(k) hash operations without touching the SkipList at all. Most reads in a
/// write-heavy workload miss the MemTable — the filter makes those misses cheap.
pub struct MemTable {
    entries: SkipList<Vec<u8>, Vec<u8>>,
    capacity_bytes: usize,
    // Separate from the Arena's tracked memory because the Arena over-allocates in slab-sized
    // chunks. size_bytes tracks the actual key+value payload so the flush threshold is
    // meaningful and predictable regardless of internal Arena fragmentation.
    size_bytes: usize,
    bloom_filter: BloomFilter,
    // Stored so that clear() can rebuild the filter with the same FPR, not a hardcoded default.
    false_positive_rate: f64,
}

impl MemTable {
    /// Creates a MemTable sized for approximately `capacity_bytes` of key+value payload.
    ///
    /// The Bloom Filter is pre-sized by estimating entry count from capacity. We assume an
    /// average of 100 bytes per entry as a heuristic — this is intentionally conservative to
    /// avoid under-sizing the filter (which would increase the false positive rate above the
    /// configured target).
    pub fn new(capacity_bytes: usize, false_positive_rate: f64) -> Self {
        assert!(capacity_bytes > 0, "Capacity must be greater than zero");

        let avg_entry_size = 100;
        let num_elements = capacity_bytes / avg_entry_size;

        Self {
            entries: SkipList::new(),
            capacity_bytes,
            size_bytes: 0,
            bloom_filter: BloomFilter::new(num_elements, false_positive_rate),
            false_positive_rate,
        }
    }

    /// Inserts or overwrites a key. An empty value represents a tombstone (deletion marker).
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.bloom_filter.set(&key);
        self.size_bytes += key.len() + value.len();
        self.entries.insert(key, value);
    }

    /// Returns the value for `key`, or `None` if definitely absent.
    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        if !self.bloom_filter.contains(key) {
            return None;
        }

        // The SkipList is keyed on Vec<u8>, so we must convert the slice to a Vec for the
        // lookup. This allocation is the cost of not having a Borrow-generic SkipList key type.
        // It is acceptable here because this path is already behind a Bloom Filter check.
        let search_key = key.to_vec();
        self.entries.get(&search_key)
    }

    /// Returns true when the MemTable has filled to its capacity and must be flushed.
    pub fn needs_flush(&self) -> bool {
        self.approximate_memory_usage() >= self.capacity_bytes
    }

    /// An approximation of memory consumed by this MemTable.
    ///
    /// We add both sources because the Arena may hold many small slab pages that are not
    /// reflected in size_bytes, and size_bytes tracks payload that might not yet have caused
    /// a new Arena slab to be allocated (e.g., in-place value updates).
    pub fn approximate_memory_usage(&self) -> usize {
        self.entries.memory_usage() + self.size_bytes
    }

    /// Resets the MemTable to empty after a successful flush.
    ///
    /// The SkipList is dropped entirely (freeing the Arena pages) and a fresh Bloom Filter is
    /// allocated. We store `false_positive_rate` on the struct precisely so this rebuild uses
    /// the same configured FPR rather than a hardcoded default.
    pub fn clear(&mut self) {
        self.entries = SkipList::new();
        let num_elements = self.capacity_bytes / 100;
        self.bloom_filter = BloomFilter::new(num_elements, self.false_positive_rate);
        self.size_bytes = 0;
    }

    /// Returns all entries in sorted key order for writing to an SSTable.
    pub fn entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        self.entries.iter().collect()
    }
}
