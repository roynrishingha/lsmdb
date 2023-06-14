//! # MemTable
//!
//! `MemTable` is a memory-based key-value storage that is part of a Log-Structured Merge (LSM) Tree storage engine.
//! It provides fast read and write operations by maintaining key-value pairs in memory.
//!
//! It imports dependencies like:
//! bytes, which provides an efficient byte buffer implementation,
//! crossbeam_skiplist, which provides a concurrent skip list data structure.
//! It also imports Arc from the standard library for atomic reference counting.
//!

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::{ops::Bound, sync::Arc};

struct MemTable {
    // A shared reference-counted skip map of byte sequences.
    entries: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new empty memory table
    fn new() -> Self {
        Self {
            entries: Arc::new(SkipMap::new()),
        }
    }

    /// Get corresponding `value` by `key`
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.entries.get(key).map(|entry| entry.value().to_owned())
    }

    /// Set a key-value pair into MemTable
    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.entries
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// Check if the MemTable is empty
    fn is_empty(&self) -> bool {
        // Check if the skip map is empty
        self.entries.is_empty()
    }

    /// Remove a key from the MemTable
    fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        if let Some(value) = self.get(key) {
            // Remove the key-value pair from the skip map
            self.entries.remove(key);
            Ok(())
        } else {
            // Return an error if the key does not exist
            Err(())
        }
    }

    /// Get the number of key-value pairs in the MemTable
    fn len(&self) -> usize {
        // Get the number of elements in the skip map
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_empty_memtable() {
        let memtable = MemTable::new();
        assert_eq!(memtable.len(), 0);
    }

    #[test]
    fn set_single_entry() {
        let key = b"key1";
        let value = b"value1";

        let mut memtable = MemTable::new();

        memtable.set(key, value);

        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(key), Some(Bytes::copy_from_slice(value)));
    }

    #[test]
    fn memtable_set_and_get() {
        let mut memtable = MemTable::new();

        memtable.set(b"a", b"apple");
        memtable.set(b"b", b"banana");
        memtable.set(b"c", b"cheese");
        memtable.set(b"d", b"dragon fruit");
        memtable.set(b"e", b"elderberry fruit");

        assert_eq!(memtable.get(b"a"), Some(Bytes::copy_from_slice(b"apple")));
        assert_eq!(memtable.get(b"b"), Some(Bytes::copy_from_slice(b"banana")));
        assert_eq!(memtable.get(b"c"), Some(Bytes::copy_from_slice(b"cheese")));
        assert_eq!(
            memtable.get(b"d"),
            Some(Bytes::copy_from_slice(b"dragon fruit"))
        );
        assert_eq!(
            memtable.get(b"e"),
            Some(Bytes::copy_from_slice(b"elderberry fruit"))
        );

        assert_eq!(memtable.len(), 5);
    }

    #[test]
    fn remove_key_success() {
        let mut memtable = MemTable::new();

        memtable.set(b"a", b"apple");
        let res = memtable.remove(b"a");

        assert!(res.is_ok());
        assert_eq!(memtable.get(b"a"), None);
        assert_eq!(memtable.len(), 0);
    }

    #[test]
    fn remove_non_exist_key_should_give_error() {
        let mut memtable = MemTable::new();

        memtable.set(b"key1", b"apple");
        // key "a" doesn't exist, therefore should fail.
        let res = memtable.remove(b"a");
        assert!(res.is_err());
    }
}
