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

use crate::write_ahead_log::WriteAheadLog;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::{collections::HashSet, io, path::Path, sync::Arc};

pub(crate) struct MemTable {
    entries: Arc<SkipMap<Bytes, Bytes>>,
    deleted_keys: HashSet<Bytes>,
    wal: WriteAheadLog,
}

impl MemTable {
    pub(crate) fn new(log_file_path: &Path) -> io::Result<Self> {
        let wal = WriteAheadLog::new(log_file_path)?;

        Ok(Self {
            entries: Arc::new(SkipMap::new()),
            deleted_keys: HashSet::new(),
            wal,
        })
    }

    /// Get corresponding `value` by `key`
    pub(crate) fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key_bytes = Bytes::copy_from_slice(key);

        if self.deleted_keys.contains(&key_bytes) {
            None
        } else {
            self.entries.get(key).map(|entry| entry.value().to_owned())
        }
    }

    /// Set a key-value pair into MemTable
    pub(crate) fn set(&mut self, key: &[u8], value: &[u8]) {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);

        self.deleted_keys.remove(&key_bytes);
        self.entries.insert(key_bytes, value_bytes);
    }

    /// Check if the MemTable is empty
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove a key from the MemTable
    pub(crate) fn remove(&mut self, key: &[u8]) -> Result<(), ()> {
        let key_bytes = Bytes::copy_from_slice(key);

        if self.deleted_keys.contains(&key_bytes) {
            // Return an error if the key is already deleted
            Err(())
        } else if let Some(value) = self.get(key) {
            // First delete the key from SkipMap and add the key to deleted list
            self.entries.remove(key);
            self.deleted_keys.insert(key_bytes.clone());

            Ok(())
        } else {
            // Return an error if the key does not exist
            Err(())
        }
    }

    /// Get the number of key-value pairs in the MemTable
    pub(crate) fn len(&self) -> usize {
        // Get the number of elements in the skip map
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};

    fn setup_test_env() -> (MemTable, TempDir) {
        let temp_dir = tempdir().expect("Failed to create temporary directory");
        let log_file_path = temp_dir.path().join("test_memtable.log");

        let memtable_result = MemTable::new(&log_file_path);

        assert!(memtable_result.is_ok());

        let memtable = memtable_result.unwrap();

        (memtable, temp_dir)
    }

    fn teardown_test_env(temp_dir: TempDir) {
        temp_dir
            .close()
            .expect("Failed to delete temporary directory");
    }

    #[test]
    fn create_empty_memtable() {
        let (memtable, temp_dir) = setup_test_env();

        assert_eq!(memtable.entries.len(), 0);

        teardown_test_env(temp_dir);
    }

    #[test]
    fn set_single_entry() {
        let (mut memtable, temp_dir) = setup_test_env();

        let key = b"key1";
        let value = b"value1";

        memtable.set(key, value);

        assert_eq!(memtable.len(), 1);
        assert_eq!(memtable.get(key), Some(Bytes::copy_from_slice(value)));

        teardown_test_env(temp_dir);
    }

    #[test]
    fn memtable_set_and_get() {
        let (mut memtable, temp_dir) = setup_test_env();

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

        teardown_test_env(temp_dir);
    }

    #[test]
    fn remove_key_success() {
        let (mut memtable, temp_dir) = setup_test_env();

        memtable.set(b"a", b"apple");
        let res = memtable.remove(b"a");

        assert!(res.is_ok());
        assert_eq!(memtable.get(b"a"), None);
        assert_eq!(memtable.len(), 0);

        teardown_test_env(temp_dir);
    }

    #[test]
    fn remove_non_exist_key_should_give_error() {
        let (mut memtable, temp_dir) = setup_test_env();

        memtable.set(b"key1", b"apple");
        // key "a" doesn't exist, therefore should fail.
        let res = memtable.remove(b"a");
        assert!(res.is_err());

        teardown_test_env(temp_dir);
    }
}
