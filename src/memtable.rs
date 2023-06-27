//! # MemTable
//!
//! The `MemTable` (short for memory table) is an in-memory data structure that stores recently written data before it is flushed to disk. It serves as a write buffer and provides fast write operations.
//!
//! ## Constants
//!
//! The implementation defines the following constants:
//!
//! ### `DEFAULT_MEMTABLE_CAPACITY`
//!
//! Represents the default maximum size of the MemTable. By default, it is set to 1 gigabyte (1GB).
//! ```rs
//! pub(crate) static DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Gigabytes.to_bytes(1);
//! ```
//!
//! ### `DEFAULT_FALSE_POSITIVE_RATE`
//!
//! Represents the default false positive rate for the Bloom filter used in the `MemTable`. By default, it is set to 0.0001 (0.01%).
//!
//! ```rs
//! pub(crate) static DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.0001;
//! ```
//!
//! ## Structure
//!
//! The **`MemTable`** structure represents the in-memory data structure and contains the following fields:
//!
//! ```rs
//! pub(crate) struct MemTable {
//!     entries: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
//!     entry_count: usize,
//!     size: usize,
//!     capacity: usize,
//!     bloom_filter: BloomFilter,
//!     size_unit: SizeUnit,
//!     false_positive_rate: f64,
//! }
//! ```
//!
//! ### `entries`
//!
//! The entries field is of type `Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>`. It holds the key-value pairs of the `MemTable` in sorted order using a `BTreeMap`. The `Arc` (Atomic Reference Counting) and `Mutex` types allow for concurrent access and modification of the `entries` data structure from multiple threads, ensuring thread safety.
//!
//! ### `entry_count`
//!
//! The `entry_count` field is of type `usize` and represents the number of key-value entries currently stored in the `MemTable`.
//!
//! ### `size`
//!
//! The `size` field is of type `usize` and represents the current size of the `MemTable` in bytes. It is updated whenever a new key-value pair is added or removed.
//!
//! ### `capacity`
//!
//! The `capacity` field is of type `usize` and represents the maximum allowed size for the `MemTable` in bytes. It is used to enforce size limits and trigger flush operations when the `MemTable` exceeds this capacity.
//!
//! ### `bloom_filter`
//!
//! The `bloom_filter` field is of type `BloomFilter` and is used to probabilistically determine whether a `key` may exist in the `MemTable` without accessing the `entries` map. It helps improve performance by reducing unnecessary lookups in the map.
//!
//! ### `size_unit`
//!
//! The `size_unit` field is of type `SizeUnit` and represents the unit of measurement used for `capacity` and `size` calculations. It allows for flexibility in specifying the capacity and size of the `MemTable` in different units (e.g., bytes, kilobytes, megabytes, etc.).
//!
//! ### `false_positive_rate`
//!
//! The `false_positive_rate` field is of type `f64` and represents the desired false positive rate for the bloom filter. It determines the trade-off between memory usage and the accuracy of the bloom filter.
//!
//! ## Constructor Methods
//!
//! ### `new`
//!
//! ```rs
//! pub(crate) fn new() -> Self
//! ```
//!
//! The `new` method creates a new `MemTable` instance with the default capacity. It internally calls the `with_capacity_and_rate` method, passing the default capacity and false positive rate.
//!
//! ### `with_capacity_and_rate`
//!
//! ```rs
//! pub(crate) fn with_capacity_and_rate(
//!     size_unit: SizeUnit,
//!     capacity: usize,
//!     false_positive_rate: f64,
//! ) -> Self
//! ```
//!
//! The `with_capacity_and_rate` method creates a new `MemTable` with the specified capacity, size unit, and false positive rate. It initializes the `entries` field as an empty `BTreeMap`, sets the `entry_count` and `size` to zero, and creates a new `BloomFilter` with the given capacity and false positive rate. The capacity is converted to bytes based on the specified size unit.
//!
//! ## Public Methods
//!
//! ### `set`
//!
//! ```rs
//! pub(crate) fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()>
//! ```
//!
//! The `set` method inserts a new key-value pair into the `MemTable`. It first acquires a lock on the `entries` field to ensure thread-safety. If the key is not present in the `BloomFilter`, it adds the key-value pair to the `entries` map, updates the `entry_count` and `size`, and sets the key in the `BloomFilter`. If the key already exists, an `AlreadyExists` error is returned.
//!
//! ### `get`
//!
//! ```sh
//! pub(crate) fn get(&self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>>
//! ```
//!
//! The `get` method retrieves the value associated with a given key from the `MemTable`. It first checks if the key is present in the `BloomFilter`. If it is, it acquires a lock on the `entries` field and returns the associated value. If the key is not present in the `BloomFilter`, it returns `None`.
//!
//! ### `remove`
//!
//! ```sh
//! pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<Option<(Vec<u8>, Vec<u8>)>>
//! ```
//!
//! The `remove` method removes a key-value pair from the `MemTable` based on a given key. It first checks if the key is present in the `BloomFilter`. If it is, it acquires a lock on the `entries` field and removes the key-value pair from the `entries` map. It updates the `entry_count` and `size` accordingly and returns the removed key-value pair as a tuple. If the key is not present in the `BloomFilter`, it returns `None`.
//!
//! ### `clear`
//!
//! ```rs
//! pub(crate) fn clear(&mut self) -> io::Result<()>
//! ```
//!
//! The `clear` method removes all key-value entries from the `MemTable`. It acquires a lock on the `entries` field, clears the `entries` map, and sets the `entry_count` and `size` fields to zero.
//!
//! ### `entries`
//!
//! ```rs
//! pub(crate) fn entries(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>>
//! ```
//!
//! The `entries` method returns a vector of all key-value pairs in the `MemTable`. It acquires a lock on the `entries` field and iterates over the key-value pairs in the `entries` map. It clones each key-value pair and collects them into a vector, which is then returned.
//!
//! ## Internal Method
//!
//! ### `capacity`
//!
//! ```rs
//! pub(crate) fn capacity(&self) -> usize
//! ```
//!
//! The `capacity` method returns the capacity of the `MemTable` in bytes.
//!
//! ### `size`
//!
//! ```rs
//! pub(crate) fn size(&self) -> usize
//! ```
//!
//! The `size` method returns the current size of the `MemTable` in the specified size unit. It divides the internal `size` by the number of bytes in one unit of the specified size unit.
//!
//! ### `false_positive_rate`
//!
//! ```rs
//! pub(crate) fn false_positive_rate(&self) -> f64
//! ```
//!
//! The `false_positive_rate` method returns the false positive rate of the `MemTable`.
//!
//! ### `size_unit`
//!
//! ```rs
//! pub(crate) fn size_unit(&self) -> SizeUnit
//! ```
//!
//! The `size_unit` method returns the size unit used by the `MemTable`.
//!
//! ## Error Handling
//!
//! All the methods that involve acquiring a lock on the `entries` field use the `io::Error` type to handle potential errors when obtaining the lock. If an error occurs during the locking process, an `io::Error` instance is created with a corresponding error message.
//!
//! ## Thread Safety
//!
//! The `MemTable` implementation ensures thread safety by using an `Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>` for storing the key-value entries. The `Arc` allows multiple ownership of the `entries` map across threads, and the `Mutex` ensures exclusive access to the map during modification operations, preventing data races.
//!
//! The locking mechanism employed by the `Mutex` guarantees that only one thread can modify the `entries` map at a time, while allowing multiple threads to read from it simultaneously.
//!

use crate::{api::SizeUnit, sst::BloomFilter};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

/// Setting default capacity to be 1GB.
pub(crate) static DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Gigabytes.to_bytes(1);

// 0.0001% false positive rate.
pub(crate) static DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.0001;

/// The MemTable is an in-memory data structure that stores recently written data before it is flushed to disk.
pub struct MemTable {
    /// Stores key-value pairs in sorted order.
    entries: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    /// The number of key-value entries present in the MemTable.
    entry_count: usize,
    /// Current size of the MemTable in bytes.
    size: usize,
    /// The maximum allowed size for the MemTable in MB. This is used to enforce size limits and trigger `flush` operations.
    capacity: usize,
    bloom_filter: BloomFilter,
    size_unit: SizeUnit,
    false_positive_rate: f64,
}

impl MemTable {
    /// Creates a new MemTable with the default MemTable capacity of 1GB.
    pub(crate) fn new() -> Self {
        Self::with_capacity_and_rate(
            SizeUnit::Bytes,
            DEFAULT_MEMTABLE_CAPACITY,
            DEFAULT_FALSE_POSITIVE_RATE,
        )
    }

    /// Creates a new MemTable with the specified capacity, size_change and false positive rate.
    pub(crate) fn with_capacity_and_rate(
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
    ) -> Self {
        assert!(capacity > 0, "Capacity must be greater than zero");

        let capacity_bytes = size_unit.to_bytes(capacity);

        // Average key-value entry size in bytes.
        let avg_entry_size = 100;
        let num_elements = capacity_bytes / avg_entry_size;

        let bloom_filter = BloomFilter::new(num_elements, false_positive_rate);

        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            entry_count: 0,
            size: SizeUnit::Bytes.to_bytes(0),
            capacity: capacity_bytes,
            bloom_filter,
            size_unit: SizeUnit::Bytes,
            false_positive_rate,
        }
    }

    /// Inserts a key-value pair into the MemTable.
    pub(crate) fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        let mut entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;

        if !self.bloom_filter.contains(&key) {
            let size_change = key.len() + value.len();
            entries.insert(key.clone(), value);

            self.entry_count += 1;
            self.size += size_change;

            self.bloom_filter.set(&key);

            return Ok(());
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "key already exists",
        ))
    }

    /// Get the value of the given key
    pub(crate) fn get(&self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        if !self.bloom_filter.contains(&key) {
            return Ok(None);
        }

        let entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;
        Ok(entries.get(&key).cloned())
    }

    /// Remove the key-value entry of the given key.
    pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<Option<(Vec<u8>, Vec<u8>)>> {
        if !self.bloom_filter.contains(&key) {
            return Ok(None);
        }

        let mut entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;

        if let Some((key, value)) = entries.remove_entry(&key) {
            self.entry_count -= 1;
            self.size -= key.len() + value.len();

            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
    }

    /// Clears all key-value entries in the MemTable.
    pub(crate) fn clear(&mut self) -> io::Result<()> {
        let mut entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;

        entries.clear();
        self.entry_count = 0;
        self.size = 0;
        Ok(())
    }

    pub(crate) fn entries(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;

        Ok(entries
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn size(&self) -> usize {
        self.size / self.size_unit.to_bytes(1)
    }

    pub(crate) fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate
    }

    pub(crate) fn size_unit(&self) -> SizeUnit {
        self.size_unit.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! kv {
        ($k:expr, $v:expr) => {
            ($k.as_bytes().to_vec(), $v.as_bytes().to_vec())
        };
    }

    fn generate_dummy_kv_pairs() -> Vec<(Vec<u8>, Vec<u8>)> {
        let pairs = vec![
            kv!("key1", "value1"),
            kv!("key2", "value2"),
            kv!("key3", "value3"),
        ];

        pairs
    }

    #[test]
    fn create_empty_memtable() {
        let m = MemTable::new();
        assert_eq!(m.size, 0);
        assert_eq!(m.entry_count, 0);
    }

    #[test]
    fn create_single_entry() {
        let mut m = MemTable::new();

        let (key, value) = generate_dummy_kv_pairs()
            .pop()
            .expect("Failed to get first key-value pair");

        let _ = m.set(key.clone(), value.clone());

        assert_eq!(m.entry_count, 1);
        assert_eq!(m.size, key.len() + value.len());

        assert_eq!(m.get(key).unwrap().unwrap(), value);
    }

    #[test]
    fn set_and_get() {
        let mut m = MemTable::new();

        // setup dummy key-value pairs
        let kv = generate_dummy_kv_pairs();

        // set all dummy key-value pairs to mem-table
        let mut size = 0;
        kv.iter().for_each(|(key, value)| {
            let _ = m.set(key.clone(), value.clone());

            size += key.len() + value.len();

            assert_eq!(m.get(key.clone()).unwrap().unwrap(), value.clone());
        });

        assert_eq!(m.entry_count, kv.len());
        assert_eq!(m.size, size);
    }

    #[test]
    fn set_and_remove() {
        let mut m = MemTable::new();

        let kv = generate_dummy_kv_pairs();

        let mut size = 0;

        // set key-value
        kv.iter().for_each(|(key, value)| {
            let _ = m.set(key.clone(), value.clone());
            size += key.len() + value.len();
            assert_eq!(m.size, size);
        });

        // remove key-value
        kv.iter().for_each(|(key, value)| {
            let _ = m.remove(key.clone());
            size -= key.len() + value.len();
            assert_eq!(m.size, size);
        });

        assert_eq!(m.size, 0);
    }
}
