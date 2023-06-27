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
pub(crate) struct MemTable {
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
