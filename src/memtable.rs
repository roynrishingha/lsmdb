use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

/// Setting default capacity to be 1GB.
pub(crate) static MEMTABLE_DEFAULT_CAPACITY: usize = 1024;

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
}

impl MemTable {
    /// Creates a new MemTable with the default MemTable capacity of 1GB.
    pub(crate) fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            entry_count: 0,
            size: 0,
            capacity: MEMTABLE_DEFAULT_CAPACITY,
        }
    }

    /// Creates a new MemTable with the specified maximum size in MB.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
            entry_count: 0,
            size: 0,
            capacity,
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

        let size_change = key.len() + value.len();
        entries.insert(key, value);

        self.entry_count += 1;
        self.size += size_change;

        Ok(())
    }

    /// Get the value of the given key
    pub(crate) fn get(&self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;
        Ok(entries.get(&key).cloned())
    }

    /// Remove the key-value entry of the given key.
    pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        let mut entries = self.entries.lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire lock on MemTable entries.",
            )
        })?;

        let value = entries.remove(&key);

        if let Some(v) = &value {
            self.entry_count -= 1;
            self.size -= key.len() + v.len();
        }

        Ok(value)
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
    pub(crate) fn get_capacity(&self) -> usize {
        self.capacity
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
        let mut pairs = Vec::new();

        pairs.push(kv!("key1", "value1"));
        pairs.push(kv!("key2", "value2"));
        pairs.push(kv!("key3", "value3"));

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

        m.set(key.clone(), value.clone());

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
            m.set(key.clone(), value.clone());

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
            m.set(key.clone(), value.clone());
            size += key.len() + value.len();
            assert_eq!(m.size, size);
        });

        // remove key-value
        kv.iter().for_each(|(key, value)| {
            m.remove(key.clone());
            size -= key.len() + value.len();
            assert_eq!(m.size, size);
        });

        assert_eq!(m.size, 0);
    }
}
