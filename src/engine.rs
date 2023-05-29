use crate::{helper::generate_timestamp, mem_table::MemTable, wal::Wal};
use std::{fs, path::PathBuf};

/// StorageEngine represents a storage engine that combines a memory table and a write-ahead log (WAL)
/// for data storage and retrieval.
pub struct StorageEngine {
    pub dir_path: PathBuf,
    pub mem_table: MemTable,
    pub wal: Wal,
}

impl StorageEngine {
    /// Constructs a new instance of the StorageEngine.
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where the storage files are located.
    ///
    /// # Returns
    ///
    /// A new instance of the StorageEngine.
    ///
    /// # Panics
    ///
    /// This method will panic if loading the WAL and MemTable from the given directory path fails.
    pub fn new(path: &str) -> Self {
        let dir_path = PathBuf::from(path);

        if !dir_path.exists() {
            // Create the directory if it doesn't exist
            fs::create_dir_all(&dir_path).expect("Failed to create directory for StorageEngine");
        }

        let (wal, mem_table) =
            Wal::load_wal_from_dir(&dir_path).expect("Failed to load from given directory path");

        StorageEngine {
            dir_path,
            mem_table,
            wal,
        }
    }

    /// Retrieves a value from the storage engine based on the provided key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key used for retrieval.
    ///
    /// # Returns
    ///
    /// An optional StorageEngineEntry containing the key, value, and timestamp if the key exists
    /// in the storage engine; otherwise, None.
    pub fn get(&self, key: &[u8]) -> Option<StorageEngineEntry> {
        if let Some(mem_table_entry) = self.mem_table.get(key) {
            return mem_table_entry
                .value
                .as_ref()
                .map(|value| StorageEngineEntry {
                    key: mem_table_entry.key.clone(),
                    value: value.clone(),
                    timestamp: mem_table_entry.timestamp,
                });
        }
        None
    }

    /// Sets a key-value pair in the storage engine.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `value` - The value associated with the key.
    ///
    /// # Returns
    ///
    /// A Result indicating success (Ok(1)) or failure (Err(0)).
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<usize, usize> {
        let timestamp = generate_timestamp();

        let wal_res = self.wal.set(key, value, timestamp);
        if wal_res.is_err() {
            return Err(0);
        }

        if self.wal.flush().is_err() {
            return Err(0);
        }

        self.mem_table.set(key, value, timestamp);

        Ok(1)
    }

    /// Deletes a key-value pair from the storage engine.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    ///
    /// # Returns
    ///
    /// A Result indicating success (Ok(1)) or failure (Err(0)).
    pub fn delete(&mut self, key: &[u8]) -> Result<usize, usize> {
        let timestamp = generate_timestamp();

        let wal_res = self.wal.delete(key, timestamp);

        if wal_res.is_err() {
            return Err(0);
        }

        if self.wal.flush().is_err() {
            return Err(0);
        }

        self.mem_table.delete(key, timestamp);

        Ok(1)
    }
}

/// Represents an entry in the storage engine, containing the key, value, and timestamp.
#[derive(Debug, Clone)]
pub struct StorageEngineEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: u128,
}

impl StorageEngineEntry {
    /// Retrieves the key of the storage engine entry.
    ///
    /// # Returns
    ///
    /// A reference to the key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Retrieves the value of the storage engine entry.
    ///
    /// # Returns
    ///
    /// A reference to the value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Retrieves the timestamp of the storage engine entry.
    ///
    /// # Returns
    ///
    /// The timestamp.
    pub fn timestamp(&self) -> u128 {
        self.timestamp
    }
}

impl PartialEq for StorageEngineEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value && self.timestamp == other.timestamp
    }
}
