//! # `lsmdb` API
//!
//! ## `StorageEngine`
//!
//! The `StorageEngine` struct represents the main component of the LSM Tree storage engine. It consists of the following fields:
//!
//! - `memtable`: An instance of the `MemTable` struct that serves as an in-memory table for storing key-value pairs. It provides efficient write operations.
//! - `wal`: An instance of the `WriteAheadLog` struct that handles write-ahead logging. It ensures durability by persistently storing write operations before they are applied to the memtable and SSTables.
//! - `sstables`: A vector of `SSTable` instances, which are on-disk sorted string tables storing key-value pairs. The SSTables are organized in levels, where each level contains larger and more compacted tables.
//! - `dir`: An instance of the `DirPath` struct that holds the directory paths for the root directory, write-ahead log directory, and SSTable directory.
//!
//! The `StorageEngine` struct provides methods for interacting with the storage engine:
//!
//! - `new`: Creates a new instance of the `StorageEngine` struct. It initializes the memtable, write-ahead log, and SSTables.
//! - `put`: Inserts a new key-value pair into the storage engine. It writes the key-value entry to the memtable and the write-ahead log. If the memtable reaches its capacity, it is flushed to an SSTable.
//! - `get`: Retrieves the value associated with a given key from the storage engine. It first searches in the memtable, which has the most recent data. If the key is not found in the memtable, it searches in the SSTables, starting from the newest levels and moving to the older ones.
//! - `remove`: Removes a key-value pair from the storage engine. It first checks if the key exists in the memtable. If not, it searches for the key in the SSTables and removes it from there. The removal operation is also logged in the write-ahead log for durability.
//! - `update`: Updates the value associated with a given key in the storage engine. It first removes the existing key-value pair using the `remove` method and then inserts the updated pair using the `put` method.
//! - `clear`: Clears the storage engine by deleting the memtable and write-ahead log. It creates a new instance of the storage engine, ready to be used again.
//!
//! ## DirPath
//!
//! The `DirPath` struct represents the directory paths used by the storage engine. It consists of the following fields:
//!
//! - `root`: A `PathBuf` representing the root directory path, which serves as the parent directory for the write-ahead log and SSTable directories.
//! - `wal`: A `PathBuf` representing the write-ahead log directory path, where the write-ahead log file is stored.
//! - `sst`: A `PathBuf` representing the SSTable directory path, where the SSTable files are stored.
//!
//! The `DirPath` struct provides methods for building and retrieving the directory paths.
//!
//! ## SizeUnit
//!
// The `SizeUnit` enum represents the unit of measurement for capacity and size. It includes the following variants:
//!
//! - `Bytes`: Represents the byte unit.
//! - `Kilobytes`: Represents the kilobyte unit.
//! - `Megabytes`: Represents the megabyte unit.
//! - `Gigabytes`: Represents the gigabyte unit.
//!
//! The `SizeUnit` enum provides a method `to_bytes` for converting a given value to bytes based on the selected unit.
//!
//! ## Helper Functions
//!
//! The code includes several helper functions:
//!
//! - `with_capacity`: A helper function that creates a new instance of the `StorageEngine` struct with a specified capacity for the memtable.
//! - `with_capacity_and_rate`: A helper function that creates a new instance of the `StorageEngine` struct with a specified capacity for the memtable and a compaction rate for the SSTables.
//! - `flush_memtable`: A helper function that flushes the contents of the memtable to an SSTable. It creates a new SSTable file and writes the key-value pairs from the memtable into it. After flushing, the memtable is cleared.
//! - `recover_memtable`: A helper function that recovers the contents of the memtable from the write-ahead log during initialization. It reads the logged write operations from the write-ahead log and applies them to the memtable.
//!
//! These helper functions assist in initializing the storage engine, flushing the memtable to an SSTable when it reaches its capacity, and recovering the memtable from the write-ahead log during initialization, ensuring durability and maintaining data consistency.
//!

use crate::{
    memtable::{MemTable, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
    sst::SSTable,
    write_ahead_log::{EntryKind, WriteAheadLog, WriteAheadLogEntry, WAL_FILE_NAME},
};
use std::{fs, io, path::PathBuf};

pub struct StorageEngine {
    pub memtable: MemTable,
    pub wal: WriteAheadLog,
    pub sstables: Vec<SSTable>,
    pub dir: DirPath,
}

pub struct DirPath {
    root: PathBuf,
    wal: PathBuf,
    sst: PathBuf,
}

/// Represents the unit of measurement for capacity and size.
#[derive(Clone)]
pub enum SizeUnit {
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
}

impl StorageEngine {
    /// Creates a new instance of the `StorageEngine` struct.
    ///
    /// It initializes the memtable, write-ahead log, and SSTables.
    ///
    /// # Arguments
    ///
    /// * `dir` - A string representing the directory path where the database files will be stored.
    ///
    /// # Returns
    ///
    /// A Result containing the `StorageEngine` instance if successful, or an `io::Error` if an error occurred.
    pub fn new(dir: PathBuf) -> io::Result<Self> {
        let dir = DirPath::build(dir)?;

        StorageEngine::with_capacity(dir, SizeUnit::Bytes, DEFAULT_MEMTABLE_CAPACITY)
    }

    /// Inserts a new key-value pair into the storage engine.
    ///
    /// It writes the key-value entry to the memtable and the write-ahead log. If the memtable reaches its capacity, it is flushed to an SSTable.
    ///
    /// # Arguments
    ///
    /// * `key` - A string representing the key.
    /// * `value` - A string representing the value.
    ///
    /// # Returns
    ///
    /// A Result indicating success or an `io::Error` if an error occurred.
    pub fn put(&mut self, key: &str, value: &str) -> io::Result<()> {
        // Convert the key and value into Vec<u8> from given &str.
        let key = key.as_bytes().to_vec();
        let value = value.as_bytes().to_vec();

        // Write the key-value entry to the sequential wal and mark it as an insert entry.
        self.wal
            .append(EntryKind::Insert, key.clone(), value.clone())?;

        // Check if the MemTable has reached its capacity or size threshold for flushing to SSTable.
        if self.memtable.size() >= self.memtable.capacity() {
            // Get the current capacity.
            let capacity = self.memtable.capacity();

            // Get the current size_unit.
            let size_unit = self.memtable.size_unit();

            // Get the current false_positive_rate.
            let false_positive_rate = self.memtable.false_positive_rate();

            // Flush MemTable to SSTable.
            self.flush_memtable()?;

            // Create a new empty MemTable.
            self.memtable =
                MemTable::with_capacity_and_rate(size_unit, capacity, false_positive_rate);
        }

        // Write the key-value in MemTable entries by calling `set` method of MemTable.
        self.memtable.set(key, value)?;

        // Return Ok(()), if everything goes well

        Ok(())
    }

    /// Retrieves the value associated with a given key from the storage engine.
    ///
    /// It first searches in the memtable, which has the most recent data. If the key is not found in the memtable, it searches in the SSTables, starting from the newest levels and moving to the older ones.
    ///
    /// # Arguments
    ///
    /// * `key` - A string representing the key to search for.
    ///
    /// # Returns
    ///
    /// A Result containing an Option:
    /// - `Some(value)` if the key is found and associated value is returned.
    /// - `None` if the key is not found.
    ///
    /// An `io::Error` is returned if an error occurred.
    pub fn get(&self, key: &str) -> io::Result<Option<String>> {
        // Convert the key into Vec<u8> from given &str.
        let key = key.as_bytes().to_vec();

        // Search in the MemTable first.
        if let Some(value) = self.memtable.get(key.clone())? {
            return Ok(Some(String::from_utf8_lossy(&value).to_string()));
        }

        // Search in the SSTable
        for sstable in &self.sstables {
            if let Some(value) = sstable.get(key.clone()) {
                return Ok(Some(String::from_utf8_lossy(&value).to_string()));
            }
        }

        // Invalid key. No value found.
        Ok(None)
    }

    /// Removes a key-value pair from the storage engine.
    ///
    /// It first checks if the key exists in the memtable. If not, it searches for the key in the SSTables and removes it from there. The removal operation is also logged in the write-ahead log for durability.
    ///
    /// # Arguments
    ///
    /// * `key` - A string representing the key to remove.
    ///
    /// # Returns
    ///
    /// A Result indicating success.
    pub fn remove(&mut self, key: &str) -> io::Result<()> {
        // Convert the key and value into Vec<u8> from given &str.
        let key = key.as_bytes().to_vec();

        // Check if the key exists in the MemTable.
        if let Some((_res_key, value)) = self.memtable.remove(key.clone())? {
            // Remove the entry from the MemTable and add a remove log into WAL.
            self.wal.append(EntryKind::Remove, key, value)?;
        } else {
            // If the key is not found in the MemTable, search for it in the SSTable.
            for sstable in &mut self.sstables {
                if let Some(value) = sstable.get(key.clone()) {
                    // If the key is found in an SSTable, remove it from the SSTable and add a remove log into WAL.
                    sstable.remove(key.clone())?;
                    self.wal.append(EntryKind::Remove, key, value)?;

                    // Exit the loop after removing the key from one SSTable
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn update(&mut self, key: &str, value: &str) -> io::Result<()> {
        // Call remove method defined in StorageEngine.
        self.remove(key)?;

        // Call set method defined in StorageEngine.
        self.put(key, value)?;

        // return Ok(()) if the update is successfull.
        Ok(())
    }

    pub fn clear(mut self) -> io::Result<Self> {
        // Get the current capacity.
        let capacity = self.memtable.capacity();

        // Get the current size_unit.
        let size_unit = self.memtable.size_unit();

        // Get the current false_positive_rate.
        let false_positive_rate = self.memtable.false_positive_rate();

        // Delete the memtable by calling the `clear` method defined in MemTable.
        self.memtable.clear()?;

        // Delete the wal by calling the `clear` method defined in WriteAheadLog.
        self.wal.clear()?;

        // Call the build method of StorageEngine and return a new instance.
        StorageEngine::with_capacity_and_rate(self.dir, size_unit, capacity, false_positive_rate)
    }

    pub(crate) fn with_capacity(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
    ) -> io::Result<Self> {
        Self::with_capacity_and_rate(dir, size_unit, capacity, DEFAULT_FALSE_POSITIVE_RATE)
    }

    pub fn with_capacity_and_rate(
        dir: DirPath,
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
    ) -> io::Result<Self> {
        // The WAL file path.
        let wal_file_path = dir.wal.join(WAL_FILE_NAME);

        // Check if the WAL file exists and has contents.
        let wal_exists = wal_file_path.exists();
        let wal_empty = wal_exists && fs::metadata(&wal_file_path)?.len() == 0;

        if wal_empty {
            // WAL file empty, create a new WAL and MemTable.
            let memtable =
                MemTable::with_capacity_and_rate(size_unit, capacity, false_positive_rate);

            let wal = WriteAheadLog::new(&dir.wal)?;
            let sstables = Vec::new();

            Ok(Self {
                memtable,
                wal,
                sstables,
                dir,
            })
        } else {
            // WAL file has logs, recover the MemTable from the WAL.
            let mut wal = WriteAheadLog::new(&dir.wal)?;

            // I should not create empty sstable. I need to load existing sstables if exists. Otherwise new empty one should be used.
            let sstables = Vec::new();

            let entries = wal.recover()?;

            let memtable =
                StorageEngine::recover_memtable(entries, size_unit, capacity, false_positive_rate)?;

            Ok(Self {
                memtable,
                wal,
                sstables,
                dir,
            })
        }
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        // Create a new SSTable.
        let mut sstable = SSTable::new(self.dir.sst.clone());

        // Iterate over the entries in the MemTable and write them to the SSTable.
        for (key, value) in self.memtable.entries()? {
            sstable.set(key.clone(), value.clone())?;
        }

        // Clear the MemTable after flushing its contents to the SSTable.
        self.memtable.clear()?;

        Ok(())
    }

    fn recover_memtable(
        entries: Vec<WriteAheadLogEntry>,
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
    ) -> io::Result<MemTable> {
        let mut memtable =
            MemTable::with_capacity_and_rate(size_unit, capacity, false_positive_rate);

        // Iterate over the WAL entries
        for entry in entries {
            match entry.entry_kind {
                EntryKind::Insert => {
                    memtable.set(entry.key, entry.value)?;
                }
                EntryKind::Remove => {
                    memtable.remove(entry.key)?;
                }
            }
        }

        Ok(memtable)
    }
}

impl DirPath {
    fn build(directory_path: PathBuf) -> std::io::Result<Self> {
        let root = directory_path;
        let wal = root.join("wal");
        let sst = root.join("sst");

        // Ensure all required directories exist
        std::fs::create_dir_all(&wal)?;
        std::fs::create_dir_all(&sst)?;

        Ok(Self { root, wal, sst })
    }

    fn get_dir(&self) -> &str {
        self.root
            .to_str()
            .expect("Failed to convert path to string.")
    }
}

impl SizeUnit {
    pub(crate) const fn to_bytes(&self, value: usize) -> usize {
        match self {
            Self::Bytes => value,
            Self::Kilobytes => value * 1024,
            Self::Megabytes => value * 1024 * 1024,
            Self::Gigabytes => value * 1024 * 1024 * 1024,
        }
    }
}
