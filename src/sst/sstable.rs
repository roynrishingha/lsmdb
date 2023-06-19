use super::bloom_filter::BloomFilter;
use std::{convert::TryInto, path::PathBuf};

/// Represents a single key-value entry with an SSTable.
/// Contains fileds to store the key, value, and timestamp.
pub(crate) struct SSTableEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) timestamp: u64,
}

/// Stores metadata associated with the SSTable
pub(crate) struct SSTableMetadata {
    pub(crate) timestamp: u64,
}

/// Represents an individual SSTable file.
/// Contains fileds to store the file path, level, bloom filter, metadata
pub(crate) struct SSTable {
    file_path: PathBuf,
    level: u32,
    bloom_filter: BloomFilter,
    metadata: SSTableMetadata,
}

/// Represents a level within the LSM-Tree structure.
/// Contains a collections of SSTables belonging to that level.
pub(crate) struct SSTableLevel {
    pub(crate) level_number: u32,
    pub(crate) sstables: Vec<SSTable>,
}

/// Representing a collection of SSTableLevels.
/// Contains the levels and provides methods for accessing and manipulating the SSTables.
pub(crate) struct SSTableSet {
    levels: Vec<SSTableLevel>,
}

impl SSTable {
    pub(crate) fn new(
        file_path: PathBuf,
        level: u32,
        bloom_filter: BloomFilter,
        metadata: SSTableMetadata,
    ) -> Self {
        Self {
            file_path,
            level,
            bloom_filter,
            metadata,
        }
    }

    pub(crate) fn file_path(&self) -> &PathBuf {
        &self.file_path
    }

    pub(crate) fn level(&self) -> u32 {
        self.level
    }

    pub(crate) fn bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }

    pub(crate) fn metadata(&self) -> &SSTableMetadata {
        &self.metadata
    }
}

impl SSTableLevel {
    /// Create a new SSTableLevel with the specified level number.
    pub(crate) fn new(level_number: u32) -> Self {
        SSTableLevel {
            level_number,
            sstables: Vec::new(),
        }
    }

    /// Add an SSTable to the level.
    pub(crate) fn add_sstable(&mut self, sstable: SSTable) {
        self.sstables.push(sstable);
    }

    /// Get the number of SSTables in the level.
    pub(crate) fn num_sstables(&self) -> usize {
        self.sstables.len()
    }

    /// Get a reference to an SSTable in the level by its index.
    pub(crate) fn get_sstable(&self, index: usize) -> Option<&SSTable> {
        self.sstables.get(index)
    }

    /// Get a mutable reference to an SSTable in the level by its index.
    pub(crate) fn get_sstable_mut(&mut self, index: usize) -> Option<&mut SSTable> {
        self.sstables.get_mut(index)
    }
}

impl SSTableMetadata {
    fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl SSTableSet {
    /// Create a new empty SSTableSet.
    pub(crate) fn new() -> Self {
        Self { levels: Vec::new() }
    }

    /// Get the number of levels in the SSTablesSet.
    pub(crate) fn num_levels(&self) -> usize {
        self.levels.len()
    }

    /// Get a reference to the specific level by its index.
    pub(crate) fn get_level(&self, index: usize) -> Option<&SSTableLevel> {
        self.levels.get(index)
    }

    /// Get a mutable reference to the specific level by its index.
    pub(crate) fn get_level_mut(&mut self, index: usize) -> Option<&mut SSTableLevel> {
        self.levels.get_mut(index)
    }

    /// Add a new SSTable to a specific level in the SSTableSet.
    pub(crate) fn add_sstable(&mut self, level: usize, sstable: SSTable) {
        while level >= self.levels.len() {
            self.levels
                .push(SSTableLevel::new(self.levels.len().try_into().unwrap()));
        }
        if let Some(level) = self.levels.get_mut(level) {
            level.add_sstable(sstable);
        }
    }
}
