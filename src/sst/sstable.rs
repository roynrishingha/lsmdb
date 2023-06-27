//! # SSTable (Sorted String Table)
//!
//! An SSTable, or Sorted String Table, is an immutable on-disk data structure that stores key-value pairs in a sorted order.
//! It serves as the persistent storage layer for the LSM Tree-based engine.
//! SSTables are typically stored as multiple files, each containing a sorted range of key-value pairs.
//!
//! When the MemTable reaches a certain threshold size, it is "flushed" to disk as a new SSTable file.
//! The MemTable is atomically replaced with an empty one, allowing new write operations to continue. This process is known as a "memtable flush."
//!
//! ```text
//! +-----------------------+
//! |       SSTable         |
//! +-----------------------+
//! |  - file_path          |   (PathBuf)
//! |  - blocks             |   (Vec<Block>)
//! |  - created_at         |   (DateTime<Utc>)
//! +-----------------------+
//! |  + new(dir: PathBuf)  |   -> SSTable
//! |  + set(key, value)    |   -> Result<(), io::Error>
//! |  + get(key)           |   -> Option<Vec<u8>>
//! |  + remove(key)        |   -> Result<(), io::Error>
//! +-----------------------+
//!
//! +-----------------------+
//! |        Block          |
//! +-----------------------+
//! |  - data               |   (Vec<u8>)
//! |  - index              |   (HashMap<Arc<Vec<u8>>, usize>)
//! |  - entry_count        |   (usize)
//! +-----------------------+
//! |  + new()              |   -> Block
//! |  + is_full(size)      |   -> bool
//! |  + set_entry(key, value) | -> Result<(), io::Error>
//! |  + remove_entry(key)  |   -> bool
//! |  + get_value(key)     |   -> Option<Vec<u8>>
//! |  + entry_count()      |   -> usize
//! +-----------------------+
//! ```
//!
//! The `SSTable` struct represents the Sorted String Table and contains the following fields:
//! - `file_path`: Stores the path of the SSTable file (`PathBuf`).
//! - `blocks`: Represents a collection of blocks that hold the data (`Vec<Block>`).
//! - `created_at`: Indicates the creation timestamp of the SSTable (`DateTime<Utc>`).
//!
//! The `SSTable` struct provides the following methods:
//!
//! - `new(dir: PathBuf) -> SSTable`: Creates a new instance of the `SSTable` struct given a directory path and initializes its fields. Returns the created `SSTable`.
//!
//! - `set(key: Vec<u8>, value: Vec<u8>) -> Result<(), io::Error>`: Sets an entry with the provided key and value in the `SSTable`. It internally manages the blocks and their capacity to store entries. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! - `get(key: Vec<u8>) -> Option<Vec<u8>>`: Retrieves the value associated with the provided key from the `SSTable`. It iterates over the blocks to find the key-value pair. Returns `Option<Vec<u8>>` with the value if found, or `None` if the key is not present.
//!
//! - `remove(key: Vec<u8>) -> Result<(), io::Error>`: Removes the entry with the provided key from the `SSTable`. It iterates over the blocks in reverse order to delete from the most recent block first. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! The `Block` struct represents an individual block within the SSTable and contains the following fields:
//!
//! - `data`: Stores the data entries within the block (`Vec<u8>`).
//! - `index`: Maintains an index for efficient key-based lookups (`HashMap<Arc<Vec<u8>>, usize>`).
//! - `entry_count`: Tracks the number of entries in the block (usize).
//!
//! The `Block` struct provides the following methods:
//!
//! - `new() -> Block`: Creates a new instance of the `Block` struct and initializes its fields. Returns the created `Block`.
//!
//! - `is_full(entry_size: usize) -> bool`: Checks if the block is full given the size of an entry. It compares the combined size of the existing data and the new entry size with the predefined block size. Returns `true` if the block is full, `false` otherwise.
//!
//! - `set_entry(key: &[u8], value: &[u8]) -> Result<(), io::Error>`: Sets an entry with the provided key and value in the block. It calculates the entry size, checks if the block has enough capacity, and adds the entry to the block's data and index. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! - `remove_entry(key: &[u8]) -> bool`: Removes the entry with the provided key from the block. It searches for the key in the index, clears the entry in the data vector, and updates the entry count. Returns `true` if the entry was found and removed, `false` otherwise.
//!
//! - `get_value(key: &[u8]) -> Option<Vec<u8>>`: Retrieves the value associated with the provided key from the block. It looks up the key in the index, extracts the value bytes from the data vector, and returns them as a new `Vec<u8>`. Returns `Option<Vec<u8>>` with the value if found, or `None` if the key is not present.
//!
//! - `entry_count() -> usize`: Returns the number of entries in the block.
//!
//! Together, the `SSTable` and `Block` form the basic components of the SSTable implementation, providing efficient storage and retrieval of key-value pairs with support for adding and removing entries.
//!

use super::sst_block::Block;
use chrono::{DateTime, Utc};
use std::{io, path::PathBuf};

pub struct SSTable {
    file_path: PathBuf,
    blocks: Vec<Block>,
    created_at: DateTime<Utc>,
}

impl SSTable {
    pub(crate) fn new(dir: PathBuf) -> Self {
        let created_at = Utc::now();
        let file_name = format!("sstable_{}.dat", created_at.timestamp_millis());
        let file_path = dir.join(file_name);

        Self {
            file_path,
            blocks: Vec::new(),
            created_at,
        }
    }

    pub(crate) fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        if self.blocks.is_empty() || self.blocks.last().unwrap().is_full(key.len() + value.len()) {
            let new_block = Block::new();
            self.blocks.push(new_block);
        }

        let last_block = self.blocks.last_mut().unwrap();
        last_block.set_entry(&key, &value)?;

        Ok(())
    }

    pub(crate) fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        for block in &self.blocks {
            if let Some(value) = block.get_value(&key) {
                return Some(value);
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<()> {
        // Iterate over the blocks in reverse order to delete from the most recent block first.
        for block in self.blocks.iter_mut().rev() {
            if block.remove_entry(&key) {
                return Ok(());
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Key not found in SSTable",
        ))
    }
}
