use bit_vec::BitVec;
use std::{
    fs::{self, metadata},
    io::{self, Write},
    path::PathBuf,
};

use super::sstable::SSTableEntry;

/// A helper struct to assist in writing key-value entries to an SSTable.
/// It can handle sorting, compression, and writing entries to the SSTable file.
pub(crate) struct SSTableWriter {
    file_path: PathBuf,
    entry_count: usize,
    sorted_entries: Vec<SSTableEntry>,
    current_level: u32,
    timestamp: u64,
}

impl SSTableWriter {
    pub(crate) fn new(file_path: PathBuf, current_level: u32, timestamp: u64) -> Self {
        Self {
            file_path,
            entry_count: 0,
            sorted_entries: Vec::new(),
            current_level,
            timestamp,
        }
    }

    pub(crate) fn write_entry(&mut self, key: Vec<u8>, value: Vec<u8>, timestamp: u64) {
        let sst_entry = SSTableEntry {
            key,
            value,
            timestamp,
        };

        self.sorted_entries.push(sst_entry);
        self.entry_count += 1;
    }

    pub fn is_full(&self, capacity: usize) -> bool {
        // Check if the file size exceeds the capacity
        if let Ok(file_size) = metadata(&self.file_path).map(|md| md.len() as usize) {
            file_size >= capacity
        } else {
            false
        }
    }

    pub(crate) fn finalize(&mut self) -> io::Result<()> {
        // Sort the key-value entries.
        self.sort_entries();

        // Compress the sorted entries to the SSTable file.
        let compressed_entries = self.compress_entries();

        // Write the compressed entries to the SSTable file.
        self.write_to_disk(&compressed_entries)?;

        Ok(())
    }

    fn sort_entries(&mut self) {
        self.sorted_entries.sort_by(|a, b| a.key.cmp(&b.key));
    }

    fn compress_entries(&self) -> Vec<u8> {
        // Create a buffer to store the compressed data.
        let mut compressed_data = Vec::new();

        // Iterate over each entry in sorted_entries.
        for entry in &self.sorted_entries {
            // Compress the value using LZ4 compression algorithm.
            let compressed_value = lz4_flex::block::compress(&entry.value);

            // Serialize the compressed entry.
            let mut serialized_entry = Vec::new();
            serialized_entry.extend_from_slice(&entry.key);
            serialized_entry.extend_from_slice(&compressed_value);

            // Append the serialized entry to the compressed_data buffer.
            compressed_data.extend_from_slice(&serialized_entry);
        }

        // Return the compressed data.
        compressed_data
    }

    fn write_to_disk(&self, compressed_data: &[u8]) -> io::Result<()> {
        // Open the file for writing.
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(&self.file_path)?;

        file.write_all(compressed_data)?;

        Ok(())
    }
}
