//! # Write-Ahead Log (WAL)
//!
//! The Sequential Write-Ahead Log (WAL) is a crucial component of the LSM Tree storage engine.
//! It provides durability and atomicity guarantees by logging write operations before they are applied to the main data structure.
//!
//! When a write operation is received, the key-value pair is first appended to the WAL.
//! In the event of a crash or system failure, the WAL can be replayed to recover the data modifications and bring the MemTable back to a consistent state.
//!
//! ## WriteAheadLog Structure
//!
//! The `WriteAheadLog` structure represents the write-ahead log (WAL) and contains the following field:
//!
//! ```rs
//! struct WriteAheadLog {
//!     log_file: Arc<Mutex<File>>,
//! }
//! ```
//!
//! ### log_file
//!
//! The `log_file` field is of type `Arc<Mutex<File>>`. It represents the WAL file and provides concurrent access and modification through the use of an `Arc` (Atomic Reference Counting) and `Mutex`.
//!
//! ## Log File Structure Diagram
//!
//! The `log_file` is structured as follows:
//!
//! ```text
//! +-------------------+
//! |  Entry Length     |   (4 bytes)
//! +-------------------+
//! |   Entry Kind      |   (1 byte)
//! +-------------------+
//! |   Key Length      |   (4 bytes)
//! +-------------------+
//! |  Value Length     |   (4 bytes)
//! +-------------------+
//! |       Key         |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |      Value        |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |  Entry Length     |   (4 bytes)
//! +-------------------+
//! |   Entry Kind      |   (1 byte)
//! +-------------------+
//! |   Key Length      |   (4 bytes)
//! +-------------------+
//! |  Value Length     |   (4 bytes)
//! +-------------------+
//! |       Key         |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |      Value        |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! ```
//!
//! - **Entry Length**: A 4-byte field representing the total length of the entry in bytes.
//! - **Entry Kind**: A 1-byte field indicating the type of entry (Insert or Remove).
//! - **Key Length**: A 4-byte field representing the length of the key in bytes.
//! - **Key**: The actual key data, which can vary in size.
//! - **Value** Length: A 4-byte field representing the length of the value in bytes.
//! - **Value**: The actual value data, which can also vary in size.
//!
//! Each entry is written sequentially into the `log_file` using the `write_all` method, ensuring that the entries are stored contiguously. New entries are appended to the end of the `log_file` after the existing entries.
//!
//! ## Constants
//!
//! A constant named `WAL_FILE_NAME` is defined, representing the name of the WAL file.
//!
//! ```rs
//! static WAL_FILE_NAME: &str = "lsmdb_wal.bin";
//! ```
//!
//! ## `EntryKind`
//!
//! ```rs
//! enum EntryKind {
//!     Insert = 1,
//!     Remove = 2,
//! }
//! ```
//!
//! The `EntryKind` enum represents the kind of entry stored in the WAL. It has two variants: `Insert` and `Remove`. Each variant is associated with an integer value used for serialization.
//!
//! ## `WriteAheadLogEntry`
//!
//! ```rs
//! struct WriteAheadLogEntry {
//!     entry_kind: EntryKind,
//!     key: Vec<u8>,
//!     value: Vec<u8>,
//! }
//! ```
//!
//! The `WriteAheadLogEntry` represents a single entry in the Write-Ahead Log. It contains the following fields:
//!
//! - **`entry_kind`**: An enumeration (`EntryKind`) representing the type of the entry (insert or remove).
//! - **`key`**: A vector of bytes (`Vec<u8>`) representing the key associated with the entry.
//! - **`value`**: A vector of bytes (`Vec<u8>`) representing the value associated with the entry.
//!
//! ## `WriteAheadLogEntry` Methods
//!
//! ### `new`
//!
//! ```rs
//! fn new(entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8>) -> Self
//! ```
//!
//! The `new` method creates a new instance of the `WriteAheadLogEntry` struct. It takes the `entry_kind`, `key`, and `value` as parameters and initializes the corresponding fields.
//!
//! ### `serialize`
//!
//! ```rs
//! fn serialize(&self) -> Vec<u8>
//! ```
//!
//! The `serialize` method serializes the `WriteAheadLogEntry` into a vector of bytes.
//! It calculates the length of the entry, then serializes the length, entry kind, key length, value length, key, and value into the vector. The serialized data is returned.
//!
//! ### `deserialize`
//!
//! ```rs
//! fn deserialize(serialized_data: &[u8]) -> io::Result<Self>
//! ```
//!
//! This method deserializes a `WriteAheadLogEntry` from the provided serialized data.
//! It performs validation checks on the length and structure of the serialized data and returns an `io::Result` containing the deserialized entry if successful.
//!
//! ## `WriteAheadLog` Methods
//!
//! ### `new`
//!
//! ```rs
//! fn new(directory_path: &PathBuf) -> io::Result<Self>
//! ```
//!
//! The `new` method is a constructor function that creates a new `WriteAheadLog` instance.
//! It takes a `directory_path` parameter as a `PathBuf` representing the directory path where the WAL file will be stored.
//!
//! If the directory doesn't exist, it creates it. It then opens the log file with read, append, and create options, and initializes the log_file field.
//!
//! ### `append`
//!
//! ```rs
//! fn append(&mut self, entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8> ) -> io::Result<()>
//! ```
//!
//! The `append` method appends a new entry to the Write-Ahead Log.
//! It takes an `entry_kind` parameter of type `EntryKind`, a `key` parameter of type `Vec<u8>`, and a `value` parameter of type `Vec<u8>`. The method acquires a lock on the `log_file` to ensure mutual exclusion when writing to the file.
//!
//! It creates a `WriteAheadLogEntry` with the provided parameters, serializes it, and writes the serialized data to the log file.
//!
//! Finally, it flushes the log file to ensure the data is persisted. If the operation succeeds, `Ok(())` is returned; otherwise, an `io::Error` instance is created and returned.
//!
//! ### `recover`
//!
//! ```rs
//! fn recover(&mut self) -> io::Result<Vec<WriteAheadLogEntry>>
//! ```
//!
//! The `recover` method reads and recovers the entries from the Write-Ahead Log. The method acquires a lock on the `log_file` to ensure exclusive access during the recovery process.
//!
//! It reads the log file and deserializes the data into a vector of `WriteAheadLogEntry` instances.
//! It continues reading and deserializing until the end of the log file is reached. The recovered entries are returned as a vector.
//!
//! ### `clear`
//!
//! ```rs
//! fn clear(&mut self) -> io::Result<()>
//! ```
//!
//! The `clear` method clears the contents of the WAL file. It acquires a lock on the `log_file` to ensure exclusive access when truncating and seeking the file.
//! The method sets the length of the file to `0` using the `set_len` method, effectively truncating it. Then, it seeks to the start of the file using `seek` with `SeekFrom::Start(0)` to reset the file pointer.
//! If the operation succeeds, `Ok(())` is returned; otherwise, an `io::Error` instance is created and returned.
//!
//! ## Thread Safety
//!
//! The `WriteAheadLog` implementation ensures thread safety by using an `Arc<Mutex<File>>` for the `log_file` field. The `Arc` allows multiple ownership of the WAL file across threads, and the `Mutex` ensures exclusive access to the file during write, recovery, and clear operations, preventing data races.
//!
//! The locking mechanism employed by the `Mutex` guarantees that only one thread can modify the WAL file at a time, while allowing multiple threads to read from it simultaneously.
//!

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) static WAL_FILE_NAME: &str = "lsmdb_wal.bin";

pub struct WriteAheadLog {
    log_file: Arc<Mutex<File>>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum EntryKind {
    Insert = 1,
    Remove = 2,
}

#[derive(PartialEq, Debug)]
pub(crate) struct WriteAheadLogEntry {
    pub(crate) entry_kind: EntryKind,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl WriteAheadLog {
    pub(crate) fn new(directory_path: &PathBuf) -> io::Result<Self> {
        // Convert directory path to a PathBuf.
        let dir_path = PathBuf::from(directory_path);

        // Create the directory if it doesn't exist.
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }

        // Generate the file path within the directory.
        let file_path = dir_path.join(WAL_FILE_NAME);

        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(file_path)?;

        Ok(Self {
            log_file: Arc::new(Mutex::new(log_file)),
        })
    }

    pub(crate) fn append(
        &mut self,
        entry_kind: EntryKind,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> io::Result<()> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::other(format!("Failed to obtain lock: {:?}", poison_error))
        })?;

        let entry = WriteAheadLogEntry::new(entry_kind, key, value);

        let serialized_data = entry.serialize();

        log_file.write_all(&serialized_data)?;

        log_file.flush()?;

        Ok(())
    }

    pub(crate) fn recover(&mut self) -> io::Result<Vec<WriteAheadLogEntry>> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::other(format!("Failed to obtain lock: {:?}", poison_error))
        })?;

        let mut entries = Vec::new();

        loop {
            let mut serialized_data = Vec::new();
            log_file.read_to_end(&mut serialized_data)?;

            if serialized_data.is_empty() {
                // Reached the end of the log file
                break;
            }

            let entry = WriteAheadLogEntry::deserialize(&serialized_data)?;

            entries.push(entry);
        }
        Ok(entries)
    }

    pub(crate) fn clear(&mut self) -> io::Result<()> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::other(format!("Failed to obtain lock: {:?}", poison_error))
        })?;

        log_file.set_len(0)?;
        log_file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

impl WriteAheadLogEntry {
    pub(crate) fn new(entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            entry_kind,
            key,
            value,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        // Calculate entry length
        let entry_len = 4 + 1 + 4 + 4 + self.key.len() + self.value.len();

        let mut serialized_data = Vec::with_capacity(entry_len);

        // Serialize entry length
        serialized_data.extend_from_slice(&(entry_len as u32).to_le_bytes());

        // Serialize entry kind
        serialized_data.push(self.entry_kind as u8);

        // Serialize key length
        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        // Serialize value length
        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        // Serialize key
        serialized_data.extend_from_slice(&self.key);

        // Serialize value
        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }

    fn deserialize(serialized_data: &[u8]) -> io::Result<Self> {
        if serialized_data.len() < 13 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let entry_len = u32::from_le_bytes([
            serialized_data[0],
            serialized_data[1],
            serialized_data[2],
            serialized_data[3],
        ]) as usize;

        if serialized_data.len() != entry_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let entry_kind = match serialized_data[4] {
            1 => EntryKind::Insert,
            2 => EntryKind::Remove,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid entry kind value",
                ))
            }
        };

        let key_len = u32::from_le_bytes([
            serialized_data[5],
            serialized_data[6],
            serialized_data[7],
            serialized_data[8],
        ]) as usize;
        let value_len = u32::from_le_bytes([
            serialized_data[9],
            serialized_data[10],
            serialized_data[11],
            serialized_data[12],
        ]) as usize;

        if serialized_data.len() != 13 + key_len + value_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let key = serialized_data[13..(13 + key_len)].to_vec();
        let value = serialized_data[(13 + key_len)..].to_vec();

        Ok(WriteAheadLogEntry::new(entry_kind, key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        // Create a sample WriteAheadLogEntry.
        let entry_kind = EntryKind::Insert;
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        let original_entry = WriteAheadLogEntry::new(entry_kind, key.clone(), value.clone());

        // Serialize the entry.
        let serialized_data = original_entry.serialize();

        // Verify serialized data.
        let expected_entry_len = 4 + 1 + 4 + 4 + key.len() + value.len();
        assert_eq!(serialized_data.len(), expected_entry_len);

        // Deserialize the serialized data.
        let deserialized_entry =
            WriteAheadLogEntry::deserialize(&serialized_data).expect("Failed to deserialize");

        // Verify deserialized entry matches the original entry
        assert_eq!(deserialized_entry, original_entry);
    }
}
