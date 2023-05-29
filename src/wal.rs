use crate::{
    helper::{generate_timestamp, get_files_with_ext},
    mem_table::MemTable,
};
use std::{
    fs::{remove_file, File, OpenOptions},
    io::{self, prelude::*, BufReader, BufWriter},
    path::{Path, PathBuf},
};

/// Write Ahead Log(WAL)
///
/// An append-only file that holds the operations performed on the MemTable.
/// The WAL is intended for recovery of the MemTable when the server is shutdown.
pub struct Wal {
    pub path: PathBuf,
    pub file: BufWriter<File>,
}

/// WALEntry
pub struct WalEntry {
    /// key is always there and used to query value
    key: Vec<u8>,
    /// value may or may not exist
    value: Option<Vec<u8>>,
    /// when the entry is created or modified
    timestamp: u128,
    /// state of the entry
    deleted: bool,
}

/// WAL iterator to iterate over the items in a WAL file.
pub struct WalIterator {
    reader: BufReader<File>,
}

impl Wal {
    /// Creates a new WAL in a given directory.
    pub fn new(dir_path: &Path) -> std::io::Result<Self> {
        let now = generate_timestamp();
        let path = Path::new(dir_path).join(now.to_string() + ".wal");
        let file = OpenOptions::new().append(true).create(true).open(&path)?;

        let file = BufWriter::new(file);

        Ok(Wal { path, file })
    }

    /// Creates a WAL from an existing file path.
    pub fn from_path(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let file = BufWriter::new(file);

        Ok(Wal {
            path: path.to_owned(),
            file,
        })
    }

    /// Loads the WAL(s) within a directory, returning a new WAL and the recovered MemTable.
    ///
    /// If multiple WALs exist in a directory, they are merged by file date.
    pub fn load_wal_from_dir(dir_path: &Path) -> io::Result<(Self, MemTable)> {
        // get existing `wal` files and sort it
        let mut wal_files = get_files_with_ext(dir_path, "wal");
        wal_files.sort();

        // create new MemTable
        let mut new_mem_table = MemTable::new();
        // create new Wal directory
        let mut new_wal_dir = Wal::new(dir_path)?;

        for wal_file in wal_files.iter() {
            if let Ok(wal) = Wal::from_path(wal_file) {
                for wal_entry in wal.into_iter() {
                    if wal_entry.deleted {
                        new_mem_table.delete(wal_entry.key.as_slice(), wal_entry.timestamp);
                        new_wal_dir.delete(wal_entry.key.as_slice(), wal_entry.timestamp)?;
                    } else {
                        new_mem_table.set(
                            wal_entry.key.as_slice(),
                            wal_entry.value.as_ref().unwrap().as_slice(),
                            wal_entry.timestamp,
                        );

                        new_wal_dir.set(
                            wal_entry.key.as_slice(),
                            wal_entry.value.unwrap().as_slice(),
                            wal_entry.timestamp,
                        )?;
                    }
                }
            }
        }
        new_wal_dir.flush().unwrap();
        wal_files
            .into_iter()
            .for_each(|file| remove_file(file).unwrap());

        Ok((new_wal_dir, new_mem_table))
    }

    /// Sets a Key-Value pair and the operation is appended to the WAL.
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Deletes a Key-Value pair and the operation is appended to the WAL.
    ///
    /// This is achieved using tombstones.
    ///
    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&key.len().to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;

        Ok(())
    }

    /// Flushes the WAL to disk.
    ///
    /// This is useful for applying bulk operations and flushing the final result to
    /// disk. Waiting to flush after the bulk operations have been performed will improve
    /// write performance substantially.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl WalIterator {
    /// Creates a new WALIterator from a path to a WAL file.
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WalIterator { reader })
    }
}

impl Iterator for WalIterator {
    type Item = WalEntry;

    /// Gets the next entry in the WAL file.
    fn next(&mut self) -> Option<WalEntry> {
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);

        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;

        let mut key = vec![0; key_len];
        let mut value = None;
        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }

        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);

        Some(WalEntry {
            key,
            value,
            timestamp,
            deleted,
        })
    }
}

impl IntoIterator for Wal {
    type IntoIter = WalIterator;
    type Item = WalEntry;

    /// Converts a WAL into a `WALIterator` to iterate over the entries.
    fn into_iter(self) -> WalIterator {
        WalIterator::new(self.path).unwrap()
    }
}
