use crate::{
    memtable::{MemTable, DEFAULT_FALSE_POSITIVE_RATE, DEFAULT_MEMTABLE_CAPACITY},
    sst::SSTable,
    write_ahead_log::{EntryKind, WriteAheadLog, WriteAheadLogEntry, WAL_FILE_NAME},
};
use std::{fs, io, path::PathBuf};

pub struct StorageEngine {
    memtable: MemTable,
    wal: WriteAheadLog,
    sstables: Vec<SSTable>,
    dir: DirPath,
}

struct DirPath {
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
    pub fn new(dir: &str) -> io::Result<Self> {
        StorageEngine::with_capacity(dir, SizeUnit::Bytes, DEFAULT_MEMTABLE_CAPACITY)
    }

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
        StorageEngine::with_capacity_and_rate(
            self.dir.get_dir(),
            size_unit,
            capacity,
            false_positive_rate,
        )
    }

    pub(crate) fn with_capacity(
        dir: &str,
        size_unit: SizeUnit,
        capacity: usize,
    ) -> io::Result<Self> {
        Self::with_capacity_and_rate(dir, size_unit, capacity, DEFAULT_FALSE_POSITIVE_RATE)
    }

    pub fn with_capacity_and_rate(
        dir: &str,
        size_unit: SizeUnit,
        capacity: usize,
        false_positive_rate: f64,
    ) -> io::Result<Self> {
        let dir_path = DirPath::build(dir);

        // The WAL file path.
        let wal_file_path = dir_path.wal.join(WAL_FILE_NAME);

        // Check if the WAL file exists and has contents.
        let wal_exists = wal_file_path.exists();
        let wal_empty = wal_exists && fs::metadata(&wal_file_path)?.len() == 0;

        if wal_empty {
            // WAL file empty, create a new WAL and MemTable.
            let memtable =
                MemTable::with_capacity_and_rate(size_unit, capacity, false_positive_rate);

            let wal = WriteAheadLog::new(&dir_path.wal)?;
            let sstables = Vec::new();

            Ok(Self {
                memtable,
                wal,
                sstables,
                dir: dir_path,
            })
        } else {
            // WAL file has logs, recover the MemTable from the WAL.
            let mut wal = WriteAheadLog::new(&dir_path.wal)?;

            // I should not create empty sstable. I need to load existing sstables if exists. Otherwise new empty one should be used.
            let sstables = Vec::new();

            let entries = wal.recover()?;

            let memtable =
                StorageEngine::recover_memtable(entries, size_unit, capacity, false_positive_rate)?;

            Ok(Self {
                memtable,
                wal,
                sstables,
                dir: dir_path,
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
    fn build(directory_path: &str) -> Self {
        let root = PathBuf::from(directory_path);
        let wal = root.join("wal");
        let sst = root.join("sst");

        Self { root, wal, sst }
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
