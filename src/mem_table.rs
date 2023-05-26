/// Entry for MemTable
#[derive(Debug, PartialEq)]
pub struct MemTableEntry {
    /// key is always there and used to query value
    pub key: Vec<u8>,
    /// value may or may not exist
    pub value: Option<Vec<u8>>,
    /// when the entry is created or modified
    pub timestamp: u128,
    /// state of the entry
    pub deleted: bool,
}

/// Each table consists of multiple entries
/// entries are then stored into WAL: Write Ahead Log
/// to recover MemTable from dataloss
pub struct MemTable {
    // TODO: This should be using a `SkipList` instead of a Vector
    /// All entries are stored in memory before copied into WAL
    /// Then it'll be written in SSTable
    pub entries: Vec<MemTableEntry>,
    /// Size of the MemTable
    pub size: usize,
}

impl MemTable {
    /// create a new MemTable with empty values
    pub fn new() -> Self {
        MemTable {
            entries: Vec::new(),
            size: 0,
        }
    }

    /// Set a new entry in MemTable
    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let memtable_entry = new_memtable_entry(key, Some(value), timestamp, false);

        match self.get_index(key) {
            // If a Value existed on the deleted record, then add the difference of the new and old Value to the MemTable's size.
            Ok(id) => {
                if let Some(stored_value) = self.entries[id].value.as_ref() {
                    if value.len() < stored_value.len() {
                        self.size -= stored_value.len() - value.len();
                    } else {
                        self.size += value.len() - stored_value.len()
                    }
                } else {
                    self.size += value.len();
                }
                self.entries[id] = memtable_entry;
            }
            Err(id) => {
                // Increase the size of the MemTable by the Key size, Value size, Timestamp size (16 bytes), Tombstone size (1 byte)
                self.size += key.len() + value.len() + 16 + 1;
                self.entries.insert(id, memtable_entry)
            }
        }
    }

    /// Get an entry from MemTable
    ///
    /// If no record with the same key exists in the MemTable, return None.
    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(id) = self.get_index(key) {
            return Some(&self.entries[id]);
        }
        None
    }

    /// Delete an entry from MemTable
    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let memtable_entry = new_memtable_entry(key, None, timestamp, true);

        // check if entry exists for the given key
        match self.get_index(key) {
            Ok(id) => {
                // If a Value existed on the deleted record, then subtract the size of the Value from the MemTable.
                if let Some(existed_value) = self.entries[id].value.as_ref() {
                    self.size -= existed_value.len();
                }

                // update stored entry with new entry
                self.entries[id] = memtable_entry;
            }
            Err(id) => {
                // Increase the size of the MemTable by the Key size, Timestamp size (16 bytes), Tombstone size (1 byte).
                self.size += key.len() + 16 + 1;

                // Insert new entry
                self.entries.insert(id, memtable_entry);
            }
        }
    }
}

// Methods for internal usecases.
// A user will not directly call these methods.
impl MemTable {
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key, |entry| entry.key.as_slice())
    }
}

/// Creates a new MemTableEntry
/// Takes key: &[u8], value: Option<&[u8]>, timestamp: u128, deleted: bool
/// Returns: MemTableEntry
pub fn new_memtable_entry(
    key: &[u8],
    value: Option<&[u8]>,
    timestamp: u128,
    deleted: bool,
) -> MemTableEntry {
    MemTableEntry {
        key: key.to_owned(),
        value: value.map(|v| v.to_vec()),
        timestamp,
        deleted,
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}
