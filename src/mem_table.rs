/// Entry for MemTable
#[derive(Debug, PartialEq)]
struct MemTableEntry {
    /// key is always there and used to query value
    key: Vec<u8>,
    /// value may or may not exist
    value: Option<Vec<u8>>,
    /// when the entry is created or modified
    timestamp: u128,
    /// state of the entry
    deleted: bool,
}

/// Each table consists of multiple entries
/// entries are then stored into WAL: Write Ahead Log
/// to recover MemTable from dataloss
struct MemTable {
    // TODO: This should be using a `SkipList` instead of a Vector
    /// All entries are stored in memory before copied into WAL
    /// Then it'll be written in SSTable
    entries: Vec<MemTableEntry>,
    /// Size of the MemTable
    size: usize,
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
fn new_memtable_entry(
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

#[cfg(test)]
mod tests {
    use crate::mem_table::new_memtable_entry;

    use super::{MemTable, MemTableEntry};

    #[test]
    fn create_empty_mem_table() {
        let mem_table = MemTable::new();
        assert_eq!(mem_table.size, 0);
    }

    #[test]
    fn set_single_entry() {
        let mut mem_table = MemTable::new();

        let key = b"key1";
        let value = b"value1";

        // set the entry
        mem_table.set(key, value, 10);

        let expected_entry = new_memtable_entry(key, Some(value), 10, false);

        // query the entry and compare
        assert_eq!(mem_table.get(key), Some(&expected_entry));
    }

    #[test]
    fn test_delete_entry() {
        let mut memtable = MemTable::new();
        let key = b"key";
        let value = b"value";

        memtable.set(key, value, 20);
        memtable.delete(key, 30);

        let expected_entry = new_memtable_entry(key, None, 30, true);

        assert_eq!(memtable.get(key), Some(&expected_entry));
    }

    #[test]
    fn test_mem_table_put_start() {
        let mut table = MemTable::new();
        table.set(b"Lime", b"Lime Smoothie", 0); // 17 + 16 + 1
        table.set(b"Orange", b"Orange Smoothie", 10); // 21 + 16 + 1

        table.set(b"Apple", b"Apple Smoothie", 20); // 19 + 16 + 1

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 20);
        assert_eq!(table.entries[0].deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 0);
        assert_eq!(table.entries[1].deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert_eq!(table.entries[2].deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_middle() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 10);

        table.set(b"Lime", b"Lime Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert_eq!(table.entries[0].deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 20);
        assert_eq!(table.entries[1].deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 10);
        assert_eq!(table.entries[2].deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_end() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);

        table.set(b"Orange", b"Orange Smoothie", 20);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert_eq!(table.entries[0].deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"Lime Smoothie");
        assert_eq!(table.entries[1].timestamp, 10);
        assert_eq!(table.entries[1].deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert_eq!(table.entries[2].deleted, false);

        assert_eq!(table.size, 108);
    }

    #[test]
    fn test_mem_table_put_overwrite() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        table.set(b"Lime", b"A sour fruit", 30);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value.as_ref().unwrap(), b"Apple Smoothie");
        assert_eq!(table.entries[0].timestamp, 0);
        assert_eq!(table.entries[0].deleted, false);
        assert_eq!(table.entries[1].key, b"Lime");
        assert_eq!(table.entries[1].value.as_ref().unwrap(), b"A sour fruit");
        assert_eq!(table.entries[1].timestamp, 30);
        assert_eq!(table.entries[1].deleted, false);
        assert_eq!(table.entries[2].key, b"Orange");
        assert_eq!(table.entries[2].value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(table.entries[2].timestamp, 20);
        assert_eq!(table.entries[2].deleted, false);

        assert_eq!(table.size, 107);
    }

    #[test]
    fn test_mem_table_get_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 10);
        table.set(b"Orange", b"Orange Smoothie", 20);

        let entry = table.get(b"Orange").unwrap();

        assert_eq!(entry.key, b"Orange");
        assert_eq!(entry.value.as_ref().unwrap(), b"Orange Smoothie");
        assert_eq!(entry.timestamp, 20);
    }

    #[test]
    fn test_mem_table_get_not_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);
        table.set(b"Lime", b"Lime Smoothie", 0);
        table.set(b"Orange", b"Orange Smoothie", 0);

        let res = table.get(b"Potato");
        assert_eq!(res.is_some(), false);
    }

    #[test]
    fn test_mem_table_delete_exists() {
        let mut table = MemTable::new();
        table.set(b"Apple", b"Apple Smoothie", 0);

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert_eq!(res.deleted, true);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert_eq!(table.entries[0].deleted, true);

        assert_eq!(table.size, 22);
    }

    #[test]
    fn test_mem_table_delete_empty() {
        let mut table = MemTable::new();

        table.delete(b"Apple", 10);

        let res = table.get(b"Apple").unwrap();
        assert_eq!(res.key, b"Apple");
        assert_eq!(res.value, None);
        assert_eq!(res.timestamp, 10);
        assert_eq!(res.deleted, true);

        assert_eq!(table.entries[0].key, b"Apple");
        assert_eq!(table.entries[0].value, None);
        assert_eq!(table.entries[0].timestamp, 10);
        assert_eq!(table.entries[0].deleted, true);

        assert_eq!(table.size, 22);
    }
}
