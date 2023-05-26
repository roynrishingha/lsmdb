use crate::mem_table::{new_memtable_entry, MemTable};

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
