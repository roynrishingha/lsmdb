use lsmdb::engine::*;
use std::fs;
use std::sync::{Arc, Mutex};

#[test]
fn test_storage_engine() {
    // Create a new storage engine instance
    let mut engine = StorageEngine::new("./test_dir");

    // Write data to the storage engine
    let result = engine.set(b"key1", b"value1");
    assert_eq!(result, Ok(1));

    // Read data from the storage engine
    let entry = engine.get(b"key1");
    assert_eq!(
        entry.clone(),
        Some(StorageEngineEntry {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            timestamp: entry.unwrap().timestamp()
        })
    );

    // Delete data from the storage engine
    let result = engine.delete(b"key1");
    assert_eq!(result, Ok(1));

    // Verify that the data is deleted
    let entry = engine.get(b"key1");
    assert_eq!(entry, None);

    // Cleanup: Remove the test directory
    // Attempt to remove the directory "./test_dir"
    // If an error occurs during the removal, the error will be captured and handled
    if let Err(e) = fs::remove_dir_all("./test_dir") {
        // Print an error message indicating that the removal of the test directory failed,
        // along with the specific error encountered
        println!("Failed to remove test directory: {:?}", e);
    }
}

#[test]
fn test_storage_engine_concurrent_writes() {
    // Create a new storage engine instance
    let engine = Arc::new(Mutex::new(StorageEngine::new("./test_dir")));

    // Spawn multiple threads to concurrently write data to the storage engine
    let num_threads = 10;
    let mut handles = vec![];

    for i in 0..num_threads {
        let engine_ref = Arc::clone(&engine);
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let handle = std::thread::spawn(move || {
            let mut engine = engine_ref.lock().unwrap();
            engine.set(key.as_bytes(), value.as_bytes())
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        if let Err(e) = handle.join() {
            panic!("Thread panicked: {:?}", e);
        }
    }

    // Verify that all writes were successful
    for i in 0..num_threads {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        let engine = engine.lock().unwrap();
        let entry = engine.get(key.as_bytes()).unwrap();
        assert_eq!(
            entry,
            StorageEngineEntry {
                key: key.as_bytes().to_vec(),
                value: value.as_bytes().to_vec(),
                timestamp: entry.timestamp,
            }
        );
    }

    // Cleanup: Remove the test directory
    // Attempt to remove the directory "./test_dir"
    // If an error occurs during the removal, the error will be captured and handled
    if let Err(e) = fs::remove_dir_all("./test_dir") {
        // Print an error message indicating that the removal of the test directory failed,
        // along with the specific error encountered
        println!("Failed to remove test directory: {:?}", e);
    }
}

#[test]
fn test_storage_engine_delete() {
    // Create a new storage engine instance
    let mut engine = StorageEngine::new("./test_dir");

    // Write data to the storage engine
    engine.set(b"key1", b"value1").expect("Failed to set data");
    engine.set(b"key2", b"value2").expect("Failed to set data");

    // Verify that the data exists
    let entry1 = engine.get(b"key1").expect("Failed to get data");
    let entry2 = engine.get(b"key2").expect("Failed to get data");
    assert_eq!(entry1.value, b"value1");
    assert_eq!(entry2.value, b"value2");

    // Delete an entry from the storage engine
    engine.delete(b"key1").expect("Failed to delete data");

    // Verify that the deleted entry is no longer accessible
    let deleted_entry = engine.get(b"key1");
    assert!(deleted_entry.is_none());

    // Verify that the remaining entry still exists
    let remaining_entry = engine.get(b"key2").expect("Failed to get data");
    assert_eq!(remaining_entry.value, b"value2");

    // Cleanup: Remove the test directory
    // Attempt to remove the directory "./test_dir"
    // If an error occurs during the removal, the error will be captured and handled
    if let Err(e) = fs::remove_dir_all("./test_dir") {
        // Print an error message indicating that the removal of the test directory failed,
        // along with the specific error encountered
        println!("Failed to remove test directory: {:?}", e);
    }
}
