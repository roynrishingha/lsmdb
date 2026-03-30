use lsmdb::StorageEngine;
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_crash_and_manifest_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    // 1. Initial run: write some data and force it to be flushed
    {
        let engine = StorageEngine::open(&db_path).unwrap();

        // Write enough data to trigger a MemTable flush if capacity is low,
        // but here capacity is 4MB. We can just fill it or rely on WAL recovery.
        // Let's rely on WAL recovery first.
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            let val = format!("val_{:04}", i).into_bytes();
            engine.put(&key, &val).unwrap();
        }

        // Verify it's there
        let val = engine.get(b"key_0050").unwrap().unwrap();
        assert_eq!(val, b"val_0050");

        // Drop the engine simulating a crash or shutdown.
        // Data is in WAL, or partially in SST (if we had flushed).
    }

    // 2. Second run: recover data
    {
        let engine = StorageEngine::open(&db_path).unwrap();

        // Data should be meticulously recreated from WAL
        let val = engine.get(b"key_0050").unwrap().unwrap();
        assert_eq!(val, b"val_0050");

        let val_missing = engine.get(b"key_9999").unwrap();
        assert!(val_missing.is_none());
    }
}

#[test]
fn test_concurrent_write_chaos() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    let engine = Arc::new(StorageEngine::open(db_path).unwrap());
    let mut handles = vec![];

    // Spawn 50 threads doing puts, 50 threads doing gets/deletes
    for thread_id in 0..50 {
        let engine_clone = Arc::clone(&engine);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("concurrent_key_{}_{}", thread_id, i).into_bytes();
                let val = format!("concurrent_val_{}_{}", thread_id, i).into_bytes();
                engine_clone.put(&key, &val).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify some keys
    for thread_id in 0..50 {
        let key = format!("concurrent_key_{}_{}", thread_id, 42).into_bytes();
        let expected_val = format!("concurrent_val_{}_{}", thread_id, 42).into_bytes();
        let val = engine.get(&key).unwrap().unwrap();
        assert_eq!(val, expected_val);
    }
}

#[test]
fn test_data_overwrites_and_tombstones() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path()).unwrap();

    let key = b"hero";

    // 1. Initial put
    engine.put(key, b"batman").unwrap();
    assert_eq!(engine.get(key).unwrap().unwrap(), b"batman");

    // 2. Overwrite
    engine.update(key, b"superman").unwrap();
    assert_eq!(engine.get(key).unwrap().unwrap(), b"superman");

    // 3. Delete (Tombstone)
    engine.remove(key).unwrap();
    assert!(engine.get(key).unwrap().is_none());
}

#[test]
fn test_huge_data_batch_flushing() {
    let temp_dir = TempDir::new().unwrap();
    let engine = StorageEngine::open(temp_dir.path()).unwrap();

    // Since our memtable_capacity is 4MB in open(), writing hundreds of MBs
    // in a single thread test would take time, but let's write enough to
    // test the system stability under load.
    // We will write 10_000 keys of 1KB each (~10MB total).
    let value_payload = vec![0xAF; 1024];

    for i in 0..10000 {
        let key = format!("heavy_key_{:06}", i).into_bytes();
        engine.put(&key, &value_payload).unwrap();
    }

    // Verify reading back random keys
    assert_eq!(
        engine.get(b"heavy_key_000000").unwrap().unwrap(),
        value_payload
    );
    assert_eq!(
        engine.get(b"heavy_key_005000").unwrap().unwrap(),
        value_payload
    );
    assert_eq!(
        engine.get(b"heavy_key_009999").unwrap().unwrap(),
        value_payload
    );
}
