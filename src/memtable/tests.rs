use super::*;

macro_rules! kv {
    ($k:expr, $v:expr) => {
        ($k.as_bytes().to_vec(), $v.as_bytes().to_vec())
    };
}

fn generate_dummy_kv_pairs() -> Vec<(Vec<u8>, Vec<u8>)> {
    vec![
        kv!("key1", "value1"),
        kv!("key2", "value2"),
        kv!("key3", "value3"),
    ]
}

#[test]
fn create_empty_memtable() {
    let m = MemTable::new(1024 * 1024, 0.01);
    assert!(m.approximate_memory_usage() < 256); // Dummy Head node Overhead is ~152 bytes
    assert_eq!(m.entries().len(), 0);
}

#[test]
fn test_set_and_get() {
    let mut m = MemTable::new(1024 * 1024, 0.01);
    let kv = generate_dummy_kv_pairs();

    for (k, v) in &kv {
        m.set(k.clone(), v.clone());
    }

    for (k, v) in &kv {
        assert_eq!(m.get(k).unwrap(), v);
    }

    // Non-existent key
    assert_eq!(m.get(b"missing"), None);
}

#[test]
fn test_overwrite_key() {
    let mut m = MemTable::new(1024 * 1024, 0.01);

    m.set(b"key".to_vec(), b"v1".to_vec());
    assert_eq!(m.get(b"key").unwrap(), b"v1");

    m.set(b"key".to_vec(), b"v2".to_vec());
    assert_eq!(m.get(b"key").unwrap(), b"v2");
}

#[test]
fn test_needs_flush() {
    // 4 MB capacity
    let mut m = MemTable::new(4 * 1024 * 1024, 0.01);

    assert!(!m.needs_flush());

    // Insert a huge payload bypassing natural memory limit (which allocates an entire block via Arena)
    // We insert slightly more than 4MB.
    m.set(b"massive_key".to_vec(), vec![0u8; 4_000_000]);
    m.set(b"another_key".to_vec(), vec![0u8; 300_000]);

    // The arena easily exceeded 4MB capacity.
    assert!(m.needs_flush());
}

#[test]
fn test_concurrent_reads() {
    let mut m = MemTable::new(1024 * 1024, 0.01);
    m.set(b"key".to_vec(), b"value".to_vec());

    let m_ref = &m;
    std::thread::scope(|s| {
        for _ in 0..10 {
            s.spawn(move || {
                assert_eq!(m_ref.get(b"key").unwrap(), b"value");
            });
        }
    });
}
