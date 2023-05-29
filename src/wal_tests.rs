use crate::{helper::generate_timestamp, wal::Wal};

use rand::Rng;
use std::fs::{create_dir, remove_dir_all};
use std::fs::{metadata, File, OpenOptions};
use std::io::prelude::*;
use std::io::BufReader;
use std::path::PathBuf;

fn check_entry(
    reader: &mut BufReader<File>,
    key: &[u8],
    value: Option<&[u8]>,
    timestamp: u128,
    deleted: bool,
) {
    let mut len_buffer = [0; 8];
    reader.read_exact(&mut len_buffer).unwrap();
    let file_key_len = usize::from_le_bytes(len_buffer);
    assert_eq!(file_key_len, key.len());

    let mut bool_buffer = [0; 1];
    reader.read_exact(&mut bool_buffer).unwrap();
    let file_deleted = bool_buffer[0] != 0;
    assert_eq!(file_deleted, deleted);

    if deleted {
        let mut file_key = vec![0; file_key_len];
        reader.read_exact(&mut file_key).unwrap();
        assert_eq!(file_key, key);
    } else {
        reader.read_exact(&mut len_buffer).unwrap();
        let file_value_len = usize::from_le_bytes(len_buffer);
        assert_eq!(file_value_len, value.unwrap().len());
        let mut file_key = vec![0; file_key_len];
        reader.read_exact(&mut file_key).unwrap();
        assert_eq!(file_key, key);
        let mut file_value = vec![0; file_value_len];
        reader.read_exact(&mut file_value).unwrap();
        assert_eq!(file_value, value.unwrap());
    }

    let mut timestamp_buffer = [0; 16];
    reader.read_exact(&mut timestamp_buffer).unwrap();
    let file_timestamp = u128::from_le_bytes(timestamp_buffer);
    assert_eq!(file_timestamp, timestamp);
}

#[test]
fn test_write_one() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let timestamp = generate_timestamp();

    let mut wal = Wal::new(&dir).unwrap();
    wal.set(b"Lime", b"Lime Smoothie", timestamp).unwrap();
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    check_entry(
        &mut reader,
        b"Lime",
        Some(b"Lime Smoothie"),
        timestamp,
        false,
    );

    remove_dir_all(&dir).unwrap();
}

#[test]
fn test_write_many() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let timestamp = generate_timestamp();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
        (b"Apple", Some(b"Apple Smoothie")),
        (b"Lime", Some(b"Lime Smoothie")),
        (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = Wal::new(&dir).unwrap();

    for e in entries.iter() {
        wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
    }
    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for e in entries.iter() {
        check_entry(&mut reader, e.0, e.1, timestamp, false);
    }

    remove_dir_all(&dir).unwrap();
}

#[test]
fn test_write_delete() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let timestamp = generate_timestamp();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
        (b"Apple", Some(b"Apple Smoothie")),
        (b"Lime", Some(b"Lime Smoothie")),
        (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = Wal::new(&dir).unwrap();

    for e in entries.iter() {
        wal.set(e.0, e.1.unwrap(), timestamp).unwrap();
    }
    for e in entries.iter() {
        wal.delete(e.0, timestamp).unwrap();
    }

    wal.flush().unwrap();

    let file = OpenOptions::new().read(true).open(&wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for e in entries.iter() {
        check_entry(&mut reader, e.0, e.1, timestamp, false);
    }
    for e in entries.iter() {
        check_entry(&mut reader, e.0, None, timestamp, true);
    }

    remove_dir_all(&dir).unwrap();
}

#[test]
fn test_read_wal_none() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let (new_wal, new_mem_table) = Wal::load_wal_from_dir(&dir).unwrap();
    assert_eq!(new_mem_table.entries.len(), 0);

    let m = metadata(new_wal.path).unwrap();
    assert_eq!(m.len(), 0);

    remove_dir_all(&dir).unwrap();
}

#[test]
fn test_read_wal_one() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
        (b"Apple", Some(b"Apple Smoothie")),
        (b"Lime", Some(b"Lime Smoothie")),
        (b"Orange", Some(b"Orange Smoothie")),
    ];

    let mut wal = Wal::new(&dir).unwrap();

    for (i, e) in entries.iter().enumerate() {
        wal.set(e.0, e.1.unwrap(), i as u128).unwrap();
    }
    wal.flush().unwrap();

    let (new_wal, new_mem_table) = Wal::load_wal_from_dir(&dir).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries.iter().enumerate() {
        check_entry(&mut reader, e.0, e.1, i as u128, false);

        let mem_e = new_mem_table.get(e.0).unwrap();
        assert_eq!(mem_e.key, e.0);
        assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
        assert_eq!(mem_e.timestamp, i as u128);
    }

    remove_dir_all(&dir).unwrap();
}

#[test]
fn test_read_wal_multiple() {
    let mut rng = rand::thread_rng();
    let dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
    create_dir(&dir).unwrap();

    let entries_1: Vec<(&[u8], Option<&[u8]>)> = vec![
        (b"Apple", Some(b"Apple Smoothie")),
        (b"Lime", Some(b"Lime Smoothie")),
        (b"Orange", Some(b"Orange Smoothie")),
    ];
    let mut wal_1 = Wal::new(&dir).unwrap();
    for (i, e) in entries_1.iter().enumerate() {
        wal_1.set(e.0, e.1.unwrap(), i as u128).unwrap();
    }
    wal_1.flush().unwrap();

    let entries_2: Vec<(&[u8], Option<&[u8]>)> = vec![
        (b"Strawberry", Some(b"Strawberry Smoothie")),
        (b"Blueberry", Some(b"Blueberry Smoothie")),
        (b"Orange", Some(b"Orange Milkshake")),
    ];
    let mut wal_2 = Wal::new(&dir).unwrap();
    for (i, e) in entries_2.iter().enumerate() {
        wal_2.set(e.0, e.1.unwrap(), (i + 3) as u128).unwrap();
    }
    wal_2.flush().unwrap();

    let (new_wal, new_mem_table) = Wal::load_wal_from_dir(&dir).unwrap();

    let file = OpenOptions::new().read(true).open(&new_wal.path).unwrap();
    let mut reader = BufReader::new(file);

    for (i, e) in entries_1.iter().enumerate() {
        check_entry(&mut reader, e.0, e.1, i as u128, false);

        let mem_e = new_mem_table.get(e.0).unwrap();
        if i != 2 {
            assert_eq!(mem_e.key, e.0);
            assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_eq!(mem_e.timestamp, i as u128);
        } else {
            assert_eq!(mem_e.key, e.0);
            assert_ne!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
            assert_ne!(mem_e.timestamp, i as u128);
        }
    }
    for (i, e) in entries_2.iter().enumerate() {
        check_entry(&mut reader, e.0, e.1, (i + 3) as u128, false);

        let mem_e = new_mem_table.get(e.0).unwrap();
        assert_eq!(mem_e.key, e.0);
        assert_eq!(mem_e.value.as_ref().unwrap().as_slice(), e.1.unwrap());
        assert_eq!(mem_e.timestamp, (i + 3) as u128);
    }

    remove_dir_all(&dir).unwrap();
}
