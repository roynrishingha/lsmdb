use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

pub(crate) static WAL_FILE_NAME: &str = "lsmdb_wal.bin";

pub(crate) struct WriteAheadLog {
    log_file: Arc<Mutex<File>>,
}

pub(crate) enum EntryKind {
    Insert = 1,
    Remove = 2,
}

type WalFileEntry = Vec<(EntryKind, Vec<u8>, Vec<u8>)>;

impl WriteAheadLog {
    pub(crate) fn new(directory_path: &str) -> io::Result<Self> {
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

    pub(crate) fn append_entry(
        &mut self,
        entry_kind: EntryKind,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> io::Result<()> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to obtain lock: {:?}", poison_error),
            )
        })?;

        let entry_len = 4 + 1 + 4 + key.len() + 4 + value.len();
        let entry_len_bytes: [u8; 4] = (entry_len as u32).to_be_bytes();

        log_file.write_all(&entry_len_bytes)?;
        log_file.write_all(&(entry_kind as u8).to_be_bytes())?;
        log_file.write_all(&(key.len() as u32).to_be_bytes())?;
        log_file.write_all(&key)?;
        log_file.write_all(&(value.len() as u32).to_be_bytes())?;
        log_file.write_all(&value)?;

        log_file.flush()?;

        Ok(())
    }

    pub(crate) fn recover_entries(&mut self) -> io::Result<WalFileEntry> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to obtain lock: {:?}", poison_error),
            )
        })?;

        let mut log_contents = Vec::new();
        log_file.read_to_end(&mut log_contents)?;

        let mut entries = Vec::new();
        let mut pos = 0;

        while pos < log_contents.len() {
            let entry_len_bytes = &log_contents[pos..pos + 4];
            let entry_len = u32::from_be_bytes([
                entry_len_bytes[0],
                entry_len_bytes[1],
                entry_len_bytes[2],
                entry_len_bytes[3],
            ]) as usize;

            let entry_data = &log_contents[pos + 4..pos + entry_len];

            let entry_kind_byte = entry_data[0];
            let entry_kind = match entry_kind_byte {
                1 => EntryKind::Insert,
                2 => EntryKind::Remove,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid entry kind",
                    ))
                }
            };

            let key_len_bytes = &entry_data[1..5];
            let key_len = u32::from_be_bytes([
                key_len_bytes[0],
                key_len_bytes[1],
                key_len_bytes[2],
                key_len_bytes[3],
            ]) as usize;

            let key = entry_data[5..5 + key_len].to_vec();

            let value_len_bytes = &entry_data[5 + key_len..5 + key_len + 4];
            let value_len = u32::from_be_bytes([
                value_len_bytes[0],
                value_len_bytes[1],
                value_len_bytes[2],
                value_len_bytes[3],
            ]) as usize;

            let value = entry_data[5 + key_len + 4..].to_vec();

            entries.push((entry_kind, key, value));

            pos += entry_len;
        }

        Ok(entries)
    }

    pub(crate) fn clear(&mut self) -> io::Result<()> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to obtain lock: {:?}", poison_error),
            )
        })?;

        log_file.set_len(0)?;
        log_file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}
