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

#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum EntryKind {
    Insert = 1,
    Remove = 2,
}

#[derive(PartialEq, Debug)]
pub(crate) struct WriteAheadLogEntry {
    pub(crate) entry_kind: EntryKind,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl WriteAheadLog {
    pub(crate) fn new(directory_path: &PathBuf) -> io::Result<Self> {
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

    pub(crate) fn append(
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

        let entry = WriteAheadLogEntry::new(entry_kind, key, value);

        let serialized_data = entry.serialize();

        log_file.write_all(&serialized_data)?;

        log_file.flush()?;

        Ok(())
    }

    pub(crate) fn recover(&mut self) -> io::Result<Vec<WriteAheadLogEntry>> {
        let mut log_file = self.log_file.lock().map_err(|poison_error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to obtain lock: {:?}", poison_error),
            )
        })?;

        let mut entries = Vec::new();

        loop {
            let mut serialized_data = Vec::new();
            log_file.read_to_end(&mut serialized_data)?;

            if serialized_data.is_empty() {
                // Reached the end of the log file
                break;
            }

            let entry = WriteAheadLogEntry::deserialize(&serialized_data)?;

            entries.push(entry);
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

impl WriteAheadLogEntry {
    pub(crate) fn new(entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            entry_kind,
            key,
            value,
        }
    }

    fn serialize(&self) -> Vec<u8> {
        // Calculate entry length
        let entry_len = 4 + 1 + 4 + 4 + self.key.len() + self.value.len();

        let mut serialized_data = Vec::with_capacity(entry_len);

        // Serialize entry length
        serialized_data.extend_from_slice(&(entry_len as u32).to_le_bytes());

        // Serialize entry kind
        serialized_data.push(self.entry_kind as u8);

        // Serialize key length
        serialized_data.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        // Serialize value length
        serialized_data.extend_from_slice(&(self.value.len() as u32).to_le_bytes());

        // Serialize key
        serialized_data.extend_from_slice(&self.key);

        // Serialize value
        serialized_data.extend_from_slice(&self.value);

        serialized_data
    }

    fn deserialize(serialized_data: &[u8]) -> io::Result<Self> {
        if serialized_data.len() < 13 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let entry_len = u32::from_le_bytes([
            serialized_data[0],
            serialized_data[1],
            serialized_data[2],
            serialized_data[3],
        ]) as usize;

        if serialized_data.len() != entry_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let entry_kind = match serialized_data[4] {
            1 => EntryKind::Insert,
            2 => EntryKind::Remove,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid entry kind value",
                ))
            }
        };

        let key_len = u32::from_le_bytes([
            serialized_data[5],
            serialized_data[6],
            serialized_data[7],
            serialized_data[8],
        ]) as usize;
        let value_len = u32::from_le_bytes([
            serialized_data[9],
            serialized_data[10],
            serialized_data[11],
            serialized_data[12],
        ]) as usize;

        if serialized_data.len() != 13 + key_len + value_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid serialized data length",
            ));
        }

        let key = serialized_data[13..(13 + key_len)].to_vec();
        let value = serialized_data[(13 + key_len)..].to_vec();

        Ok(WriteAheadLogEntry::new(entry_kind, key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        // Create a sample WriteAheadLogEntry.
        let entry_kind = EntryKind::Insert;
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        let original_entry = WriteAheadLogEntry::new(entry_kind, key.clone(), value.clone());

        // Serialize the entry.
        let serialized_data = original_entry.serialize();

        // Verify serialized data.
        let expected_entry_len = 4 + 1 + 4 + 4 + key.len() + value.len();
        assert_eq!(serialized_data.len(), expected_entry_len);

        // Deserialize the serialized data.
        let deserialized_entry =
            WriteAheadLogEntry::deserialize(&serialized_data).expect("Failed to deserialize");

        // Verify deserialized entry matches the original entry
        assert_eq!(deserialized_entry, original_entry);
    }
}
