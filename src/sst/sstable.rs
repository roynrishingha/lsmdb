use super::sst_block::Block;
use chrono::{DateTime, Utc};
use std::{io, path::PathBuf};

pub struct SSTable {
    file_path: PathBuf,
    blocks: Vec<Block>,
    created_at: DateTime<Utc>,
}

impl SSTable {
    pub(crate) fn new(dir: PathBuf) -> Self {
        let created_at = Utc::now();
        let file_name = format!("sstable_{}.dat", created_at.timestamp_millis());
        let file_path = dir.join(file_name);

        Self {
            file_path,
            blocks: Vec::new(),
            created_at,
        }
    }

    pub(crate) fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        if self.blocks.is_empty() || self.blocks.last().unwrap().is_full(key.len() + value.len()) {
            let new_block = Block::new();
            self.blocks.push(new_block);
        }

        let last_block = self.blocks.last_mut().unwrap();
        last_block.set_entry(&key, &value)?;

        Ok(())
    }

    pub(crate) fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        for block in &self.blocks {
            if let Some(value) = block.get_value(&key) {
                return Some(value);
            }
        }
        None
    }

    pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<()> {
        // Iterate over the blocks in reverse order to delete from the most recent block first.
        for block in self.blocks.iter_mut().rev() {
            if block.remove_entry(&key) {
                return Ok(());
            }
        }

        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Key not found in SSTable",
        ))
    }
}
