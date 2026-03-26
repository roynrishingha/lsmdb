use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Represents a delta change to the state of the LSM Tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionEdit {
    /// Adds a new SSTable to a specific Level
    AddTable { level: u32, sst_id: u64 },
    /// Removes an SSTable from a specific Level (due to Compaction)
    RemoveTable { level: u32, sst_id: u64 },
}

impl VersionEdit {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(13);
        match self {
            VersionEdit::AddTable { level, sst_id } => {
                buf.push(1);
                buf.extend_from_slice(&level.to_le_bytes());
                buf.extend_from_slice(&sst_id.to_le_bytes());
            }
            VersionEdit::RemoveTable { level, sst_id } => {
                buf.push(2);
                buf.extend_from_slice(&level.to_le_bytes());
                buf.extend_from_slice(&sst_id.to_le_bytes());
            }
        }
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 13 {
            return None;
        }
        let tag = bytes[0];
        let level = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        let sst_id = u64::from_le_bytes(bytes[5..13].try_into().unwrap());

        match tag {
            1 => Some(VersionEdit::AddTable { level, sst_id }),
            2 => Some(VersionEdit::RemoveTable { level, sst_id }),
            _ => None,
        }
    }
}

/// The Manifest persistently tracks the definitive layout of SSTables across all Levels.
/// If an `.sst` file exists on disk but is NOT active in the Manifest, it is an orphaned
/// ghost file from a crashed Compaction and must be safely ignored/deleted.
pub struct Manifest {
    file: File,
}

impl Manifest {
    /// Opens the Manifest log in append mode. Creates it if it doesn't exist.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self { file })
    }

    /// Logs a specific state mutation (e.g. creating a Level 0 table, or merging tables to Level 1).
    pub fn log_edit(&mut self, edit: &VersionEdit) -> Result<(), anyhow::Error> {
        let bytes = edit.to_bytes();
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Replays the entire history of the Manifest to reconstruct the layout of the Database.
    /// Returns a Vector for each Level, containing the `sst_id` of active tables.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<Vec<u64>>, anyhow::Error> {
        let mut levels: Vec<Vec<u64>> = Vec::new();

        if !path.as_ref().exists() {
            return Ok(levels);
        }

        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut ptr = 0;
        while ptr + 13 <= data.len() {
            if let Some(edit) = VersionEdit::from_bytes(&data[ptr..ptr + 13]) {
                match edit {
                    VersionEdit::AddTable { level, sst_id } => {
                        let lvl = level as usize;
                        if levels.len() <= lvl {
                            levels.resize(lvl + 1, Vec::new());
                        }
                        levels[lvl].push(sst_id);
                    }
                    VersionEdit::RemoveTable { level, sst_id } => {
                        let lvl = level as usize;
                        if lvl < levels.len() {
                            levels[lvl].retain(|&id| id != sst_id);
                        }
                    }
                }
            }
            ptr += 13;
        }

        Ok(levels)
    }
}
