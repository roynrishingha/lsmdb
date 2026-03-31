#![allow(unused)]

use std::fs::File;
use std::io::Write;

#[derive(Debug, Clone, Copy)]
pub enum Opcode {
    Put = 1,
    Delete = 2,
}

/// A logical database operation (Put or Delete) before serialization.
///
/// The sequence number establishes a total causal ordering across all operations. During
/// recovery, replaying records in seq_num order guarantees the MemTable ends up in the
/// exact state it would have been in had the crash not occurred.
pub struct Record {
    pub opcode: Opcode,
    pub seq_num: u64,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

impl Record {
    /// Serializes the Record to bytes for embedding as a WAL chunk payload.
    ///
    /// Wire format: `[Opcode (1)] [Seq (8 LE)] [KeyLen (2 LE)] [Key] [ValLen (4 LE)] [Val]`
    ///
    /// `KeyLen` is u16 (max 65 KB) and `ValLen` is u32 (max 4 GB). Keys are intentionally
    /// restricted: an LSM-Tree benefits from short keys because they are copied into every
    /// SkipList node, every index block entry, and every Bloom Filter hash. Values can be
    /// arbitrarily large (stored out-of-line in the WAL payload).
    pub fn serialize(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(self.opcode as u8);
        bytes.extend_from_slice(&self.seq_num.to_le_bytes());

        bytes.extend_from_slice(&(self.key.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&self.key);

        bytes.extend_from_slice(&(self.val.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.val);

        bytes
    }
}

const BLOCK_SIZE: usize = crate::constants::WAL_BLOCK_SIZE;
const HEADER_SIZE: usize = crate::constants::WAL_HEADER_SIZE;
const MAX_PAYLOAD_SIZE: usize = crate::constants::WAL_MAX_PAYLOAD_SIZE;

#[derive(Debug, Clone, Copy)]
enum ChunkType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

/// A fixed-width slot inside a 32 KB WAL block.
///
/// Records that span multiple 32 KB blocks are split into First/Middle/Last chunks. This
/// lets the recovery reader reassemble records without knowing their total size upfront —
/// it reads chunks in order until it sees a `Last` (or `Full`) chunk type.
///
/// The payload is raw serialized `Record` bytes. Keeping `Chunk` unaware of `Record`
/// structure means the chunking logic is reusable for any payload and easier to test.
struct Chunk {
    pub checksum: u32,
    pub length: u16,
    pub chunk_type: ChunkType,
    pub payload: Vec<u8>,
}

impl Chunk {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        bytes.extend_from_slice(&self.checksum.to_le_bytes());
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.push(self.chunk_type as u8);
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}

struct WalWriter {
    file: File,
    // Tracks where the write head sits inside the current 32 KB block.
    // Each time we write a chunk we advance this by (HEADER_SIZE + payload_len).
    // When it would leave less than HEADER_SIZE bytes in the block, we pad to the
    // next block boundary — partial headers at block boundaries would confuse recovery.
    block_offset: usize,
}

impl WalWriter {
    fn append_record(&mut self, record: Record) -> std::io::Result<()> {
        self.write_physical_chunks(&record.serialize())
    }

    // Slices `data` into chunks that fit within the remaining space in each 32 KB physical block.
    // Chunking here (rather than at the Record level) decouples the logical write API from the
    // physical block structure, making the format easy to change without touching record logic.
    fn write_physical_chunks(&mut self, mut data: &[u8]) -> std::io::Result<()> {
        let mut is_first = true;

        while !data.is_empty() {
            let leftover_space_in_block = BLOCK_SIZE - self.block_offset;

            if leftover_space_in_block < HEADER_SIZE {
                // Pad the tail of the block to zero so the recovery reader always finds a clean
                // 32 KB boundary. A partial header here would be indistinguishable from
                // a truncated record written during a crash.
                let padding = vec![0; leftover_space_in_block];
                self.file.write_all(&padding)?;
                self.block_offset = 0;
                continue;
            }
            let avail = leftover_space_in_block - HEADER_SIZE;
            let fragment_len = data.len().min(avail);
            let is_last = fragment_len == data.len();

            let chunk_type = if is_first && is_last {
                ChunkType::Full
            } else if is_first {
                ChunkType::First
            } else if is_last {
                ChunkType::Last
            } else {
                ChunkType::Middle
            };

            let payload = data[..fragment_len].to_vec();

            // CRC covers both chunk_type and payload — not just payload — so a corrupted
            // type byte (e.g., Full mutated to Last) also fails the checksum rather than
            // silently producing a wrong reassembly during recovery.
            let mut crc_data = Vec::with_capacity(1 + payload.len());
            crc_data.push(chunk_type as u8);
            crc_data.extend_from_slice(&payload);
            let checksum = crc32fast::hash(&crc_data);

            let chunk = Chunk {
                checksum,
                length: fragment_len as u16,
                chunk_type,
                payload,
            };

            self.file.write_all(&chunk.to_bytes())?;
            // `flush()` pushes to the OS kernel buffer; `sync_data()` (called in maybe_sync)
            // pushes to physical storage. We always call flush() to ensure the write reaches
            // the kernel even when WAL_SYNC_ON_WRITE is false.
            self.file.flush()?;

            self.block_offset += HEADER_SIZE + fragment_len;
            data = &data[fragment_len..];
            is_first = false;
        }

        Ok(())
    }
}

pub struct Wal {
    writer: WalWriter,
    reader: Option<WalReader>,
    current_file_num: u64,
    dir_path: std::path::PathBuf,
}

impl Wal {
    /// Opens or creates the next WAL file in the directory.
    ///
    /// File numbers are monotonically increasing so older files can be GC'd by number
    /// comparison rather than by examining their contents. On startup we always continue
    /// writing to the highest-numbered existing file — if the last file was partially written
    /// (process crash), the recovery reader will encounter a checksum failure at the truncated
    /// chunk and stop, leaving previously valid records accessible.
    pub fn new(dir_path: impl Into<std::path::PathBuf>) -> Result<Self, anyhow::Error> {
        let dir_path = dir_path.into();
        std::fs::create_dir_all(&dir_path)?;

        let mut max_num = 0;
        if let Ok(entries) = std::fs::read_dir(&dir_path) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if let Some(name_str) = name.to_str()
                    && name_str.ends_with(".log")
                    && let Ok(num) = name_str.trim_end_matches(".log").parse::<u64>()
                {
                    max_num = max_num.max(num);
                }
            }
        }

        let current_file_num = if max_num == 0 { 1 } else { max_num };
        let file_path = dir_path.join(format!("{:05}.log", current_file_num));

        let writer = WalWriter {
            file: File::options().create(true).append(true).open(file_path)?,
            block_offset: 0,
        };

        Ok(Self {
            writer,
            reader: None,
            current_file_num,
            dir_path,
        })
    }

    /// Appends a Put record. Returns only after the record is safely in the WAL.
    pub fn add(&mut self, seq_num: u64, key: Vec<u8>, value: Vec<u8>) -> Result<(), anyhow::Error> {
        self.writer.append_record(Record {
            opcode: Opcode::Put,
            seq_num,
            key,
            val: value,
        })?;
        self.maybe_sync()
    }

    /// Appends a Delete tombstone. An empty `val` signals deletion during recovery.
    pub fn remove(&mut self, seq_num: u64, key: Vec<u8>) -> Result<(), anyhow::Error> {
        self.writer.append_record(Record {
            opcode: Opcode::Delete,
            seq_num,
            key,
            val: vec![],
        })?;
        self.maybe_sync()
    }

    // When WAL_SYNC_ON_WRITE is false, the OS can coalesce and reorder writes for throughput,
    // but a power loss may drop the last few records. The tradeoff is documented in constants.rs.
    fn maybe_sync(&mut self) -> Result<(), anyhow::Error> {
        if crate::constants::WAL_SYNC_ON_WRITE {
            self.writer.file.sync_data()?;
        }
        Ok(())
    }

    /// Gets the current active WAL file number.
    pub fn current_file_num(&self) -> u64 {
        self.current_file_num
    }

    /// Freezes the current WAL file and rotates to a new `.log` file.
    pub fn rotate(&mut self) -> Result<(), anyhow::Error> {
        self.current_file_num += 1;
        let file_path = self
            .dir_path
            .join(format!("{:05}.log", self.current_file_num));
        self.writer = WalWriter {
            file: File::options().create(true).append(true).open(file_path)?,
            block_offset: 0,
        };
        Ok(())
    }

    /// Deletes WAL files whose data has been durably flushed to an SSTable.
    ///
    /// Only called after the SSTable has been fully written and the MANIFEST updated.
    /// Deleting before that point would make the corresponding writes unrecoverable on a
    /// crash between the SSTable write and the MANIFEST update.
    pub fn delete_old_files(&self, up_to_inclusive: u64) -> Result<(), anyhow::Error> {
        if !self.dir_path.exists() {
            return Ok(());
        }

        let entries = std::fs::read_dir(&self.dir_path)?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            if let Some(name_str) = name.to_str()
                && name_str.ends_with(".log")
                && let Ok(num) = name_str.trim_end_matches(".log").parse::<u64>()
                && num <= up_to_inclusive
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// Replays all WAL records in file-number order to reconstruct the MemTable state.
    ///
    /// Files are read in ascending numeric order so older writes are replayed before newer ones,
    /// preserving the original causal order. A checksum failure in the reader stops recovery
    /// at that point — records before the failure are valid, and records after it (if any) belong
    /// to an interrupted write and should be discarded.
    pub fn recover(&mut self) -> Result<Vec<Record>, anyhow::Error> {
        let mut records = Vec::new();

        if !self.dir_path.exists() {
            return Ok(records);
        }

        let mut files: Vec<_> = std::fs::read_dir(&self.dir_path)?
            .flatten()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                if name.ends_with(".log") {
                    let num: u64 = name.trim_end_matches(".log").parse().ok()?;
                    Some((num, e.path()))
                } else {
                    None
                }
            })
            .collect();

        files.sort_by_key(|(num, _)| *num);

        for (_, path) in files {
            let file = File::open(path)?;
            self.reader = Some(WalReader::new(file));

            if let Some(reader) = &mut self.reader {
                while let Some(record) = reader.next_record()? {
                    records.push(record);
                }
            }
        }

        self.reader = None;

        Ok(records)
    }
}

struct WalReader {
    file: File,
    buffer: [u8; BLOCK_SIZE],
    buffer_offset: usize,
    buffer_len: usize,
}

impl WalReader {
    pub fn new(file: File) -> Self {
        Self {
            file,
            buffer: [0; BLOCK_SIZE],
            buffer_offset: 0,
            buffer_len: 0,
        }
    }

    /// Reads exactly `len` bytes from the block buffer.
    /// If the block is exhausted, it reads the next block from the file.
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        let mut read_so_far = 0;
        let total_to_read = buf.len();

        while read_so_far < total_to_read {
            // Refill buffer if empty
            if self.buffer_offset >= self.buffer_len {
                use std::io::Read;
                self.buffer_len = self.file.read(&mut self.buffer)?;
                self.buffer_offset = 0;

                if self.buffer_len == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Unexpected EOF",
                    ));
                }
            }

            let avail = self.buffer_len - self.buffer_offset;
            let needed = total_to_read - read_so_far;
            let take = avail.min(needed);

            buf[read_so_far..read_so_far + take]
                .copy_from_slice(&self.buffer[self.buffer_offset..self.buffer_offset + take]);

            self.buffer_offset += take;
            read_so_far += take;
        }

        Ok(())
    }

    /// Reads chunks and reassembles them into a single logical Record.
    pub fn next_record(&mut self) -> Result<Option<Record>, anyhow::Error> {
        let mut record_payload = Vec::new();
        let mut reading_fragmented = false;

        loop {
            // Check if we are at the end of the file/buffer cleanly
            if self.buffer_offset >= self.buffer_len {
                use std::io::Read;
                self.buffer_len = self.file.read(&mut self.buffer)?;
                self.buffer_offset = 0;
                if self.buffer_len == 0 {
                    // EOF reached cleanly if we haven't started a fragmented record
                    if reading_fragmented {
                        return Err(anyhow::anyhow!(
                            "WAL corruption: Unexpected EOF during fragmented record"
                        ));
                    }
                    return Ok(None);
                }
            }

            // Check if we hit padding. If the remaining space in the block
            // is less than HEADER_SIZE, skip to the next block.
            let leftover = BLOCK_SIZE - (self.buffer_offset % BLOCK_SIZE);
            if leftover < HEADER_SIZE {
                self.buffer_offset += leftover;
                continue;
            }

            let mut header = [0u8; HEADER_SIZE];
            if let Err(e) = self.read_exact(&mut header) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof && record_payload.is_empty() {
                    return Ok(None);
                }
                return Err(e.into());
            }

            let checksum = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
            let length = u16::from_le_bytes([header[4], header[5]]) as usize;
            let chunk_type = header[6];

            // If length is 0 and chunk_type is 0, this is pure padding. Skip the rest of this block.
            if length == 0 && chunk_type == 0 {
                let leftover = BLOCK_SIZE - (self.buffer_offset % BLOCK_SIZE);
                self.buffer_offset += leftover;
                continue;
            }

            let mut chunk_payload = vec![0u8; length];
            self.read_exact(&mut chunk_payload)?;

            // Verify CRC
            let mut crc_data = Vec::with_capacity(1 + length);
            crc_data.push(chunk_type);
            crc_data.extend_from_slice(&chunk_payload);
            let computed_checksum = crc32fast::hash(&crc_data);

            if checksum != computed_checksum {
                return Err(anyhow::anyhow!("WAL corruption: Checksum mismatch"));
            }

            record_payload.extend_from_slice(&chunk_payload);

            match chunk_type {
                1 => {
                    // Full
                    if reading_fragmented {
                        return Err(anyhow::anyhow!(
                            "WAL corruption: Full chunk inside fragmented record"
                        ));
                    }
                    break;
                }
                2 => {
                    // First
                    if reading_fragmented {
                        return Err(anyhow::anyhow!(
                            "WAL corruption: First chunk inside fragmented record"
                        ));
                    }
                    reading_fragmented = true;
                }
                3 => {
                    // Middle
                    if !reading_fragmented {
                        return Err(anyhow::anyhow!(
                            "WAL corruption: Middle chunk outside fragmented record"
                        ));
                    }
                }
                4 => {
                    // Last
                    if !reading_fragmented {
                        return Err(anyhow::anyhow!(
                            "WAL corruption: Last chunk outside fragmented record"
                        ));
                    }
                    break;
                }
                _ => return Err(anyhow::anyhow!("WAL corruption: Unknown chunk type")),
            }
        }

        // Deserialize the raw string of bytes back into a Record!
        if record_payload.len() < 15 {
            return Err(anyhow::anyhow!("WAL corruption: Record payload too small"));
        }

        let opcode = match record_payload[0] {
            1 => Opcode::Put,
            2 => Opcode::Delete,
            _ => return Err(anyhow::anyhow!("WAL corruption: Invalid opcode")),
        };

        let seq_num = u64::from_le_bytes(record_payload[1..9].try_into().unwrap());
        let key_len = u16::from_le_bytes(record_payload[9..11].try_into().unwrap()) as usize;
        let key = record_payload[11..11 + key_len].to_vec();

        let val_offset = 11 + key_len;
        if record_payload.len() < val_offset + 4 {
            return Err(anyhow::anyhow!(
                "WAL corruption: Invalid value length boundary"
            ));
        }

        let val_len = u32::from_le_bytes(
            record_payload[val_offset..val_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let val = record_payload[val_offset + 4..val_offset + 4 + val_len].to_vec();

        Ok(Some(Record {
            opcode,
            seq_num,
            key,
            val,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::{Read, Seek, SeekFrom};
    use tempfile::tempfile;

    // A helper to create a dummy record with a specific payload size.
    // The exact logical sizes don't matter as much as the resulting serialized byte length.
    fn create_dummy_record_of_size(target_serialized_size: usize) -> Record {
        // Opcode (1) + Seq (8) + KeyLen (2) + ValLen (4) = 15 bytes of logical overhead.
        let val_size = target_serialized_size.saturating_sub(15 + 4); // 4 bytes for key "test"
        Record {
            opcode: Opcode::Put,
            seq_num: 42,
            key: b"test".to_vec(),
            val: vec![0xAB; val_size],
        }
    }

    #[test]
    fn test_record_serialization() {
        let record = Record {
            opcode: Opcode::Put,
            seq_num: 1,
            key: b"k".to_vec(),
            val: b"v".to_vec(),
        };
        let bytes = record.serialize();
        // 1 + 8 + 2 + 1 + 4 + 1 = 17 bytes
        assert_eq!(bytes.len(), 17);
        assert_eq!(bytes[0], 1); // Opcode::Put
    }

    #[test]
    fn test_wal_writer_small_chunk() {
        let mut file = tempfile().unwrap();
        let mut writer = WalWriter {
            file: file.try_clone().unwrap(),
            block_offset: 0,
        };

        let record = create_dummy_record_of_size(100);
        writer.append_record(record).unwrap();

        // The chunk written should be HEADER_SIZE (7) + 100 = 107 bytes.
        assert_eq!(writer.block_offset, 107);

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        assert_eq!(data.len(), 107);
        // length field is at offset 4 and 5
        let len = u16::from_le_bytes([data[4], data[5]]);
        assert_eq!(len, 100);
        // chunk type is at offset 6
        assert_eq!(data[6], ChunkType::Full as u8);
    }

    #[test]
    fn test_wal_writer_large_chunk_split() {
        let mut file = tempfile().unwrap();
        let mut writer = WalWriter {
            file: file.try_clone().unwrap(),
            block_offset: 0,
        };

        // We want a payload of 40,000 bytes. This is > MAX_PAYLOAD_SIZE (32,761)
        // so it must be split into a First chunk and a Last chunk.
        let record = create_dummy_record_of_size(40000);
        writer.append_record(record).unwrap();

        // 1st chunk: HEADER (7) + 32,761 payload = 32,768 (fills block 0)
        // Remaining payload: 40,000 - 32,761 = 7,239 bytes.
        // 2nd chunk (in block 1): HEADER (7) + 7,239 payload = 7,246 bytes.
        assert_eq!(writer.block_offset, 7246);

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        assert_eq!(data.len(), 32768 + 7246);

        // Check First chunk header
        let chunk1_type = data[6];
        assert_eq!(chunk1_type, ChunkType::First as u8);
        let chunk1_len = u16::from_le_bytes([data[4], data[5]]);
        assert_eq!(chunk1_len as usize, MAX_PAYLOAD_SIZE);

        // Check Last chunk header (starts at offset 32768)
        let chunk2_type = data[32768 + 6];
        assert_eq!(chunk2_type, ChunkType::Last as u8);
        let chunk2_len = u16::from_le_bytes([data[32768 + 4], data[32768 + 5]]);
        assert_eq!(chunk2_len as usize, 7239);
    }

    #[test]
    fn test_wal_writer_block_padding() {
        let mut file = tempfile().unwrap();
        let mut writer = WalWriter {
            file: file.try_clone().unwrap(),
            block_offset: 0,
        };

        // Write a record that leaves exactly 5 bytes in the block.
        // A block is 32768. 5 bytes left means we write 32763 bytes.
        // 32763 bytes - 7 bytes header = 32756 bytes payload.
        let record1 = create_dummy_record_of_size(32756);
        writer.append_record(record1).unwrap();

        assert_eq!(writer.block_offset, 32763);

        // Now write another small record (e.g., 100 bytes).
        // It needs a 7-byte header, but only 5 bytes are left in the block!
        // So the writer MUST pad the 5 bytes with zeros, and start the new chunk at offset 32768.
        let record2 = create_dummy_record_of_size(100);
        writer.append_record(record2).unwrap();

        // The new block offset should be the size of the new chunk (7 + 100)
        assert_eq!(writer.block_offset, 107);

        file.seek(SeekFrom::Start(0)).unwrap();
        let mut data = Vec::new();
        file.read_to_end(&mut data).unwrap();

        // 32768 (Block 0 completely filled with padding) + 107 (Block 1)
        assert_eq!(data.len(), 32768 + 107);

        // Verify the padding bytes are strictly zero
        for i in 32763..32768 {
            assert_eq!(data[i], 0);
        }

        // Verify the second chunk started exactly at the beginning of the next block
        let chunk2_type = data[32768 + 6];
        assert_eq!(chunk2_type, ChunkType::Full as u8);
    }

    #[test]
    fn test_wal_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();

        // Should create 00001.log
        assert_eq!(wal.current_file_num, 1);
        let file1_path = dir.path().join("00001.log");
        assert!(file1_path.exists());

        // Rotate
        wal.rotate().unwrap();

        // Should create 00002.log
        assert_eq!(wal.current_file_num, 2);
        let file2_path = dir.path().join("00002.log");
        assert!(file2_path.exists());

        // Write something to file 2
        wal.add(1, b"key".to_vec(), b"val".to_vec()).unwrap();

        let meta = fs::metadata(&file2_path).unwrap();
        assert!(meta.len() > 0);
    }

    #[test]
    fn test_wal_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();

        // Write Normal Record
        wal.add(1, b"key1".to_vec(), b"val1".to_vec()).unwrap();

        // Write Huge Fragmented Record
        let huge_val = vec![0xAB; 40000];
        wal.add(2, b"key2".to_vec(), huge_val.clone()).unwrap();

        // Write Tombstone Record
        wal.remove(3, b"key1".to_vec()).unwrap();

        // Rotate & Write more
        wal.rotate().unwrap();
        wal.add(4, b"key3".to_vec(), b"val3".to_vec()).unwrap();

        // RECOVER
        let records = wal.recover().unwrap();
        assert_eq!(records.len(), 4);

        assert_eq!(records[0].seq_num, 1);
        assert_eq!(records[0].key, b"key1");
        assert_eq!(records[0].val, b"val1");

        assert_eq!(records[1].seq_num, 2);
        assert_eq!(records[1].key, b"key2");
        assert_eq!(records[1].val, huge_val);

        assert_eq!(records[2].seq_num, 3);
        assert!(matches!(records[2].opcode, Opcode::Delete));
        assert_eq!(records[2].key, b"key1");
        assert!(records[2].val.is_empty());

        assert_eq!(records[3].seq_num, 4);
        assert_eq!(records[3].key, b"key3");
        assert_eq!(records[3].val, b"val3");
    }
}
