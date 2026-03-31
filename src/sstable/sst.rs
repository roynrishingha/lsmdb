use super::{
    block::{BlockBuilder, BlockReader},
    varint,
};
use crate::BlockCache;
use crate::bloom_filter::BloomFilter;
use crate::constants::{COMPRESSION_NONE, COMPRESSION_SNAPPY};
use memmap2::Mmap;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    ops::Not,
    path::PathBuf,
};

pub struct SSTableBuilder {
    file: File,
    path: PathBuf,
    tmp_path: PathBuf,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
    offset: u64,
    bloom_filter: BloomFilter,
}

impl SSTableBuilder {
    pub fn new(path: PathBuf) -> Self {
        let tmp_path = path.with_extension("tmp");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&tmp_path)
            .unwrap();

        Self {
            file,
            path,
            tmp_path,
            data_block_builder: BlockBuilder::new(),
            index_block_builder: BlockBuilder::new(),
            offset: 0,
            // A 4 MB MemTable holds roughly 40k entries at ~100 bytes each. We use 100k as the
            // filter capacity to avoid undersizing if entries are smaller than average, which
            // would push the actual FPR above the configured 1% target.
            bloom_filter: BloomFilter::new(100_000, 0.01),
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        self.bloom_filter.set(key);
        self.data_block_builder.add(key, value);

        if self.data_block_builder.is_block_maxed().not() {
            return;
        }

        let last_key = self.data_block_builder.last_key();
        let raw_data = self.data_block_builder.finish().to_vec();
        let block_len_on_disk = self.write_compressed_block(&raw_data);

        let mut value_bytes = Vec::new();
        varint::encode_u64(self.offset, &mut value_bytes);
        varint::encode_u64(block_len_on_disk, &mut value_bytes);

        self.index_block_builder.add(&last_key, &value_bytes);
        self.offset += block_len_on_disk;

        self.data_block_builder = BlockBuilder::new();
    }

    // Snappy is chosen over zlib/lz4 here because it gives lower CPU cost at the cost of
    // compression ratio. For an LSM-Tree, CPU is often more precious than disk space because
    // compaction frequently decompresses and recompresses blocks. Snappy keeps compaction
    // latency predictable. The 1-byte type prefix allows the format to be extended to other
    // algorithms without a schema change.
    fn write_compressed_block(&mut self, raw_data: &[u8]) -> u64 {
        let compressed = snap::raw::Encoder::new()
            .compress_vec(raw_data)
            .unwrap_or_else(|_| raw_data.to_vec());
        self.file.write_all(&[COMPRESSION_SNAPPY]).unwrap();
        self.file.write_all(&compressed).unwrap();
        1 + compressed.len() as u64
    }

    pub fn finish(&mut self) -> std::io::Result<()> {
        if self.data_block_builder.buffer_len() > 0 {
            let last_key = self.data_block_builder.last_key();
            let raw_data = self.data_block_builder.finish().to_vec();
            let block_len_on_disk = self.write_compressed_block(&raw_data);

            let mut value_bytes = Vec::new();
            varint::encode_u64(self.offset, &mut value_bytes);
            varint::encode_u64(block_len_on_disk, &mut value_bytes);

            self.index_block_builder.add(&last_key, &value_bytes);
            self.offset += block_len_on_disk;

            self.data_block_builder = BlockBuilder::new();
        }

        let index_offset = self.offset;
        let index_data = self.index_block_builder.finish();
        let index_size = index_data.len() as u64;

        self.file.write_all(index_data)?;
        self.offset += index_size;

        // Bloom Filter Block
        let filter_offset = self.offset;
        let filter_data = self.bloom_filter.to_bytes();
        let filter_size = filter_data.len() as u64;

        self.file.write_all(&filter_data)?;
        self.offset += filter_size;

        // The 48-byte footer is written last and at a fixed position (file_len - 48) so the
        // reader can open any SSTable and immediately find the index and filter block locations
        // without parsing the file from the beginning.
        let mut footer = vec![0u8; 48];
        footer[0..8].copy_from_slice(&index_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&index_size.to_le_bytes());
        footer[16..24].copy_from_slice(&filter_offset.to_le_bytes());
        footer[24..32].copy_from_slice(&filter_size.to_le_bytes());
        // 32..48 are padding/reserved for future fields (e.g., checksum of the footer itself).

        self.file.write_all(&footer)?;
        self.offset += 48;

        self.file.sync_all()?;

        // Writing to a `.tmp` file and renaming atomically on finish guarantees the final
        // `.sst` file is always either fully written or absent — never a partial file.
        // A crash mid-write leaves a `.tmp` orphan, which the next startup can safely ignore
        // because the MANIFEST has not recorded it.
        std::fs::rename(&self.tmp_path, &self.path)?;

        Ok(())
    }
}

pub struct SSTableReader {
    pub id: u64,
    pub mmap: Mmap,
    pub index_data: Vec<u8>,
    pub bloom_filter: BloomFilter,
}

impl SSTableReader {
    pub fn new(path: PathBuf) -> Self {
        // INFO: Extract the ID from the filename
        let id_str = path.file_stem().and_then(|s| s.to_str()).unwrap_or("0");
        let id = id_str.parse::<u64>().unwrap_or(0);

        let file = File::open(&path).unwrap();
        let file_len = file.metadata().unwrap().len() as usize;

        // mmap maps the file into virtual address space. Reads then become page faults handled
        // by the OS, which reads the data from the file system. This avoids explicit read() calls
        // and lets the OS page cache do the caching instead of us managing a buffer ourselves.
        // SAFETY: The file is opened read-only and SSTable files are immutable once written —
        // no other thread or process will write to this file while we hold the mmap.
        let mmap = unsafe { Mmap::map(&file).unwrap() };

        // Footer = last 48 bytes
        let footer = &mmap[file_len - 48..];

        let index_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap()) as usize;
        let index_size = u64::from_le_bytes(footer[8..16].try_into().unwrap()) as usize;
        let filter_offset = u64::from_le_bytes(footer[16..24].try_into().unwrap()) as usize;
        let filter_size = u64::from_le_bytes(footer[24..32].try_into().unwrap()) as usize;

        // The index block and filter block are sliced out of the mmap and owned as Vec<u8>.
        // This means they're always resident in memory. For a production system with thousands
        // of open SSTables this would be a concern, but it lets every key lookup skip a page
        // fault for the index and filter, which is the common-case hot path.
        let index_data = mmap[index_offset..index_offset + index_size].to_vec();
        let filter_data = &mmap[filter_offset..filter_offset + filter_size];

        let bloom_filter =
            BloomFilter::from_bytes(filter_data).expect("Corrupt Bloom Filter metadata in SSTable");

        Self {
            id,
            mmap,
            index_data,
            bloom_filter,
        }
    }

    /// Looks up `key` in this SSTable, optionally consulting a shared LRU block cache.
    /// Returns `Some(value_bytes)` if found, or `None` if the Bloom Filter or Index
    /// rules out the key.
    pub fn get(&self, key: &[u8], cache: Option<&BlockCache>) -> Option<Vec<u8>> {
        // High speed in-memory Bloom Filter check avoids 99% of useless disk reads
        if !self.bloom_filter.contains(key) {
            return None;
        }

        let index_block = BlockReader::new(&self.index_data);

        // 1. Ask the Index Block: "What is the first data block whose last_key is >= my search key?"
        // This is exactly what `lookup` does! It returns the value for that key.
        if let Some(value_bytes) = index_block.lookup(key) {
            // 2. The value is just the offset and size varints for the Data Block!
            let mut ptr = 0;
            let (offset, len1) = varint::decode_u64(&value_bytes[ptr..])?;
            ptr += len1;
            let (size, _) = varint::decode_u64(&value_bytes[ptr..])?;

            // The cache is keyed by (sst_id, block_offset) — a tuple that uniquely identifies
            // a block across all open SSTables. We cache the *decompressed* block so subsequent
            // reads can skip both the mmap slice and the Snappy decode step.
            let mut cached_block = None;
            if let Some(c) = cache
                && let Ok(mut lru) = c.write()
                && let Some(block) = lru.get(&(self.id, offset))
            {
                cached_block = Some(std::sync::Arc::clone(block));
            }

            let block_data: std::sync::Arc<Vec<u8>> = if let Some(b) = cached_block {
                b
            } else {
                // INFO: Slice the raw block bytes out of the mmap
                let raw_block = &self.mmap[offset as usize..offset as usize + size as usize];

                // INFO: First byte is the compression type; remainder is the block payload.
                let compression_type = raw_block[0];
                let payload = &raw_block[1..];

                let decompressed = if compression_type == COMPRESSION_SNAPPY {
                    let mut decoder = snap::raw::Decoder::new();
                    decoder.decompress_vec(payload).ok()?
                } else if compression_type == COMPRESSION_NONE {
                    payload.to_vec()
                } else {
                    return None; // Unknown compression type
                };

                let arc_data = std::sync::Arc::new(decompressed);

                // INFO: Store the *decompressed* block in the LRU cache
                if let Some(c) = cache
                    && let Ok(mut lru) = c.write()
                {
                    lru.put((self.id, offset), std::sync::Arc::clone(&arc_data));
                }
                arc_data
            };

            // 4. Ask the Data Block to find the exact key.
            let block_reader = BlockReader::new(&block_data);
            if let Some(val) = block_reader.get(key) {
                return Some(val.to_vec());
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sstable_builder_init() {
        let file = NamedTempFile::new().unwrap();
        let sstable = SSTableBuilder::new(file.path().to_path_buf());

        assert!(file.path().exists());
        assert_eq!(sstable.offset, 0);
    }

    #[test]
    fn test_sstable_reader_init() {
        let file = NamedTempFile::new().unwrap();
        let mut sstable = SSTableBuilder::new(file.path().to_path_buf());

        sstable.add(b"apple", b"val_apple");
        sstable.add(b"banana", b"val_banana");
        sstable.add(b"cat", b"val_cat");
        sstable.finish().unwrap();

        // Open the file with our new SSTableReader
        let reader = SSTableReader::new(file.path().to_path_buf());

        assert!(reader.index_data.len() > 0);

        // Ensure the index block is a valid block format
        let index_block = BlockReader::new(&reader.index_data);
        assert!(index_block.num_restarts > 0);
    }

    #[test]
    fn test_sstable_builder_flush() {
        let file = NamedTempFile::new().unwrap();
        let mut sstable = SSTableBuilder::new(file.path().to_path_buf());

        let long_bytes = vec![0; 5000];
        sstable.add(b"long_key", &long_bytes);

        assert!(sstable.offset > 0);
    }

    #[test]
    fn test_sstable_builder_full_lifecycle() {
        let file = NamedTempFile::new().unwrap();
        let mut sstable = SSTableBuilder::new(file.path().to_path_buf());

        sstable.add(b"apple", b"val_apple");
        sstable.add(b"banana", b"val_banana");
        sstable.add(b"cat", b"val_cat");

        assert_eq!(sstable.offset, 0); // Not flushed yet

        sstable.finish().unwrap();

        // Check that the file on disk explicitly matches our internal tracker
        let metadata = std::fs::metadata(file.path()).unwrap();
        assert_eq!(metadata.len(), sstable.offset);

        // Assert that at least the 48 byte footer plus the index block was written
        assert!(sstable.offset > 48);
    }

    #[test]
    fn test_sstable_reader_get() {
        let file = NamedTempFile::new().unwrap();
        let mut sstable = SSTableBuilder::new(file.path().to_path_buf());

        // Add enough keys to span multiple Data Blocks (at least 2 blocks)
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let val = format!("value{:04}", i);
            sstable.add(key.as_bytes(), val.as_bytes());
        }
        sstable.finish().unwrap();

        let reader = SSTableReader::new(file.path().to_path_buf());

        // Test grabbing the very first key
        assert_eq!(reader.get(b"key0000", None).unwrap(), b"value0000");

        // Test grabbing something in the middle
        assert_eq!(reader.get(b"key0500", None).unwrap(), b"value0500");

        // Test grabbing the very last key
        assert_eq!(reader.get(b"key0999", None).unwrap(), b"value0999");

        // Test random non-existent keys
        assert_eq!(reader.get(b"key0000_not_exist", None), None);
        assert_eq!(reader.get(b"missing", None), None);
        assert_eq!(reader.get(b"zebra", None), None);
    }
}
