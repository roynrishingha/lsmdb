use super::{block::BlockReader, sst::SSTableReader, varint};
use crate::constants::{COMPRESSION_NONE, COMPRESSION_SNAPPY};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::PathBuf;

// Reads the 1-byte compression type sentinel written by SSTableBuilder and decompresses.
// Returning None (instead of panicking) on an unknown type makes the reader forward-compatible:
// a file written by a future version of lsmdb with a new compressor can be gracefully skipped
// rather than crashing all existing readers.
fn decompress_block(raw_block: &[u8]) -> Option<Vec<u8>> {
    let compression_type = raw_block[0];
    let payload = &raw_block[1..];
    match compression_type {
        t if t == COMPRESSION_SNAPPY => snap::raw::Decoder::new().decompress_vec(payload).ok(),
        t if t == COMPRESSION_NONE => Some(payload.to_vec()),
        _ => None,
    }
}

pub struct SSTableIterator {
    reader: SSTableReader,

    current_block_idx: usize,
    current_block_data: Option<Vec<u8>>,
    current_block_ptr: usize,
    // `current_key` is accumulated incrementally because Data Blocks use prefix compression:
    // each stored key is only the suffix that differs from the previous key. Rebuilding the
    // full key here avoids allocating full keys inside the block format itself.
    current_key: Vec<u8>,

    // `index_ptr` is a cursor into the raw Index Block bytes. Advancing it linearly as we
    // load blocks is O(N) total. If instead we re-parsed the entire Index Block on every
    // `load_next_block` call it would be O(N²) over the whole iteration.
    index_ptr: usize,
    index_key: Vec<u8>,
}

impl SSTableIterator {
    pub fn new(reader: SSTableReader) -> Self {
        let mut iter = Self {
            reader,
            current_block_idx: 0,
            current_block_data: None,
            current_block_ptr: 0,
            current_key: Vec::new(),
            index_ptr: 0,
            index_key: Vec::new(),
        };

        iter.load_next_block();
        iter
    }

    fn load_next_block(&mut self) {
        let index_block = BlockReader::new(&self.reader.index_data);

        // `restarts_offset` marks the start of the restart-point array at the tail of the
        // index block. Once index_ptr reaches it, we have consumed all index entries.
        if self.index_ptr >= index_block.restarts_offset {
            self.current_block_data = None;
            return;
        }

        let ptr = self.index_ptr;
        let (shared_len, len1) = varint::decode_u32(&index_block.data[ptr..]).unwrap();
        let mut ptr = ptr + len1;

        let (unshared_len, len2) = varint::decode_u32(&index_block.data[ptr..]).unwrap();
        ptr += len2;

        let (value_len, len3) = varint::decode_u32(&index_block.data[ptr..]).unwrap();
        ptr += len3;

        self.index_key.truncate(shared_len as usize);
        let unshared_bytes = &index_block.data[ptr..ptr + unshared_len as usize];
        self.index_key.extend_from_slice(unshared_bytes);
        ptr += unshared_len as usize;

        let value_bytes = &index_block.data[ptr..ptr + value_len as usize];
        ptr += value_len as usize;

        self.index_ptr = ptr;

        // Each index entry value is [data_block_offset, data_block_size] encoded as varints.
        // The offset is absolute within the mmap — no need to track a running base pointer.
        let mut v_ptr = 0;
        let (offset, v_len1) = varint::decode_u64(&value_bytes[v_ptr..]).unwrap();
        v_ptr += v_len1;
        let (size, _) = varint::decode_u64(&value_bytes[v_ptr..]).unwrap();

        let raw_block = &self.reader.mmap[offset as usize..offset as usize + size as usize];
        let Some(block_data) = decompress_block(raw_block) else {
            // Silently skip blocks with unrecognized compression rather than panicking,
            // so a partially-migrated file doesn't take down the whole compaction run.
            return;
        };

        self.current_block_data = Some(block_data);
        self.current_block_ptr = 0;
        self.current_block_idx += 1;
        self.current_key.clear();
    }
}

impl Iterator for SSTableIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(block_data) = &self.current_block_data {
                let block_reader = BlockReader::new(block_data);

                // Exhausted this block — advance to the next one and retry in the same loop
                // iteration rather than returning None prematurely.
                if self.current_block_ptr >= block_reader.restarts_offset {
                    self.load_next_block();
                    continue;
                }

                let ptr = self.current_block_ptr;
                let (shared_len, len1) = varint::decode_u32(&block_data[ptr..]).unwrap();
                let mut ptr = ptr + len1;

                let (unshared_len, len2) = varint::decode_u32(&block_data[ptr..]).unwrap();
                ptr += len2;

                let (value_len, len3) = varint::decode_u32(&block_data[ptr..]).unwrap();
                ptr += len3;

                self.current_key.truncate(shared_len as usize);
                let unshared_bytes = &block_data[ptr..ptr + unshared_len as usize];
                self.current_key.extend_from_slice(unshared_bytes);
                ptr += unshared_len as usize;

                let value_bytes = &block_data[ptr..ptr + value_len as usize];
                ptr += value_len as usize;

                self.current_block_ptr = ptr;

                return Some((self.current_key.clone(), value_bytes.to_vec()));
            } else {
                return None;
            }
        }
    }
}

// ---------------------------------------------------------
// COMPACTION
// ---------------------------------------------------------

struct HeapItem {
    key: Vec<u8>,
    value: Vec<u8>,
    // The caller passes input_paths newest-first. `table_index` 0 is therefore the newest
    // table. When two tables contain the same key, the one with the lower `table_index`
    // (newest) must win — its version supersedes the older one.
    table_index: usize,
}

// Rust's BinaryHeap is a max-heap, so we reverse the key comparison to get min-key-first
// ordering. On equal keys, we further sort by table_index ascending — the item with the
// lowest table_index (newest table) pops first, giving us automatic conflict resolution
// without any extra logic in the merge loop.
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.table_index.cmp(&self.table_index))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.table_index == other.table_index
    }
}
impl Eq for HeapItem {}

/// Merges `input_paths` (newest-first) into one SSTable at `output_path`.
///
/// This is a k-way merge using a min-heap. We seed the heap with the first entry from each
/// input iterator and then repeatedly pop the smallest key. When the same key appears in
/// multiple tables, the heap's Ord impl ensures the newest table's version is popped first
/// (see `HeapItem::cmp`) — subsequent occurrences of the same key are silently skipped via
/// `last_key_written`. This is how tombstones and overwrites are resolved: the newest version
/// of a key (even a tombstone) is the one that survives into the output SSTable.
pub fn compact(input_paths: Vec<PathBuf>, output_path: PathBuf) -> std::io::Result<()> {
    let mut iterators: Vec<SSTableIterator> = input_paths
        .iter()
        .map(|path| SSTableIterator::new(SSTableReader::new(path.clone())))
        .collect();

    let mut heap = BinaryHeap::new();

    for (idx, iter) in iterators.iter_mut().enumerate() {
        if let Some((k, v)) = iter.next() {
            heap.push(HeapItem {
                key: k,
                value: v,
                table_index: idx,
            });
        }
    }

    let mut builder = super::sst::SSTableBuilder::new(output_path);
    let mut last_key_written: Option<Vec<u8>> = None;

    while let Some(item) = heap.pop() {
        // The heap guarantees the newest version of each key is popped first. We write it once
        // and skip any subsequent pops of the same key (older versions from other tables).
        if last_key_written.as_ref() != Some(&item.key) {
            builder.add(&item.key, &item.value);
            last_key_written = Some(item.key.clone());
        }

        if let Some((k, v)) = iterators[item.table_index].next() {
            heap.push(HeapItem {
                key: k,
                value: v,
                table_index: item.table_index,
            });
        }
    }

    builder.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::sst::SSTableBuilder;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sstable_iterator() {
        let file = NamedTempFile::new().unwrap();
        let mut sstable = SSTableBuilder::new(file.path().to_path_buf());

        // Fill spanning multiple blocks
        for i in 0..1000 {
            let key = format!("key{:04}", i);
            let val = format!("value{:04}", i);
            sstable.add(key.as_bytes(), val.as_bytes());
        }
        sstable.finish().unwrap();

        let reader = SSTableReader::new(file.path().to_path_buf());
        let mut iter = SSTableIterator::new(reader);

        for i in 0..1000 {
            let (k, v) = iter.next().unwrap();
            let key = format!("key{:04}", i);
            let val = format!("value{:04}", i);
            assert_eq!(k, key.as_bytes());
            assert_eq!(v, val.as_bytes());
        }

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_compaction_basic_merge() {
        let file1 = NamedTempFile::new().unwrap();
        let file2 = NamedTempFile::new().unwrap();
        let output = NamedTempFile::new().unwrap();

        let mut t1 = SSTableBuilder::new(file1.path().to_path_buf());
        t1.add(b"a", b"1");
        t1.add(b"c", b"3");
        t1.add(b"e", b"5");
        t1.finish().unwrap();

        let mut t2 = SSTableBuilder::new(file2.path().to_path_buf());
        t2.add(b"b", b"2");
        t2.add(b"d", b"4");
        t2.add(b"f", b"6");
        t2.finish().unwrap();

        compact(
            vec![file1.path().to_path_buf(), file2.path().to_path_buf()],
            output.path().to_path_buf(),
        )
        .unwrap();

        let reader = SSTableReader::new(output.path().to_path_buf());
        let mut iter = SSTableIterator::new(reader);

        assert_eq!(iter.next().unwrap(), (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"c".to_vec(), b"3".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"d".to_vec(), b"4".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"e".to_vec(), b"5".to_vec()));
        assert_eq!(iter.next().unwrap(), (b"f".to_vec(), b"6".to_vec()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_compaction_overwrite_resolution() {
        let file_old = NamedTempFile::new().unwrap();
        let file_new = NamedTempFile::new().unwrap();
        let output = NamedTempFile::new().unwrap();

        let mut t_old = SSTableBuilder::new(file_old.path().to_path_buf());
        t_old.add(b"apple", b"old_val");
        t_old.add(b"banana", b"old_val");
        t_old.finish().unwrap();

        let mut t_new = SSTableBuilder::new(file_new.path().to_path_buf());
        t_new.add(b"apple", b"new_val");
        t_new.add(b"cat", b"new_val"); // entirely new key
        t_new.finish().unwrap();

        // Run Compaction! input_tables are ordered [NEWEST, OLDEST]
        compact(
            vec![file_new.path().to_path_buf(), file_old.path().to_path_buf()],
            output.path().to_path_buf(),
        )
        .unwrap();

        let reader = SSTableReader::new(output.path().to_path_buf());
        let mut iter = SSTableIterator::new(reader);

        // "apple" exists in both, but the NEWEST value must win.
        assert_eq!(
            iter.next().unwrap(),
            (b"apple".to_vec(), b"new_val".to_vec())
        );
        // "banana" only exists in the old one
        assert_eq!(
            iter.next().unwrap(),
            (b"banana".to_vec(), b"old_val".to_vec())
        );
        // "cat" only exists in the new one
        assert_eq!(iter.next().unwrap(), (b"cat".to_vec(), b"new_val".to_vec()));
        assert_eq!(iter.next(), None);
    }
}
