//! # lsmdb
//!
//! A persistent key-value storage engine built on a **Log-Structured Merge Tree (LSM-Tree)**.
//!
//! ## Why LSM-Tree?
//!
//! Traditional B-Tree engines perform random in-place writes, which are slow on both HDDs (seek
//! latency) and SSDs (write amplification from page-level overwriting). LSM-Trees turn all writes
//! into sequential appends — first to the WAL, then to the MemTable, then batched to disk as
//! immutable SSTables — making writes as fast as the underlying storage's sequential throughput.
//!
//! ## Design Decisions Worth Knowing
//!
//! - **WAL before MemTable**: Every write hits the WAL first. If the process crashes between the
//!   WAL append and the MemTable insert, the WAL replay at startup recovers the write. Without
//!   this ordering, acknowledged writes could vanish on crash.
//! - **Immutable SSTables**: Once a MemTable is flushed, its SSTable is never mutated. This means
//!   reads are always consistent and compaction can merge files without coordination with readers.
//! - **Multi-level compaction**: L0 files can overlap in key range (they are flushed sequentially).
//!   Higher levels are sorted and non-overlapping. Compacting L0→L1 and cascading upward keeps
//!   read amplification bounded — otherwise a read could scan every L0 file on a miss.
//! - **Bloom Filters**: A point query that misses everything in memory would otherwise read every
//!   SSTable. Bloom Filters give a definitive "not present" answer in O(k) hash operations with
//!   ~1% false positive rate, eliminating almost all unnecessary disk seeks.
//! - **LRU Block Cache**: SSTables are divided into 4 KB Data Blocks. Hot blocks (recent or
//!   repeated reads) are kept in an LRU cache so repeated reads don't pay the mmap page fault cost.
//!
//! ---
//!
//! Author: Nrishinghananda Roy

mod bloom_filter;
pub mod constants;
mod memtable;
mod sstable;
mod wal;

use crate::constants::{
    BLOCK_CACHE_CAPACITY, BLOOM_FILTER_FPR, L0_COMPACTION_TRIGGER, L1_MAX_BYTES,
    LEVEL_SIZE_MULTIPLIER, MAX_LEVELS, MEMTABLE_CAPACITY_BYTES,
};
use crate::memtable::MemTable;
use crate::sstable::{Manifest, SSTableBuilder, SSTableReader, VersionEdit, compaction::compact};
use crate::wal::Wal;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

pub type BlockCache = Arc<RwLock<lru::LruCache<(u64, u64), Arc<Vec<u8>>>>>;

/// The central coordinator of the LSM-Tree storage engine.
///
/// ## Concurrency Model
///
/// We deliberately chose different primitives for different resources based on their access
/// patterns:
///
/// - **`active_memtable` — `Mutex`**: Reads and writes both mutate state (reads charge the bloom
///   filter, writes insert entries). Under sustained write pressure, `RwLock` write-locks can
///   starve waiting readers on Linux's futex scheduler. `Mutex` gives fair FIFO ordering.
///
/// - **`immutable_memtable` — `Mutex<Option<Arc<MemTable>>>`**: Wrapping in `Arc` lets the
///   background flush thread clone the reference and release the lock immediately — it can then
///   spend seconds writing the SSTable to disk with no lock held. Without the `Arc`, the lock
///   would be held for the entire multi-second flush, blocking the next write stall check.
///
/// - **`wal` — `Mutex`**: WAL appends are inherently sequential (each record has a monotonic
///   sequence number). Allowing concurrent writes would corrupt the physical block layout.
///   `RwLock` read-access on a WAL is meaningless, so `Mutex` is the honest choice.
///
/// - **`sstables` — `RwLock`**: Many threads can read the SSTable list concurrently during
///   parallel point queries. Only a flush or compaction mutates it, which is rare. This is the
///   one place where the reader/writer split actually pays off.
pub struct StorageEngine {
    // Mutex over RwLock: see concurrency model above.
    active_memtable: Arc<Mutex<MemTable>>,
    // Arc<MemTable> inside the Option so the flush thread doesn't hold the lock during I/O.
    immutable_memtable: Arc<Mutex<Option<Arc<MemTable>>>>,
    // Append-only, never read concurrently — Mutex is correct.
    wal: Arc<Mutex<Wal>>,
    sstables: Arc<RwLock<Vec<Vec<SSTableReader>>>>,
    manifest: Arc<RwLock<Manifest>>,
    memtable_capacity: usize,
    next_seq_num: Arc<AtomicU64>,
    db_path: Arc<PathBuf>,
    block_cache: BlockCache,
    // A Condvar stalls writers when the immutable slot is occupied (flush in progress).
    // Without this, a second flush trigger while one is running would silently drop data.
    // Writers block here instead of racing or returning an error.
    flush_condvar: Arc<(Mutex<bool>, Condvar)>,
}

impl StorageEngine {
    /// Opens or creates the database at the given path.
    ///
    /// WAL replay happens unconditionally at startup: any records in the WAL that were not yet
    /// flushed to an SSTable (e.g., data written between the last flush and a crash) are
    /// replayed into the MemTable. This is safe to do even after a clean shutdown because the
    /// WAL is rotated (the old file is GC'd) only after the corresponding SSTable is confirmed
    /// written to disk.
    ///
    /// The SSTable list is rebuilt from the MANIFEST, not by scanning the `sst/` directory.
    /// Scanning the directory would pick up partially-written files from interrupted flushes.
    /// The MANIFEST only records files that were fully written and renamed atomically.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, anyhow::Error> {
        let db_path = path.into();
        std::fs::create_dir_all(&db_path)?;

        let wal_dir = db_path.join("wal");
        let sst_dir = db_path.join("sst");

        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&sst_dir)?;

        let mut wal = Wal::new(wal_dir)?;

        let memtable_capacity = MEMTABLE_CAPACITY_BYTES;
        let mut memtable = MemTable::new(memtable_capacity, BLOOM_FILTER_FPR);

        let mut max_seq = 0;

        if let Ok(records) = wal.recover() {
            for record in records {
                max_seq = max_seq.max(record.seq_num);
                memtable.set(record.key, record.val);
            }
        }

        let manifest_path = db_path.join("MANIFEST");
        let active_ssts = Manifest::recover(&manifest_path)?;
        let manifest = Manifest::open(&manifest_path)?;

        let mut sstables: Vec<Vec<SSTableReader>> = Vec::new();

        for (level, sst_ids) in active_ssts.iter().enumerate() {
            let mut level_readers = Vec::new();
            for sst_id in sst_ids {
                let path = sst_dir.join(format!("{}.sst", sst_id));
                if path.exists() {
                    level_readers.push(SSTableReader::new(path));
                }
            }
            // L0 files can overlap in key range because each flush writes independent ranges.
            // Sorting newest-first ensures we always return the most recent value on a read
            // without comparing timestamps inside individual entries.
            if level == 0 {
                level_readers.sort_by(|a, b| b.id.cmp(&a.id));
            }
            sstables.push(level_readers);
        }

        // Always guarantee at least one slot for L0 so flush logic never has to bounds-check.
        if sstables.is_empty() {
            sstables.push(Vec::new());
        }

        Ok(Self {
            active_memtable: Arc::new(Mutex::new(memtable)),
            immutable_memtable: Arc::new(Mutex::new(None)),
            wal: Arc::new(Mutex::new(wal)),
            sstables: Arc::new(RwLock::new(sstables)),
            manifest: Arc::new(RwLock::new(manifest)),
            memtable_capacity,
            next_seq_num: Arc::new(AtomicU64::new(max_seq + 1)),
            db_path: Arc::new(db_path),
            block_cache: Arc::new(RwLock::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(BLOCK_CACHE_CAPACITY).unwrap(),
            ))),
            flush_condvar: Arc::new((Mutex::new(false), Condvar::new())),
        })
    }

    /// Inserts a key-value pair.
    ///
    /// The WAL is written **before** the MemTable. If the process is killed between these two
    /// steps, WAL replay at startup re-inserts the entry. Reversing the order would mean a crash
    /// after the MemTable insert but before the WAL write loses a durably acknowledged write.
    ///
    /// `SeqCst` ordering on the sequence number is intentional: the sequence number establishes
    /// a total order across all operations (puts, removes). A weaker ordering could allow two
    /// threads to observe the same sequence number or observe insertions out of order during
    /// WAL recovery.
    pub fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), anyhow::Error> {
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        let seq = self.next_seq_num.fetch_add(1, Ordering::SeqCst);

        // WAL first — crash durability requires the log precede the in-memory change.
        self.wal
            .lock()
            .map_err(|_| anyhow::anyhow!("WAL lock poisoned"))?
            .add(seq, key.clone(), value.clone())?;

        let needs_flush = {
            let mut memtable = self
                .active_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("MemTable lock poisoned"))?;
            memtable.set(key, value);
            memtable.needs_flush()
        };

        if needs_flush {
            self.trigger_background_flush()?;
        }

        Ok(())
    }

    /// Retrieves the most recent value for a key, or `None` if absent or deleted.
    ///
    /// The search order — active MemTable → immutable MemTable → SSTables newest-to-oldest —
    /// guarantees that we always find the **latest** version first. Because LSM-Trees are
    /// append-only, older versions of a key coexist with newer ones on disk; the search order
    /// resolves which version wins without a merge step on every read.
    ///
    /// An empty value (`val.is_empty()`) means the key was deleted via a tombstone. We return
    /// `None` rather than the empty bytes so callers see a clean "not found" — they should not
    /// need to know the deletion mechanism.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, anyhow::Error> {
        let key = key.as_ref();

        {
            let memtable = self
                .active_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("MemTable lock poisoned"))?;
            if let Some(val) = memtable.get(key) {
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(val.clone()));
            }
        }

        {
            let imm = self
                .immutable_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("Immutable MemTable lock poisoned"))?;

            if let Some(imm_memtable) = imm.as_ref()
                && let Some(val) = imm_memtable.get(key)
            {
                if val.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(val.clone()));
            }
        }

        {
            let sstables = self
                .sstables
                .read()
                .map_err(|_| anyhow::anyhow!("SSTables read lock poisoned"))?;

            for level in sstables.iter() {
                for reader in level.iter() {
                    if let Some(val) = reader.get(key, Some(&self.block_cache)) {
                        if val.is_empty() {
                            return Ok(None);
                        }
                        return Ok(Some(val));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Updates a key. Identical to `put` — the LSM-Tree model has no true in-place update.
    ///
    /// The newest entry always wins on a read, so writing a new version of the key is the
    /// correct way to update it. The old version is removed transparently during compaction.
    pub fn update<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), anyhow::Error> {
        self.put(key, value)
    }

    /// Marks a key as deleted by writing a tombstone (empty value).
    ///
    /// LSM-Trees cannot physically remove a key from an immutable SSTable. Instead, a tombstone
    /// entry shadows all older versions of the key during reads and compaction. The actual disk
    /// space is recovered when compaction encounters the tombstone and discards both it and every
    /// older version of that key in the lower levels.
    ///
    /// The tombstone must be WAL-logged for the same reason as `put`: a crash between writing
    /// the tombstone to the MemTable and logging it would resurrect the deleted key on recovery.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<(), anyhow::Error> {
        let key = key.as_ref().to_vec();
        let seq = self.next_seq_num.fetch_add(1, Ordering::SeqCst);
        let tombstone_val: Vec<u8> = vec![];

        self.wal
            .lock()
            .map_err(|_| anyhow::anyhow!("WAL lock poisoned"))?
            .remove(seq, key.clone())?;

        let needs_flush = {
            let mut memtable = self
                .active_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("MemTable lock poisoned"))?;
            memtable.set(key, tombstone_val);
            memtable.needs_flush()
        };

        if needs_flush {
            self.trigger_background_flush()?;
        }

        Ok(())
    }

    /// Destroys all data in the database and resets it to a clean empty state.
    ///
    /// This deletes the entire SSTable directory, WAL directory, and MANIFEST, then
    /// re-initializes them. It exists primarily for testing teardown; in production you
    /// would almost never call this.
    pub fn clear(&self) -> Result<(), anyhow::Error> {
        {
            let mut memtable = self
                .active_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("MemTable lock poisoned"))?;
            memtable.clear();
        }

        {
            let mut imm = self
                .immutable_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("Immutable MemTable lock poisoned"))?;
            *imm = None;
        }

        let mut sstables = self
            .sstables
            .write()
            .map_err(|_| anyhow::anyhow!("SSTables lock poisoned"))?;
        sstables.clear();

        let sst_dir = self.db_path.join("sst");
        let wal_dir = self.db_path.join("wal");
        let manifest_path = self.db_path.join("MANIFEST");

        let _ = std::fs::remove_dir_all(&sst_dir);
        let _ = std::fs::remove_dir_all(&wal_dir);
        let _ = std::fs::remove_file(&manifest_path);

        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&sst_dir)?;

        {
            let mut wal_writer = self
                .wal
                .lock()
                .map_err(|_| anyhow::anyhow!("WAL lock poisoned"))?;
            *wal_writer = Wal::new(wal_dir)?;
        }

        let manifest = crate::sstable::Manifest::open(&manifest_path)?;
        let mut manifest_lock = self
            .manifest
            .write()
            .map_err(|_| anyhow::anyhow!("Manifest lock poisoned"))?;
        *manifest_lock = manifest;
        sstables.push(Vec::new());

        let mut cache = self
            .block_cache
            .write()
            .map_err(|_| anyhow::anyhow!("Block Cache lock poisoned"))?;
        cache.clear();

        Ok(())
    }

    // Moves the full MemTable into the immutable slot, installs a fresh active MemTable,
    // and spawns a background thread to persist the immutable one to disk.
    //
    // Writers that arrive while a flush is in progress (immutable slot occupied) park on the
    // Condvar rather than returning an error or silently discarding data. This "write stall"
    // is intentional: it applies back-pressure to the caller so the engine never loses writes.
    // The alternative — returning a "try again" error — would require every caller to implement
    // retry logic, which is worse API design.
    fn trigger_background_flush(&self) -> Result<(), anyhow::Error> {
        let (flush_mutex, flush_condvar) = &*self.flush_condvar;

        {
            let mut flushing = flush_mutex.lock().unwrap();
            while *flushing {
                flushing = flush_condvar.wait(flushing).unwrap();
            }
            *flushing = true;
        }

        {
            let mut active = self
                .active_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("MemTable lock poisoned"))?;
            let mut imm = self
                .immutable_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("Immutable MemTable lock poisoned"))?;

            let empty_memtable =
                MemTable::new(self.memtable_capacity, crate::constants::BLOOM_FILTER_FPR);
            let memtable_to_flush = std::mem::replace(&mut *active, empty_memtable);

            *imm = Some(Arc::new(memtable_to_flush));

            // Roll the WAL to a new file before releasing the locks. Any writes that arrive
            // after this point will go to the new WAL file. This means the old WAL file
            // contains exactly the records belonging to the MemTable being flushed —
            // once that SSTable is confirmed on disk, the old WAL file can be safely deleted.
            let wal_dir = self.db_path.join("wal");
            if let Ok(mut wal) = Wal::new(wal_dir) {
                wal.add(0, b"".to_vec(), b"".to_vec()).unwrap_or(());
                let mut wal_writer = self
                    .wal
                    .lock()
                    .map_err(|_| anyhow::anyhow!("WAL lock poisoned"))?;
                *wal_writer = wal;
            }
        };

        let imm_memtable_arc = Arc::clone(&self.immutable_memtable);
        let sstables_arc = Arc::clone(&self.sstables);
        let manifest_arc = Arc::clone(&self.manifest);
        let db_path_arc = Arc::clone(&self.db_path);
        let wal_arc = Arc::clone(&self.wal);
        let condvar_arc = Arc::clone(&self.flush_condvar);

        std::thread::spawn(move || {
            if let Err(e) = Self::flush_immutable_memtable(
                imm_memtable_arc,
                sstables_arc,
                manifest_arc,
                db_path_arc,
                wal_arc,
            ) {
                eprintln!("Background flush failed: {}", e);
            }

            // Unblock all writers waiting in trigger_background_flush. notify_all (not
            // notify_one) is deliberate: there may be multiple stalled writers from
            // different threads that can now all proceed.
            let (mutex, condvar) = &*condvar_arc;
            let mut flushing = mutex.lock().unwrap();
            *flushing = false;
            condvar.notify_all();
        });

        Ok(())
    }

    // Writes the immutable MemTable to a new SSTable file on disk.
    //
    // We clone the Arc<MemTable> and immediately release the Mutex so writers are not blocked
    // for the duration of disk I/O (which could take seconds on a slow or heavily-loaded disk).
    // The Arc ensures the MemTable data stays alive for the duration of the write even after
    // the Mutex guard is dropped.
    //
    // SSTable filenames are Unix millisecond timestamps. This gives them a natural sort order
    // (newest = largest number) which the reader uses to check most-recent-first in L0.
    //
    // WAL GC must happen AFTER the immutable slot is cleared. If we deleted WAL files first
    // and then crashed before clearing the slot, recovery would not find the WAL records and
    // would miss those writes. Clearing the slot first is the safe ordering.
    fn flush_immutable_memtable(
        immutable_memtable: Arc<Mutex<Option<Arc<MemTable>>>>,
        sstables: Arc<RwLock<Vec<Vec<SSTableReader>>>>,
        manifest: Arc<RwLock<crate::sstable::Manifest>>,
        db_path: Arc<PathBuf>,
        wal: Arc<Mutex<Wal>>,
    ) -> Result<(), anyhow::Error> {
        let memtable_arc = {
            let imm = immutable_memtable
                .lock()
                .map_err(|_| anyhow::anyhow!("Immutable MemTable lock poisoned"))?;
            match imm.as_ref() {
                Some(m) => Arc::clone(m),
                None => return Ok(()),
            }
        };

        if memtable_arc.approximate_memory_usage() == 0 {
            return Ok(());
        }

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let sst_path = db_path.join(format!("sst/{}.sst", timestamp));
        let mut sst_builder = SSTableBuilder::new(sst_path.clone());

        for (k, v) in memtable_arc.entries() {
            sst_builder.add(k, v);
        }

        sst_builder.finish()?;

        // Insert at index 0 so newest files are always first in L0 (see open() comment
        // about why L0 must be searched newest-first).
        {
            let mut sstables_write = sstables
                .write()
                .map_err(|_| anyhow::anyhow!("SSTables lock poisoned"))?;
            sstables_write[0].insert(0, SSTableReader::new(sst_path));
        }

        let sst_id = timestamp as u64;
        if let Ok(mut m_lock) = manifest.write() {
            let _ = m_lock.log_edit(&VersionEdit::AddTable { level: 0, sst_id });
        }

        // Read the current WAL file number before clearing the immutable slot.
        // Any WAL file strictly older than the current one is safe to delete — it belongs
        // to the MemTable we just persisted. We use saturating_sub(1) because file 0 is
        // the sentinel empty file written during WAL rotation.
        let safe_to_delete_wal_num = {
            let wal_lock = wal.lock().unwrap();
            wal_lock.current_file_num().saturating_sub(1)
        };

        {
            let mut imm = immutable_memtable.lock().unwrap();
            *imm = None;
        }

        if safe_to_delete_wal_num > 0 {
            let wal_lock = wal.lock().unwrap();
            let _ = wal_lock.delete_old_files(safe_to_delete_wal_num);
        }

        let _ = Self::run_compaction(sstables, manifest, db_path);

        Ok(())
    }

    // Compacts levels starting from L0, cascading upward until no level exceeds its budget.
    //
    // L0 triggers by file count (not byte size) because L0 files can overlap in key range.
    // More L0 files means more files to scan on a read miss. Keeping L0 small bounds read
    // amplification. Higher levels use byte budgets because they are sorted and non-overlapping
    // — a single well-sized file is as fast to search as multiple small ones via the index.
    //
    // We log VersionEdits to the MANIFEST BEFORE updating the in-memory sstables list. If we
    // did it afterward and crashed between the two steps, the in-memory list would be stale on
    // restart but the MANIFEST would reflect the correct state (the MANIFEST is authoritative).
    // Doing it before the in-memory update means a crash leaves the MANIFEST correct and the
    // in-memory state is rebuilt from it at startup.
    //
    // Input SSTable files are deleted only after both the MANIFEST is updated and the in-memory
    // list no longer references them — a read thread holding a reference to a now-deleted file
    // would segfault on mmap access otherwise.
    fn run_compaction(
        sstables: Arc<RwLock<Vec<Vec<SSTableReader>>>>,
        manifest: Arc<RwLock<Manifest>>,
        db_path: Arc<PathBuf>,
    ) -> Result<(), anyhow::Error> {
        let max_levels = MAX_LEVELS;

        for level in 0..max_levels.saturating_sub(1) {
            let next_level = level + 1;

            let (needs_compact, input_ids, input_paths) = {
                let sst_read = sstables.read().unwrap();

                if sst_read.len() <= level || sst_read[level].is_empty() {
                    break;
                }

                let should_compact = if level == 0 {
                    sst_read[level].len() >= L0_COMPACTION_TRIGGER
                } else {
                    let level_budget = L1_MAX_BYTES
                        * (LEVEL_SIZE_MULTIPLIER as u64).pow(level.saturating_sub(1) as u32);

                    let total_bytes: u64 = sst_read[level]
                        .iter()
                        .map(|r| {
                            let path = db_path.join(format!("sst/{}.sst", r.id));
                            std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0)
                        })
                        .sum();

                    total_bytes > level_budget
                };

                if !should_compact {
                    break;
                }

                let ids: Vec<u64> = sst_read[level].iter().map(|r| r.id).collect();
                let paths: Vec<std::path::PathBuf> = ids
                    .iter()
                    .map(|id| db_path.join(format!("sst/{}.sst", id)))
                    .collect();

                (true, ids, paths)
            };

            if !needs_compact {
                break;
            }

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let output_path = db_path.join(format!("sst/{}.sst", timestamp));

            compact(input_paths, output_path.clone())?;

            // MANIFEST first — see function-level comment on ordering.
            {
                let mut m_lock = manifest.write().unwrap();
                m_lock.log_edit(&VersionEdit::AddTable {
                    level: next_level as u32,
                    sst_id: timestamp,
                })?;
                for id in &input_ids {
                    m_lock.log_edit(&VersionEdit::RemoveTable {
                        level: level as u32,
                        sst_id: *id,
                    })?;
                }
            }

            // In-memory list updated after MANIFEST — correctness argument above.
            {
                let mut sst_write = sstables.write().unwrap();
                while sst_write.len() <= next_level {
                    sst_write.push(Vec::new());
                }
                sst_write[next_level].insert(0, SSTableReader::new(output_path));
                sst_write[level].retain(|r| !input_ids.contains(&r.id));
            }

            // Files deleted last — only safe once no in-memory reference points to them.
            for id in &input_ids {
                let _ = std::fs::remove_file(db_path.join(format!("sst/{}.sst", id)));
            }
        }

        Ok(())
    }
}
