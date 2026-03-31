# lsmdb

[![CI](https://github.com/roynrishingha/lsmdb/workflows/CI/badge.svg)](https://github.com/roynrishingha/lsmdb/actions)
[![crates.io](https://img.shields.io/crates/v/lsmdb.svg?style=for-the-badge&color=fc8d62&logo=rust)](https://crates.io/crates/lsmdb)
[![docs.rs](https://img.shields.io/badge/docs.rs-lsmdb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs)](https://docs.rs/lsmdb) 

A persistent, crash-safe key-value storage engine built on a **Log-Structured Merge Tree (LSM-Tree)** in Rust.

---

## Why LSM-Tree?

Traditional storage engines (B-Trees) perform random in-place writes — slow on both HDDs (seek latency) and SSDs (erase-before-write amplification). An LSM-Tree converts all writes into sequential appends, making write throughput independent of dataset size. Reads are served from the freshest layer first, and background compaction periodically merges layers to bound read amplification and recover disk space.

---

## Architecture

```text
 Write path                          Read path
 ──────────                          ──────────
  put(key, value)                     get(key)
       │                                  │
       ▼                                  ▼
 ┌───────────┐                    ┌───────────────┐
 │    WAL    │ (crash durability) │ Active MemTbl │  1. freshest writes
 └─────┬─────┘                    └───────┬───────┘
       │                                  │ miss
       ▼                                  ▼
 ┌───────────────┐                ┌───────────────┐
 │ Active MemTbl │                │  Immutable    │  2. flushing
 │  (SkipList)   │                │   MemTable    │
 └────────┬──────┘                └───────┬───────┘
  full?   │                               │ miss
          ▼                               ▼
 ┌───────────────┐                ┌───────────────┐
 │  Immutable    │ background     │  Block Cache  │  3. hot blocks
 │   MemTable    │ flush thread   │  (LRU, RAM)   │
 └────────┬──────┘                └───────┬───────┘
          │                               │ miss
          ▼                               ▼
 ┌─────────────────────────────┐  ┌─────────────────────────────┐
 │  SSTable  L0  (newest)      │  │  SSTable  L0 → L1 → L2…    │
 │  SSTable  L1                │  │  Bloom Filter → Index Block  │
 │  SSTable  L2  …             │  │  → Data Block (mmap)         │
 └─────────────────────────────┘  └─────────────────────────────┘
          ▲
     Compaction: size-tiered, multi-level,
     merges SSTables, resolves tombstones, frees disk
```

---

## Features

| Feature | Details |
|---|---|
| **Write-Ahead Log (WAL)** | Every write is durably logged before the MemTable is updated. On restart, any un-flushed records are replayed. WAL uses 32 KB fixed-size blocks with CRC32 chunk checksums for reliable crash recovery. |
| **Arena-backed SkipList MemTable** | Writes land in a lock-free SkipList backed by a bump-pointer Arena allocator. No per-node `malloc` overhead. Mutex (not RwLock) ensures fair scheduling under write pressure. |
| **Immutable SSTables** | Once a MemTable fills (default 4 MB), it is asynchronously flushed to an immutable SSTable — a block-structured file with prefix-compressed Data Blocks, an Index Block, and a Bloom Filter. |
| **Snappy Compression** | Every Data Block is Snappy-compressed on write and decompressed on read. A 1-byte type prefix in the file format allows adding new codecs without a schema change. |
| **Bloom Filters** | Each SSTable carries a serialized Bloom Filter (1% FPR by default). A point-query miss eliminates 99% of unnecessary disk reads in O(k) hash operations. |
| **LRU Block Cache** | Decompressed 4 KB Data Blocks are kept in an in-memory LRU cache. Repeated reads of a hot working set pay only the cache lookup cost. |
| **Multi-level Compaction** | L0 compacts to L1 when L0 reaches 4 SSTables. Each level N has 10× the byte budget of level N-1, up to 7 levels. A k-way merge resolves overwrites and tombstones. |
| **MANIFEST / VersionEdit** | Atomic file-rename guarantees SSTables are always fully written or absent. The MANIFEST records which SSTables exist at which level, so startup is always consistent even after a crash mid-compaction. |

---

## Usage

```toml
[dependencies]
lsmdb = "1"
```

```rust
use lsmdb::StorageEngine;

// Open (or create) a database at the given path.
let engine = StorageEngine::open("./my_db")?;

// Write
engine.put("user:42", "Alice")?;

// Read
if let Some(val) = engine.get("user:42")? {
    println!("{}", String::from_utf8_lossy(&val)); // "Alice"
}

// Delete (tombstone — space recovered during compaction)
engine.remove("user:42")?;
```

---

## Interactive CLI

```bash
cargo run
```

```text
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
┃     lsmdb  —  LSM-Tree Storage Engine     ┃
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  db path: /home/user/.lsmdb

lsmdb> put hello world
OK hello (31 µs)
lsmdb> get hello
hello → world
  (4 µs)
lsmdb> delete hello
OK hello (28 µs)
lsmdb> exit
Goodbye!
```

An optional path argument overrides the default `~/.lsmdb`:

```bash
cargo run -- /tmp/test_db
```

---

## Configuration

All tuning knobs live in [`src/constants.rs`](src/constants.rs) with documented tradeoffs. Key defaults:

| Constant | Default | Controls |
|---|---|---|
| `MEMTABLE_CAPACITY_BYTES` | 4 MB | When a MemTable is promoted to immutable and flushed |
| `BLOOM_FILTER_FPR` | 1% | False positive rate — lower = fewer disk reads, larger filter |
| `WAL_SYNC_ON_WRITE` | `true` | `fdatasync()` after every write — durability vs latency |
| `L0_COMPACTION_TRIGGER` | 4 files | L0 file count before compaction to L1 |
| `LEVEL_SIZE_MULTIPLIER` | 10× | Byte budget ratio between levels |
| `BLOCK_CACHE_CAPACITY` | 100 blocks | Number of 4 KB decompressed blocks kept in the LRU |
