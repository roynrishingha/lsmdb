//! Tuning knobs for the LSM-Tree engine.
//!
//! All constants are gathered here so that tuning the engine means changing one file, not
//! hunting for magic numbers scattered across the codebase. Each constant documents the
//! tradeoff it controls so you can make an informed decision when adjusting it for your workload.

/// Maximum size of a MemTable before it is promoted to immutable and scheduled for flushing.
///
/// Larger values amortize flush overhead over more writes (better write throughput) but increase
/// peak memory usage and the amount of data exposed to loss if the process crashes without a WAL.
/// 4 MB is a conservative default; production systems like RocksDB default to 64 MB.
pub const MEMTABLE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

/// Desired false positive rate for the Bloom Filter embedded in each SSTable.
///
/// The Bloom Filter prevents unnecessary SSTable reads on a lookup miss. A lower FPR means fewer
/// wasted reads but requires more bits per key (more memory and a larger filter block on disk).
/// The relationship is: `m = -n * ln(p) / ln(2)²`. At 1% FPR and 100K elements, the filter
/// occupies ~120 KB — negligible compared to a 4 MB SSTable.
pub const BLOOM_FILTER_FPR: f64 = 0.01;

/// Physical block size for the Write-Ahead Log.
///
/// WAL records are chunked to fit within 32 KB fixed-size blocks. This alignment matters for
/// crash recovery: if a write is interrupted, we can detect corruption at a clean 32 KB boundary
/// rather than mid-stream, avoiding ambiguity about whether a partial record is valid.
/// 32 KB matches the default block size in LevelDB and aligns well with modern SSD erase units.
pub const WAL_BLOCK_SIZE: usize = 32768;

/// Byte cost of the WAL chunk header: CRC32 (4) + length (2) + chunk type (1).
///
/// The CRC32 catches bit-flips or partial block writes during crash recovery. The chunk type
/// (Full/First/Middle/Last) lets the reader reassemble records split across block boundaries.
pub const WAL_HEADER_SIZE: usize = 7;

/// Maximum payload bytes in one WAL chunk, after reserving space for the header.
///
/// Records larger than this are split into multiple chunks (First/Middle/Last). This is a derived
/// constant — it exists to make the splitting arithmetic in `WalWriter` readable.
pub const WAL_MAX_PAYLOAD_SIZE: usize = WAL_BLOCK_SIZE - WAL_HEADER_SIZE;

/// Maximum size of a Data Block inside an SSTable.
///
/// Smaller blocks improve point-query performance (less data to decompress and scan per lookup)
/// but increase index overhead and reduce compression ratio (Snappy works better on larger
/// inputs). 4 KB matches a typical OS page and is the LevelDB default.
pub const SSTABLE_BLOCK_SIZE: usize = 4096;

/// How often a full, uncompressed key is written to a Data Block (the "restart interval").
///
/// Data Blocks use prefix compression: each key stores only the bytes that differ from the
/// previous key. This reduces block size but makes random seeks inside a block expensive — you
/// have to scan from the last restart point. A restart every 16 keys bounds the scan length to
/// at most 16 entries per lookup within a block.
pub const SSTABLE_RESTART_INTERVAL: usize = 16;

/// Allocation unit for the MemTable's Arena allocator.
///
/// The Arena hands out memory in large slabs to avoid per-node `malloc` overhead, which would
/// dominate insert latency for millions of small SkipList nodes. 4 MB slabs match
/// `MEMTABLE_CAPACITY_BYTES` so the Arena and MemTable scale together.
pub const ARENA_BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// Number of L0 SSTables that triggers a compaction into L1.
///
/// L0 files are written sequentially during flushes and can overlap in key range. Each
/// additional L0 file adds one more file that a read miss must check. Keeping L0 small
/// directly bounds read amplification on hot-key misses. 4 is the LevelDB default.
pub const L0_COMPACTION_TRIGGER: usize = 4;

/// Maximum number of Data Blocks held in the in-memory LRU block cache.
///
/// The block cache avoids paying the mmap page-fault cost on repeated reads of the same 4 KB
/// Data Block. This capacity is intentionally modest (100 blocks × 4 KB = 400 KB) so the engine
/// does not consume surprising amounts of memory out of the box. Increase this for workloads with
/// strong temporal locality (repeated reads of a small working set).
pub const BLOCK_CACHE_CAPACITY: usize = 100;

/// Maximum number of levels in the LSM-Tree hierarchy.
///
/// With 7 levels and a 10× size multiplier per level, the tree can hold on the order of
/// L1 × 10⁶ bytes ≈ 10 TB before L7 would overflow — far beyond the expected use case.
/// Adding more levels beyond 7 rarely helps: read amplification grows linearly with level count,
/// and compaction cost at deep levels is expensive.
pub const MAX_LEVELS: usize = 7;

/// Size ratio between consecutive levels.
///
/// L(N) can hold `LEVEL_SIZE_MULTIPLIER` × more bytes than L(N-1). A smaller multiplier means
/// more frequent cross-level compaction (more write amplification, less read amplification).
/// A larger multiplier reduces compaction frequency but allows each level to grow very large
/// before triggering, increasing read amplification on misses. 10× is the industry default.
pub const LEVEL_SIZE_MULTIPLIER: usize = 10;

/// Byte budget for Level 1 before compaction into Level 2 is triggered.
///
/// Each higher level N has budget = L1_MAX_BYTES × LEVEL_SIZE_MULTIPLIER^(N-1).
/// 10 MB for L1 is deliberately small (RocksDB defaults to 256 MB) to keep the default
/// configuration lightweight. Tune this upward for production write-heavy workloads.
pub const L1_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Whether to call `fdatasync()` after every WAL record write.
///
/// When `true`, each `put`/`remove` call blocks until the kernel confirms the write has reached
/// physical storage. This prevents data loss on power failure or OS crash at the cost of
/// ~10–100 µs of latency per write (depending on the storage device). Set to `false` to let
/// the OS buffer WAL writes for throughput — but know that the last ~30 seconds of writes may
/// be lost if the machine loses power.
pub const WAL_SYNC_ON_WRITE: bool = true;

/// Compression type identifier byte prefixed to every Data Block on disk.
///
/// These sentinel values are embedded in the SSTable file so a future reader can decompress
/// without knowing out-of-band what compression was used. Adding a new algorithm means adding a
/// new constant and handling it in the read path — the format is forward-compatible.
pub const COMPRESSION_NONE: u8 = 0x00;
pub const COMPRESSION_SNAPPY: u8 = 0x01;
