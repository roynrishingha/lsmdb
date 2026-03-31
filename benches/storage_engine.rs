// Benchmarks for lsmdb's StorageEngine.
//
// The test matrix is designed so that every layer of the engine is exercised at least once:
//
//   Layer               Exercised by
//   ─────────────────── ────────────────────────────────────────────────────
//   WAL                 Any write benchmark that pushes past MEMTABLE_CAPACITY_BYTES
//   MemTable (SkipList) All write benchmarks
//   MemTable flush→SST  bench_sustained_write, bench_compaction_trigger, bench_overwrite_churn
//   Bloom Filter        bench_get_cold_sstable (cache cleared), bench_bloom_miss
//   Block Cache (LRU)   bench_get_hot_sstable (deliberate cache hits)
//   Compaction          bench_compaction_trigger (pushes L0 past its 4-file trigger)
//   Tombstone path      bench_delete_churn (remove after put, repeatedly)
//
// MEMTABLE_CAPACITY_BYTES = 4 MB. Each bench that wants to exercise disk paths writes at
// least 1× the MemTable capacity so at least one flush is guaranteed.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lsmdb::StorageEngine;
use tempfile::TempDir;

// 4 MB MemTable. We need to write more than this to guarantee a flush.
const MEMTABLE_CAPACITY: usize = 4 * 1024 * 1024;

// L0 compaction fires after 4 SSTables. Each SSTable ≈ one full MemTable.
// So 5 flushes guarantees at least one L0→L1 compaction.
const FLUSHES_FOR_COMPACTION: usize = 5;

fn make_engine() -> (StorageEngine, TempDir) {
    let dir = TempDir::new().expect("TempDir");
    let engine = StorageEngine::open(dir.path()).expect("open engine");
    (engine, dir)
}

// Generates enough key-value pairs to fill `target_bytes` of key+value payload.
// Keys are zero-padded so they are easy to reproduce deterministically.
fn generate_kv_pairs(target_bytes: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let key_size = 20usize;
    let pair_size = key_size + value_size;
    let count = (target_bytes / pair_size).max(1);
    (0..count)
        .map(|i| {
            let key = format!("key{:016}", i).into_bytes();
            let val = (i as u8..i as u8 + value_size as u8)
                .cycle()
                .take(value_size)
                .collect();
            (key, val)
        })
        .collect()
}

// Write data until the engine has flushed at least `num_flushes` MemTables.
// Returns the set of keys written (for subsequent read benchmarks).
fn warm_engine_with_flushes(
    engine: &StorageEngine,
    num_flushes: usize,
    value_size: usize,
) -> Vec<Vec<u8>> {
    let pairs = generate_kv_pairs(MEMTABLE_CAPACITY * num_flushes, value_size);
    for (k, v) in &pairs {
        engine.put(k, v).expect("put");
    }
    // Give background threads a moment to finish flushing and compacting.
    std::thread::sleep(std::time::Duration::from_millis(300));
    pairs.into_iter().map(|(k, _)| k).collect()
}

// Sustained write throughput (exercises WAL + MemTable + SSTable flush)

fn bench_sustained_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_write");

    // Write 2× the MemTable capacity so the WAL, flush thread, and SSTable builder
    // all participate. The per-iteration cost includes the occasional flush stall.
    for value_size in [64usize, 512, 4096] {
        let total_bytes = MEMTABLE_CAPACITY * 2;
        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("val_{}B", value_size)),
            &value_size,
            |b, &sz| {
                b.iter(|| {
                    let (engine, _dir) = make_engine();
                    let pairs = generate_kv_pairs(total_bytes, sz);
                    for (k, v) in &pairs {
                        engine.put(k, v).unwrap();
                    }
                    std::hint::black_box(());
                });
            },
        );
    }
    group.finish();
}

// Single-key PUT latency (WAL + MemTable only, MemTable never fills)
//
// This shows the baseline write cost before any disk I/O triggers.

fn bench_put_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_latency");

    for value_size in [16usize, 256, 4096] {
        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("val_{}B", value_size)),
            &value_size,
            |b, &sz| {
                let (engine, _dir) = make_engine();
                let val: Vec<u8> = vec![0xAB; sz];
                let mut counter = 0u64;
                b.iter(|| {
                    let key = format!("{:016}", counter).into_bytes();
                    counter += 1;
                    engine.put(&key, &val).unwrap();
                });
            },
        );
    }
    group.finish();
}

// GET from active MemTable (hot in-memory path)
//
// All keys are written below MemTable capacity so the engine never flushes.
// This isolates the SkipList lookup + Bloom Filter check cost.

fn bench_get_memtable_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_memtable_hot");

    for value_size in [64usize, 512, 4096] {
        group.throughput(Throughput::Bytes(value_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("val_{}B", value_size)),
            &value_size,
            |b, &sz| {
                let (engine, _dir) = make_engine();
                let val: Vec<u8> = vec![0xCC; sz];
                // 1,000 entries ≈ 20 KB key payload — well under the 4 MB threshold.
                let keys: Vec<Vec<u8>> = (0..1_000usize)
                    .map(|i| format!("key{:016}", i).into_bytes())
                    .collect();
                for k in &keys {
                    engine.put(k, &val).unwrap();
                }
                let mut idx = 0usize;
                b.iter(|| {
                    let result = engine.get(&keys[idx % keys.len()]).unwrap();
                    std::hint::black_box(result);
                    idx += 1;
                });
            },
        );
    }
    group.finish();
}

// GET from SSTable via block cache (warm cache path)
//
// We pre-populate past MemTable capacity so data lands in SSTables, then read
// repeatedly to build up the LRU block cache. The steady state measures the
// cache hit path: Bloom Filter → Index Block → cached decompressed Data Block.

fn bench_get_sstable_warm_cache(c: &mut Criterion) {
    let (engine, _dir) = make_engine();

    // Write 1.5× MemTable capacity to guarantee at least one SSTable, then wait.
    let keys = warm_engine_with_flushes(&engine, 2, 256);

    // Pre-warm the block cache with one read pass.
    for k in &keys {
        let _ = engine.get(k).unwrap();
    }

    let mut idx = 0usize;
    c.bench_function("get_sstable_warm_cache", |b| {
        b.iter(|| {
            let result = engine.get(&keys[idx % keys.len()]).unwrap();
            std::hint::black_box(result);
            idx += 1;
        });
    });
}

// GET miss — Bloom Filter eliminates disk I/O (cold negative path)
//
// With data in SSTables, a GET for a key that never existed must:
//   1. Miss the active MemTable (Bloom Filter says no)
//   2. Miss the immutable MemTable (empty at query time)
//   3. Check each SSTable's Bloom Filter (all should say no) → no block reads
//
// This measures the cost of Bloom Filter false-negative resolution across levels.

fn bench_get_bloom_miss(c: &mut Criterion) {
    let (engine, _dir) = make_engine();
    warm_engine_with_flushes(&engine, 3, 256);

    c.bench_function("get_bloom_miss", |b| {
        b.iter(|| {
            // This key was never inserted. Bloom Filters should reject it at each level.
            let result = engine.get(b"zzzzz_intentional_miss_key_zzzzz").unwrap();
            std::hint::black_box(result);
        });
    });
}

// Compaction-trigger write batch
//
// Writes enough data to overflow L0 past its 4-file compaction trigger, forcing
// L0 → L1 compaction. This is the heaviest benchmark: WAL + MemTable + multiple
// SSTable flushes + the background merge-sort compaction step all participate.

fn bench_compaction_trigger(c: &mut Criterion) {
    // FLUSHES_FOR_COMPACTION = 5, so 5 × 4MB = 20 MB written per iteration.
    // This benchmark has a high per-iteration cost; Criterion will run fewer samples.
    let total_bytes = MEMTABLE_CAPACITY * FLUSHES_FOR_COMPACTION;

    c.bench_function("compaction_trigger_write", |b| {
        b.iter(|| {
            let (engine, _dir) = make_engine();
            let pairs = generate_kv_pairs(total_bytes, 256);
            for (k, v) in &pairs {
                engine.put(k, v).unwrap();
            }
            // Wait for compaction to complete so the next iteration starts clean.
            std::thread::sleep(std::time::Duration::from_millis(500));
            std::hint::black_box(());
        });
    });
}

// Overwrite churn (repeated writes to the same key set)
//
// Writes the same 1,000 keys repeatedly across multiple MemTable lifetimes.
// This creates many versions of the same keys — compaction must resolve them.
// Measures overwrite throughput and exercises the in-place SkipList update path.

fn bench_overwrite_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("overwrite_churn");

    // Write the same 500 keys 20 times = 10,000 writes total = ~2–3 MemTable flushes.
    let num_keys = 500usize;
    let overwrites = 20usize;
    let val: Vec<u8> = vec![0xDE; 512];

    group.throughput(Throughput::Elements((num_keys * overwrites) as u64));
    group.bench_function("500_keys_20x", |b| {
        b.iter(|| {
            let (engine, _dir) = make_engine();
            let keys: Vec<Vec<u8>> = (0..num_keys)
                .map(|i| format!("churn_key_{:08}", i).into_bytes())
                .collect();
            for _ in 0..overwrites {
                for k in &keys {
                    engine.put(k, &val).unwrap();
                }
            }
            std::hint::black_box(());
        });
    });
    group.finish();
}

// DELETE (tombstone) throughput
//
// Each iteration writes a key then immediately tombstones it. This exercises:
//   - WAL append for both the Put and Delete records
//   - MemTable insert of an empty-value tombstone
//   - For sustained runs: SSTable writes containing tombstone records

fn bench_delete_churn(c: &mut Criterion) {
    c.bench_function("delete_churn", |b| {
        let (engine, _keep_dir) = make_engine();
        let val = vec![0xFFu8; 128];
        let mut counter = 0u64;

        b.iter(|| {
            let key = format!("{:016}", counter).into_bytes();
            counter += 1;
            engine.put(&key, &val).unwrap();
            engine.remove(&key).unwrap();
        });
    });
}

// Realistic mixed read/write workload
//
// Simulates a real-world workload: a known working set pushed to SSTables,
// then a continuous stream of 70% reads / 20% writes / 10% deletes.
// Uses a random-ish key distribution to produce both cache hits and misses.

fn bench_mixed_real(c: &mut Criterion) {
    let (engine, _dir) = make_engine();

    // Pre-populate: force at least 2 flushed SSTables as the "existing dataset".
    let keys = warm_engine_with_flushes(&engine, 2, 128);
    let val = vec![0xEEu8; 128];

    let mut op = 0usize;
    c.bench_function("mixed_70r_20w_10d", |b| {
        b.iter(|| {
            let key = &keys[op % keys.len()];
            let r = op % 10;

            if r < 7 {
                // 70% read — mix of cache hits and SSTable reads depending on key
                let result = engine.get(key).unwrap();
                std::hint::black_box(result);
            } else if r < 9 {
                // 20% write — new keys to avoid overwriting the prefilled set
                let new_key = format!("new_{:016}", op).into_bytes();
                engine.put(&new_key, &val).unwrap();
            } else {
                // 10% delete — tombstone a key from the working set
                engine.remove(key).unwrap();
            }

            op += 1;
        });
    });
}

criterion_group!(
    benches,
    bench_put_latency,
    bench_sustained_write,
    bench_get_memtable_hot,
    bench_get_sstable_warm_cache,
    bench_get_bloom_miss,
    bench_overwrite_churn,
    bench_delete_churn,
    bench_mixed_real,
    bench_compaction_trigger,
);
criterion_main!(benches);
