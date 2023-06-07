# Architecture

## Storage Engine Public APIs

- `put(key, value)`: store a key-value pair in the LSM tree.
- `delete(key)`: remove a key and its corresponding value.
- `get(key)`: get the value corresponding to a key.
- `scan(range)`: get a range of key-value pairs.

Internal APIs
- `flush()`: ensure all the operations before sync are persisted to the disk.

## Data Flow

### Write Flow

1. Write the key-value pair to WAL (write ahead log), so that it can be recovered if storage engine crashes.
2. Write the key-value pair to memtable. After writing to WAL and MemTable completes, we can notify user that the write operation is completed.
3. When a memtable is full, flush it to the disk as an SST file in the background.
4. Compact files into lower levels to maintain a good shape for the LSM Tree, so that the read amplification is low.

### Read Flow

1. Probe all the memtables from latest to oldest.
2. If the key is not found, we will then search the entire LSM tree containing SSTs to find the data.

---

## LSM Tree features

1. Data are immutable on persistent storage, which means that it is easier to offload the background tasks (compaction) to remote servers. It is also feasible to directly store and serve data from cloud-native storage systems like S3.
2. An LSM tree can balance between read, write and space amplification by changing the compaction algorithm. The data structure itself is super versatile and can be optimized for different workloads.

## LSM Tree vs B-Tree

In RB-Tree and B-Tree, all values are overwritten at it's original memory or disk space when we update the value corresponding to the the key.
But in LSM Tree, all write operations, i.e., insert, update, delete, are performed in somewhere else. 
This operations will be batched into SST (sorted string table) files and can be written to the disk. 
Once written to the disk, the file will not be changed.
These operations are applied lazily on disk with a special task called **compaction**. 
The compaction will merge multiple SST files and remove unused data.