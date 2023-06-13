# Architecture

`lsmdb` provides efficient storage and retrival of key-value pair.

## Components of `lsmdb`

The components of the `lsmdb` Storage Engine

### MemTable

A MemTable (short for memory table) is an in-memory data structure that stores key-value pairs. It acts as the primary read/write interface for the storage engine. The MemTable is typically implemented as a sorted data structure, such as a skip list or a red-black tree, to allow efficient lookups and updates.

When a write operation (e.g., insert, update, delete) occurs, the key-value pair is first written to the MemTable. As the MemTable resides in memory, write operations can be performed quickly. However, this data is not durable and can be lost in the event of a crash or system failure.

### Write-Ahead Log (WAL)

The Write-Ahead Log, often referred to as the WAL, is a mechanism used to provide durability for write operations. It ensures that data modifications are logged before they are applied to the MemTable. The WAL is typically implemented as an append-only file or a series of log segments.

When a write operation is received, the key-value pair is first appended to the WAL. This ensures that the modification is durably stored on disk, even if the MemTable resides only in memory. In the event of a crash or system failure, the WAL can be replayed to recover the data modifications and bring the MemTable back to a consistent state.

### SSTable (Sorted String Table)

An SSTable, or Sorted String Table, is an immutable on-disk data structure that stores key-value pairs in a sorted order. It serves as the persistent storage layer for the LSM Tree-based engine. SSTables are typically stored as multiple files, each containing a sorted range of key-value pairs.

When the MemTable reaches a certain threshold size, it is "flushed" to disk as a new SSTable file. The MemTable is atomically replaced with an empty one, allowing new write operations to continue. This process is known as a "memtable flush." 

Flushing the MemTable to disk has two benefits: it frees up memory for new write operations, and it creates an immutable on-disk snapshot of the MemTable contents.

The SSTable files are designed to optimize read operations. Since the data is sorted, key lookups can be performed efficiently using techniques like binary search or Bloom filters. SSTables are typically organized into multiple levels, where each level contains SSTables of increasing size. This multi-level organization allows for efficient read and write operations while minimizing disk I/O.
