# LSM Tree Storage Engine

Log-Structured merge tree is an append-friendly data structure to maintain key-value pairs.

## Components of LSM Tree

The components of the LSM Tree Storage Engine
### MemTable

In a LSM Tree storage engine, the MemTable is an in-memory data structure that holds recently updated key-value pairs. It is implemented as a sorted table-like structure, typically using a skip list or a sorted array, for efficient key lookup and range queries.

The MemTable serves as the first level of data storage in the LSM Tree and is responsible for handling write operations. When a write operation (such as a key-value pair insertion or update) is performed, the corresponding key-value pair is added to the MemTable. The MemTable allows fast write operations since it resides in memory, which provides low-latency access.

However, since the MemTable is in memory, its capacity is limited. As the MemTable fills up, its contents need to be periodically flushed to disk to make space for new updates. This process is typically triggered based on a certain size threshold or number of updates.

#### Role of Memory Table

The `MemTable` is a crucial component in the implementation of an LSM Tree-based storage engine. It represents an in-memory table that holds key-value entries before they are written to disk.

The main purpose of the `MemTable` is to provide efficient write operations by buffering the incoming key-value pairs in memory before flushing them to disk. By keeping the data in memory, write operations can be performed at a much higher speed compared to directly writing to disk.

Here are the main functions and responsibilities of the `MemTable`:

1. **Insertion of key-value entries**: The `MemTable` allows for the insertion of key-value entries. When a new key-value pair is added to the `MemTable`, it is typically appended to an in-memory data structure, such as a sorted array or a skip list. The structure is designed to provide fast insertion and lookup operations.

2. **Lookup of key-value entries**: The `MemTable` provides efficient lookup operations for retrieving the value associated with a given key. It searches for the key within the in-memory data structure to find the corresponding value. If the key is not found in the `MemTable`, it means the key is not present or has been deleted.

3. **Deletion of key-value entries**: The `MemTable` supports the deletion of key-value entries. When a deletion operation is performed, a special marker or tombstone is added to indicate that the key is deleted. This allows for efficient handling of deletions and ensures that the correct value is returned when performing lookups.

4. **Flush to disk**: Periodically, when the `MemTable` reaches a certain size or based on a predefined policy, it is necessary to flush the contents of the `MemTable` to disk. This process involves writing the key-value pairs (including tombstones) to a disk-based storage structure, such as an SSTable (Sorted String Table), which provides efficient read operations.

It's important to note that the `MemTable` is a volatile data structure that resides in memory, and its contents are not durable. To ensure durability and crash recovery, the data in the `MemTable` is periodically flushed to disk and also logged in a write-ahead log (WAL).

Overall, the `MemTable` plays a crucial role in the LSM Tree storage engine by providing fast write operations and buffering data in memory before flushing it to disk for persistence.


### Write-Ahead Log (WAL)

The Write-Ahead Log (WAL) is a persistent storage mechanism used in conjunction with the MemTable. It is an append-only log file that records all the write operations before they are applied to the MemTable. The WAL ensures durability by persistently storing all modifications to the database.

When a write operation is performed, the corresponding key-value pair modification is first written to the WAL. This ensures that the modification is durably stored on disk before being applied to the MemTable. Once the write is confirmed in the WAL, the modification is applied to the MemTable in memory.

The WAL provides crash recovery capabilities for the LSM Tree. In the event of a system crash or restart, the LSM Tree can replay the write operations recorded in the WAL to restore the MemTable to its last consistent state. This guarantees that no data is lost or corrupted during system failures.

Additionally, the WAL can also improve the performance of read operations. By storing the modifications sequentially in the WAL, disk writes can be performed more efficiently, reducing the impact of random disk access and improving overall throughput.

#### Role of WAL

The WAL (Write-Ahead Log) is a fundamental component of many database systems, including those that use the LSM Tree storage engine. It is a mechanism used to ensure durability and crash recovery by providing a reliable log of write operations before they are applied to the main data storage.

Here's how the WAL works and what it does:

1. **Logging write operations**: Whenever a write operation (insertion, update, deletion) is performed on the database, the WAL captures the details of the operation in a sequential log file. This log entry includes the necessary information to reconstruct the write operation, such as the affected data item, the type of operation, and any associated metadata.

2. **Write ordering**: The WAL enforces a "write-ahead" policy, meaning that the log entry must be written to disk before the corresponding data modification is applied to the main data storage. This ordering ensures that the log contains a reliable record of all changes before they are committed.

3. **Durability guarantee**: By writing the log entry to disk before the actual data modification, the WAL provides durability guarantees. Even if the system crashes or experiences a power failure, the log entries are preserved on disk. Upon recovery, the system can use the log to replay and apply the previously logged operations to restore the database to a consistent state.

4. **Crash recovery**: During system recovery after a crash, the WAL is consulted to bring the database back to a consistent state. The system replays the log entries that were not yet applied to the main data storage before the crash. This process ensures that all the write operations that were logged but not yet persisted are correctly applied to the database.

5. **Log compaction**: Over time, the WAL log file can grow in size, which may impact the performance of the system. To address this, log compaction techniques can be applied to periodically consolidate and remove unnecessary log entries. This process involves creating a new log file by compacting the existing log and discarding the obsolete log entries.

The use of a WAL provides several benefits for database systems, including improved durability, crash recovery, and efficient write performance. It ensures that modifications to the database are reliably captured and persisted before being applied to the main data storage, allowing for consistent and recoverable operations even in the event of failures.

### SSTables (Sorted String Tables)

SSTables are disk-based storage structures that store sorted key-value pairs. When the MemTable is flushed to disk, it is typically written as an SSTable file. Multiple SSTables may exist on disk, each representing a previous MemTable state. SSTables are immutable, which simplifies compaction and allows for efficient read operations.

### Compaction

Compaction is the process of merging and organizing multiple SSTables to improve read performance and reclaim disk space. It involves merging overlapping key ranges from different SSTables and eliminating duplicate keys. Compaction reduces the number of disk seeks required for read operations and helps maintain an efficient data structure.

### Bloom Filters

Bloom filters are probabilistic data structures that provide efficient and fast membership tests. They are used to reduce the number of disk accesses during read operations by quickly determining whether a specific key is present in an SSTable. Bloom filters can help improve read performance by reducing unnecessary disk reads.
