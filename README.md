# LSM Tree Storage Engine

This project is a storage engine implementation that combines a memory table and a write-ahead log (WAL) for data storage and retrieval. It provides a mechanism to store key-value pairs and supports operations such as setting a key-value pair, retrieving a value based on a key, and deleting a key-value pair.

- The LSM (Log-Structured Merge) Tree is a disk-based data structure optimized for read and write operations in a storage engine.
- It consists of two main components: MemTable and Write-Ahead Log (WAL).
The MemTable is an in-memory data structure that stores recently written data. It provides fast write performance.
- The WAL is a sequential write-ahead log that records all modifications before they are written to the MemTable. It ensures durability and crash recovery.
- The data in the MemTable is periodically flushed to disk as a new SSTable (Sorted String Table) when it reaches a certain size or threshold.
- SSTables are immutable and are stored on disk in sorted order. They are merged during compaction to create new compacted SSTables, reducing storage space and improving read performance.
- During reads, the LSM Tree performs a multi-level lookup, searching for the most recent value by checking the MemTable and SSTables in a sequential manner.

## Components

The project consists of the following components:

### 1. Memory Table

The memory table represents the in-memory storage for key-value pairs. It is implemented as a vector of `MemTableEntry` structs. Each `MemTableEntry` contains information such as the key, value (optional), timestamp, and deletion status. The memory table supports operations to set a new entry, get an entry based on a key, and delete an entry.

### 2. Write-Ahead Log (WAL)

The write-ahead log is an append-only file that records the operations performed on the memory table. It serves as a recovery mechanism for the memory table in case of server shutdown or data loss. The WAL is implemented as a separate file and provides methods to set a key-value pair and delete a key-value pair, which are then appended to the log.

In both the `set` and `delete` methods of the `Wal` struct, the key, value (in the case of `set`), and timestamp are written to the WAL file using the `write_all` method of the `BufWriter<File>`. The key and value are serialized as bytes before writing.

The WAL file is stored on disk in the specified directory path (`dir_path`) provided during the initialization of the `StorageEngine`. The WAL file is created in the directory with a `.wal` extension, and subsequent operations are appended to the same file.

### 3. Storage Engine

The storage engine combines the memory table and the write-ahead log to provide a complete storage solution. It manages the interaction between the memory table and the WAL, as well as provides methods to set a key-value pair, retrieve a value based on a key, and initialize the engine from an existing directory. The storage engine is the main entry point for interacting with the storage system.

### 4. Helper Functions

The project includes a set of helper functions used by the storage engine and WAL. These functions include generating timestamps and retrieving files with a specific extension from a directory.

## Usage

To use the storage engine, follow these steps:

1. Create an instance of the `StorageEngine` struct, providing the directory path where the storage files are located.
2. Use the `get` method to retrieve a value based on a key.
3. Use the `set` method to set a new key-value pair.
4. Optionally, use the `delete` method to delete a key-value pair.

## Future Improvements

Possible future improvements for this project could include:
- Implementing a skip list data structure for the memory table to improve performance.
- Adding support for additional operations such as range queries.
- Implementing a compaction mechanism to optimize storage space.
- Enhancing error handling and adding proper logging.

---

## An Ideal LSM Tree Storage Engine

The theoretical parts of the LSM Tree Storage Engine
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

---

### Here are a few additional components commonly found in LSM Tree implementations:

1. **Immutable MemTables**: Rather than directly flushing the MemTable to disk when it becomes full, LSM Trees often employ a mechanism where the MemTable is made immutable and a new empty MemTable is created. The immutable MemTable is then flushed to disk as an SSTable. This approach allows for concurrent write operations on a new MemTable while the previous MemTable is being flushed, improving write performance.

2. **SSTables (Sorted String Tables)**: SSTables are disk-based storage structures that store sorted key-value pairs. When the MemTable is flushed to disk, it is typically written as an SSTable file. Multiple SSTables may exist on disk, each representing a previous MemTable state. SSTables are immutable, which simplifies compaction and allows for efficient read operations.

3. **Compaction**: Compaction is the process of merging and organizing multiple SSTables to improve read performance and reclaim disk space. It involves merging overlapping key ranges from different SSTables and eliminating duplicate keys. Compaction reduces the number of disk seeks required for read operations and helps maintain an efficient data structure.

4. **Bloom Filters**: Bloom filters are probabilistic data structures that provide efficient and fast membership tests. They are used to reduce the number of disk accesses during read operations by quickly determining whether a specific key is present in an SSTable. Bloom filters can help improve read performance by reducing unnecessary disk reads.

5. **Tiered Storage**: LSM Trees can utilize a tiered storage approach by using different storage media, such as solid-state drives (SSDs) for faster access and hard disk drives (HDDs) for larger capacity. This approach leverages the strengths of each storage medium to balance performance and cost.

It's important to note that the implementation details of a LSM Tree storage engine can vary depending on specific requirements and design choices. The components mentioned above are common in LSM Trees, but different implementations may have variations or additional optimizations. Therefore, it's recommended to study existing LSM Tree implementations or consult relevant literature for a more comprehensive understanding and to ensure the correctness and efficiency of your specific implementation.

---
