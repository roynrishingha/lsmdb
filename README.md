# lsmdb

[<img alt="github" src="https://img.shields.io/badge/github-roynrishingha/lsmdb-8da0cb?style=for-the-badge&labelColor=555555&logo=github" height="20">](https://github.com/roynrishingha/lsmdb)
[<img alt="crates.io" src="https://img.shields.io/crates/v/lsmdb.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/lsmdb)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-lsmdb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/lsmdb)

lsmdb is an efficient storage engine that implements the Log-Structured Merge Tree (LSM-Tree) data structure, designed specifically for handling key-value pairs.

It combines an in-memory component called MemTable and an on-disk component represented by the Write-Ahead Log (WAL) and SSTables (Sorted String Tables).

## APIs

The `StorageEngine` acts as the public API for interacting with the LSM Tree. It provides high-level functions for inserting, retrieving, updating, and removing key-value pairs. The Storage Engine has the following APIs:

- **`build(dir)`**: Initializes the Storage Engine by creating the necessary directories and files. If a WAL file exists with contents, it recovers the MemTable from the WAL.
- **`insert(key, value)`**: Inserts a key-value pair into the Storage Engine. It writes the entry to the WAL and updates the MemTable.
- **`get(key)`**: Retrieves the value associated with a given key from the MemTable.
- **`remove(key)`**: Removes the key-value entry of a given key from the MemTable. It writes a remove entry to the WAL if the key exists.
- **`update(key, value)`**: Updates the value associated with a given key. It first removes the existing entry and then inserts the updated key-value pair.
- **`clear()`**: Clears the MemTable and WAL, effectively resetting the Storage Engine.

## Components

### MemTable

The MemTable is an in-memory data structure that stores recently written data before it is flushed to disk. It uses a BTreeMap to store key-value pairs in sorted order. The MemTable has the following main functions:

### Write-Ahead Log(WAL)

The Write-Ahead Log (WAL) is a persistent log that ensures durability and crash recovery. It stores the changes made to the MemTable before they are flushed to disk. The WAL has the following main functions:


## Workflow

The typical workflow of the LSM Tree Storage Engine can be summarized as follows:

1. During initialization, the Storage Engine checks if a WAL file exists. If it does and has contents, it recovers the MemTable from the WAL.
2. Clients interact with the Storage Engine by calling the provided functions.
3. When a new key-value pair is inserted, it is first written to the WAL for durability. Then, it is added to the MemTable for fast in-memory access.
4. When the MemTable reaches its capacity or a flush is triggered, it is written to disk as an SSTable file. The MemTable is then cleared, ready to store new entries.
5. During the recovery process, the entries in the WAL are replayed to reconstruct the MemTable.
6. When a key-value pair is retrieved, the MemTable is searched first. If the key is not found in the MemTable, the SSTables are checked sequentially to find the key-value pair.
7. When a key-value pair is removed, it is first removed from the MemTable. If the key exists in the MemTable, a remove entry is written to the WAL for durability.
8. Periodically, the SSTables are compacted to merge and eliminate redundant data, improving read efficiency and reducing storage space.

See the [Architecture](Architecture.md) for in depth information.
