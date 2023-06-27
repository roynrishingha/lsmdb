//! # `lsmdb`
//!
//! `lsmdb` is an efficient storage engine that implements the Log-Structured Merge Tree (LSM-Tree) data structure, designed specifically for handling key-value pairs.
//!
//! ## Public API
//!
//! The Public API of `lsmdb` crate.
//!
//! ### `StorageEngine`
//!
//! The `StorageEngine` struct represents the main component of the LSM Tree storage engine. It consists of the following fields:
//!
//! - `memtable`: An instance of the `MemTable` struct that serves as an in-memory table for storing key-value pairs. It provides efficient write operations.
//! - `wal`: An instance of the `WriteAheadLog` struct that handles write-ahead logging. It ensures durability by persistently storing write operations before they are applied to the memtable and SSTables.
//! - `sstables`: A vector of `SSTable` instances, which are on-disk sorted string tables storing key-value pairs. The SSTables are organized in levels, where each level contains larger and more compacted tables.
//! - `dir`: An instance of the `DirPath` struct that holds the directory paths for the root directory, write-ahead log directory, and SSTable directory.
//!
//! The `StorageEngine` struct provides methods for interacting with the storage engine:
//!
//! - `new`: Creates a new instance of the `StorageEngine` struct. It initializes the memtable, write-ahead log, and SSTables.
//! - `put`: Inserts a new key-value pair into the storage engine. It writes the key-value entry to the memtable and the write-ahead log. If the memtable reaches its capacity, it is flushed to an SSTable.
//! - `get`: Retrieves the value associated with a given key from the storage engine. It first searches in the memtable, which has the most recent data. If the key is not found in the memtable, it searches in the SSTables, starting from the newest levels and moving to the older ones.
//! - `remove`: Removes a key-value pair from the storage engine. It first checks if the key exists in the memtable. If not, it searches for the key in the SSTables and removes it from there. The removal operation is also logged in the write-ahead log for durability.
//! - `update`: Updates the value associated with a given key in the storage engine. It first removes the existing key-value pair using the `remove` method and then inserts the updated pair using the `put` method.
//! - `clear`: Clears the storage engine by deleting the memtable and write-ahead log. It creates a new instance of the storage engine, ready to be used again.
//!
//! ### DirPath
//! The `DirPath` struct represents the directory paths used by the storage engine. It consists of the following fields:
//!
//! - `root`: A `PathBuf` representing the root directory path, which serves as the parent directory for the write-ahead log and SSTable directories.
//! - `wal`: A `PathBuf` representing the write-ahead log directory path, where the write-ahead log file is stored.
//! - `sst`: A `PathBuf` representing the SSTable directory path, where the SSTable files are stored.
//!
//! The `DirPath` struct provides methods for building and retrieving the directory paths.
//!
//! ### SizeUnit
//!
//! The `SizeUnit` enum represents the unit of measurement for capacity and size. It includes the following variants:
//!
//! - `Bytes`: Represents the byte unit.
//! - `Kilobytes`: Represents the kilobyte unit.
//! - `Megabytes`: Represents the megabyte unit.
//! - `Gigabytes`: Represents the gigabyte unit.
//!
//! The `SizeUnit` enum provides a method `to_bytes` for converting a given value to bytes based on the selected unit.
//!
//! ### Helper Functions
//! The code includes several helper functions:
//!
//! - `with_capacity`: A helper function that creates a new instance of the `StorageEngine` struct with a specified capacity for the memtable.
//! - `with_capacity_and_rate`: A helper function
//!
//!  that creates a new instance of the `StorageEngine` struct with a specified capacity for the memtable and a compaction rate for the SSTables.
//! - `flush_memtable`: A helper function that flushes the contents of the memtable to an SSTable. It creates a new SSTable file and writes the key-value pairs from the memtable into it. After flushing, the memtable is cleared.
//! - `recover_memtable`: A helper function that recovers the contents of the memtable from the write-ahead log during initialization. It reads the logged write operations from the write-ahead log and applies them to the memtable.
//!
//! These helper functions assist in initializing the storage engine, flushing the memtable to an SSTable when it reaches its capacity, and recovering the memtable from the write-ahead log during initialization, ensuring durability and maintaining data consistency.
//!
//! ---
//!
//! ## MemTable
//!
//! The `MemTable` (short for memory table) is an in-memory data structure that stores recently written data before it is flushed to disk. It serves as a write buffer and provides fast write operations.
//!
//! ### Dependencies
//!
//! The implementation requires the following dependencies to be imported from the standard library:
//!
//! - **`std::collections::BTreeMap`**: A balanced binary search tree implementation that stores key-value pairs in sorted order.
//! - **`std::io`**: Provides input/output functionality, including error handling.
//! - **`std::sync::{Arc, Mutex}`**: Provides thread-safe shared ownership(`Arc`) and mutual exclusion (`Mutex`) for concurrent access to data.
//!
//! ### Constants
//!
//! The implementation defines the following constants:
//!
//! #### `DEFAULT_MEMTABLE_CAPACITY`
//!
//! Represents the default maximum size of the MemTable. By default, it is set to 1 gigabyte (1GB).
//! ```rs
//! pub(crate) static DEFAULT_MEMTABLE_CAPACITY: usize = SizeUnit::Gigabytes.to_bytes(1);
//! ```
//!
//! #### `DEFAULT_FALSE_POSITIVE_RATE`
//!
//! Represents the default false positive rate for the Bloom filter used in the `MemTable`. By default, it is set to 0.0001 (0.01%).
//!
//! ```rs
//! pub(crate) static DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.0001;
//! ```
//!
//! ### Structure
//!
//! The **`MemTable`** structure represents the in-memory data structure and contains the following fields:
//!
//! ```rs
//! pub(crate) struct MemTable {
//!     entries: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
//!     entry_count: usize,
//!     size: usize,
//!     capacity: usize,
//!     bloom_filter: BloomFilter,
//!     size_unit: SizeUnit,
//!     false_positive_rate: f64,
//! }
//! ```
//!
//! #### `entries`
//!
//! The entries field is of type `Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>`. It holds the key-value pairs of the `MemTable` in sorted order using a `BTreeMap`. The `Arc` (Atomic Reference Counting) and `Mutex` types allow for concurrent access and modification of the `entries` data structure from multiple threads, ensuring thread safety.
//!
//! #### `entry_count`
//!
//! The `entry_count` field is of type `usize` and represents the number of key-value entries currently stored in the `MemTable`.
//!
//! #### `size`
//!
//! The `size` field is of type `usize` and represents the current size of the `MemTable` in bytes. It is updated whenever a new key-value pair is added or removed.
//!
//! #### `capacity`
//!
//! The `capacity` field is of type `usize` and represents the maximum allowed size for the `MemTable` in bytes. It is used to enforce size limits and trigger flush operations when the `MemTable` exceeds this capacity.
//!
//! #### `bloom_filter`
//!
//! The `bloom_filter` field is of type `BloomFilter` and is used to probabilistically determine whether a `key` may exist in the `MemTable` without accessing the `entries` map. It helps improve performance by reducing unnecessary lookups in the map.
//!
//! #### `size_unit`
//!
//! The `size_unit` field is of type `SizeUnit` and represents the unit of measurement used for `capacity` and `size` calculations. It allows for flexibility in specifying the capacity and size of the `MemTable` in different units (e.g., bytes, kilobytes, megabytes, etc.).
//!
//! #### `false_positive_rate`
//!
//! The `false_positive_rate` field is of type `f64` and represents the desired false positive rate for the bloom filter. It determines the trade-off between memory usage and the accuracy of the bloom filter.
//!
//! ### Constructor Methods
//!
//! #### `new`
//!
//! ```rs
//! pub(crate) fn new() -> Self
//! ```
//!
//! The `new` method creates a new `MemTable` instance with the default capacity. It internally calls the `with_capacity_and_rate` method, passing the default capacity and false positive rate.
//!
//! #### `with_capacity_and_rate`
//!
//! ```rs
//! pub(crate) fn with_capacity_and_rate(
//!     size_unit: SizeUnit,
//!     capacity: usize,
//!     false_positive_rate: f64,
//! ) -> Self
//! ```
//!
//! The `with_capacity_and_rate` method creates a new `MemTable` with the specified capacity, size unit, and false positive rate. It initializes the `entries` field as an empty `BTreeMap`, sets the `entry_count` and `size` to zero, and creates a new `BloomFilter` with the given capacity and false positive rate. The capacity is converted to bytes based on the specified size unit.
//!
//! ### Public Methods
//!
//! #### `set`
//!
//! ```rs
//! pub(crate) fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()>
//! ```
//!
//! The `set` method inserts a new key-value pair into the `MemTable`. It first acquires a lock on the `entries` field to ensure thread-safety. If the key is not present in the `BloomFilter`, it adds the key-value pair to the `entries` map, updates the `entry_count` and `size`, and sets the key in the `BloomFilter`. If the key already exists, an `AlreadyExists` error is returned.
//!
//! #### `get`
//!
//! ```sh
//! pub(crate) fn get(&self, key: Vec<u8>) -> io::Result<Option<Vec<u8>>>
//! ```
//!
//! The `get` method retrieves the value associated with a given key from the `MemTable`. It first checks if the key is present in the `BloomFilter`. If it is, it acquires a lock on the `entries` field and returns the associated value. If the key is not present in the `BloomFilter`, it returns `None`.
//!
//! #### `remove`
//!
//! ```sh
//! pub(crate) fn remove(&mut self, key: Vec<u8>) -> io::Result<Option<(Vec<u8>, Vec<u8>)>>
//! ```
//!
//! The `remove` method removes a key-value pair from the `MemTable` based on a given key. It first checks if the key is present in the `BloomFilter`. If it is, it acquires a lock on the `entries` field and removes the key-value pair from the `entries` map. It updates the `entry_count` and `size` accordingly and returns the removed key-value pair as a tuple. If the key is not present in the `BloomFilter`, it returns `None`.
//!
//! #### `clear`
//!
//! ```rs
//! pub(crate) fn clear(&mut self) -> io::Result<()>
//! ```
//!
//! The `clear` method removes all key-value entries from the `MemTable`. It acquires a lock on the `entries` field, clears the `entries` map, and sets the `entry_count` and `size` fields to zero.
//!
//! #### `entries`
//!
//! ```rs
//! pub(crate) fn entries(&self) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>>
//! ```
//!
//! The `entries` method returns a vector of all key-value pairs in the `MemTable`. It acquires a lock on the `entries` field and iterates over the key-value pairs in the `entries` map. It clones each key-value pair and collects them into a vector, which is then returned.
//!
//! ### Internal Method
//!
//! #### `capacity`
//!
//! ```rs
//! pub(crate) fn capacity(&self) -> usize
//! ```
//!
//! The `capacity` method returns the capacity of the `MemTable` in bytes.
//!
//! #### `size`
//!
//! ```rs
//! pub(crate) fn size(&self) -> usize
//! ```
//!
//! The `size` method returns the current size of the `MemTable` in the specified size unit. It divides the internal `size` by the number of bytes in one unit of the specified size unit.
//!
//! #### `false_positive_rate`
//!
//! ```rs
//! pub(crate) fn false_positive_rate(&self) -> f64
//! ```
//!
//! The `false_positive_rate` method returns the false positive rate of the `MemTable`.
//!
//! #### `size_unit`
//!
//! ```rs
//! pub(crate) fn size_unit(&self) -> SizeUnit
//! ```
//!
//! The `size_unit` method returns the size unit used by the `MemTable`.
//!
//! ### Error Handling
//!
//! All the methods that involve acquiring a lock on the `entries` field use the `io::Error` type to handle potential errors when obtaining the lock. If an error occurs during the locking process, an `io::Error` instance is created with a corresponding error message.
//!
//! ### Thread Safety
//!
//! The `MemTable` implementation ensures thread safety by using an `Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>` for storing the key-value entries. The `Arc` allows multiple ownership of the `entries` map across threads, and the `Mutex` ensures exclusive access to the map during modification operations, preventing data races.
//!
//! The locking mechanism employed by the `Mutex` guarantees that only one thread can modify the `entries` map at a time, while allowing multiple threads to read from it simultaneously.
//!
//! ---
//!
//! ## Bloom Filter
//!
//! The Bloom Filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set. It provides a fast and memory-efficient way to check for set membership, but it introduces a small probability of false positives.
//!
//! The Bloom Filter implementation is provided as a Rust module and consists of a struct called `BloomFilter`. It uses a `BitVec` to represent the array of bits that make up the filter. The number of hash functions used by the Bloom Filter is configurable, and it keeps track of the number of elements inserted into the filter.
//!
//! ### Dependencies
//!
//! - **`std::collections::hash_map::DefaultHasher`**: Provides the default hasher implementation used for calculating hash values of keys.
//! - **`std::hash::{Hash, Hasher}`**: Defines the Hash trait used for hashing keys.
//! - **`std::sync::{Arc, Mutex}`**: Provides thread-safe shared ownership (Arc) and mutual exclusion (Mutex) for concurrent access to data.
//! - **`bit_vec::BitVec`**: Implements a bit vector data structure used to store the Bloom filter's bit array.
//!
//! ### Structure
//!
//! The BloomFilter struct represents the Bloom Filter data structure and contains the following fields:
//!
//! ```rs
//! pub(crate) struct BloomFilter {
//!     bits: Arc<Mutex<BitVec>>,
//!     num_hashes: usize,
//!     num_elements: AtomicUsize,
//! }
//! ```
//!
//! #### `bits`
//!
//! An `Arc<Mutex<BitVec>>` representing the array of bits used to store the Bloom filter.
//!
//! #### `num_hashes`
//!
//! The number of hash functions used by the Bloom filter.
//!
//! #### `num_elements`
//!
//! An `AtomicUsize` representing the number of elements inserted into the Bloom filter.
//!
//! ### Constructor Methods
//!
//! #### `new`
//!
//! ```rs
//! fn new(num_elements: usize, false_positive_rate: f64) -> Self
//! ```
//!
//! The `new` method creates a new `BloomFilter` with the specified number of elements and false positive rate. It initializes the Bloom filter's bit array, calculates the number of hash functions, and sets the initial number of elements to zero.
//!
//! ### Public Methods
//!
//! #### `set`
//!
//! ```rs
//! fn set<T: Hash>(&mut self, key: &T)```
//!
//! The `set` method inserts a key into the Bloom filter. It calculates the hash value for the key using multiple hash functions and sets the corresponding bits in the bit array to true. It also increments the element count.
//!
//! #### `contains`
//!
//! ```rs
//! fn contains<T: Hash>(&self, key: &T) -> bool
//! ```
//!
//! The `contains` method checks if a key is possibly present in the Bloom filter.
//! It calculates the hash value for the key using multiple hash functions and checks the corresponding bits in the bit array.
//! If any of the bits are false, it indicates that the key is definitely not present, and the method returns false.
//! If all bits are true, the method returns true, indicating that the key is possibly present.
//!
//! #### `num_elements`
//!
//! ```rs
//! fn num_elements(&self) -> usize
//! ```
//!
//! This method returns the current number of elements inserted into the Bloom filter.
//!
//! ### Internal Method
//!
//! #### `calculate_hash`
//!
//! ```rs
//! fn calculate_hash<T: Hash>(&self, key: &T, seed: usize) -> u64
//! ```
//!
//! This function calculates a hash value for a given key and seed. It uses a suitable hash function to hash the key and incorporates the seed value for introducing randomness.
//!
//! #### `calculate_num_bits`
//!
//! ```rs
//! fn calculate_num_bits(num_elements: usize, false_positive_rate: f64) -> usize
//! ```
//!
//! This function calculates the optimal number of bits for the Bloom filter based on the desired false positive rate and the expected number of elements. It uses a formula to estimate the number of bits required.
//!
//! #### `calculate_num_hashes`
//!
//! ```rs
//! fn calculate_num_hashes(num_bits: usize, num_elements: usize) -> usize
//! ```
//!
//! This function calculates the optimal number of hash functions for the Bloom filter based on the number of bits and the expected number of elements. It uses a formula to estimate the number of hash functions required.
//!
//! ---
//!
//! ## Write-Ahead Log (WAL)
//!
//! The Sequential Write-Ahead Log (WAL) is a crucial component of the LSM Tree storage engine.
//! It provides durability and atomicity guarantees by logging write operations before they are applied to the main data structure.
//!
//! When a write operation is received, the key-value pair is first appended to the WAL.
//! In the event of a crash or system failure, the WAL can be replayed to recover the data modifications and bring the MemTable back to a consistent state.
//!
//! ### Dependencies
//!
//! The implementation requires the following dependencies to be imported from the standard library:
//!
//! - **`std::fs`**: Provides file system-related operations.
//! - **`std::io`**: Provides input/output functionality, including error handling.
//! - **`std::path::PathBuf`**: Represents file system paths.
//! - **`std::sync::{Arc, Mutex}`**: Provides thread-safe shared ownership and synchronization.
//!
//! ### WriteAheadLog Structure
//!
//! The `WriteAheadLog` structure represents the write-ahead log (WAL) and contains the following field:
//!
//! ```rs
//! struct WriteAheadLog {
//!     log_file: Arc<Mutex<File>>,
//! }
//! ```
//!
//! #### log_file
//!
//! The `log_file` field is of type `Arc<Mutex<File>>`. It represents the WAL file and provides concurrent access and modification through the use of an `Arc` (Atomic Reference Counting) and `Mutex`.
//!
//! ### Log File Structure Diagram
//!
//! The `log_file` is structured as follows:
//!
//! ```sh
//! +-------------------+
//! |  Entry Length     |   (4 bytes)
//! +-------------------+
//! |   Entry Kind      |   (1 byte)
//! +-------------------+
//! |   Key Length      |   (4 bytes)
//! +-------------------+
//! |  Value Length     |   (4 bytes)
//! +-------------------+
//! |       Key         |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |      Value        |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |  Entry Length     |   (4 bytes)
//! +-------------------+
//! |   Entry Kind      |   (1 byte)
//! +-------------------+
//! |   Key Length      |   (4 bytes)
//! +-------------------+
//! |  Value Length     |   (4 bytes)
//! +-------------------+
//! |       Key         |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! |      Value        |   (variable)
//! |                   |
//! |                   |
//! +-------------------+
//! ```
//!
//! - **Entry Length**: A 4-byte field representing the total length of the entry in bytes.
//! - **Entry Kind**: A 1-byte field indicating the type of entry (Insert or Remove).
//! - **Key Length**: A 4-byte field representing the length of the key in bytes.
//! - **Key**: The actual key data, which can vary in size.
//! - **Value** Length: A 4-byte field representing the length of the value in bytes.
//! - **Value**: The actual value data, which can also vary in size.
//!
//! Each entry is written sequentially into the `log_file` using the `write_all` method, ensuring that the entries are stored contiguously. New entries are appended to the end of the `log_file` after the existing entries.
//!
//! ### Constants
//!
//! A constant named `WAL_FILE_NAME` is defined, representing the name of the WAL file.
//!
//! ```rs
//! static WAL_FILE_NAME: &str = "lsmdb_wal.bin";
//! ```
//!
//! ### `EntryKind`
//!
//! ```rs
//! enum EntryKind {
//!     Insert = 1,
//!     Remove = 2,
//! }
//! ```
//!
//! The `EntryKind` enum represents the kind of entry stored in the WAL. It has two variants: `Insert` and `Remove`. Each variant is associated with an integer value used for serialization.
//!
//! ### `WriteAheadLogEntry`
//!
//! ```rs
//! struct WriteAheadLogEntry {
//!     entry_kind: EntryKind,
//!     key: Vec<u8>,
//!     value: Vec<u8>,
//! }
//! ```
//!
//! The `WriteAheadLogEntry` represents a single entry in the Write-Ahead Log. It contains the following fields:
//!
//! - **`entry_kind`**: An enumeration (`EntryKind`) representing the type of the entry (insert or remove).
//! - **`key`**: A vector of bytes (`Vec<u8>`) representing the key associated with the entry.
//! - **`value`**: A vector of bytes (`Vec<u8>`) representing the value associated with the entry.
//!
//! ### `WriteAheadLogEntry` Methods
//!
//! #### `new`
//!
//! ```rs
//! fn new(entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8>) -> Self
//! ```
//!
//! The `new` method creates a new instance of the `WriteAheadLogEntry` struct. It takes the `entry_kind`, `key`, and `value` as parameters and initializes the corresponding fields.
//!
//! #### `serialize`
//!
//! ```rs
//! fn serialize(&self) -> Vec<u8>
//! ```
//!
//! The `serialize` method serializes the `WriteAheadLogEntry` into a vector of bytes.
//! It calculates the length of the entry, then serializes the length, entry kind, key length, value length, key, and value into the vector. The serialized data is returned.
//!
//! #### `deserialize`
//!
//! ```rs
//! fn deserialize(serialized_data: &[u8]) -> io::Result<Self>
//! ```
//!
//! This method deserializes a `WriteAheadLogEntry` from the provided serialized data.
//! It performs validation checks on the length and structure of the serialized data and returns an `io::Result` containing the deserialized entry if successful.
//!
//! ### `WriteAheadLog` Methods
//!
//! #### `new`
//!
//! ```rs
//! fn new(directory_path: &PathBuf) -> io::Result<Self>
//! ```
//!
// The `new` method is a constructor function that creates a new `WriteAheadLog` instance.
// It takes a `directory_path` parameter as a `PathBuf` representing the directory path where the WAL file will be stored.
//!
//! If the directory doesn't exist, it creates it. It then opens the log file with read, append, and create options, and initializes the log_file field.
//!
//! #### `append`
//!
//! ```rs
//! fn append(&mut self, entry_kind: EntryKind, key: Vec<u8>, value: Vec<u8> ) -> io::Result<()>
//! ```
//!
//! The `append` method appends a new entry to the Write-Ahead Log.
//! It takes an `entry_kind` parameter of type `EntryKind`, a `key` parameter of type `Vec<u8>`, and a `value` parameter of type `Vec<u8>`. The method acquires a lock on the `log_file` to ensure mutual exclusion when writing to the file.
//!
//! It creates a `WriteAheadLogEntry` with the provided parameters, serializes it, and writes the serialized data to the log file.
//!
//! Finally, it flushes the log file to ensure the data is persisted. If the operation succeeds, `Ok(())` is returned; otherwise, an `io::Error` instance is created and returned.
//!
//! #### `recover`
//!
//! ```rs
//! fn recover(&mut self) -> io::Result<Vec<WriteAheadLogEntry>>
//! ```
//!
//! The `recover` method reads and recovers the entries from the Write-Ahead Log. The method acquires a lock on the `log_file` to ensure exclusive access during the recovery process.
//!
//! It reads the log file and deserializes the data into a vector of `WriteAheadLogEntry` instances.
//! It continues reading and deserializing until the end of the log file is reached. The recovered entries are returned as a vector.
//!
//! #### `clear`
//!
//! ```rs
//! fn clear(&mut self) -> io::Result<()>
//! ```
//!
//! The `clear` method clears the contents of the WAL file. It acquires a lock on the `log_file` to ensure exclusive access when truncating and seeking the file.
//! The method sets the length of the file to `0` using the `set_len` method, effectively truncating it. Then, it seeks to the start of the file using `seek` with `SeekFrom::Start(0)` to reset the file pointer.
//! If the operation succeeds, `Ok(())` is returned; otherwise, an `io::Error` instance is created and returned.
//!
//! ### Thread Safety
//!
//! The `WriteAheadLog` implementation ensures thread safety by using an `Arc<Mutex<File>>` for the `log_file` field. The `Arc` allows multiple ownership of the WAL file across threads, and the `Mutex` ensures exclusive access to the file during write, recovery, and clear operations, preventing data races.
//!
//! The locking mechanism employed by the `Mutex` guarantees that only one thread can modify the WAL file at a time, while allowing multiple threads to read from it simultaneously.
//!
//! ---
//!
//! ## SSTable (Sorted String Table)
//!
//! An SSTable, or Sorted String Table, is an immutable on-disk data structure that stores key-value pairs in a sorted order.
//! It serves as the persistent storage layer for the LSM Tree-based engine.
//! SSTables are typically stored as multiple files, each containing a sorted range of key-value pairs.
//!
//! When the MemTable reaches a certain threshold size, it is "flushed" to disk as a new SSTable file.
//! The MemTable is atomically replaced with an empty one, allowing new write operations to continue. This process is known as a "memtable flush."
//!
//! ```rs
//! +-----------------------+
//! |       SSTable         |
//! +-----------------------+
//! |  - file_path          |   (PathBuf)
//! |  - blocks             |   (Vec<Block>)
//! |  - created_at         |   (DateTime<Utc>)
//! +-----------------------+
//! |  + new(dir: PathBuf)  |   -> SSTable
//! |  + set(key, value)    |   -> Result<(), io::Error>
//! |  + get(key)           |   -> Option<Vec<u8>>
//! |  + remove(key)        |   -> Result<(), io::Error>
//! +-----------------------+
//!
//! +-----------------------+
//! |        Block          |
//! +-----------------------+
//! |  - data               |   (Vec<u8>)
//! |  - index              |   (HashMap<Arc<Vec<u8>>, usize>)
//! |  - entry_count        |   (usize)
//! +-----------------------+
//! |  + new()              |   -> Block
//! |  + is_full(size)      |   -> bool
//! |  + set_entry(key, value) | -> Result<(), io::Error>
//! |  + remove_entry(key)  |   -> bool
//! |  + get_value(key)     |   -> Option<Vec<u8>>
//! |  + entry_count()      |   -> usize
//! +-----------------------+
//! ```
//!
//! The `SSTable` struct represents the Sorted String Table and contains the following fields:
//! - `file_path`: Stores the path of the SSTable file (PathBuf).
//! - `blocks`: Represents a collection of blocks that hold the data (`Vec<Block>`).
//! - `created_at`: Indicates the creation timestamp of the SSTable (`DateTime<Utc>`).
//!
//! The `SSTable` struct provides the following methods:
//!
//! - `new(dir: PathBuf) -> SSTable`: Creates a new instance of the `SSTable` struct given a directory path and initializes its fields. Returns the created `SSTable`.
//!
//! - `set(key: Vec<u8>, value: Vec<u8>) -> Result<(), io::Error>`: Sets an entry with the provided key and value in the `SSTable`. It internally manages the blocks and their capacity to store entries. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! - `get(key: Vec<u8>) -> Option<Vec<u8>>`: Retrieves the value associated with the provided key from the `SSTable`. It iterates over the blocks to find the key-value pair. Returns `Option<Vec<u8>>` with the value if found, or `None` if the key is not present.
//!
//! - `remove(key: Vec<u8>) -> Result<(), io::Error>`: Removes the entry with the provided key from the `SSTable`. It iterates over the blocks in reverse order to delete from the most recent block first. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! The `Block` struct represents an individual block within the SSTable and contains the following fields:
//!
//! - `data`: Stores the data entries within the block (`Vec<u8>`).
//! - `index`: Maintains an index for efficient key-based lookups (`HashMap<Arc<Vec<u8>>, usize>`).
//! - `entry_count`: Tracks the number of entries in the block (`usize`).
//!
//! The `Block` struct provides the following methods:
//!
//! - `new() -> Block`: Creates a new instance of the `Block` struct and initializes its fields. Returns the created `Block`.
//!
//! - `is_full(entry_size: usize) -> bool`: Checks if the block is full given the size of an entry. It compares the combined size of the existing data and the new entry size with the predefined block size. Returns `true` if the block is full, `false` otherwise.
//!
//! - `set_entry(key: &[u8], value: &[u8]) -> Result<(), io::Error>`: Sets an entry with the provided key and value in the block. It calculates the entry size, checks if the block has enough capacity, and adds the entry to the block's data and index. Returns `Result<(), io::Error>` indicating success or failure.
//!
//! - `remove_entry(key: &[u8]) -> bool`: Removes the entry with the provided key from the block. It searches for the key in the index, clears the entry in the data vector, and updates the entry count. Returns `true` if the entry was found and removed, `false` otherwise.
//!
//! - `get_value(key: &[u8]) -> Option<Vec<u8>>`: Retrieves the value associated with the provided key from the block. It looks up the key in the index, extracts the value bytes from the data vector, and returns them as a new `Vec<u8>`. Returns `Option<Vec<u8>>` with the value if found, or `None` if the key is not present.
//!
//! - `entry_count() -> usize`: Returns the number of entries in the block.
//!
//! Together, the `SSTable` and `Block` form the basic components of the SSTable implementation, providing efficient storage and retrieval of key-value pairs with support for adding and removing entries.
//!
//!
//! The `SSTable` manages multiple `Block` instances to store the data, and the `Block` handles individual block-level operations and indexing.
//!
//! A diagram illustrating how key-value pairs are stored inside a Block:
//!
//! ```rs
//! +----------------------------------+
//! |             Block                |
//! +----------------------------------+
//! |  - data: Vec<u8>                 |   // Data entries within the block
//! |  - index: HashMap<Arc<Vec<u8>>, usize> |   // Index for key-based lookups
//! |  - entry_count: usize             |   // Number of entries in the block
//! +----------------------------------+
//! |           Block Data              |
//! |   +------------------------+     |
//! |   |   Entry 1              |     |
//! |   | +-------------------+  |     |
//! |   | |   Length Prefix   |  |     |
//! |   | | (4 bytes, little- |  |     |
//! |   | |   endian format)  |  |     |
//! |   | +-------------------+  |     |
//! |   | |       Key         |  |     |
//! |   | | (variable length) |  |     |
//! |   | +-------------------+  |     |
//! |   | |      Value        |  |     |
//! |   | | (variable length) |  |     |
//! |   | +-------------------+  |     |
//! |   +------------------------+     |
//! |   |   Entry 2              |     |
//! |   |       ...              |     |
//! |   +------------------------+     |
//! +----------------------------------+
//! ```
//!
//! In the diagram:
//! - The `Block` struct represents an individual block within the SSTable.
//! - The `data` field of the `Block` is a vector (`Vec<u8>`) that stores the data entries.
//! - The `index` field is a `HashMap` that maintains the index for efficient key-based lookups.
//! - The `entry_count` field keeps track of the number of entries in the block.
//!
//! Each entry within the block consists of three parts:
//! 1. Length Prefix: A 4-byte length prefix in little-endian format, indicating the length of the value.
//! 2. Key: Variable-length key bytes.
//! 3. Value: Variable-length value bytes.
//!
//! The block's data vector (`data`) stores these entries sequentially. Each entry follows the format mentioned above, and they are concatenated one after another within the data vector. The index hashmap (`index`) maintains references to the keys and their corresponding offsets within the data vector.
//!
//! ---
//!
//! Author: Nrishinghananda Roy
//!

#![allow(dead_code)]

pub mod api;
mod memtable;
mod sst;
mod write_ahead_log;
