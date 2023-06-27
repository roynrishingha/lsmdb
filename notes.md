# NOTES

## Why use `Vec<u8>` for `key` and `value`?

1. **Lower memory overhead**: Byte arrays typically have lower memory overhead compared to strings, as they store raw binary data without additional metadata.
2. **Efficient serialization**: Byte arrays are well-suited for serialization and deserialization operations, making them efficient for storing and retrieving data from disk or network.
3. **Flexibility**: Byte arrays can represent arbitrary binary data, allowing you to store non-textual or structured data efficiently.

## `&[u8]` or `Vec<u8>` ?

`&[u8]` and `Vec<u8>` are two different types used for representing sequences of bytes (`u8` values) but with different ownership and lifetimes.
Both types can be converted to each other using methods like `as_slice()` to convert a `Vec<u8>` to a `&[u8]` or `to_vec()` to convert a `&[u8]` to a `Vec<u8>`. 

### Differences

1. `&[u8]` (byte slice):
   - A byte slice is an immutable view into a contiguous sequence of bytes.
   - It represents a borrowed reference to an existing byte sequence and does not own the data.
   - The length of the slice is fixed and cannot be changed.
   - Byte slices are often used as function parameters or return types to efficiently pass or return sequences of bytes without copying the data.

2. `Vec<u8>` (byte vector):
   - A byte vector is a growable, mutable container for a sequence of bytes.
   - It owns the underlying byte data and can dynamically resize and modify the content.
   - `Vec<u8>` provides additional methods beyond what a byte slice offers, such as pushing, popping, and modifying elements.
   - Since it owns the data, a `Vec<u8>` has a specific lifetime and is deallocated when it goes out of scope.


In summary, `&[u8]` is a borrowed reference to an existing byte sequence and is useful for working with byte data without ownership or modifying capabilities. `Vec<u8>` is a mutable container that owns the byte data and provides additional operations for dynamic modification and ownership of byte sequences.

## Mutex or RwLock ?

Using either `RwLock` or `Mutex` for protecting the `MemTable` entries in LSM Tree implementation has its own pros and cons.

### Pros of using `RwLock`:

1. **Concurrent Reads**: `RwLock` allows multiple readers to acquire a read lock simultaneously, enabling concurrent read operations. This can improve performance in scenarios where there are frequent read operations compared to write operations.
2. **Lower Contention**: Since multiple threads can read the entries concurrently, it reduces contention among threads and improves overall throughput.
3. **Thread Safety**: `RwLock` guarantees thread safety by enforcing exclusive write access but allowing concurrent read access.

### Cons of using `RwLock`:

1. **Exclusive Write Access**: While `RwLock` allows multiple threads to read concurrently, it only allows a single thread to acquire the write lock at a time. This can lead to reduced performance if there are frequent write operations or if the write operations take a significant amount of time.
2. **Potential Deadlocks**: It's important to be cautious when using `RwLock` to avoid potential deadlocks. If a thread holds a read lock and tries to acquire a write lock or vice versa, it can result in a deadlock situation.

### Pros of using `Mutex`:

1. **Simplicity**: `Mutex` is a straightforward synchronization primitive, making it easier to reason about and use correctly.
2. **Exclusive Access**: `Mutex` ensures exclusive access to the entries, which can be beneficial if the write operations require strong consistency guarantees.
3. **Avoiding Deadlocks**: Since `Mutex` allows only one thread to hold the lock at a time, the possibility of deadlocks due to lock contention is reduced.

### Cons of using `Mutex`:

1. **Limited Concurrency**: `Mutex` allows only one thread to hold the lock at a time, which means concurrent reads are not possible. This can impact performance if there are frequent read operations or if there are multiple threads that primarily perform reads.
2. **Potential Contention**: Since `Mutex` allows only one thread to hold the lock at a time, other threads attempting to acquire the lock may experience contention, leading to reduced throughput in high concurrency scenarios.


## Different types of WAL implementation

The Write-Ahead Log can be implemented as a file-based log or a sequential write-ahead log. Here are the pros and cons of of both:

### File-Based Log:

#### Pros:

- **Durability**: Writing to a file ensures that the logged data is persisted even in the event of system failures or crashes. The data remains intact and can be recovered during system startup or crash recovery.
- **Simplicity**: Implementing the WAL as a file-based log is often straightforward and easier to understand compared to other approaches.
- **Flexibility**: The file-based log can be easily managed, rotated, truncated, and archived. This allows for efficient space utilization and log management.

#### Cons:

- **Disk I/O**: Writing to a file involves disk I/O operations, which can introduce latency and impact performance, especially in high-write scenarios.
- **Synchronization Overhead**: Ensuring the durability of each write operation may require additional synchronization mechanisms such as fsync or flushing the file system cache, which can impact performance.
- **Fragmentation**: Over time, the log file may become fragmented due to various operations like rotation, truncation, and compaction. This fragmentation can impact read and write performance.

### Sequential Write-Ahead Log:

#### Pros:

- **Performance**: Sequential writes are generally faster compared to random disk writes. Writing data in sequential order minimizes seek time and maximizes disk throughput, resulting in improved performance.
- **Reduced Disk I/O**: Sequential write-ahead logging reduces disk I/O operations, as multiple write operations can be batched together and written to disk in a single sequential write operation.
- **Atomicity**: Sequential write-ahead logging ensures atomicity by guaranteeing that all the logged operations are either fully written or not written at all. This helps maintain consistency and reliability.

#### Cons:

- **Complexity**: Implementing a sequential write-ahead log may require more complex logic compared to a file-based log. Proper management of the log's sequential order and handling batched writes is essential.
- **Limited Flexibility**: Sequential write-ahead logging may have limitations on log management operations like rotation, truncation, or compaction, as maintaining the sequential order of writes is crucial.
- **Increased Memory Usage**: Maintaining an in-memory buffer to collect and batch write operations before flushing them to disk may require additional memory resources.

**Going through both options, I want to implement Write-Ahead Log as Sequential Write-Ahead Log.**

---