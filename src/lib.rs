//! LSM Tree Storage Engine Library
//!
//! This library provides a LSM Tree storage engine implementation for efficient data storage and retrieval. It is designed to support data-intensive applications that require fast and reliable storage capabilities.
//!
//! # Modules
//!
//! - `engine`: Entry point to the storage engine. Contains the main functionality for storing and retrieving data.
//! - `helper`: Utility functions and helper methods used by the storage engine.
//! - `mem_table`: In-memory table implementation for fast data writes and reads.
//! - `wal`: Write-Ahead Log (WAL) module for durability and crash recovery.
//!
//! # Usage
//!
//! Add this library as a dependency in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! lsm_tree_storage_engine = "0.1.0"
//! ```
//!
//! Import the engine module into your Rust code:
//!
//! ```rust
//! use lsm_tree_storage_engine::engine::{StorageEngine, StorageEngineEntry};
//! ```
//!
//! # Examples
//!
//! Here's an example demonstrating the basic usage of the storage engine:
//!
//! ```rust
//! use lsm_tree_storage_engine::engine::StorageEngine;
//!
//! fn main() {
//!     // Create a new storage engine instance
//!     let mut engine = StorageEngine::new("./lib_test_dir");
//!
//!     // Write data to the storage engine
//!     engine.set(b"key", b"value");
//!
//!     // Read data from the storage engine
//!     let value = engine.get(b"key");
//!     println!("Value: {:?}", value);
//!
//!#     if let Err(e) = std::fs::remove_dir_all("./lib_test_dir") {
//!#        println!("Failed to remove test directory: {:?}", e);
//!#    }
//! }
//! ```
//!
//! # Testing
//!
//! The library includes test modules to ensure the correctness of its components. Run the tests using `cargo test`:
//!
//! ```bash
//! $ cargo test
//! ```
//!
//! # Contributing
//!
//! Contributions to this lsm tree storage engine library are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request on the GitHub repository: [link-to-repository](https://github.com/roynrishingha/lsm-tree-storage-engine)
//!
//! # License
//!
//! This library is licensed under the [MIT License](https://opensource.org/licenses/MIT).
//!
//! ---
//! Author: Nrishinghananda Roy
//! Version: 0.1.0
//! ```
//!

/// Entry point to storage engine
pub mod engine;

mod helper;
mod mem_table;
mod wal;

/// Test modules for `mem_table` module.
#[cfg(test)]
mod mem_table_tests;

/// Test modules for `wal` module.
#[cfg(test)]
mod wal_tests;
