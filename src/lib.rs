//! lsmdb
//!
//! lsmdb is an efficient storage engine that implements the Log-Structured Merge Tree (LSM-Tree) data structure, designed specifically for handling key-value pairs.
//!
//! # Contributing
//!
//! Contributions to `lsmdb` are welcome! If you encounter any issues or have suggestions for improvements, please open an issue or submit a pull request on the GitHub repository: [link-to-repository](https://github.com/roynrishingha/lsmdb)
//!
//! # License
//!
//! This library is licensed under the [MIT License](https://opensource.org/licenses/MIT).
//!
//! ---
//! Author: Nrishinghananda Roy
//! Version: 0.4.0
//! ```

#![allow(dead_code)]

mod api;
mod memtable;
mod sst;
mod write_ahead_log;
