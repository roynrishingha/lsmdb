//! # `lsmdb`
//!
//! `lsmdb` is an efficient storage engine that implements the Log-Structured Merge Tree (LSM-Tree) data structure, designed specifically for handling key-value pairs.
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
