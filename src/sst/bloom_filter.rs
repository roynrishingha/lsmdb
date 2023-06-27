//! # Bloom Filter
//!
//! The Bloom Filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set. It provides a fast and memory-efficient way to check for set membership, but it introduces a small probability of false positives.
//!
//! The Bloom Filter implementation is provided as a Rust module and consists of a struct called `BloomFilter`. It uses a `BitVec` to represent the array of bits that make up the filter. The number of hash functions used by the Bloom Filter is configurable, and it keeps track of the number of elements inserted into the filter.
//!
//! ## Structure
//!
// The BloomFilter struct represents the Bloom Filter data structure and contains the following fields:
//!
//! ```rs
//! pub(crate) struct BloomFilter {
//!     bits: Arc<Mutex<BitVec>>,
//!     num_hashes: usize,
//!     num_elements: AtomicUsize,
//! }
//! ```
//!
//! ### `bits`
//!
//! An `Arc<Mutex<BitVec>>` representing the array of bits used to store the Bloom filter.
//!
//! ### `num_hashes`
//!
//! The number of hash functions used by the Bloom filter.
//!
//! ### `num_elements`
//!
//! An `AtomicUsize` representing the number of elements inserted into the Bloom filter.
//!
//! ## Constructor Methods
//!
//! ### `new`
//!
//! ```rs
//! fn new(num_elements: usize, false_positive_rate: f64) -> Self
//! ```
//!
//! The `new` method creates a new `BloomFilter` with the specified number of elements and false positive rate. It initializes the Bloom filter's bit array, calculates the number of hash functions, and sets the initial number of elements to zero.
//!
//! ## Public Methods
//!
//! ### `set`
//!
//! ```rs
//! fn set<T: Hash>(&mut self, key: &T)```
//!
//! The `set` method inserts a key into the Bloom filter. It calculates the hash value for the key using multiple hash functions and sets the corresponding bits in the bit array to true. It also increments the element count.
//!
//! ### `contains`
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
//! ### `num_elements`
//!
//! ```rs
//! fn num_elements(&self) -> usize
//! ```
//!
//! This method returns the current number of elements inserted into the Bloom filter.
//!
//! ## Internal Method
//!
//! ### `calculate_hash`
//!
//! ```rs
//! fn calculate_hash<T: Hash>(&self, key: &T, seed: usize) -> u64
//! ```
//!
//! This function calculates a hash value for a given key and seed. It uses a suitable hash function to hash the key and incorporates the seed value for introducing randomness.
//!
//! ### `calculate_num_bits`
//!
//! ```rs
//! fn calculate_num_bits(num_elements: usize, false_positive_rate: f64) -> usize
//! ```
//!
//! This function calculates the optimal number of bits for the Bloom filter based on the desired false positive rate and the expected number of elements. It uses a formula to estimate the number of bits required.
//!
//! ### `calculate_num_hashes`
//!
//! ```rs
//! fn calculate_num_hashes(num_bits: usize, num_elements: usize) -> usize
//! ```
//!
//! This function calculates the optimal number of hash functions for the Bloom filter based on the number of bits and the expected number of elements. It uses a formula to estimate the number of hash functions required.
//!

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use bit_vec::BitVec;

/// A Bloom filter is a space-efficient probabilistic data structure used to test
/// whether an element is a member of a set.
pub(crate) struct BloomFilter {
    /// The array of bits representing the Bloom filter.
    bits: Arc<Mutex<BitVec>>,
    /// The number of hash functions used by the Bloom filter.           
    num_hashes: usize,
    /// The number of elements inserted into the Bloom filter.         
    num_elements: AtomicUsize,
}

impl BloomFilter {
    /// Creates a new Bloom filter with the specified number of elements and false positive rate.
    ///
    /// # Arguments
    ///
    /// * `num_elements` - The expected number of elements to be inserted into the Bloom filter.
    /// * `false_positive_rate` - The desired false positive rate (e.g., 0.001 for 0.1%).
    ///
    /// # Panics
    ///
    /// This function will panic if `num_elements` is zero or if `false_positive_rate` is not within (0, 1).
    pub(crate) fn new(num_elements: usize, false_positive_rate: f64) -> Self {
        assert!(
            num_elements > 0,
            "Number of elements must be greater than zero"
        );
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "False positive rate must be between 0 and 1"
        );

        let num_bits = Self::calculate_num_bits(num_elements, false_positive_rate);
        let num_hashes = Self::calculate_num_hashes(num_bits, num_elements);

        let bits = Arc::new(Mutex::new(BitVec::from_elem(num_bits, false)));

        Self {
            bits,
            num_hashes,
            num_elements: AtomicUsize::new(0),
        }
    }

    /// Inserts an key into the Bloom filter.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to be inserted into the Bloom filter.
    pub(crate) fn set<T: Hash>(&mut self, key: &T) {
        let mut bits = self
            .bits
            .lock()
            .expect("Failed to acquire lock on Bloom Filter bits.");

        for i in 0..self.num_hashes {
            let hash = self.calculate_hash(key, i);

            let index = (hash % (bits.len() as u64)) as usize;

            bits.set(index, true);
        }

        // Increment the element count.
        self.num_elements.fetch_add(1, Ordering::Relaxed);
    }

    /// Checks if an key is possibly present in the Bloom filter.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to be checked.
    ///
    /// # Returns
    ///
    /// * `true` if the key is possibly present in the Bloom filter.
    /// * `false` if the key is definitely not present in the Bloom filter.
    pub(crate) fn contains<T: Hash>(&self, key: &T) -> bool {
        let bits = self
            .bits
            .lock()
            .expect("Failed to acquire lock on Bloom Filter bits.");

        for i in 0..self.num_hashes {
            let hash = self.calculate_hash(key, i);
            let index = (hash % (bits.len() as u64)) as usize;

            if !bits[index] {
                return false;
            }
        }
        // All bits are true, so the key is possibly present.
        true
    }

    /// Returns the current number of elements inserted into the Bloom filter.
    pub(crate) fn num_elements(&self) -> usize {
        // Retrieve the element count atomically.
        self.num_elements.load(Ordering::Relaxed)
    }

    // Internal helper functions

    /// Calculates a hash value for a given key and seed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to be hashed.
    /// * `seed` - The seed value for incorporating randomness.
    ///
    /// # Returns
    ///
    /// The calculated hash value as a `u64`.
    fn calculate_hash<T: Hash>(&self, key: &T, seed: usize) -> u64 {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);
        hasher.write_usize(seed);
        hasher.finish()
    }

    /// Calculates the optimal number of bits for the Bloom filter based on the desired false positive rate and the expected number of elements.
    ///
    /// # Arguments
    ///
    /// * `num_elements` - The expected number of elements.
    /// * `false_positive_rate` - The desired false positive rate.
    ///
    /// # Returns
    ///
    /// The calculated number of bits as a `usize`.
    fn calculate_num_bits(num_elements: usize, false_positive_rate: f64) -> usize {
        let num_bits_float =
            (-((num_elements as f64) * false_positive_rate.ln()) / (2.0_f64.ln().powi(2))).ceil();

        num_bits_float as usize
    }

    /// Calculates the optimal number of hash functions for the Bloom filter based on the number of bits and the expected number of elements.
    ///
    /// # Arguments
    ///
    /// * `num_bits` - The number of bits in the Bloom filter.
    /// * `num_elements` - The expected number of elements.
    ///
    /// # Returns
    ///
    /// The calculated number of hash functions as a `usize`.
    fn calculate_num_hashes(num_bits: usize, num_elements: usize) -> usize {
        let num_hashes_float = (num_bits as f64 / num_elements as f64) * 2.0_f64.ln();

        num_hashes_float.ceil() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_and_containment() {
        let mut bloom = BloomFilter::new(100, 0.001);

        // Insert an element
        bloom.set(&"apple");

        // Check containment of the inserted element
        assert!(bloom.contains(&"apple"));

        // Check containment of a non-inserted element
        assert!(!bloom.contains(&"banana"));
    }

    #[test]
    fn test_num_elements() {
        let mut bloom = BloomFilter::new(100, 0.01);

        // Insert multiple elements
        for i in 0..10 {
            bloom.set(&i);
        }

        // Check the number of elements
        assert_eq!(bloom.num_elements(), 10);
    }

    #[test]
    fn test_false_positives_high_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.1;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(num_elements, false_positive_rate);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }

    #[test]
    fn test_false_positives_medium_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.001;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(num_elements, false_positive_rate);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }

    #[test]
    fn test_false_positives_low_rate() {
        // Number of elements.
        let num_elements = 10000;

        // False Positive Rate.
        let false_positive_rate = 0.000001;

        // Create a Bloom Filter.
        let mut bloom = BloomFilter::new(num_elements, false_positive_rate);

        // Insert elements into the Bloom Filter.
        for i in 0..num_elements {
            bloom.set(&i);
        }

        let mut false_positives = 0;
        let num_tested_elements = 2000;

        // Test all non-inserted elements for containment.
        // Count the number of false positives.
        for i in num_elements..num_elements + num_tested_elements {
            if bloom.contains(&i) {
                false_positives += 1;
            }
        }

        // Calculate the observed false positive rate.
        let observed_false_positive_rate = false_positives as f64 / num_tested_elements as f64;

        // Allow for a small margin (10%) of error due to the probabilistic nature of Bloom filters.
        // Maximum Allowed False Positive Rate = False Positive Rate + (False Positive Rate * Tolerance)
        let max_allowed_false_positive_rate = false_positive_rate + (false_positive_rate * 0.1);

        assert!(
            observed_false_positive_rate <= max_allowed_false_positive_rate,
            "Observed false positive rate ({}) is greater than the maximum allowed ({})",
            observed_false_positive_rate,
            max_allowed_false_positive_rate
        );
    }
}
