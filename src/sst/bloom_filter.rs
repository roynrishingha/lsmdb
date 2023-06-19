use bit_vec::BitVec;
use std::collections::hash_map::DefaultHasher;
use std::f64::consts::LN_2;
use std::hash::{Hash, Hasher};

pub(crate) struct BloomFilter {
    filter_bits: BitVec,
    num_hash_functions: usize,
    expected_entries: usize,
}

impl BloomFilter {
    pub(crate) fn new(expected_entries: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::calculate_num_bits(expected_entries, false_positive_rate);
        let num_hash_functions = Self::calculate_num_hash_functions(expected_entries, num_bits);

        Self {
            filter_bits: BitVec::from_elem(num_bits, false),
            num_hash_functions,
            expected_entries,
        }
    }

    fn calculate_num_bits(expected_entries: usize, false_positive_rate: f64) -> usize {
        let ln2_squared = (LN_2 * LN_2) as usize;
        (-1.0 * (expected_entries as f64) * false_positive_rate.ln() / ln2_squared as f64).ceil()
            as usize
    }

    fn calculate_num_hash_functions(expected_entries: usize, num_bits: usize) -> usize {
        ((num_bits as f64 / expected_entries as f64) * LN_2).ceil() as usize
    }

    pub(crate) fn add<T: Hash>(&mut self, item: &T) {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();

        for i in 0..self.num_hash_functions {
            let index = (hash ^ (i as u64)) as usize % self.filter_bits.len();
            self.filter_bits.set(index, true);
        }
    }

    pub(crate) fn contains<T: Hash>(&self, item: &T) -> bool {
        let mut hasher = DefaultHasher::new();

        item.hash(&mut hasher);
        let hash = hasher.finish();

        for i in 0..self.num_hash_functions {
            let index = (hash ^ (i as u64)) as usize % self.filter_bits.len();
            if !self.filter_bits.get(index).unwrap() {
                return false;
            }
        }
        true
    }
}
