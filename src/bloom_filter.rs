use bit_vec::BitVec;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A probabilistic set that answers "does this key exist?" without storing the keys themselves.
///
/// We use the **Kirsch-Mitzenmacher** double-hashing trick: instead of running `k` independent
/// hash functions (expensive for large `k`), we compute one 64-bit hash and derive all `k` bit
/// positions via `h_i = h1 + i * h2`. This cuts hashing cost to O(1) regardless of `k` while
/// maintaining the same false-positive guarantees as truly independent hashes.
///
/// A `false` return is a hard guarantee: the key was never inserted. A `true` return means the
/// key was *probably* inserted — with false positive probability determined by the configured FPR.
/// The Bloom Filter never produces false negatives.
#[derive(Clone)]
pub struct BloomFilter {
    bits: BitVec,
    k_num_hashes: u32,
}

impl BloomFilter {
    /// Sizes the filter optimally for `num_elements` at the target `false_positive_rate`.
    pub fn new(num_elements: usize, false_positive_rate: f64) -> Self {
        let (num_bits, k_num_hashes) =
            Self::calculate_optimal_params(num_elements, false_positive_rate);

        Self {
            bits: BitVec::from_elem(num_bits, false),
            k_num_hashes,
        }
    }

    /// Derives the minimum bit-array size (m) and hash count (k) for the given FPR.
    ///
    /// The formulas come from the classical Bloom Filter analysis:
    /// - `m = -n * ln(p) / ln(2)²`  — minimizes total filter size for a given FPR
    /// - `k = (m / n) * ln(2)`       — minimizes FPR for a given m and n
    ///
    /// `k` is clamped to at least 1 because 0 hash functions would never set any bits,
    /// turning every `contains()` call into a false positive (every key would "exist").
    fn calculate_optimal_params(num_elements: usize, fpr: f64) -> (usize, u32) {
        let num_bits = (-1.0 * (num_elements as f64) * fpr.ln() / (std::f64::consts::LN_2.powi(2)))
            .ceil() as usize;

        let mut k =
            ((num_bits as f64 / num_elements as f64) * std::f64::consts::LN_2).ceil() as u32;

        if k == 0 {
            k = 1;
        }

        (num_bits, k)
    }

    fn hash_key(key: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Records that `key` is a member of the set.
    pub fn set(&mut self, key: &[u8]) {
        let hash = Self::hash_key(key);
        let h1 = (hash & 0xFFFFFFFF) as u32;
        let h2 = (hash >> 32) as u32;

        let m = self.bits.len();
        if m == 0 {
            return;
        }

        for i in 0..self.k_num_hashes {
            // Computed in u64 before modulo to prevent overflow when h2 is large.
            let bit_idx = (h1 as u64 + (i as u64).wrapping_mul(h2 as u64)) as usize % m;
            self.bits.set(bit_idx, true);
        }
    }

    /// Returns `false` if the key is **definitely** absent. Returns `true` if it **might** exist.
    pub fn contains(&self, key: &[u8]) -> bool {
        let hash = Self::hash_key(key);
        let h1 = (hash & 0xFFFFFFFF) as u32;
        let h2 = (hash >> 32) as u32;

        let m = self.bits.len();
        if m == 0 {
            return false;
        }

        for i in 0..self.k_num_hashes {
            let bit_idx = (h1 as u64 + (i as u64).wrapping_mul(h2 as u64)) as usize % m;
            // One clear bit is enough to prove absence — no need to check the remaining hashes.
            if !self.bits.get(bit_idx).unwrap_or(false) {
                return false;
            }
        }

        true
    }

    /// Serializes the filter to bytes for embedding in an SSTable's filter block.
    ///
    /// Format: `[k_num_hashes (4 LE bytes)] [num_bits (8 LE bytes)] [bit payload…]`
    ///
    /// `k_num_hashes` and `num_bits` are stored separately from the bit payload so that
    /// `from_bytes` can reconstruct the exact filter without any out-of-band metadata.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.k_num_hashes.to_le_bytes());
        bytes.extend_from_slice(&(self.bits.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&self.bits.to_bytes());
        bytes
    }

    /// Reconstructs a filter from a previously serialized byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 12 {
            return None;
        }
        let k_num_hashes = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let num_bits = u64::from_le_bytes(bytes[4..12].try_into().ok()?) as usize;

        let mut bits = BitVec::from_bytes(&bytes[12..]);
        // `BitVec::from_bytes` rounds up to the nearest byte boundary. We truncate back to the
        // exact bit count stored in the header so that the modulo arithmetic in `contains`
        // and `set` sees the same `m` value that was used when the filter was originally built.
        bits.truncate(num_bits);

        Some(Self { bits, k_num_hashes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.set(b"apple");
        bf.set(b"banana");
        bf.set(b"grape");

        assert!(bf.contains(b"apple"));
        assert!(bf.contains(b"banana"));
        assert!(bf.contains(b"grape"));

        assert!(!bf.contains(b"strawberry"));
        assert!(!bf.contains(b"missing"));
    }

    #[test]
    fn test_bloom_filter_false_positives() {
        let mut bf = BloomFilter::new(1000, 0.1); // 10% FPR

        for i in 0..1000 {
            let key = format!("key{}", i);
            bf.set(key.as_bytes());
        }

        let mut false_positives = 0;
        let tests = 10000;
        for i in 1000..(1000 + tests) {
            let key = format!("key{}", i);
            if bf.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / tests as f64;

        // We assert < 15% rather than exactly 10% to absorb hash distribution variance.
        // The test would be flaky at exactly 10% due to statistical noise in small samples.
        assert!(
            actual_fpr < 0.15,
            "FPR was significantly higher than estimated: {}",
            actual_fpr
        );
    }
}
