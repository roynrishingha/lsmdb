use super::varint;

const RESTART_INTERVAL: usize = crate::constants::SSTABLE_RESTART_INTERVAL;

pub struct BlockBuilder {
    /// The raw byte buffer where we write our entries.
    buffer: Vec<u8>,
    /// Array of offsets in `buffer` where a restart point begins.
    restarts: Vec<u32>,
    /// The number of entries added since the last restart point.
    counter: usize,
    /// The key of the last entry added (used to calculate prefix overlap).
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            // The first restart point is always offset 0
            restarts: vec![0],
            counter: 0,
            last_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block.
    /// Keys MUST be added in strictly increasing order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared_length = self
            .last_key
            .iter()
            .zip(key.iter())
            .take_while(|(a, b)| a == b)
            .count();

        if self.counter == RESTART_INTERVAL {
            shared_length = 0;
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
        }

        let unshared_length = key.len() - shared_length;

        varint::encode_u32(shared_length as u32, &mut self.buffer);
        varint::encode_u32(unshared_length as u32, &mut self.buffer);
        varint::encode_u32(value.len() as u32, &mut self.buffer);
        self.buffer.extend_from_slice(&key[shared_length..]);
        self.buffer.extend_from_slice(&value);

        self.counter += 1;
        self.last_key = key.to_vec();
    }

    pub fn finish(&mut self) -> &[u8] {
        // Append all restart point offsets as 4-byte integers (Little Endian).
        for &offset in &self.restarts {
            self.buffer.extend_from_slice(&offset.to_le_bytes());
        }

        // Append the total number of restart points as a 4-byte integer.
        // This MUST be the very last 4 bytes of the block!
        let num_restarts = self.restarts.len() as u32;
        self.buffer.extend_from_slice(&num_restarts.to_le_bytes());

        &self.buffer
    }

    pub fn is_block_maxed(&self) -> bool {
        self.buffer.len() >= crate::constants::SSTABLE_BLOCK_SIZE
    }

    pub fn last_key(&self) -> Vec<u8> {
        self.last_key.clone()
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }
}

pub struct BlockReader<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) restarts_offset: usize,
    pub num_restarts: usize,
}

impl<'a> BlockReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let len = data.len();
        let num_restarts = u32::from_le_bytes(data[len - 4..].try_into().unwrap()) as usize;
        let restarts_offset = len - (num_restarts * 4) - 4;

        Self {
            data,
            restarts_offset,
            num_restarts,
        }
    }

    pub fn get(&self, search_key: &[u8]) -> Option<&'a [u8]> {
        if self.num_restarts == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = self.num_restarts - 1;
        let mut best_restart_index = 0;

        while left <= right {
            let mid = left + (right - left) / 2;
            let offset = self.read_restart_offset(mid) as usize;

            // At a restart point, shared_len is ALWAYS 0.
            let mut ptr = offset;

            let (_, len1) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len1;

            let (unshared_len, len2) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len2;

            let (_, len3) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len3;

            let key_at_mid = &self.data[ptr..ptr + unshared_len as usize];

            if key_at_mid < search_key {
                best_restart_index = mid;
                left = mid + 1;
            } else if key_at_mid > search_key {
                if mid == 0 {
                    break;
                }
                right = mid - 1;
            } else {
                best_restart_index = mid;
                break;
            }
        }

        let mut ptr = self.read_restart_offset(best_restart_index) as usize;
        let mut current_key = Vec::new();

        while ptr < self.restarts_offset {
            let (shared_len, len1) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len1;

            let (unshared_len, len2) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len2;

            let (value_len, len3) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len3;

            current_key.truncate(shared_len as usize);
            let unshared_bytes = &self.data[ptr..ptr + unshared_len as usize];
            current_key.extend_from_slice(unshared_bytes);
            ptr += unshared_len as usize;

            let value_bytes = &self.data[ptr..ptr + value_len as usize];
            ptr += value_len as usize;

            if current_key.as_slice() == search_key {
                return Some(value_bytes);
            } else if current_key.as_slice() > search_key {
                // Since keys are strictly sorted in an SSTable, if we see a current_key
                // that is larger than what we are looking for, we know our search_key
                // does not exist in this block.
                return None;
            }
        }

        None
    }

    // Like `get`, but returns the FIRST key >= search_key. This is crucial for Index blocks!
    pub fn lookup(&self, search_key: &[u8]) -> Option<&'a [u8]> {
        if self.num_restarts == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = self.num_restarts - 1;
        let mut best_restart_index = 0;

        while left <= right {
            let mid = left + (right - left) / 2;
            let offset = self.read_restart_offset(mid) as usize;

            let mut ptr = offset;
            let (_, len1) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len1;
            let (unshared_len, len2) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len2;
            let (_, len3) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len3;

            let key_at_mid = &self.data[ptr..ptr + unshared_len as usize];

            if key_at_mid < search_key {
                best_restart_index = mid;
                left = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                right = mid - 1;
            }
        }

        let mut ptr = self.read_restart_offset(best_restart_index) as usize;
        let mut current_key = Vec::new();

        while ptr < self.restarts_offset {
            let (shared_len, len1) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len1;

            let (unshared_len, len2) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len2;

            let (value_len, len3) = varint::decode_u32(&self.data[ptr..])?;
            ptr += len3;

            current_key.truncate(shared_len as usize);
            let unshared_bytes = &self.data[ptr..ptr + unshared_len as usize];
            current_key.extend_from_slice(unshared_bytes);
            ptr += unshared_len as usize;

            let value_bytes = &self.data[ptr..ptr + value_len as usize];
            ptr += value_len as usize;

            // If the current key is strictly >= search_key, return its value!
            if current_key.as_slice() >= search_key {
                return Some(value_bytes);
            }
        }

        None
    }

    fn read_restart_offset(&self, index: usize) -> u32 {
        let offset = self.restarts_offset + index * 4;
        u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_add() {
        let mut builder = BlockBuilder::new();

        // Let's add two keys that share a prefix: "apple" and "appstore"
        builder.add(b"apple", b"val1");
        builder.add(b"appstore", b"val2");

        // We know that "apple" and "appstore" share the first 3 bytes ("app").
        // "apple" is the very first key, so it cannot share anything.
        // But "appstore" should report that it shares 3 bytes with the previous key.
        // "appstore" has 5 unshared bytes ("store").

        // Let's verify our state after the fact.
        assert_eq!(builder.counter, 2);
        assert_eq!(builder.last_key, b"appstore");
        assert!(builder.buffer.len() > 0);
    }

    #[test]
    fn test_block_builder_restarts() {
        let mut builder = BlockBuilder::new();

        // Add 17 keys to trigger the RESTART_INTERVAL (which is 16)
        for i in 0..17 {
            let key = format!("key{:02}", i); // "key00", "key01", etc.
            builder.add(key.as_bytes(), b"val");
        }

        // We started with 1 restart point at offset 0.
        // Hitting 16 keys triggered a new restart point.
        assert_eq!(builder.restarts.len(), 2);
        assert_eq!(builder.restarts[0], 0);

        // The second restart point should be the byte offset where "key16" began
        assert!(builder.restarts[1] > 0);

        // The counter should have been reset to 0 right before adding "key16",
        // and then incremented to 1 after adding it.
        assert_eq!(builder.counter, 1);
        assert_eq!(builder.last_key, b"key16");
    }

    #[test]
    fn test_block_builder_finish() {
        let mut builder = BlockBuilder::new();

        builder.add(b"apple", b"val1");
        builder.add(b"appstore", b"val2");

        let data = builder.finish();

        // 1. We know we have EXACTLY 1 restart point (since we only added 2 keys).
        // 2. The restart point is at offset 0.
        // Therefore, the trailer should be:
        // [0, 0, 0, 0] (offset 0 as u32) + [1, 0, 0, 0] (number of restarts as u32)

        let len = data.len();

        // Ensure the last 4 bytes are the number of restart points (which is 1)
        let num_restarts_bytes = &data[len - 4..];
        let num_restarts = u32::from_le_bytes(num_restarts_bytes.try_into().unwrap());
        assert_eq!(num_restarts, 1);

        // Ensure the 4 bytes before that are the first restart point offset (which is 0)
        let first_restart_bytes = &data[len - 8..len - 4];
        let first_restart = u32::from_le_bytes(first_restart_bytes.try_into().unwrap());
        assert_eq!(first_restart, 0);
    }

    #[test]
    fn test_block_reader_init() {
        let mut builder = BlockBuilder::new();

        builder.add(b"apple", b"val1");
        builder.add(b"appstore", b"val2");

        let data = builder.finish();

        let reader = BlockReader::new(data);

        assert_eq!(reader.num_restarts, 1);

        // The restart array starts AFTER the payload.
        // The payload for "apple" (12 bytes) and "appstore" (12 bytes) is 24 bytes long.
        assert_eq!(reader.restarts_offset, 24);
    }

    #[test]
    fn test_block_reader_restart_offset() {
        let mut builder = BlockBuilder::new();

        for i in 0..17 {
            let key = format!("key{:02}", i);
            builder.add(key.as_bytes(), b"val");
        }

        let data = builder.finish();

        let reader = BlockReader::new(data);

        assert_eq!(reader.read_restart_offset(0), 0);
        assert!(reader.read_restart_offset(1) > 0);
    }

    #[test]
    fn test_block_reader_get() {
        let mut builder = BlockBuilder::new();

        builder.add(b"apple", b"val_apple");
        builder.add(b"appstore", b"val_appstore");
        builder.add(b"banana", b"val_banana");
        builder.add(b"bear", b"val_bear");
        builder.add(b"cat", b"val_cat");

        let data = builder.finish();
        let reader = BlockReader::new(data);

        // Found keys
        assert_eq!(reader.get(b"apple"), Some(b"val_apple".as_slice()));
        assert_eq!(reader.get(b"appstore"), Some(b"val_appstore".as_slice()));
        assert_eq!(reader.get(b"cat"), Some(b"val_cat".as_slice()));

        // Missing keys
        assert_eq!(reader.get(b"a"), None); // Before everything
        assert_eq!(reader.get(b"bat"), None); // In the middle
        assert_eq!(reader.get(b"zebra"), None); // After everything
    }
}
