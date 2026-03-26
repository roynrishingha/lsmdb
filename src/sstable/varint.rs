pub(crate) fn encode_u32(mut value: u32, buf: &mut Vec<u8>) {
    loop {
        // Take the bottom 7 bits. `0x7f` is binary `01111111`.
        let mut byte = (value & 0x7f) as u8;

        // Shift the original value right by 7 bits to bring the next 7 bits down.
        value >>= 7;

        // If there are still bits left in the value to encode, we must set the
        // MSB (Most Significant Bit) of our `byte` to 1. `0x80` is binary `10000000`.
        if value != 0 {
            byte |= 0x80;
        }

        buf.push(byte);

        // If nothing is left, we are done!
        if value == 0 {
            break;
        }
    }
}

pub(crate) fn encode_u64(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        // Take the bottom 7 bits. `0x7f` is binary `01111111`.
        let mut byte = (value & 0x7f) as u8;

        // Shift the original value right by 7 bits to bring the next 7 bits down.
        value >>= 7;

        // If there are still bits left in the value to encode, we must set the
        // MSB (Most Significant Bit) of our `byte` to 1. `0x80` is binary `10000000`.
        if value != 0 {
            byte |= 0x80;
        }

        buf.push(byte);

        // If nothing is left, we are done!
        if value == 0 {
            break;
        }
    }
}

pub(crate) fn decode_u32(buf: &[u8]) -> Option<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in buf {
        bytes_read += 1;

        // Extract the bottom 7 bits and shift them into the correct place
        let lower_7_bits = (byte & 0x7f) as u32;
        result |= lower_7_bits << shift;

        // If the 8th bit (MSB) is NOT set, this is the last byte of the varint!
        if byte & 0x80 == 0 {
            return Some((result, bytes_read));
        }

        shift += 7;

        // A u32 can take at most 5 bytes to encode in base-128.
        if shift >= 35 {
            return None; // Corrupted varint (too long for u32)
        }
    }

    // We reached the end of the buffer, but the last byte still had the continuation bit set!
    None
}

pub(crate) fn decode_u64(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in buf {
        bytes_read += 1;

        // Extract the bottom 7 bits and shift them into the correct place
        let lower_7_bits = (byte & 0x7f) as u64;
        result |= lower_7_bits << shift;

        // If the 8th bit (MSB) is NOT set, this is the last byte of the varint!
        if byte & 0x80 == 0 {
            return Some((result, bytes_read));
        }

        shift += 7;

        // A u64 can take at most 10 bytes to encode in base-128.
        if shift >= 70 {
            return None; // Corrupted varint (too long for u64)
        }
    }

    // We reached the end of the buffer, but the last byte still had the continuation bit set!
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encode_decode() {
        let mut buf = Vec::new();

        let values = vec![0, 1, 127, 128, 255, 300, 16383, 16384, u32::MAX];

        for &val in &values {
            buf.clear();
            encode_u32(val, &mut buf);

            // Check that it's actually saving space
            if val < 128 {
                assert_eq!(buf.len(), 1);
            } else if val < 16384 {
                assert_eq!(buf.len(), 2);
            }

            let (decoded, len) = decode_u32(&buf).unwrap();
            assert_eq!(decoded, val);
            assert_eq!(len, buf.len());
        }
    }
}
