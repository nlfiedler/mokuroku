//
// Copyright (c) 2020 Nathan Fiedler
//

//!
//! The `base32` module provides encoding and decoding functions for converting
//! binary data to and from UTF-8 alphanumeric text using the base32hex encoding
//! defined in RFC-4648.
//!
//! This encoding is useful for emitting numeric index keys and querying those
//! values with a range query. This derives from the fact that base32hex encoded
//! text preserves the bitwise sort order of the represented data.
//!
//! Similarly, the default composite key separator character (the zero byte)
//! will continue to work adequately even for index keys that could themselves
//! contain a zero, if not encoded.
//!

// The base32hex alphabet.
const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUV";

///
/// Encode the binary data using base32hex encoding.
///
pub fn encode(input: &[u8]) -> Vec<u8> {
    // this code was derived from github:naim94a/binascii-rs
    let data_len = input.len() * 8 / 5;
    let pad_len = 8 - (data_len % 8);
    let output_len = data_len + if pad_len == 8 { 0 } else { pad_len };
    let mut output = Vec::with_capacity(output_len);
    for block_idx in 0..=(input.len() / 5) {
        let block_len = if input.len() > block_idx * 5 + 5 {
            block_idx * 5 + 5
        } else {
            input.len()
        };
        let block = &input[block_idx * 5..block_len];
        let mut num = 0u64;
        for (idx, ch) in block.iter().enumerate() {
            num |= (*ch as u64) << (64 - 8 - idx * 8);
        }
        // this will overfill with zeros which is fixed up later
        for i in 0..8 {
            let digit_idx = (num >> (64 - 5 - i * 5)) & 0b11111;
            output.push(ALPHABET[digit_idx as usize]);
        }
    }
    // replace the trailing zeros filled in above with padding
    #[allow(clippy::needless_range_loop)]
    for idx in data_len + 1..output_len {
        output[idx] = b'=';
    }
    // fix up any zero overfill in the edge cases
    output.truncate(output_len);
    output
}

// The UTF-8 character set encoded as values for base32hex.
#[rustfmt::skip]
const INV_ALPHABET: [i8; 256] = [
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1
];

///
/// Decode the base32hex encoded data into raw binary data.
///
/// Lowercase letters in the input are treated as the equivalent uppercase.
///
/// If the input is invalid for any reason, `None` is returned. This includes
/// incorrect input length, invalid characters, or any character other than `=`
/// appearing after the first padding character.
///
pub fn decode(input: &[u8]) -> Option<Vec<u8>> {
    let remainder = input.len() % 8;
    // this code was derived from github:naim94a/binascii-rs
    let padding = 8 - remainder;
    let input_len = input.len() + if padding != 8 { padding } else { 0 };
    let mut output_len = input_len * 5 / 8;
    let mut output = Vec::with_capacity(output_len);
    let mut eof = false;
    for block_idx in 0..=(input.len() / 8) {
        let block_end = if input.len() > block_idx * 8 + 8 {
            block_idx * 8 + 8
        } else {
            input.len()
        };
        let block = &input[block_idx * 8..block_end];
        let mut num = 0u64;
        for (idx, ch) in block.iter().enumerate() {
            // ensure that once padding has been found, only padding remains
            // until the end of the input
            if *ch == b'=' {
                eof = true;
                continue;
            } else if eof {
                return None;
            }
            let c_val = INV_ALPHABET[*ch as usize];
            if c_val < 0 {
                return None;
            }
            num |= (c_val as u64) << (64 - 5 - idx * 5);
            output_len = block_idx * 5 + (idx * 5 / 8) + 1;
        }
        for i in 0..5 {
            output.push(((num >> (64 - 8 - i * 8)) & 0xff) as u8);
        }
    }
    if !eof && (remainder == 1 || remainder == 3 || remainder == 6) {
        // input had no padding and was an invalid length
        return None;
    }
    output.truncate(output_len);
    Some(output)
}

#[cfg(test)]
mod test {
    use super::{decode, encode};
    use rand::prelude::*;

    #[test]
    fn valid() {
        let input: Vec<u8> = vec![];
        assert_eq!(encode(&input), b"");
        assert_eq!(decode(&input), Some(input));

        let input = vec![b'f'];
        assert_eq!(encode(&input), b"CO======");
        assert_eq!(decode(b"CO======"), Some(input));

        let input = vec![b'f', b'o'];
        assert_eq!(encode(&input), b"CPNG====");
        assert_eq!(decode(b"CPNG===="), Some(input));

        let input = vec![b'f', b'o', b'o'];
        assert_eq!(encode(&input), b"CPNMU===");
        // various padding length and mixed letter case
        assert_eq!(decode(b"CPNMU"), Some(input.clone()));
        assert_eq!(decode(b"CPNMU="), Some(input.clone()));
        assert_eq!(decode(b"CPNMU=="), Some(input.clone()));
        assert_eq!(decode(b"CPNMU==="), Some(input.clone()));
        assert_eq!(decode(b"cpnmu==="), Some(input.clone()));
        assert_eq!(decode(b"CpNmU===="), Some(input.clone()));
        assert_eq!(decode(b"cPnMu====="), Some(input));

        let input = vec![b'f', b'o', b'o', b'b'];
        assert_eq!(encode(&input), b"CPNMUOG=");
        assert_eq!(decode(b"CPNMUOG="), Some(input));

        let input = vec![b'f', b'o', b'o', b'b', b'a'];
        assert_eq!(encode(&input), b"CPNMUOJ1");
        assert_eq!(decode(b"CPNMUOJ1"), Some(input));

        let input = vec![b'f', b'o', b'o', b'b', b'a', b'r'];
        assert_eq!(encode(&input), b"CPNMUOJ1E8======");
        assert_eq!(decode(b"CPNMUOJ1E8======"), Some(input));
    }

    #[test]
    fn ensure_sort_order() {
        // Generate some random data, encode it, sort the list, then visit
        // pairs, decode, and ensure values are in sorted order (i.e. the
        // encoded sort and raw data should sort the same).
        let mut rng = rand::thread_rng();
        let mut collection: Vec<Vec<u8>> = Vec::new();
        for _ in 0..100 {
            let mut data = [0u8; 8];
            rng.fill_bytes(&mut data);
            let encoded = encode(&data);
            collection.push(encoded);
        }
        collection.sort_unstable();
        for pair in collection.windows(2) {
            let a = decode(&pair[0]);
            let b = decode(&pair[1]);
            assert!(a <= b, "elements out of order");
        }
    }

    #[test]
    fn invalid_chars() {
        assert_eq!(decode(b"CO==12=="), None);
        assert_eq!(decode(b","), None);
        assert_eq!(decode(b"!"), None);
        assert_eq!(decode(b"?"), None);
        assert_eq!(decode(b"CPNMW"), None);
        assert_eq!(decode(b"CPNMX"), None);
        assert_eq!(decode(b"CPNMY"), None);
        assert_eq!(decode(b"CPNMZ"), None);
    }

    #[test]
    fn invalid_length() {
        assert_eq!(decode(b"A"), None);
        assert_eq!(decode(b"ABC"), None);
        assert_eq!(decode(b"ABCDEF"), None);
        assert_eq!(decode(b"00000000A"), None);
        assert_eq!(decode(b"00000000ABC"), None);
        assert_eq!(decode(b"00000000ABCDEF"), None);
    }
}
