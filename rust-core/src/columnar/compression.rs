//! Columnar Compression
//!
//! Specialized compression algorithms for columnar data:
//! - Delta encoding for sorted integers
//! - Delta-of-delta for timestamps
//! - Run-length for low cardinality
//! - Dictionary for symbols
//! - Bit-packing for narrow integers
//! - LZ4/Zstd for general purpose

use std::io::{Read, Write};

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    None,
    Delta,
    DeltaDelta,
    RunLength,
    Dictionary,
    BitPack,
    LZ4,
    Zstd,
}

/// Compression result
pub struct CompressedData {
    pub codec: Codec,
    pub original_size: usize,
    pub compressed_size: usize,
    pub data: Vec<u8>,
}

// =============================================================================
// Delta Encoding (for sorted integers)
// =============================================================================

/// Delta encode i64 values
pub fn delta_encode_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(values.len() * 5); // Variable length encoding

    // Write first value as-is
    result.extend_from_slice(&values[0].to_le_bytes());

    // Write deltas
    let mut prev = values[0];
    for &val in &values[1..] {
        let delta = val - prev;
        encode_varint_signed(&mut result, delta);
        prev = val;
    }

    result
}

/// Delta decode i64 values
pub fn delta_decode_i64(data: &[u8], count: usize) -> Vec<i64> {
    if data.len() < 8 || count == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(count);
    let mut pos = 0;

    // Read first value
    let first = i64::from_le_bytes(data[0..8].try_into().unwrap());
    result.push(first);
    pos = 8;

    // Read deltas
    let mut prev = first;
    while result.len() < count && pos < data.len() {
        let (delta, bytes_read) = decode_varint_signed(&data[pos..]);
        pos += bytes_read;
        let val = prev + delta;
        result.push(val);
        prev = val;
    }

    result
}

// =============================================================================
// Delta-of-Delta Encoding (for timestamps)
// =============================================================================

/// Delta-of-delta encode i64 values (optimal for timestamps)
pub fn delta_delta_encode_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(values.len() * 3);

    // Write first value
    result.extend_from_slice(&values[0].to_le_bytes());

    if values.len() == 1 {
        return result;
    }

    // Write first delta
    let first_delta = values[1] - values[0];
    result.extend_from_slice(&first_delta.to_le_bytes());

    // Write delta-of-deltas
    let mut prev = values[1];
    let mut prev_delta = first_delta;

    for &val in &values[2..] {
        let delta = val - prev;
        let delta_delta = delta - prev_delta;
        encode_varint_signed(&mut result, delta_delta);
        prev = val;
        prev_delta = delta;
    }

    result
}

/// Delta-of-delta decode i64 values
pub fn delta_delta_decode_i64(data: &[u8], count: usize) -> Vec<i64> {
    if data.len() < 16 || count == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(count);
    let mut pos = 0;

    // Read first value
    let first = i64::from_le_bytes(data[0..8].try_into().unwrap());
    result.push(first);
    pos = 8;

    if count == 1 {
        return result;
    }

    // Read first delta
    let first_delta = i64::from_le_bytes(data[8..16].try_into().unwrap());
    let second = first + first_delta;
    result.push(second);
    pos = 16;

    // Read delta-of-deltas
    let mut prev = second;
    let mut prev_delta = first_delta;

    while result.len() < count && pos < data.len() {
        let (delta_delta, bytes_read) = decode_varint_signed(&data[pos..]);
        pos += bytes_read;

        let delta = prev_delta + delta_delta;
        let val = prev + delta;
        result.push(val);
        prev = val;
        prev_delta = delta;
    }

    result
}

// =============================================================================
// Run-Length Encoding (for low cardinality)
// =============================================================================

/// Run-length encode i64 values
pub fn rle_encode_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut current = values[0];
    let mut count: u32 = 1;

    for &val in &values[1..] {
        if val == current && count < u32::MAX {
            count += 1;
        } else {
            // Write run
            result.extend_from_slice(&current.to_le_bytes());
            result.extend_from_slice(&count.to_le_bytes());
            current = val;
            count = 1;
        }
    }

    // Write last run
    result.extend_from_slice(&current.to_le_bytes());
    result.extend_from_slice(&count.to_le_bytes());

    result
}

/// Run-length decode i64 values
pub fn rle_decode_i64(data: &[u8], expected_count: usize) -> Vec<i64> {
    let mut result = Vec::with_capacity(expected_count);
    let mut pos = 0;

    while pos + 12 <= data.len() && result.len() < expected_count {
        let value = i64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
        let count = u32::from_le_bytes(data[pos + 8..pos + 12].try_into().unwrap()) as usize;
        pos += 12;

        for _ in 0..count.min(expected_count - result.len()) {
            result.push(value);
        }
    }

    result
}

// =============================================================================
// Dictionary Encoding (for symbols/strings)
// =============================================================================

/// Dictionary encoder for strings
pub struct DictionaryEncoder {
    dict: Vec<String>,
    index: std::collections::HashMap<String, u32>,
}

impl DictionaryEncoder {
    pub fn new() -> Self {
        Self {
            dict: Vec::new(),
            index: std::collections::HashMap::new(),
        }
    }

    /// Encode a value, returns dictionary index
    pub fn encode(&mut self, value: &str) -> u32 {
        if let Some(&idx) = self.index.get(value) {
            idx
        } else {
            let idx = self.dict.len() as u32;
            self.dict.push(value.to_string());
            self.index.insert(value.to_string(), idx);
            idx
        }
    }

    /// Get the dictionary
    pub fn dictionary(&self) -> &[String] {
        &self.dict
    }

    /// Serialize dictionary to bytes
    pub fn serialize_dict(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Write count
        result.extend_from_slice(&(self.dict.len() as u32).to_le_bytes());

        // Write strings
        for s in &self.dict {
            let bytes = s.as_bytes();
            result.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            result.extend_from_slice(bytes);
        }

        result
    }
}

impl Default for DictionaryEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Dictionary decoder for strings
pub struct DictionaryDecoder {
    dict: Vec<String>,
}

impl DictionaryDecoder {
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut dict = Vec::new();
        let mut pos = 0;

        if data.len() < 4 {
            return Self { dict };
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        pos = 4;

        for _ in 0..count {
            if pos + 4 > data.len() {
                break;
            }
            let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + len > data.len() {
                break;
            }
            let s = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
            dict.push(s);
            pos += len;
        }

        Self { dict }
    }

    pub fn decode(&self, index: u32) -> Option<&str> {
        self.dict.get(index as usize).map(|s| s.as_str())
    }
}

// =============================================================================
// Bit-Packing (for narrow integers)
// =============================================================================

/// Bit-pack u32 values with given bit width
pub fn bitpack_u32(values: &[u32], bits: u8) -> Vec<u8> {
    let total_bits = values.len() * bits as usize;
    let total_bytes = (total_bits + 7) / 8;
    let mut result = vec![0u8; total_bytes];

    let mask = (1u32 << bits) - 1;
    let mut bit_pos = 0;

    for &val in values {
        let val = val & mask;
        let byte_pos = bit_pos / 8;
        let bit_offset = bit_pos % 8;

        // Write value across bytes
        let mut remaining = bits as usize;
        let mut shift = 0;

        while remaining > 0 {
            let bits_in_byte = (8 - bit_offset).min(remaining);
            let byte_mask = ((1u32 << bits_in_byte) - 1) as u8;
            let value_part = ((val >> shift) & byte_mask as u32) as u8;

            result[byte_pos + shift / 8] |= value_part << bit_offset;

            remaining -= bits_in_byte;
            shift += bits_in_byte;
        }

        bit_pos += bits as usize;
    }

    result
}

/// Unpack bit-packed u32 values
pub fn bitunpack_u32(data: &[u8], bits: u8, count: usize) -> Vec<u32> {
    let mut result = Vec::with_capacity(count);
    let mask = (1u32 << bits) - 1;
    let mut bit_pos = 0;

    for _ in 0..count {
        let byte_pos = bit_pos / 8;
        let bit_offset = bit_pos % 8;

        if byte_pos >= data.len() {
            break;
        }

        let mut val = 0u32;
        let mut remaining = bits as usize;
        let mut shift = 0;

        while remaining > 0 && byte_pos + shift / 8 < data.len() {
            let bits_in_byte = (8 - bit_offset).min(remaining);
            let byte_mask = ((1u32 << bits_in_byte) - 1) as u8;
            let value_part = (data[byte_pos + shift / 8] >> bit_offset) & byte_mask;

            val |= (value_part as u32) << shift;

            remaining -= bits_in_byte;
            shift += bits_in_byte;
        }

        result.push(val & mask);
        bit_pos += bits as usize;
    }

    result
}

// =============================================================================
// Variable-length Integer Encoding
// =============================================================================

fn encode_varint_signed(output: &mut Vec<u8>, value: i64) {
    // ZigZag encoding
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint_unsigned(output, zigzag);
}

fn encode_varint_unsigned(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decode_varint_signed(data: &[u8]) -> (i64, usize) {
    let (zigzag, bytes) = decode_varint_unsigned(data);
    let value = ((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64);
    (value, bytes)
}

fn decode_varint_unsigned(data: &[u8]) -> (u64, usize) {
    let mut value = 0u64;
    let mut shift = 0;
    let mut bytes = 0;

    for &b in data {
        bytes += 1;
        value |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            break;
        }
        shift += 7;
    }

    (value, bytes)
}

// =============================================================================
// LZ4 Compression (wrapper)
// =============================================================================

/// Compress with LZ4 (placeholder - would use lz4 crate)
pub fn lz4_compress(data: &[u8]) -> Vec<u8> {
    // In production, use lz4::compress
    data.to_vec()
}

/// Decompress with LZ4
pub fn lz4_decompress(data: &[u8], _original_size: usize) -> Vec<u8> {
    // In production, use lz4::decompress
    data.to_vec()
}

// =============================================================================
// Auto-select Compression
// =============================================================================

/// Analyze data and select best compression codec
pub fn auto_select_codec(values: &[i64]) -> Codec {
    if values.is_empty() {
        return Codec::None;
    }

    // Check if sorted (delta encoding)
    let is_sorted = values.windows(2).all(|w| w[0] <= w[1]);

    // Check cardinality
    let unique: std::collections::HashSet<_> = values.iter().collect();
    let cardinality = unique.len();

    // Check for timestamp-like pattern (regular intervals)
    let is_timestamp_like = if values.len() > 2 {
        let deltas: Vec<i64> = values.windows(2).map(|w| w[1] - w[0]).collect();
        let delta_deltas: Vec<i64> = deltas.windows(2).map(|w| w[1] - w[0]).collect();
        let zero_count = delta_deltas.iter().filter(|&&x| x == 0).count();
        zero_count > delta_deltas.len() / 2
    } else {
        false
    };

    if is_timestamp_like {
        Codec::DeltaDelta
    } else if is_sorted {
        Codec::Delta
    } else if cardinality < values.len() / 10 {
        Codec::RunLength
    } else {
        Codec::LZ4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_roundtrip() {
        let values: Vec<i64> = (0..1000).map(|x| x * 100).collect();
        let encoded = delta_encode_i64(&values);
        let decoded = delta_decode_i64(&encoded, values.len());
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_delta_delta_roundtrip() {
        // Simulate timestamps with regular intervals
        let values: Vec<i64> = (0..1000).map(|x| 1000000 + x * 1000000).collect();
        let encoded = delta_delta_encode_i64(&values);
        let decoded = delta_delta_decode_i64(&encoded, values.len());
        assert_eq!(values, decoded);
    }

    #[test]
    fn test_rle_roundtrip() {
        let values = vec![1i64, 1, 1, 2, 2, 3, 3, 3, 3, 3];
        let encoded = rle_encode_i64(&values);
        let decoded = rle_decode_i64(&encoded, values.len());
        assert_eq!(values, decoded);
    }
}
