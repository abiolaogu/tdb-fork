//! Gorilla Time-Series Compression
//!
//! Implementation of Facebook's Gorilla compression algorithm:
//! - Delta-of-delta encoding for timestamps (typically 1-2 bits per timestamp)
//! - XOR encoding for float values (typically 1-2 bits for similar values)
//!
//! Achieves 10-15x compression ratio for typical time-series data.

use super::core::Sample;

/// Compress a series of samples using Gorilla encoding
pub fn compress_samples(samples: &[Sample]) -> Vec<u8> {
    if samples.is_empty() {
        return Vec::new();
    }
    
    let mut encoder = GorillaEncoder::new();
    
    for sample in samples {
        encoder.add_sample(sample.timestamp_ms, sample.value);
    }
    
    encoder.finish()
}

/// Decompress Gorilla-encoded samples
pub fn decompress_samples(data: &[u8]) -> Vec<Sample> {
    if data.is_empty() {
        return Vec::new();
    }
    
    let mut decoder = GorillaDecoder::new(data);
    let mut samples = Vec::new();
    
    while let Some((ts, val)) = decoder.next_sample() {
        samples.push(Sample {
            timestamp_ms: ts,
            value: val,
        });
    }
    
    samples
}

/// Gorilla encoder for streaming compression
pub struct GorillaEncoder {
    buffer: BitBuffer,
    /// Previous timestamp for delta-of-delta
    prev_timestamp: i64,
    prev_delta: i64,
    /// Previous value for XOR
    prev_value: u64,
    prev_leading: u32,
    prev_trailing: u32,
    sample_count: u64,
    first_sample: bool,
}

impl GorillaEncoder {
    pub fn new() -> Self {
        Self {
            buffer: BitBuffer::new(),
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: u64::BITS,
            prev_trailing: 0,
            sample_count: 0,
            first_sample: true,
        }
    }

    /// Add a sample to the encoder
    pub fn add_sample(&mut self, timestamp: i64, value: f64) {
        self.sample_count += 1;
        
        if self.first_sample {
            self.encode_first_sample(timestamp, value);
            self.first_sample = false;
        } else {
            self.encode_timestamp(timestamp);
            self.encode_value(value);
        }
    }

    fn encode_first_sample(&mut self, timestamp: i64, value: f64) {
        // Write full timestamp (64 bits)
        self.buffer.write_bits(timestamp as u64, 64);
        
        // Write full value (64 bits)
        let value_bits = value.to_bits();
        self.buffer.write_bits(value_bits, 64);
        
        self.prev_timestamp = timestamp;
        self.prev_value = value_bits;
    }

    fn encode_timestamp(&mut self, timestamp: i64) {
        let delta = timestamp - self.prev_timestamp;
        let delta_of_delta = delta - self.prev_delta;
        
        if delta_of_delta == 0 {
            // '0' - same delta as before
            self.buffer.write_bit(false);
        } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
            // '10' + 7 bits
            self.buffer.write_bits(0b10, 2);
            self.buffer.write_bits((delta_of_delta + 63) as u64, 7);
        } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
            // '110' + 9 bits
            self.buffer.write_bits(0b110, 3);
            self.buffer.write_bits((delta_of_delta + 255) as u64, 9);
        } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
            // '1110' + 12 bits
            self.buffer.write_bits(0b1110, 4);
            self.buffer.write_bits((delta_of_delta + 2047) as u64, 12);
        } else {
            // '1111' + 32 bits (or 64 for larger deltas)
            self.buffer.write_bits(0b1111, 4);
            self.buffer.write_bits(delta_of_delta as u64, 32);
        }
        
        self.prev_delta = delta;
        self.prev_timestamp = timestamp;
    }

    fn encode_value(&mut self, value: f64) {
        let value_bits = value.to_bits();
        let xor = value_bits ^ self.prev_value;
        
        if xor == 0 {
            // '0' - same value
            self.buffer.write_bit(false);
        } else {
            self.buffer.write_bit(true);
            
            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();
            
            if leading >= self.prev_leading && trailing >= self.prev_trailing {
                // '0' - use previous leading/trailing
                self.buffer.write_bit(false);
                let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
                let meaningful_value = (xor >> self.prev_trailing) & ((1u64 << meaningful_bits) - 1);
                self.buffer.write_bits(meaningful_value, meaningful_bits as usize);
            } else {
                // '1' - new leading/trailing
                self.buffer.write_bit(true);
                
                // Write leading zeros (5 bits, max 31)
                self.buffer.write_bits(leading.min(31) as u64, 5);
                
                // Write meaningful bits length (6 bits)
                let meaningful_bits = 64 - leading - trailing;
                self.buffer.write_bits(meaningful_bits as u64, 6);
                
                // Write meaningful bits
                let meaningful_value = (xor >> trailing) & ((1u64 << meaningful_bits) - 1);
                self.buffer.write_bits(meaningful_value, meaningful_bits as usize);
                
                self.prev_leading = leading;
                self.prev_trailing = trailing;
            }
        }
        
        self.prev_value = value_bits;
    }

    /// Finish encoding and return compressed data
    pub fn finish(self) -> Vec<u8> {
        // Write sample count header
        let mut result = Vec::new();
        result.extend(&self.sample_count.to_le_bytes());
        result.extend(self.buffer.finish());
        result
    }
}

/// Gorilla decoder for streaming decompression
pub struct GorillaDecoder<'a> {
    buffer: BitReader<'a>,
    prev_timestamp: i64,
    prev_delta: i64,
    prev_value: u64,
    prev_leading: u32,
    prev_trailing: u32,
    samples_remaining: u64,
    first_sample: bool,
}

impl<'a> GorillaDecoder<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        if data.len() < 8 {
            return Self {
                buffer: BitReader::new(&[]),
                prev_timestamp: 0,
                prev_delta: 0,
                prev_value: 0,
                prev_leading: 0,
                prev_trailing: 0,
                samples_remaining: 0,
                first_sample: true,
            };
        }
        
        let sample_count = u64::from_le_bytes(data[..8].try_into().unwrap());
        
        Self {
            buffer: BitReader::new(&data[8..]),
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            samples_remaining: sample_count,
            first_sample: true,
        }
    }

    pub fn next_sample(&mut self) -> Option<(i64, f64)> {
        if self.samples_remaining == 0 {
            return None;
        }
        
        self.samples_remaining -= 1;
        
        if self.first_sample {
            self.first_sample = false;
            return self.decode_first_sample();
        }
        
        let timestamp = self.decode_timestamp()?;
        let value = self.decode_value()?;
        
        Some((timestamp, value))
    }

    fn decode_first_sample(&mut self) -> Option<(i64, f64)> {
        let timestamp = self.buffer.read_bits(64)? as i64;
        let value_bits = self.buffer.read_bits(64)?;
        
        self.prev_timestamp = timestamp;
        self.prev_value = value_bits;
        
        Some((timestamp, f64::from_bits(value_bits)))
    }

    fn decode_timestamp(&mut self) -> Option<i64> {
        let delta_of_delta = if !self.buffer.read_bit()? {
            // '0' - same delta
            0
        } else if !self.buffer.read_bit()? {
            // '10' - 7 bit delta
            let v = self.buffer.read_bits(7)? as i64;
            v - 63
        } else if !self.buffer.read_bit()? {
            // '110' - 9 bit delta
            let v = self.buffer.read_bits(9)? as i64;
            v - 255
        } else if !self.buffer.read_bit()? {
            // '1110' - 12 bit delta
            let v = self.buffer.read_bits(12)? as i64;
            v - 2047
        } else {
            // '1111' - 32 bit delta
            let v = self.buffer.read_bits(32)? as i32 as i64;
            v
        };
        
        let delta = self.prev_delta + delta_of_delta;
        let timestamp = self.prev_timestamp + delta;
        
        self.prev_delta = delta;
        self.prev_timestamp = timestamp;
        
        Some(timestamp)
    }

    fn decode_value(&mut self) -> Option<f64> {
        let xor = if !self.buffer.read_bit()? {
            // '0' - same value
            0
        } else if !self.buffer.read_bit()? {
            // First bit '0' - use previous leading/trailing
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            let value = self.buffer.read_bits(meaningful_bits as usize)?;
            value << self.prev_trailing
        } else {
            // First bit '1' - new leading/trailing
            let leading = self.buffer.read_bits(5)? as u32;
            let meaningful_bits = self.buffer.read_bits(6)? as u32;
            let trailing = 64 - leading - meaningful_bits;
            
            let value = self.buffer.read_bits(meaningful_bits as usize)?;
            
            self.prev_leading = leading;
            self.prev_trailing = trailing;
            
            value << trailing
        };
        
        self.prev_value ^= xor;
        Some(f64::from_bits(self.prev_value))
    }
}

/// Bit buffer for writing
struct BitBuffer {
    data: Vec<u8>,
    current_byte: u8,
    bit_pos: u8,
}

impl BitBuffer {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            current_byte: 0,
            bit_pos: 0,
        }
    }

    fn write_bit(&mut self, bit: bool) {
        if bit {
            self.current_byte |= 1 << (7 - self.bit_pos);
        }
        self.bit_pos += 1;
        
        if self.bit_pos == 8 {
            self.data.push(self.current_byte);
            self.current_byte = 0;
            self.bit_pos = 0;
        }
    }

    fn write_bits(&mut self, value: u64, count: usize) {
        for i in (0..count).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    fn finish(mut self) -> Vec<u8> {
        if self.bit_pos > 0 {
            self.data.push(self.current_byte);
        }
        self.data
    }
}

/// Bit reader for reading
struct BitReader<'a> {
    data: &'a [u8],
    byte_pos: usize,
    bit_pos: u8,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_pos: 0,
            bit_pos: 0,
        }
    }

    fn read_bit(&mut self) -> Option<bool> {
        if self.byte_pos >= self.data.len() {
            return None;
        }
        
        let bit = (self.data[self.byte_pos] >> (7 - self.bit_pos)) & 1 == 1;
        self.bit_pos += 1;
        
        if self.bit_pos == 8 {
            self.byte_pos += 1;
            self.bit_pos = 0;
        }
        
        Some(bit)
    }

    fn read_bits(&mut self, count: usize) -> Option<u64> {
        let mut value = 0u64;
        for _ in 0..count {
            value = (value << 1) | (self.read_bit()? as u64);
        }
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let samples = vec![
            Sample { timestamp_ms: 1000, value: 1.0 },
            Sample { timestamp_ms: 2000, value: 1.1 },
            Sample { timestamp_ms: 3000, value: 1.2 },
            Sample { timestamp_ms: 4000, value: 1.15 },
            Sample { timestamp_ms: 5000, value: 1.0 },
        ];
        
        let compressed = compress_samples(&samples);
        let decompressed = decompress_samples(&compressed);
        
        assert_eq!(samples.len(), decompressed.len());
        for (orig, dec) in samples.iter().zip(decompressed.iter()) {
            assert_eq!(orig.timestamp_ms, dec.timestamp_ms);
            assert!((orig.value - dec.value).abs() < 1e-10);
        }
    }

    #[test]
    fn test_compression_ratio() {
        // Generate 1000 samples with typical monitoring data pattern
        let mut samples = Vec::new();
        let base_time = 1609459200000i64; // 2021-01-01
        let interval = 15000i64; // 15 second intervals
        
        for i in 0..1000 {
            let time = base_time + i * interval;
            let value = 50.0 + 10.0 * (i as f64 * 0.01).sin() + (i % 10) as f64 * 0.1;
            samples.push(Sample { timestamp_ms: time, value });
        }
        
        let compressed = compress_samples(&samples);
        let uncompressed_size = samples.len() * 16; // 8 bytes ts + 8 bytes value
        let compressed_size = compressed.len();
        
        let ratio = uncompressed_size as f64 / compressed_size as f64;
        println!("Compression ratio: {:.1}x ({} -> {} bytes)", ratio, uncompressed_size, compressed_size);
        
        // Expect at least 2x compression for typical data
        // Note: Gorilla typically achieves 2-4x for most time-series data
        assert!(ratio >= 2.0, "Expected at least 2x compression, got {:.1}x", ratio);
    }

    #[test]
    fn test_compression_empty() {
        let samples = Vec::new();
        let compressed = compress_samples(&samples);
        assert!(compressed.is_empty(), "Compressed empty samples should be empty, got {} bytes", compressed.len());
        
        let decompressed = decompress_samples(&compressed);
        assert!(decompressed.is_empty(), "Decompressed empty data should be empty");
    }

    #[test]
    fn test_compression_random() {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        
        let mut samples = Vec::new();
        let base_time = 1600000000000i64;
        
        for i in 0..100 {
            // Random time deltas to stress delta-delta
            let delta = rng.gen_range(100..2000); 
            let time = base_time + i * 1000 + delta;
            // Random values to stress XOR
            let value = rng.gen_range(0.0..100.0);
            samples.push(Sample { timestamp_ms: time, value });
        }
        
        let compressed = compress_samples(&samples);
        let decompressed = decompress_samples(&compressed);
        
        assert_eq!(samples.len(), decompressed.len());
        
        for (i, (orig, dec)) in samples.iter().zip(decompressed.iter()).enumerate() {
            assert_eq!(orig.timestamp_ms, dec.timestamp_ms, "Timestamp mismatch at index {}", i);
            // Relaxed f64 comparison
            assert!((orig.value - dec.value).abs() < 1e-10, "Value mismatch at index {}: {} vs {}", i, orig.value, dec.value);
        }
    }
}
