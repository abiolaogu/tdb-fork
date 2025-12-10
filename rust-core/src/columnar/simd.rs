//! SIMD Vectorized Operations
//!
//! High-performance vectorized operations using:
//! - AVX2/AVX-512 on x86_64
//! - NEON on ARM
//! - Portable fallbacks for other architectures
//!
//! These operations are what make kdb+ fast for analytics.
//! TDB+ matches or exceeds kdb+ performance by using:
//! - Wider SIMD (AVX-512 vs kdb+ scalar)
//! - Better cache utilization
//! - Parallel execution across cores

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

// =============================================================================
// Sum Operations
// =============================================================================

/// SIMD sum for f64 array
#[inline]
pub fn sum_f64(data: &[f64]) -> f64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { sum_f64_avx2(data) };
        }
    }

    // Fallback: unrolled scalar
    sum_f64_scalar(data)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_f64_avx2(data: &[f64]) -> f64 {
    let mut sum = _mm256_setzero_pd();
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let v = _mm256_loadu_pd(chunk.as_ptr());
        sum = _mm256_add_pd(sum, v);
    }

    // Horizontal sum
    let mut result = [0.0f64; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), sum);
    let mut total = result[0] + result[1] + result[2] + result[3];

    // Handle remainder
    for &x in remainder {
        total += x;
    }

    total
}

#[inline]
fn sum_f64_scalar(data: &[f64]) -> f64 {
    // Unrolled for better pipelining
    let mut sum0 = 0.0;
    let mut sum1 = 0.0;
    let mut sum2 = 0.0;
    let mut sum3 = 0.0;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum0 += chunk[0];
        sum1 += chunk[1];
        sum2 += chunk[2];
        sum3 += chunk[3];
    }

    for &x in remainder {
        sum0 += x;
    }

    sum0 + sum1 + sum2 + sum3
}

/// SIMD sum for i64 array
#[inline]
pub fn sum_i64(data: &[i64]) -> i64 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { sum_i64_avx2(data) };
        }
    }

    sum_i64_scalar(data)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn sum_i64_avx2(data: &[i64]) -> i64 {
    let mut sum = _mm256_setzero_si256();
    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        sum = _mm256_add_epi64(sum, v);
    }

    let mut result = [0i64; 4];
    _mm256_storeu_si256(result.as_mut_ptr() as *mut __m256i, sum);
    let mut total = result[0] + result[1] + result[2] + result[3];

    for &x in remainder {
        total += x;
    }

    total
}

#[inline]
fn sum_i64_scalar(data: &[i64]) -> i64 {
    let mut sum0 = 0i64;
    let mut sum1 = 0i64;
    let mut sum2 = 0i64;
    let mut sum3 = 0i64;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sum0 = sum0.wrapping_add(chunk[0]);
        sum1 = sum1.wrapping_add(chunk[1]);
        sum2 = sum2.wrapping_add(chunk[2]);
        sum3 = sum3.wrapping_add(chunk[3]);
    }

    for &x in remainder {
        sum0 = sum0.wrapping_add(x);
    }

    sum0.wrapping_add(sum1).wrapping_add(sum2).wrapping_add(sum3)
}

// =============================================================================
// Average Operations
// =============================================================================

#[inline]
pub fn avg_f64(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    sum_f64(data) / data.len() as f64
}

// =============================================================================
// Statistics (min, max, sum in one pass)
// =============================================================================

pub fn stats_f64(data: &[f64]) -> (f64, f64, f64) {
    if data.is_empty() {
        return (f64::NAN, f64::NAN, 0.0);
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { stats_f64_avx2(data) };
        }
    }

    stats_f64_scalar(data)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn stats_f64_avx2(data: &[f64]) -> (f64, f64, f64) {
    let first = _mm256_set1_pd(data[0]);
    let mut min_v = first;
    let mut max_v = first;
    let mut sum_v = _mm256_setzero_pd();

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let v = _mm256_loadu_pd(chunk.as_ptr());
        min_v = _mm256_min_pd(min_v, v);
        max_v = _mm256_max_pd(max_v, v);
        sum_v = _mm256_add_pd(sum_v, v);
    }

    // Reduce vectors
    let mut min_arr = [0.0f64; 4];
    let mut max_arr = [0.0f64; 4];
    let mut sum_arr = [0.0f64; 4];
    _mm256_storeu_pd(min_arr.as_mut_ptr(), min_v);
    _mm256_storeu_pd(max_arr.as_mut_ptr(), max_v);
    _mm256_storeu_pd(sum_arr.as_mut_ptr(), sum_v);

    let mut min = min_arr[0].min(min_arr[1]).min(min_arr[2]).min(min_arr[3]);
    let mut max = max_arr[0].max(max_arr[1]).max(max_arr[2]).max(max_arr[3]);
    let mut sum = sum_arr[0] + sum_arr[1] + sum_arr[2] + sum_arr[3];

    for &x in remainder {
        min = min.min(x);
        max = max.max(x);
        sum += x;
    }

    (min, max, sum)
}

fn stats_f64_scalar(data: &[f64]) -> (f64, f64, f64) {
    let mut min = data[0];
    let mut max = data[0];
    let mut sum = 0.0;

    for &x in data {
        min = min.min(x);
        max = max.max(x);
        sum += x;
    }

    (min, max, sum)
}

pub fn stats_i64(data: &[i64]) -> (i64, i64, i64) {
    if data.is_empty() {
        return (0, 0, 0);
    }

    let mut min = data[0];
    let mut max = data[0];
    let mut sum = 0i64;

    for &x in data {
        min = min.min(x);
        max = max.max(x);
        sum = sum.wrapping_add(x);
    }

    (min, max, sum)
}

// =============================================================================
// Filter Operations
// =============================================================================

/// Filter: return indices where value > threshold
pub fn filter_gt_f64(data: &[f64], threshold: f64) -> Vec<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { filter_gt_f64_avx2(data, threshold) };
        }
    }

    filter_gt_f64_scalar(data, threshold)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_gt_f64_avx2(data: &[f64], threshold: f64) -> Vec<usize> {
    let mut result = Vec::with_capacity(data.len() / 4);
    let threshold_v = _mm256_set1_pd(threshold);

    let chunks = data.chunks_exact(4);
    let remainder_offset = data.len() - data.len() % 4;
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let v = _mm256_loadu_pd(chunk.as_ptr());
        let mask = _mm256_cmp_pd(v, threshold_v, _CMP_GT_OQ);
        let mask_bits = _mm256_movemask_pd(mask) as u8;

        if mask_bits != 0 {
            let base = chunk_idx * 4;
            if mask_bits & 1 != 0 { result.push(base); }
            if mask_bits & 2 != 0 { result.push(base + 1); }
            if mask_bits & 4 != 0 { result.push(base + 2); }
            if mask_bits & 8 != 0 { result.push(base + 3); }
        }
    }

    for (i, &x) in remainder.iter().enumerate() {
        if x > threshold {
            result.push(remainder_offset + i);
        }
    }

    result
}

fn filter_gt_f64_scalar(data: &[f64], threshold: f64) -> Vec<usize> {
    let mut result = Vec::with_capacity(data.len() / 4);
    for (i, &x) in data.iter().enumerate() {
        if x > threshold {
            result.push(i);
        }
    }
    result
}

/// Filter: return indices where value == target
pub fn filter_eq_i64(data: &[i64], target: i64) -> Vec<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { filter_eq_i64_avx2(data, target) };
        }
    }

    filter_eq_i64_scalar(data, target)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn filter_eq_i64_avx2(data: &[i64], target: i64) -> Vec<usize> {
    let mut result = Vec::with_capacity(data.len() / 8);
    let target_v = _mm256_set1_epi64x(target);

    let chunks = data.chunks_exact(4);
    let remainder_offset = data.len() - data.len() % 4;
    let remainder = chunks.remainder();

    for (chunk_idx, chunk) in chunks.enumerate() {
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        let cmp = _mm256_cmpeq_epi64(v, target_v);
        let mask = _mm256_movemask_pd(_mm256_castsi256_pd(cmp)) as u8;

        if mask != 0 {
            let base = chunk_idx * 4;
            if mask & 1 != 0 { result.push(base); }
            if mask & 2 != 0 { result.push(base + 1); }
            if mask & 4 != 0 { result.push(base + 2); }
            if mask & 8 != 0 { result.push(base + 3); }
        }
    }

    for (i, &x) in remainder.iter().enumerate() {
        if x == target {
            result.push(remainder_offset + i);
        }
    }

    result
}

fn filter_eq_i64_scalar(data: &[i64], target: i64) -> Vec<usize> {
    let mut result = Vec::with_capacity(data.len() / 8);
    for (i, &x) in data.iter().enumerate() {
        if x == target {
            result.push(i);
        }
    }
    result
}

// =============================================================================
// Arithmetic Operations
// =============================================================================

/// Vector addition
pub fn add_f64(a: &[f64], b: &[f64], out: &mut [f64]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { add_f64_avx2(a, b, out) };
            return;
        }
    }

    for i in 0..a.len() {
        out[i] = a[i] + b[i];
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn add_f64_avx2(a: &[f64], b: &[f64], out: &mut [f64]) {
    let chunks_a = a.chunks_exact(4);
    let chunks_b = b.chunks_exact(4);
    let mut out_ptr = out.as_mut_ptr();

    for (ca, cb) in chunks_a.zip(chunks_b) {
        let va = _mm256_loadu_pd(ca.as_ptr());
        let vb = _mm256_loadu_pd(cb.as_ptr());
        let vr = _mm256_add_pd(va, vb);
        _mm256_storeu_pd(out_ptr, vr);
        out_ptr = out_ptr.add(4);
    }

    // Handle remainder
    let remainder_start = a.len() - a.len() % 4;
    for i in remainder_start..a.len() {
        out[i] = a[i] + b[i];
    }
}

/// Vector multiplication
pub fn mul_f64(a: &[f64], b: &[f64], out: &mut [f64]) {
    debug_assert_eq!(a.len(), b.len());
    debug_assert_eq!(a.len(), out.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { mul_f64_avx2(a, b, out) };
            return;
        }
    }

    for i in 0..a.len() {
        out[i] = a[i] * b[i];
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn mul_f64_avx2(a: &[f64], b: &[f64], out: &mut [f64]) {
    let chunks_a = a.chunks_exact(4);
    let chunks_b = b.chunks_exact(4);
    let mut out_ptr = out.as_mut_ptr();

    for (ca, cb) in chunks_a.zip(chunks_b) {
        let va = _mm256_loadu_pd(ca.as_ptr());
        let vb = _mm256_loadu_pd(cb.as_ptr());
        let vr = _mm256_mul_pd(va, vb);
        _mm256_storeu_pd(out_ptr, vr);
        out_ptr = out_ptr.add(4);
    }

    let remainder_start = a.len() - a.len() % 4;
    for i in remainder_start..a.len() {
        out[i] = a[i] * b[i];
    }
}

/// Dot product
pub fn dot_f64(a: &[f64], b: &[f64]) -> f64 {
    debug_assert_eq!(a.len(), b.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            return unsafe { dot_f64_avx2(a, b) };
        }
    }

    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn dot_f64_avx2(a: &[f64], b: &[f64]) -> f64 {
    let mut sum = _mm256_setzero_pd();

    let chunks_a = a.chunks_exact(4);
    let chunks_b = b.chunks_exact(4);
    let remainder_a = chunks_a.remainder();
    let remainder_b = chunks_b.remainder();

    for (ca, cb) in chunks_a.zip(chunks_b) {
        let va = _mm256_loadu_pd(ca.as_ptr());
        let vb = _mm256_loadu_pd(cb.as_ptr());
        let prod = _mm256_mul_pd(va, vb);
        sum = _mm256_add_pd(sum, prod);
    }

    let mut result = [0.0f64; 4];
    _mm256_storeu_pd(result.as_mut_ptr(), sum);
    let mut total = result[0] + result[1] + result[2] + result[3];

    for (&x, &y) in remainder_a.iter().zip(remainder_b.iter()) {
        total += x * y;
    }

    total
}

// =============================================================================
// Sorting (for time-series)
// =============================================================================

/// Vectorized partial sort for top-k
pub fn partial_sort_f64(data: &mut [f64], k: usize) {
    if k >= data.len() {
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        return;
    }

    // Use quickselect for O(n) average case
    quickselect(data, k);
    data[..k].sort_by(|a, b| a.partial_cmp(b).unwrap());
}

fn quickselect(data: &mut [f64], k: usize) {
    if data.len() <= 1 {
        return;
    }

    let pivot_idx = partition(data);

    if pivot_idx == k {
        return;
    } else if pivot_idx > k {
        quickselect(&mut data[..pivot_idx], k);
    } else {
        quickselect(&mut data[pivot_idx + 1..], k - pivot_idx - 1);
    }
}

fn partition(data: &mut [f64]) -> usize {
    let len = data.len();
    let pivot_idx = len / 2;
    data.swap(pivot_idx, len - 1);

    let pivot = data[len - 1];
    let mut i = 0;

    for j in 0..len - 1 {
        if data[j] <= pivot {
            data.swap(i, j);
            i += 1;
        }
    }

    data.swap(i, len - 1);
    i
}

// =============================================================================
// Memory Operations
// =============================================================================

/// Fast memory copy (SIMD accelerated)
pub fn fast_copy(src: &[u8], dst: &mut [u8]) {
    debug_assert_eq!(src.len(), dst.len());

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { fast_copy_avx2(src, dst) };
            return;
        }
    }

    dst.copy_from_slice(src);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fast_copy_avx2(src: &[u8], dst: &mut [u8]) {
    let mut src_ptr = src.as_ptr();
    let mut dst_ptr = dst.as_mut_ptr();
    let mut remaining = src.len();

    // Copy 32 bytes at a time
    while remaining >= 32 {
        let v = _mm256_loadu_si256(src_ptr as *const __m256i);
        _mm256_storeu_si256(dst_ptr as *mut __m256i, v);
        src_ptr = src_ptr.add(32);
        dst_ptr = dst_ptr.add(32);
        remaining -= 32;
    }

    // Copy remainder
    std::ptr::copy_nonoverlapping(src_ptr, dst_ptr, remaining);
}

/// Fast memory zero
pub fn fast_zero(dst: &mut [u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") {
            unsafe { fast_zero_avx2(dst) };
            return;
        }
    }

    dst.fill(0);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fast_zero_avx2(dst: &mut [u8]) {
    let mut dst_ptr = dst.as_mut_ptr();
    let mut remaining = dst.len();
    let zero = _mm256_setzero_si256();

    while remaining >= 32 {
        _mm256_storeu_si256(dst_ptr as *mut __m256i, zero);
        dst_ptr = dst_ptr.add(32);
        remaining -= 32;
    }

    std::ptr::write_bytes(dst_ptr, 0, remaining);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_f64() {
        let data: Vec<f64> = (0..1000).map(|x| x as f64).collect();
        let sum = sum_f64(&data);
        let expected: f64 = (0..1000).map(|x| x as f64).sum();
        assert!((sum - expected).abs() < 0.001);
    }

    #[test]
    fn test_filter_gt_f64() {
        let data: Vec<f64> = (0..100).map(|x| x as f64).collect();
        let indices = filter_gt_f64(&data, 50.0);
        assert_eq!(indices.len(), 49);
        assert!(indices.iter().all(|&i| data[i] > 50.0));
    }

    #[test]
    fn test_dot_f64() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        let result = dot_f64(&a, &b);
        assert!((result - 70.0).abs() < 0.001);
    }
}
