//! Vector Operations for Columnar Data
//!
//! kdb+-style vector operations that operate on entire columns at once.
//! These are the building blocks for high-performance analytics.

use super::simd;

/// Trait for vectorized operations
pub trait VectorOps<T> {
    fn sum(&self) -> T;
    fn avg(&self) -> f64;
    fn min(&self) -> T;
    fn max(&self) -> T;
    fn count(&self) -> usize;
}

impl VectorOps<f64> for [f64] {
    fn sum(&self) -> f64 {
        simd::sum_f64(self)
    }

    fn avg(&self) -> f64 {
        simd::avg_f64(self)
    }

    fn min(&self) -> f64 {
        let (min, _, _) = simd::stats_f64(self);
        min
    }

    fn max(&self) -> f64 {
        let (_, max, _) = simd::stats_f64(self);
        max
    }

    fn count(&self) -> usize {
        self.len()
    }
}

impl VectorOps<i64> for [i64] {
    fn sum(&self) -> i64 {
        simd::sum_i64(self)
    }

    fn avg(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        self.sum() as f64 / self.len() as f64
    }

    fn min(&self) -> i64 {
        let (min, _, _) = simd::stats_i64(self);
        min
    }

    fn max(&self) -> i64 {
        let (_, max, _) = simd::stats_i64(self);
        max
    }

    fn count(&self) -> usize {
        self.len()
    }
}

/// Extension trait for vector arithmetic
pub trait VectorArith<T> {
    fn add(&self, other: &Self) -> Vec<T>;
    fn sub(&self, other: &Self) -> Vec<T>;
    fn mul(&self, other: &Self) -> Vec<T>;
    fn div(&self, other: &Self) -> Vec<T>;
    fn scale(&self, factor: T) -> Vec<T>;
}

impl VectorArith<f64> for [f64] {
    fn add(&self, other: &Self) -> Vec<f64> {
        let mut result = vec![0.0; self.len()];
        simd::add_f64(self, other, &mut result);
        result
    }

    fn sub(&self, other: &Self) -> Vec<f64> {
        self.iter().zip(other.iter()).map(|(a, b)| a - b).collect()
    }

    fn mul(&self, other: &Self) -> Vec<f64> {
        let mut result = vec![0.0; self.len()];
        simd::mul_f64(self, other, &mut result);
        result
    }

    fn div(&self, other: &Self) -> Vec<f64> {
        self.iter().zip(other.iter()).map(|(a, b)| a / b).collect()
    }

    fn scale(&self, factor: f64) -> Vec<f64> {
        self.iter().map(|&x| x * factor).collect()
    }
}

/// Extension trait for filtering
pub trait VectorFilter<T> {
    fn where_gt(&self, threshold: T) -> Vec<usize>;
    fn where_lt(&self, threshold: T) -> Vec<usize>;
    fn where_eq(&self, value: T) -> Vec<usize>;
    fn where_ne(&self, value: T) -> Vec<usize>;
    fn take_indices(&self, indices: &[usize]) -> Vec<T>;
}

impl VectorFilter<f64> for [f64] {
    fn where_gt(&self, threshold: f64) -> Vec<usize> {
        simd::filter_gt_f64(self, threshold)
    }

    fn where_lt(&self, threshold: f64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| x < threshold)
            .map(|(i, _)| i)
            .collect()
    }

    fn where_eq(&self, value: f64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| (x - value).abs() < f64::EPSILON)
            .map(|(i, _)| i)
            .collect()
    }

    fn where_ne(&self, value: f64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| (x - value).abs() >= f64::EPSILON)
            .map(|(i, _)| i)
            .collect()
    }

    fn take_indices(&self, indices: &[usize]) -> Vec<f64> {
        indices.iter().filter_map(|&i| self.get(i).copied()).collect()
    }
}

impl VectorFilter<i64> for [i64] {
    fn where_gt(&self, threshold: i64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| x > threshold)
            .map(|(i, _)| i)
            .collect()
    }

    fn where_lt(&self, threshold: i64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| x < threshold)
            .map(|(i, _)| i)
            .collect()
    }

    fn where_eq(&self, value: i64) -> Vec<usize> {
        simd::filter_eq_i64(self, value)
    }

    fn where_ne(&self, value: i64) -> Vec<usize> {
        self.iter()
            .enumerate()
            .filter(|(_, &x)| x != value)
            .map(|(i, _)| i)
            .collect()
    }

    fn take_indices(&self, indices: &[usize]) -> Vec<i64> {
        indices.iter().filter_map(|&i| self.get(i).copied()).collect()
    }
}

/// Rolling/windowed operations
pub trait VectorRolling {
    fn rolling_sum(&self, window: usize) -> Vec<f64>;
    fn rolling_avg(&self, window: usize) -> Vec<f64>;
    fn rolling_min(&self, window: usize) -> Vec<f64>;
    fn rolling_max(&self, window: usize) -> Vec<f64>;
    fn rolling_std(&self, window: usize) -> Vec<f64>;
}

impl VectorRolling for [f64] {
    fn rolling_sum(&self, window: usize) -> Vec<f64> {
        if self.len() < window {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.len() - window + 1);
        let mut sum: f64 = self[..window].iter().sum();
        result.push(sum);

        for i in window..self.len() {
            sum += self[i] - self[i - window];
            result.push(sum);
        }

        result
    }

    fn rolling_avg(&self, window: usize) -> Vec<f64> {
        self.rolling_sum(window)
            .into_iter()
            .map(|s| s / window as f64)
            .collect()
    }

    fn rolling_min(&self, window: usize) -> Vec<f64> {
        if self.len() < window {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.len() - window + 1);

        // Use deque for O(n) rolling min
        let mut deque: std::collections::VecDeque<usize> = std::collections::VecDeque::new();

        for i in 0..self.len() {
            // Remove elements outside window
            while !deque.is_empty() && *deque.front().unwrap() + window <= i {
                deque.pop_front();
            }

            // Remove elements larger than current
            while !deque.is_empty() && self[*deque.back().unwrap()] >= self[i] {
                deque.pop_back();
            }

            deque.push_back(i);

            if i >= window - 1 {
                result.push(self[*deque.front().unwrap()]);
            }
        }

        result
    }

    fn rolling_max(&self, window: usize) -> Vec<f64> {
        if self.len() < window {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.len() - window + 1);
        let mut deque: std::collections::VecDeque<usize> = std::collections::VecDeque::new();

        for i in 0..self.len() {
            while !deque.is_empty() && *deque.front().unwrap() + window <= i {
                deque.pop_front();
            }

            while !deque.is_empty() && self[*deque.back().unwrap()] <= self[i] {
                deque.pop_back();
            }

            deque.push_back(i);

            if i >= window - 1 {
                result.push(self[*deque.front().unwrap()]);
            }
        }

        result
    }

    fn rolling_std(&self, window: usize) -> Vec<f64> {
        let avg = self.rolling_avg(window);
        let mut result = Vec::with_capacity(avg.len());

        for (i, &mean) in avg.iter().enumerate() {
            let variance: f64 = self[i..i + window]
                .iter()
                .map(|x| (x - mean).powi(2))
                .sum::<f64>()
                / window as f64;
            result.push(variance.sqrt());
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_ops() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(data.sum(), 15.0);
        assert_eq!(data.avg(), 3.0);
        assert_eq!(data.min(), 1.0);
        assert_eq!(data.max(), 5.0);
    }

    #[test]
    fn test_vector_filter() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let indices = data.where_gt(3.0);
        assert_eq!(indices, vec![3, 4]);
    }

    #[test]
    fn test_rolling_avg() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let avg = data.rolling_avg(3);
        assert_eq!(avg.len(), 3);
        assert!((avg[0] - 2.0).abs() < 0.001);
        assert!((avg[1] - 3.0).abs() < 0.001);
        assert!((avg[2] - 4.0).abs() < 0.001);
    }
}
