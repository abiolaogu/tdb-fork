//! Intelligent Prefetching
//!
//! Predicts access patterns and prefetches data to minimize latency.
//! Uses multiple strategies:
//! - Sequential detection
//! - Stride detection
//! - Markov chain prediction

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::HybridStats;

/// Prefetch manager
pub struct Prefetcher {
    /// Access history for pattern detection
    history: Mutex<AccessHistory>,

    /// Lookahead distance
    lookahead: usize,

    /// Prefetch queue
    prefetch_queue: mpsc::UnboundedSender<PrefetchRequest>,

    /// Statistics
    stats: Arc<HybridStats>,
}

struct AccessHistory {
    /// Recent key accesses
    recent: VecDeque<Vec<u8>>,

    /// Detected patterns
    patterns: HashMap<PatternKey, Pattern>,

    /// Maximum history size
    max_size: usize,
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct PatternKey(Vec<u8>);

#[derive(Clone)]
struct Pattern {
    /// Next likely keys (with probabilities)
    predictions: Vec<(Vec<u8>, f32)>,

    /// Pattern type
    pattern_type: PatternType,

    /// Hit count
    hits: u64,
}

#[derive(Clone, Copy)]
enum PatternType {
    Sequential,
    Stride(i64),
    Markov,
}

struct PrefetchRequest {
    keys: Vec<Vec<u8>>,
    priority: u8,
}

impl Prefetcher {
    pub fn new(lookahead: usize, stats: Arc<HybridStats>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Spawn prefetch worker
        tokio::spawn(async move {
            while let Some(request) = rx.recv().await {
                // Process prefetch requests
                // In production, this would actually prefetch from storage
                let _req: PrefetchRequest = request;
            }
        });

        Self {
            history: Mutex::new(AccessHistory {
                recent: VecDeque::with_capacity(1000),
                patterns: HashMap::new(),
                max_size: 1000,
            }),
            lookahead,
            prefetch_queue: tx,
            stats,
        }
    }

    /// Record an access and potentially trigger prefetch
    pub fn on_access(&self, key: &[u8]) {
        let mut history = self.history.lock();

        // Detect patterns
        let predictions = self.detect_pattern(&history, key);

        // Add to history
        history.recent.push_back(key.to_vec());
        if history.recent.len() > history.max_size {
            history.recent.pop_front();
        }

        // Update pattern statistics
        self.update_patterns(&mut history, key);

        // Trigger prefetch if we have predictions
        if !predictions.is_empty() {
            let _ = self.prefetch_queue.send(PrefetchRequest {
                keys: predictions,
                priority: 1,
            });
        }
    }

    fn detect_pattern(&self, history: &AccessHistory, current: &[u8]) -> Vec<Vec<u8>> {
        let mut predictions = Vec::new();

        // Check for sequential pattern (keys with incrementing suffix)
        if let Some(stride) = self.detect_sequential(history, current) {
            for i in 1..=self.lookahead {
                if let Some(next) = self.apply_stride(current, stride * i as i64) {
                    predictions.push(next);
                }
            }
        }

        // Check Markov prediction
        let key = PatternKey(current.to_vec());
        if let Some(pattern) = history.patterns.get(&key) {
            for (next_key, prob) in &pattern.predictions {
                if *prob > 0.3 { // Only predict if probability > 30%
                    predictions.push(next_key.clone());
                }
            }
        }

        predictions
    }

    fn detect_sequential(&self, history: &AccessHistory, current: &[u8]) -> Option<i64> {
        if history.recent.len() < 2 {
            return None;
        }

        // Check last few accesses for stride pattern
        let recent: Vec<_> = history.recent.iter().rev().take(5).collect();

        // Try to detect numeric suffix pattern
        let current_num = self.extract_numeric_suffix(current)?;
        let prev_num = self.extract_numeric_suffix(recent.first()?)?;

        let stride = current_num - prev_num;
        if stride == 0 {
            return None;
        }

        // Verify stride is consistent
        for window in recent.windows(2) {
            let a = self.extract_numeric_suffix(window[1])?;
            let b = self.extract_numeric_suffix(window[0])?;
            if b - a != stride {
                return None;
            }
        }

        Some(stride)
    }

    fn extract_numeric_suffix(&self, key: &[u8]) -> Option<i64> {
        // Find last numeric portion
        let s = std::str::from_utf8(key).ok()?;
        let num_start = s.rfind(|c: char| !c.is_ascii_digit())?;
        s[num_start + 1..].parse().ok()
    }

    fn apply_stride(&self, key: &[u8], stride: i64) -> Option<Vec<u8>> {
        let s = std::str::from_utf8(key).ok()?;
        let num_start = s.rfind(|c: char| !c.is_ascii_digit())? + 1;

        let current_num: i64 = s[num_start..].parse().ok()?;
        let next_num = current_num + stride;

        if next_num < 0 {
            return None;
        }

        let mut result = s[..num_start].to_string();
        result.push_str(&next_num.to_string());
        Some(result.into_bytes())
    }

    fn update_patterns(&self, history: &mut AccessHistory, current: &[u8]) {
        // Update Markov chain
        if let Some(prev) = history.recent.back() {
            let key = PatternKey(prev.clone());

            let pattern = history.patterns.entry(key).or_insert(Pattern {
                predictions: Vec::new(),
                pattern_type: PatternType::Markov,
                hits: 0,
            });

            pattern.hits += 1;

            // Update predictions
            let current_vec = current.to_vec();
            if let Some(pred) = pattern.predictions.iter_mut().find(|(k, _)| k == &current_vec) {
                pred.1 = (pred.1 * (pattern.hits - 1) as f32 + 1.0) / pattern.hits as f32;
            } else {
                pattern.predictions.push((current_vec, 1.0 / pattern.hits as f32));
            }

            // Keep only top predictions
            pattern.predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
            pattern.predictions.truncate(10);
        }
    }
}
