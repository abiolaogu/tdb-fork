//! Predictable Latency SLAs
//!
//! Guarantees for response time similar to Aerospike's SLA model:
//! - Sub-millisecond reads from RAM
//! - Predictable tail latencies
//! - Request prioritization
//! - Backpressure and admission control
//! - Latency budgets per operation

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;

/// SLA tier with latency guarantees
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SlaTier {
    /// Critical operations: < 1ms p99
    Critical,
    /// High priority: < 5ms p99
    High,
    /// Normal priority: < 10ms p99
    Normal,
    /// Background operations: best effort
    Background,
}

impl SlaTier {
    /// Target p99 latency for this tier
    pub fn target_p99(&self) -> Duration {
        match self {
            SlaTier::Critical => Duration::from_micros(1000),
            SlaTier::High => Duration::from_millis(5),
            SlaTier::Normal => Duration::from_millis(10),
            SlaTier::Background => Duration::from_millis(100),
        }
    }

    /// Maximum concurrent operations per tier
    pub fn max_concurrent(&self) -> usize {
        match self {
            SlaTier::Critical => 10000,
            SlaTier::High => 5000,
            SlaTier::Normal => 2000,
            SlaTier::Background => 500,
        }
    }
}

/// Latency histogram for tracking percentiles
pub struct LatencyHistogram {
    /// Buckets in microseconds: 0-100, 100-200, ..., 900-1000, 1000-2000, ..., >100ms
    buckets: [AtomicU64; 64],

    /// Total count
    count: AtomicU64,

    /// Sum for average calculation
    sum_us: AtomicU64,

    /// Min/Max tracking
    min_us: AtomicU64,
    max_us: AtomicU64,
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            min_us: AtomicU64::new(u64::MAX),
            max_us: AtomicU64::new(0),
        }
    }

    /// Record a latency sample
    pub fn record(&self, latency: Duration) {
        let us = latency.as_micros() as u64;

        // Update bucket
        let bucket = self.bucket_for_latency(us);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);

        // Update statistics
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);

        // Update min/max (CAS loop)
        let mut current_min = self.min_us.load(Ordering::Relaxed);
        while us < current_min {
            match self.min_us.compare_exchange_weak(
                current_min,
                us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        let mut current_max = self.max_us.load(Ordering::Relaxed);
        while us > current_max {
            match self.max_us.compare_exchange_weak(
                current_max,
                us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
    }

    /// Get percentile value (0-100)
    pub fn percentile(&self, p: f64) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }

        let target = ((p / 100.0) * count as f64) as u64;
        let mut cumulative = 0u64;

        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return Duration::from_micros(self.latency_for_bucket(i));
            }
        }

        Duration::from_micros(self.max_us.load(Ordering::Relaxed))
    }

    /// Get p50 (median)
    pub fn p50(&self) -> Duration {
        self.percentile(50.0)
    }

    /// Get p99
    pub fn p99(&self) -> Duration {
        self.percentile(99.0)
    }

    /// Get p999
    pub fn p999(&self) -> Duration {
        self.percentile(99.9)
    }

    /// Get average
    pub fn avg(&self) -> Duration {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let sum = self.sum_us.load(Ordering::Relaxed);
        Duration::from_micros(sum / count)
    }

    /// Get min
    pub fn min(&self) -> Duration {
        let min = self.min_us.load(Ordering::Relaxed);
        if min == u64::MAX {
            Duration::ZERO
        } else {
            Duration::from_micros(min)
        }
    }

    /// Get max
    pub fn max(&self) -> Duration {
        Duration::from_micros(self.max_us.load(Ordering::Relaxed))
    }

    /// Get count
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    fn bucket_for_latency(&self, us: u64) -> usize {
        if us < 1000 {
            // 0-1ms: 100us buckets
            (us / 100) as usize
        } else if us < 10000 {
            // 1-10ms: 1ms buckets
            10 + ((us - 1000) / 1000) as usize
        } else if us < 100000 {
            // 10-100ms: 10ms buckets
            20 + ((us - 10000) / 10000) as usize
        } else {
            // >100ms
            63
        }
    }

    fn latency_for_bucket(&self, bucket: usize) -> u64 {
        if bucket < 10 {
            bucket as u64 * 100 + 50
        } else if bucket < 20 {
            (bucket - 10) as u64 * 1000 + 1500
        } else if bucket < 30 {
            (bucket - 20) as u64 * 10000 + 15000
        } else {
            100000
        }
    }

    /// Reset histogram
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
        self.sum_us.store(0, Ordering::Relaxed);
        self.min_us.store(u64::MAX, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Admission controller for backpressure
pub struct AdmissionController {
    /// Semaphores per tier
    semaphores: [Arc<Semaphore>; 4],

    /// Current queue depth per tier
    queue_depth: [AtomicUsize; 4],

    /// Rejected count
    rejected: AtomicU64,

    /// SLA violations
    violations: AtomicU64,
}

impl AdmissionController {
    pub fn new() -> Self {
        Self {
            semaphores: [
                Arc::new(Semaphore::new(SlaTier::Critical.max_concurrent())),
                Arc::new(Semaphore::new(SlaTier::High.max_concurrent())),
                Arc::new(Semaphore::new(SlaTier::Normal.max_concurrent())),
                Arc::new(Semaphore::new(SlaTier::Background.max_concurrent())),
            ],
            queue_depth: std::array::from_fn(|_| AtomicUsize::new(0)),
            rejected: AtomicU64::new(0),
            violations: AtomicU64::new(0),
        }
    }

    /// Try to acquire admission for an operation
    pub async fn try_acquire(&self, tier: SlaTier) -> Option<AdmissionGuard> {
        let idx = tier as usize;

        let permit = self.semaphores[idx].clone().try_acquire_owned().ok()?;
        self.queue_depth[idx].fetch_add(1, Ordering::Relaxed);

        Some(AdmissionGuard {
            tier,
            permit,
            queue_depth: &self.queue_depth[idx],
            start: Instant::now(),
        })
    }

    /// Acquire admission, waiting if necessary (with timeout)
    pub async fn acquire(&self, tier: SlaTier, timeout: Duration) -> Option<AdmissionGuard> {
        let idx = tier as usize;

        let permit = tokio::time::timeout(
            timeout,
            self.semaphores[idx].clone().acquire_owned(),
        )
        .await
        .ok()?
        .ok()?;

        self.queue_depth[idx].fetch_add(1, Ordering::Relaxed);

        Some(AdmissionGuard {
            tier,
            permit,
            queue_depth: &self.queue_depth[idx],
            start: Instant::now(),
        })
    }

    /// Record a rejection
    pub fn reject(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an SLA violation
    pub fn record_violation(&self) {
        self.violations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current queue depth
    pub fn queue_depth(&self, tier: SlaTier) -> usize {
        self.queue_depth[tier as usize].load(Ordering::Relaxed)
    }

    /// Get rejection count
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Get violation count
    pub fn violations(&self) -> u64 {
        self.violations.load(Ordering::Relaxed)
    }
}

impl Default for AdmissionController {
    fn default() -> Self {
        Self::new()
    }
}

/// Guard that releases admission when dropped
pub struct AdmissionGuard {
    tier: SlaTier,
    permit: tokio::sync::OwnedSemaphorePermit,
    queue_depth: *const AtomicUsize,
    start: Instant,
}

unsafe impl Send for AdmissionGuard {}

impl AdmissionGuard {
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn tier(&self) -> SlaTier {
        self.tier
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        unsafe {
            (*self.queue_depth).fetch_sub(1, Ordering::Relaxed);
        }
    }
}

/// SLA Monitor for tracking compliance
pub struct SlaMonitor {
    /// Histograms per operation type
    histograms: RwLock<std::collections::HashMap<String, LatencyHistogram>>,

    /// Admission controller
    pub admission: AdmissionController,

    /// Window for rolling stats (seconds)
    window_secs: u64,

    /// Start of current window
    window_start: Mutex<Instant>,
}

impl SlaMonitor {
    pub fn new(window_secs: u64) -> Self {
        Self {
            histograms: RwLock::new(std::collections::HashMap::new()),
            admission: AdmissionController::new(),
            window_secs,
            window_start: Mutex::new(Instant::now()),
        }
    }

    /// Start tracking an operation
    pub fn start_operation(&self, tier: SlaTier) -> OperationTracker {
        OperationTracker {
            tier,
            start: Instant::now(),
        }
    }

    /// Complete an operation and record latency
    pub fn complete_operation(&self, tracker: OperationTracker, operation: &str) {
        let latency = tracker.start.elapsed();

        // Record in histogram
        {
            let mut histograms = self.histograms.write();
            let histogram = histograms
                .entry(operation.to_string())
                .or_insert_with(LatencyHistogram::new);
            histogram.record(latency);
        }

        // Check SLA violation
        if latency > tracker.tier.target_p99() {
            self.admission.record_violation();
        }

        // Check if window should roll
        self.maybe_roll_window();
    }

    /// Get p99 latency for an operation
    pub fn p99(&self, operation: &str) -> Duration {
        self.histograms
            .read()
            .get(operation)
            .map(|h| h.p99())
            .unwrap_or(Duration::ZERO)
    }

    /// Get statistics for all operations
    pub fn stats(&self) -> Vec<OperationStats> {
        self.histograms
            .read()
            .iter()
            .map(|(name, h)| OperationStats {
                operation: name.clone(),
                count: h.count(),
                avg: h.avg(),
                p50: h.p50(),
                p99: h.p99(),
                p999: h.p999(),
                min: h.min(),
                max: h.max(),
            })
            .collect()
    }

    /// Check SLA compliance
    pub fn is_compliant(&self, tier: SlaTier) -> bool {
        let histograms = self.histograms.read();
        for h in histograms.values() {
            if h.p99() > tier.target_p99() {
                return false;
            }
        }
        true
    }

    fn maybe_roll_window(&self) {
        let mut window_start = self.window_start.lock();
        if window_start.elapsed().as_secs() >= self.window_secs {
            // Reset histograms
            for h in self.histograms.write().values() {
                h.reset();
            }
            *window_start = Instant::now();
        }
    }
}

impl Default for SlaMonitor {
    fn default() -> Self {
        Self::new(60) // 1 minute window
    }
}

/// Tracker for measuring operation latency
pub struct OperationTracker {
    tier: SlaTier,
    start: Instant,
}

impl OperationTracker {
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Statistics for an operation
#[derive(Debug, Clone)]
pub struct OperationStats {
    pub operation: String,
    pub count: u64,
    pub avg: Duration,
    pub p50: Duration,
    pub p99: Duration,
    pub p999: Duration,
    pub min: Duration,
    pub max: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let h = LatencyHistogram::new();

        for i in 0..100 {
            h.record(Duration::from_micros(i * 10));
        }

        assert_eq!(h.count(), 100);
        assert!(h.p50() >= Duration::from_micros(400));
        assert!(h.p99() >= Duration::from_micros(900));
    }

    #[test]
    fn test_sla_tiers() {
        assert!(SlaTier::Critical < SlaTier::High);
        assert!(SlaTier::High < SlaTier::Normal);
        assert!(SlaTier::Normal < SlaTier::Background);
    }
}
