//! Rate limiting middleware for REST API
//!
//! Provides token bucket rate limiting with configurable limits per IP and user.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

/// Rate limiter configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Time window duration
    pub window: Duration,
    /// Whether to limit by IP address
    pub by_ip: bool,
    /// Whether to limit by user ID
    pub by_user: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window: Duration::from_secs(60),
            by_ip: true,
            by_user: false,
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: u32,
    max_tokens: u32,
    last_refill: Instant,
    refill_rate: Duration,
}

impl TokenBucket {
    fn new(max_tokens: u32, window: Duration) -> Self {
        Self {
            tokens: max_tokens,
            max_tokens,
            last_refill: Instant::now(),
            refill_rate: window / max_tokens,
        }
    }

    fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let tokens_to_add = (elapsed.as_millis() / self.refill_rate.as_millis()) as u32;
        
        if tokens_to_add > 0 {
            self.tokens = (self.tokens + tokens_to_add).min(self.max_tokens);
            self.last_refill = now;
        }
    }

    fn tokens_remaining(&self) -> u32 {
        self.tokens
    }

    fn retry_after(&self) -> Duration {
        if self.tokens > 0 {
            Duration::ZERO
        } else {
            self.refill_rate
        }
    }
}

/// Rate limiter with support for multiple keys
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: Arc<Mutex<HashMap<String, TokenBucket>>>,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Check if a request is allowed
    pub fn check(&self, key: &str) -> RateLimitResult {
        let mut buckets = self.buckets.lock();
        
        let bucket = buckets.entry(key.to_string()).or_insert_with(|| {
            TokenBucket::new(self.config.max_requests, self.config.window)
        });

        if bucket.try_consume() {
            RateLimitResult::Allowed {
                remaining: bucket.tokens_remaining(),
                limit: self.config.max_requests,
            }
        } else {
            RateLimitResult::Limited {
                retry_after: bucket.retry_after(),
                limit: self.config.max_requests,
            }
        }
    }

    /// Check by IP address
    pub fn check_ip(&self, ip: IpAddr) -> RateLimitResult {
        self.check(&format!("ip:{}", ip))
    }

    /// Check by user ID
    pub fn check_user(&self, user_id: &str) -> RateLimitResult {
        self.check(&format!("user:{}", user_id))
    }

    /// Check by IP and user combined
    pub fn check_request(&self, ip: Option<IpAddr>, user_id: Option<&str>) -> RateLimitResult {
        // Check IP limit first
        if self.config.by_ip {
            if let Some(ip) = ip {
                let result = self.check_ip(ip);
                if matches!(result, RateLimitResult::Limited { .. }) {
                    return result;
                }
            }
        }

        // Check user limit
        if self.config.by_user {
            if let Some(user_id) = user_id {
                let result = self.check_user(user_id);
                if matches!(result, RateLimitResult::Limited { .. }) {
                    return result;
                }
            }
        }

        RateLimitResult::Allowed {
            remaining: self.config.max_requests,
            limit: self.config.max_requests,
        }
    }

    /// Clean up expired buckets
    pub fn cleanup(&self) {
        let mut buckets = self.buckets.lock();
        let now = Instant::now();
        let expiry = self.config.window * 2;
        
        buckets.retain(|_, bucket| {
            now.duration_since(bucket.last_refill) < expiry
        });
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

/// Result of a rate limit check
#[derive(Debug, Clone)]
pub enum RateLimitResult {
    /// Request is allowed
    Allowed {
        /// Remaining requests in window
        remaining: u32,
        /// Total limit
        limit: u32,
    },
    /// Request is rate limited
    Limited {
        /// Time until next request is allowed
        retry_after: Duration,
        /// Total limit
        limit: u32,
    },
}

impl RateLimitResult {
    /// Check if request is allowed
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }

    /// Get retry-after duration if limited
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Limited { retry_after, .. } => Some(*retry_after),
            Self::Allowed { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_requests() {
        let limiter = RateLimiter::new(RateLimitConfig {
            max_requests: 5,
            window: Duration::from_secs(1),
            ..Default::default()
        });

        for _ in 0..5 {
            assert!(limiter.check("test").is_allowed());
        }
    }

    #[test]
    fn test_rate_limiter_blocks_excess() {
        let limiter = RateLimiter::new(RateLimitConfig {
            max_requests: 2,
            window: Duration::from_secs(1),
            ..Default::default()
        });

        assert!(limiter.check("test").is_allowed());
        assert!(limiter.check("test").is_allowed());
        assert!(!limiter.check("test").is_allowed());
    }

    #[test]
    fn test_rate_limiter_by_ip() {
        let limiter = RateLimiter::new(RateLimitConfig {
            max_requests: 1,
            ..Default::default()
        });

        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();

        assert!(limiter.check_ip(ip1).is_allowed());
        assert!(!limiter.check_ip(ip1).is_allowed());
        assert!(limiter.check_ip(ip2).is_allowed()); // Different IP
    }
}
