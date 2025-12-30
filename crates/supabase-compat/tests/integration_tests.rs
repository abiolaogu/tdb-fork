//! Integration tests for Supabase REST API

use supabase_rest::{RateLimiter, RateLimitConfig};
use std::time::Duration;

#[test]
fn test_rate_limiter_integration() {
    let limiter = RateLimiter::new(RateLimitConfig {
        max_requests: 10,
        window: Duration::from_secs(60),
        by_ip: true,
        by_user: false,
    });

    // Should allow initial requests
    for i in 0..10 {
        let result = limiter.check("test-client");
        assert!(result.is_allowed(), "Request {} should be allowed", i);
    }

    // 11th request should be blocked
    let result = limiter.check("test-client");
    assert!(!result.is_allowed(), "11th request should be blocked");
}

#[test]
fn test_rate_limiter_per_ip() {
    let limiter = RateLimiter::new(RateLimitConfig {
        max_requests: 1,
        window: Duration::from_secs(60),
        by_ip: true,
        by_user: false,
    });

    let ip1: std::net::IpAddr = "10.0.0.1".parse().unwrap();
    let ip2: std::net::IpAddr = "10.0.0.2".parse().unwrap();

    // First request from each IP should work
    assert!(limiter.check_ip(ip1).is_allowed());
    assert!(limiter.check_ip(ip2).is_allowed());

    // Second request from IP1 should be blocked
    assert!(!limiter.check_ip(ip1).is_allowed());
    
    // IP2 should also be blocked now
    assert!(!limiter.check_ip(ip2).is_allowed());
}

#[test]
fn test_rate_limiter_retry_after() {
    let limiter = RateLimiter::new(RateLimitConfig {
        max_requests: 1,
        window: Duration::from_secs(60),
        by_ip: true,
        by_user: false,
    });

    // First request allowed
    let result = limiter.check("client");
    assert!(result.is_allowed());
    assert!(result.retry_after().is_none());

    // Second request blocked with retry-after
    let result = limiter.check("client");
    assert!(!result.is_allowed());
    assert!(result.retry_after().is_some());
}

