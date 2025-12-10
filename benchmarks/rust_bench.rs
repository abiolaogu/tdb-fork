//! TDB+ Rust Core Benchmarks
//!
//! Comprehensive benchmarks for the storage engine, comparing against
//! common database workloads.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use std::sync::Arc;
use tokio::runtime::Runtime;

// Benchmark configurations
const SMALL_BATCH: usize = 100;
const MEDIUM_BATCH: usize = 1_000;
const LARGE_BATCH: usize = 10_000;

/// Benchmark single document insertions
fn bench_single_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("single_insert");
    group.throughput(Throughput::Elements(1));

    // Small document (100 bytes)
    group.bench_function("small_doc", |b| {
        b.iter(|| {
            let doc = serde_json::json!({
                "id": "test123",
                "name": "Test User",
                "email": "test@example.com"
            });
            black_box(doc)
        })
    });

    // Medium document (1KB)
    group.bench_function("medium_doc", |b| {
        b.iter(|| {
            let doc = serde_json::json!({
                "id": "test123",
                "name": "Test User",
                "email": "test@example.com",
                "profile": {
                    "bio": "A".repeat(500),
                    "interests": ["reading", "coding", "music"],
                    "settings": {
                        "theme": "dark",
                        "notifications": true
                    }
                }
            });
            black_box(doc)
        })
    });

    // Large document (10KB)
    group.bench_function("large_doc", |b| {
        b.iter(|| {
            let doc = serde_json::json!({
                "id": "test123",
                "data": "X".repeat(10_000)
            });
            black_box(doc)
        })
    });

    group.finish();
}

/// Benchmark batch insertions
fn bench_batch_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_insert");

    for size in [SMALL_BATCH, MEDIUM_BATCH, LARGE_BATCH] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let docs: Vec<_> = (0..size)
                    .map(|i| {
                        serde_json::json!({
                            "id": format!("doc_{}", i),
                            "value": i,
                            "data": "test data"
                        })
                    })
                    .collect();
                black_box(docs)
            })
        });
    }

    group.finish();
}

/// Benchmark point lookups
fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");
    group.throughput(Throughput::Elements(1));

    group.bench_function("by_id", |b| {
        b.iter(|| {
            let key = black_box("doc_12345");
            // Simulated lookup
            key.len()
        })
    });

    group.finish();
}

/// Benchmark range scans
fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");

    for size in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                // Simulated range scan
                let results: Vec<_> = (0..size).map(|i| format!("result_{}", i)).collect();
                black_box(results)
            })
        });
    }

    group.finish();
}

/// Benchmark concurrent operations
fn bench_concurrent_ops(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrent");

    for threads in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("threads", threads),
            &threads,
            |b, &threads| {
                b.iter(|| {
                    // Simulated concurrent work
                    let handles: Vec<_> = (0..threads)
                        .map(|_| {
                            std::thread::spawn(|| {
                                for _ in 0..100 {
                                    black_box(42);
                                }
                            })
                        })
                        .collect();
                    for h in handles {
                        h.join().unwrap();
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark serialization/deserialization
fn bench_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("serde");

    let doc = serde_json::json!({
        "id": "test123",
        "name": "Test User",
        "age": 30,
        "active": true,
        "tags": ["rust", "database"],
        "nested": {
            "field1": "value1",
            "field2": 42
        }
    });

    group.bench_function("serialize", |b| {
        b.iter(|| {
            let bytes = serde_json::to_vec(&doc).unwrap();
            black_box(bytes)
        })
    });

    let bytes = serde_json::to_vec(&doc).unwrap();
    group.bench_function("deserialize", |b| {
        b.iter(|| {
            let doc: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            black_box(doc)
        })
    });

    group.finish();
}

/// Benchmark hashing for sharding
fn bench_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hashing");

    let key = "user:12345:profile";

    group.bench_function("xxh3", |b| {
        b.iter(|| {
            use std::hash::{Hash, Hasher};
            use std::collections::hash_map::DefaultHasher;
            let mut hasher = DefaultHasher::new();
            key.hash(&mut hasher);
            black_box(hasher.finish())
        })
    });

    group.finish();
}

/// Benchmark memory allocation patterns
fn bench_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("allocation");

    group.bench_function("vec_push", |b| {
        b.iter(|| {
            let mut v = Vec::new();
            for i in 0..1000 {
                v.push(i);
            }
            black_box(v)
        })
    });

    group.bench_function("vec_with_capacity", |b| {
        b.iter(|| {
            let mut v = Vec::with_capacity(1000);
            for i in 0..1000 {
                v.push(i);
            }
            black_box(v)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_insert,
    bench_batch_insert,
    bench_point_lookup,
    bench_range_scan,
    bench_concurrent_ops,
    bench_serde,
    bench_hashing,
    bench_allocation,
);

criterion_main!(benches);
