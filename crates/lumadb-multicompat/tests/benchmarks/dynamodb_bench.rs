//! Enhanced DynamoDB Benchmarks with Comparison Data

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine, BatchOperation};
use lumadb_multicompat::core::{QueryFilter, KeyCondition};

fn setup_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== PutItem Benchmarks =====

fn benchmark_put_item(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("dynamodb/put_item");

    for size in [1, 10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut i = 0u64;
            b.iter(|| {
                let s = storage.clone();
                rt.block_on(async {
                    for _ in 0..size {
                        let key = Value::String(format!("bench_key_{}", i));
                        let mut row = Row::new();
                        row.push("pk", key.clone());
                        row.push("data", Value::String("benchmark_data_payload".into()));
                        row.push("number", Value::Integer(i as i64));
                        s.execute_kv_put("BenchPut", black_box(key), black_box(row)).await.ok();
                        i += 1;
                    }
                });
            });
        });
    }

    group.finish();
}

// ===== GetItem Benchmarks =====

fn benchmark_get_item(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    // Prepopulate with 10K items
    rt.block_on(async {
        for i in 0..10000 {
            let key = Value::String(format!("get_bench_{}", i));
            let mut row = Row::new();
            row.push("pk", key.clone());
            row.push("data", Value::String(format!("data_{}", i)));
            storage.execute_kv_put("BenchGet", key, row).await.ok();
        }
    });

    let mut group = c.benchmark_group("dynamodb/get_item");
    group.throughput(Throughput::Elements(1));

    // Get existing items
    group.bench_function("existing_item", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let key = Value::String(format!("get_bench_{}", i % 10000));
                s.execute_kv_get("BenchGet", black_box(key)).await.ok();
            });
            i += 1;
        });
    });

    // Get with cache hits (same key)
    group.bench_function("cached_item", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let key = Value::String("get_bench_0".into());
                s.execute_kv_get("BenchGet", black_box(key)).await.ok();
            });
        });
    });

    group.finish();
}

// ===== BatchWriteItem Benchmarks =====

fn benchmark_batch_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("dynamodb/batch_write");

    for batch_size in [1, 5, 10, 25].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), batch_size, |b, &batch_size| {
            let mut batch = 0u64;
            b.iter(|| {
                let s = storage.clone();
                let bn = batch;
                rt.block_on(async {
                    let ops: Vec<BatchOperation> = (0..batch_size)
                        .map(|i| BatchOperation::Put {
                            table: "BenchBatch".into(),
                            key: Value::Integer((bn * 1000 + i as u64) as i64),
                            value: {
                                let mut r = Row::new();
                                r.push("id", Value::Integer((bn * 1000 + i as u64) as i64));
                                r.push("data", Value::String("batch_data".into()));
                                r
                            },
                        })
                        .collect();
                    s.batch_write(black_box(ops)).await.ok();
                });
                batch += 1;
            });
        });
    }

    group.finish();
}

// ===== Query Benchmarks =====

fn benchmark_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    // Prepopulate: 100 users, each with 100 orders
    rt.block_on(async {
        for user in 0..100 {
            for order in 0..100 {
                let key = Value::Object([
                    ("pk".into(), Value::String(format!("USER#{}", user))),
                    ("sk".into(), Value::String(format!("ORDER#{:05}", order))),
                ].into());
                let mut row = Row::new();
                row.push("pk", Value::String(format!("USER#{}", user)));
                row.push("sk", Value::String(format!("ORDER#{:05}", order)));
                row.push("amount", Value::Integer((order * 10) as i64));
                storage.execute_kv_put("BenchQuery", key, row).await.ok();
            }
        }
    });

    let mut group = c.benchmark_group("dynamodb/query");

    // Query all items for a user (100 items)
    group.bench_function("full_partition", |b| {
        let mut user = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let filter = QueryFilter {
                    key_condition: Some(KeyCondition {
                        partition_key: ("pk".into(), Value::String(format!("USER#{}", user % 100))),
                        sort_key: None,
                    }),
                    ..Default::default()
                };
                s.execute_kv_query("BenchQuery", black_box(filter)).await.ok();
            });
            user += 1;
        });
    });

    // Query with limit
    group.bench_function("with_limit_10", |b| {
        let mut user = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let filter = QueryFilter {
                    key_condition: Some(KeyCondition {
                        partition_key: ("pk".into(), Value::String(format!("USER#{}", user % 100))),
                        sort_key: None,
                    }),
                    limit: Some(10),
                    ..Default::default()
                };
                s.execute_kv_query("BenchQuery", black_box(filter)).await.ok();
            });
            user += 1;
        });
    });

    group.finish();
}

// ===== Cache Performance =====

fn benchmark_cache(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = LumaStorage::new();

    // Warm cache with common queries
    rt.block_on(async {
        storage.execute_sql("SELECT 1", vec![]).await.ok();
    });

    let mut group = c.benchmark_group("dynamodb/cache");

    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                storage.execute_sql(black_box("SELECT 1"), vec![]).await.ok();
            });
        });
    });

    group.bench_function("cache_miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            rt.block_on(async {
                storage.execute_sql(&format!("SELECT {}", i), vec![]).await.ok();
            });
            i += 1;
        });
    });

    group.finish();
}

// ===== Concurrent Access =====

fn benchmark_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    // Prepopulate
    rt.block_on(async {
        for i in 0..1000 {
            let key = Value::Integer(i);
            let mut row = Row::new();
            row.push("id", Value::Integer(i));
            storage.execute_kv_put("BenchConcurrent", key, row).await.ok();
        }
    });

    let mut group = c.benchmark_group("dynamodb/concurrent");

    for parallelism in [4, 8, 16].iter() {
        group.throughput(Throughput::Elements(*parallelism as u64));
        
        group.bench_with_input(BenchmarkId::from_parameter(parallelism), parallelism, |b, &parallelism| {
            b.iter(|| {
                let s = storage.clone();
                rt.block_on(async {
                    let handles: Vec<_> = (0..parallelism)
                        .map(|t| {
                            let storage = s.clone();
                            tokio::spawn(async move {
                                let key = Value::Integer((t * 100) as i64);
                                storage.execute_kv_get("BenchConcurrent", key).await.ok()
                            })
                        })
                        .collect();
                    
                    for h in handles {
                        h.await.ok();
                    }
                });
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_put_item,
    benchmark_get_item,
    benchmark_batch_write,
    benchmark_query,
    benchmark_cache,
    benchmark_concurrent,
);

criterion_main!(benches);
