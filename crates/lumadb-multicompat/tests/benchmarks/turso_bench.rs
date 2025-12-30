//! Turso adapter benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine};

fn setup_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

fn bench_execute(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("turso/execute");
    group.throughput(Throughput::Elements(1));

    group.bench_function("simple_select", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql(black_box("SELECT 1"), vec![]).await.unwrap();
            });
        });
    });

    group.bench_function("with_args", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql(
                    black_box("SELECT ?, ?, ?"),
                    vec![
                        Value::Integer(1),
                        Value::String("hello".into()),
                        Value::Float(3.14),
                    ]
                ).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("turso/batch");
    
    for count in [3, 5, 10] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(format!("{}_statements", count), |b| {
            b.iter(|| {
                let s = storage.clone();
                rt.block_on(async {
                    for i in 0..count {
                        s.execute_sql(
                            &format!("SELECT {}", i),
                            vec![]
                        ).await.unwrap();
                    }
                });
            });
        });
    }

    group.finish();
}

fn bench_pipeline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("turso/pipeline");
    group.throughput(Throughput::Elements(1));

    group.bench_function("transaction_3_ops", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql("BEGIN", vec![]).await.ok();
                
                let mut row = Row::new();
                row.push("id", Value::Integer(i as i64));
                s.execute_kv_put("PipelineBench", Value::Integer(i as i64), row).await.ok();
                
                s.execute_sql("COMMIT", vec![]).await.ok();
            });
            i += 1;
        });
    });

    group.finish();
}

fn bench_blob_handling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("turso/blob");
    
    let small_blob = vec![0u8; 1024];       // 1KB
    let medium_blob = vec![0u8; 64 * 1024]; // 64KB

    group.bench_function("1kb_blob", |b| {
        let blob = small_blob.clone();
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            let b = blob.clone();
            rt.block_on(async {
                let mut row = Row::new();
                row.push("id", Value::Integer(i as i64));
                row.push("data", Value::Bytes(b));
                s.execute_kv_put("BlobBench", Value::Integer(i as i64), row).await.ok();
            });
            i += 1;
        });
    });

    group.bench_function("64kb_blob", |b| {
        let blob = medium_blob.clone();
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            let b = blob.clone();
            rt.block_on(async {
                let mut row = Row::new();
                row.push("id", Value::Integer(i as i64));
                row.push("data", Value::Bytes(b));
                s.execute_kv_put("BlobBenchLarge", Value::Integer(i as i64), row).await.ok();
            });
            i += 1;
        });
    });

    group.finish();
}

fn bench_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    // Prepopulate
    rt.block_on(async {
        for i in 0..100 {
            let mut row = Row::new();
            row.push("id", Value::Integer(i));
            storage.execute_kv_put("ConcurrentBench", Value::Integer(i), row).await.ok();
        }
    });

    let mut group = c.benchmark_group("turso/concurrent");
    group.throughput(Throughput::Elements(10));

    group.bench_function("10_parallel_reads", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let handles: Vec<_> = (0..10)
                    .map(|i| {
                        let storage = s.clone();
                        tokio::spawn(async move {
                            storage.execute_kv_get("ConcurrentBench", Value::Integer(i % 100)).await.ok()
                        })
                    })
                    .collect();
                
                for h in handles {
                    h.await.ok();
                }
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_execute,
    bench_batch,
    bench_pipeline,
    bench_blob_handling,
    bench_concurrent,
);

criterion_main!(benches);
