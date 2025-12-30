//! Performance benchmarks for multi-protocol compatibility layer
//!
//! Run with: cargo bench -p lumadb-multicompat

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;

use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine, BatchOperation};

fn setup_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

fn bench_kv_put(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("kv_put");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_put", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let key = Value::Integer(i as i64);
                let mut row = Row::new();
                row.push("id", key.clone());
                row.push("data", Value::String("benchmark_data".into()));
                s.execute_kv_put("BenchPut", black_box(key), black_box(row)).await.unwrap();
            });
            i += 1;
        });
    });

    group.finish();
}

fn bench_kv_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    // Prepopulate
    rt.block_on(async {
        for i in 0..1000 {
            let key = Value::Integer(i);
            let mut row = Row::new();
            row.push("id", key.clone());
            row.push("data", Value::String("test".into()));
            storage.execute_kv_put("BenchGet", key, row).await.unwrap();
        }
    });

    let mut group = c.benchmark_group("kv_get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_get", |b| {
        let mut i = 0i64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                let key = Value::Integer(i % 1000);
                s.execute_kv_get("BenchGet", black_box(key)).await.unwrap();
            });
            i += 1;
        });
    });

    group.finish();
}

fn bench_batch_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("batch_write");
    
    for size in [10, 25, 50].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_function(format!("batch_{}", size), |b| {
            let mut batch_num = 0u64;
            b.iter(|| {
                let s = storage.clone();
                let sz = *size;
                let bn = batch_num;
                rt.block_on(async {
                    let ops: Vec<BatchOperation> = (0..sz)
                        .map(|i| BatchOperation::Put {
                            table: "BenchBatch".into(),
                            key: Value::Integer((bn * sz as u64 + i as u64) as i64),
                            value: {
                                let mut r = Row::new();
                                r.push("id", Value::Integer((bn * sz as u64 + i as u64) as i64));
                                r
                            },
                        })
                        .collect();
                    s.batch_write(black_box(ops)).await.unwrap();
                });
                batch_num += 1;
            });
        });
    }

    group.finish();
}

fn bench_sql_execute(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("sql_execute");
    group.throughput(Throughput::Elements(1));

    group.bench_function("select_simple", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql(black_box("SELECT 1"), vec![]).await.unwrap();
            });
        });
    });

    group.bench_function("select_with_params", |b| {
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql(
                    black_box("SELECT * FROM test WHERE id = ?"),
                    vec![Value::Integer(1)],
                ).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_cache(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = LumaStorage::new();

    // Warm cache
    rt.block_on(async {
        storage.execute_sql("SELECT 1", vec![]).await.unwrap();
    });

    let mut group = c.benchmark_group("cache");
    group.throughput(Throughput::Elements(1));

    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                storage.execute_sql(black_box("SELECT 1"), vec![]).await.unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_kv_put,
    bench_kv_get,
    bench_batch_write,
    bench_sql_execute,
    bench_cache,
);

criterion_main!(benches);
