//! D1 adapter benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, StorageEngine};

fn setup_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

fn bench_simple_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("d1/query");
    group.throughput(Throughput::Elements(1));

    group.bench_function("select_1", |b| {
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
                    black_box("SELECT ? as a, ? as b"),
                    vec![Value::Integer(1), Value::String("test".into())]
                ).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_batch_queries(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("d1/batch");
    
    for count in [5, 10] {
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

fn bench_insert(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = setup_storage();

    let mut group = c.benchmark_group("d1/insert");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_row", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let s = storage.clone();
            rt.block_on(async {
                s.execute_sql(
                    black_box("INSERT INTO test VALUES (?, ?)"),
                    vec![Value::Integer(i as i64), Value::String("data".into())]
                ).await.unwrap();
            });
            i += 1;
        });
    });

    group.finish();
}

fn bench_cache_performance(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let storage = LumaStorage::new();

    // Warm cache
    rt.block_on(async {
        storage.execute_sql("SELECT 1", vec![]).await.unwrap();
    });

    let mut group = c.benchmark_group("d1/cache");
    group.throughput(Throughput::Elements(1));

    group.bench_function("cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                storage.execute_sql(black_box("SELECT 1"), vec![]).await.unwrap();
            });
        });
    });

    group.bench_function("cache_miss", |b| {
        let mut i = 0u64;
        b.iter(|| {
            rt.block_on(async {
                storage.execute_sql(
                    &format!("SELECT {}", i),
                    vec![]
                ).await.unwrap();
            });
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_query,
    bench_batch_queries,
    bench_insert,
    bench_cache_performance,
);

criterion_main!(benches);
