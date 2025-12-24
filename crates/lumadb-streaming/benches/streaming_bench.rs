use criterion::{black_box, criterion_group, criterion_main, Criterion};
use lumadb_streaming::{StreamingEngine, ProduceRecord, RaftStub};
use lumadb_streaming::reactor::Reactor;
use lumadb_storage::StorageEngine;
use lumadb_common::config::{StorageConfig, StreamingConfig};
use std::sync::Arc;
use tokio::runtime::Runtime;

fn bench_produce(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = StreamingConfig {
        segment_size_bytes: 10 * 1024 * 1024,
        ..Default::default()
    };
    
    // Use temp dirs
    let storage_dir = tempfile::tempdir().unwrap();
    
    let storage_config = StorageConfig {
        path: storage_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    
    // Construct real storage engine
    let storage = rt.block_on(async {
        Arc::new(StorageEngine::new(&storage_config).await.expect("Failed to create storage"))
    });

    let engine = rt.block_on(async {
        Arc::new(StreamingEngine::new(&config, 
            storage, 
            std::sync::Arc::new(RaftStub)
        ).await.expect("Failed to create streaming engine"))
    });

    let reactor = Arc::new(Reactor::new());
    
    reactor.start();

    // Create topic
    rt.block_on(async {
        let topic_config = lumadb_common::types::TopicConfig::new("bench-topic", 1, 1);
        engine.create_topic(topic_config).await.expect("Failed to create topic");
    });

    let record = ProduceRecord {
        key: None,
        value: serde_json::json!({"data": "x".repeat(1024)}),
        headers: None,
        partition: None,
    };

    c.bench_function("produce_1kb", |b| {
        b.to_async(&rt).iter(|| async {
            engine.produce(
                black_box("bench-topic"),
                black_box(&[record.clone()]),
                black_box(0)
            ).await.unwrap();
        })
    });
    
    reactor.stop();
}

criterion_group!(benches, bench_produce);
criterion_main!(benches);
