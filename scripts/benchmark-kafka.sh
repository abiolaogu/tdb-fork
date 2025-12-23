#!/bin/bash
# scripts/benchmark-kafka.sh

echo "Starting LumaDB Kafka Benchmark..."

# 1. Ensure LumaDB is running
# ./bin/lumadb server --config configs/lumadb.production.yaml &
# PID=$!
# sleep 5

# 2. Benchmark using kafka-producer-perf-test (from standard Kafka tools)
echo "Running Producer Performance Test..."
if command -v kafka-producer-perf-test.sh &> /dev/null; then
    kafka-producer-perf-test.sh \
        --topic test-perf \
        --num-records 1000000 \
        --record-size 1024 \
        --throughput -1 \
        --producer-props bootstrap.servers=localhost:9092
else
    echo "kafka-producer-perf-test.sh not found. Using internal mock benchmark."
    # Run the internal benchmark test (if compiled)
    # cargo test --release --test kafka_bench
    echo "Run: cargo test kafka::perf::tests::test_buffer_pool_perf -- --nocapture"
fi

# 3. Clean up
# kill $PID
