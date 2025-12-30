# LumaDB Multicompat Test Suite Documentation

## Overview

This document describes the comprehensive testing and benchmarking suite for the LumaDB multi-protocol compatibility layer.

## Test Structure

```
tests/
├── unit/                              # Unit tests for internal components
│   ├── dynamodb_translator_test.rs    # DynamoDB JSON translation
│   ├── d1_converter_test.rs           # D1 value conversion
│   └── turso_value_test.rs            # Turso type handling
├── integration/                       # SDK integration tests
│   ├── dynamodb_sdk_test.rs           # AWS SDK simulation
│   ├── d1_client_test.rs              # D1 client simulation
│   └── turso_client_test.rs           # Turso client simulation
└── benchmarks/                        # Performance benchmarks
    ├── dynamodb_bench.rs              # DynamoDB operations
    ├── d1_bench.rs                    # D1/SQL operations
    └── turso_bench.rs                 # Turso operations

benches/
└── protocol_bench.rs                  # Main Criterion benchmark suite
```

## Running Tests

### All Tests
```bash
cargo test -p lumadb-multicompat
```

### Unit Tests Only
```bash
cargo test -p lumadb-multicompat unit
```

### Integration Tests Only
```bash
cargo test -p lumadb-multicompat integration
```

### Specific Protocol Tests
```bash
# DynamoDB tests
cargo test -p lumadb-multicompat dynamodb

# D1 tests
cargo test -p lumadb-multicompat d1

# Turso tests
cargo test -p lumadb-multicompat turso
```

## Running Benchmarks

### Full Benchmark Suite
```bash
cargo bench -p lumadb-multicompat
```

### Specific Benchmarks
```bash
# DynamoDB benchmarks only
cargo bench -p lumadb-multicompat -- dynamodb

# Query benchmarks
cargo bench -p lumadb-multicompat -- query
```

### Performance Comparison
```bash
# Save baseline
cargo bench -p lumadb-multicompat -- --save-baseline main

# Compare against baseline
cargo bench -p lumadb-multicompat -- --baseline main
```

## Test Coverage

### DynamoDB Adapter (50+ tests)

| Category | Tests | Description |
|----------|-------|-------------|
| Type Conversion | 32 | S, N, B, BOOL, NULL, L, M, SS, NS, BS parsing |
| Key Conditions | 5 | Equality, GT, begins_with, attribute names |
| CRUD Operations | 8 | PutItem, GetItem, DeleteItem, UpdateItem |
| Query/Scan | 6 | Partition key, sort key, limit, pagination |
| Batch Operations | 4 | BatchWriteItem with 25 items |
| TransactWrite | 3 | Transactional integrity |
| Table Operations | 5 | Create, Describe, List, Delete |
| Error Handling | 4 | Not found, already exists |
| Concurrency | 4 | Parallel reads/writes |

### D1 Adapter (25+ tests)

| Category | Tests | Description |
|----------|-------|-------------|
| Value Conversion | 15 | JSON to Value, primitive types |
| Response Format | 5 | Cloudflare wrapper, meta, errors |
| SQL Execution | 8 | Simple, parameterized, batch |
| Caching | 3 | Cache hits, invalidation |

### Turso Adapter (30+ tests)

| Category | Tests | Description |
|----------|-------|-------------|
| Value Types | 8 | null, integer, float, text, blob |
| Response Format | 5 | Execute, batch, pipeline, error |
| Statement Execution | 6 | Simple, positional args, named args |
| Pipeline | 4 | Transaction semantics |
| Concurrency | 4 | Parallel reads/writes |
| Blob Handling | 3 | Base64 encode/decode |

## Benchmark Groups

### DynamoDB (`dynamodb/`)

| Benchmark | Description | Target |
|-----------|-------------|--------|
| `put_item/1` | Single PutItem | < 100μs |
| `put_item/10` | 10 PutItems | < 1ms |
| `put_item/100` | 100 PutItems | < 10ms |
| `get_item/existing` | Get existing item | < 50μs |
| `get_item/cached` | Get cached item | < 10μs |
| `batch_write/25` | BatchWriteItem | < 5ms |
| `query/full_partition` | Query 100 items | < 5ms |
| `query/with_limit_10` | Query with limit | < 1ms |

### D1 (`d1/`)

| Benchmark | Description | Target |
|-----------|-------------|--------|
| `query/select_1` | Simple SELECT | < 50μs |
| `query/select_with_params` | Parameterized | < 100μs |
| `batch/5_statements` | 5 SQL statements | < 500μs |
| `cache/cache_hit` | Cache hit latency | < 10μs |

### Turso (`turso/`)

| Benchmark | Description | Target |
|-----------|-------------|--------|
| `execute/simple_select` | Simple statement | < 50μs |
| `execute/with_args` | With arguments | < 100μs |
| `batch/5_statements` | Batch execution | < 500μs |
| `pipeline/transaction_3_ops` | Transaction | < 500μs |
| `blob/1kb_blob` | 1KB blob write | < 200μs |
| `concurrent/10_parallel` | 10 parallel ops | < 2ms |

## CI/CD Integration

### GitHub Actions Workflow

The `.github/workflows/multicompat-ci.yml` provides:

1. **Test Suite** - Runs on every push/PR
   - Formatting check
   - Clippy lints
   - Unit tests
   - Doc tests

2. **Benchmarks** - Runs on main branch pushes
   - Full benchmark suite
   - Results saved as artifacts

3. **Performance Regression** - Runs on PRs
   - Compares against base branch
   - Warns on >20% regression

4. **Integration Tests** - Runs on every push
   - Starts server
   - Health check
   - Full integration test suite

5. **Security Audit** - Runs cargo-audit

6. **Code Coverage** - Generates lcov report

## Performance Targets

| Operation | P50 Target | P99 Target |
|-----------|------------|------------|
| KV Get | 50μs | 200μs |
| KV Put | 100μs | 500μs |
| KV Delete | 100μs | 500μs |
| Batch Write (25) | 5ms | 20ms |
| Query (100 items) | 5ms | 20ms |
| SQL SELECT | 50μs | 200μs |
| Cache Hit | 10μs | 50μs |

## Regression Detection

Performance regressions are automatically detected by:

1. Baseline benchmarks saved on main branch
2. PR benchmarks compared against baseline
3. >20% regression triggers warning
4. Results uploaded as PR artifacts

## Local Development

### Quick Test Cycle
```bash
# Fast unit tests
cargo test -p lumadb-multicompat --lib

# Run specific test
cargo test -p lumadb-multicompat test_put_item

# Watch mode (requires cargo-watch)
cargo watch -x 'test -p lumadb-multicompat'
```

### Debugging Failed Tests
```bash
# Verbose output
RUST_BACKTRACE=1 cargo test -p lumadb-multicompat -- --nocapture

# Single test with tracing
RUST_LOG=debug cargo test -p lumadb-multicompat test_name -- --nocapture
```

### Profile-Guided Optimization
```bash
# Generate PGO data
cargo bench -p lumadb-multicompat -- --profile-time 60

# Build with PGO
RUSTFLAGS="-Cprofile-use=target/pgo-data" cargo build --release
```
