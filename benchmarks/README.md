# Benchmarks

This directory contains benchmarks for LumaDB components.

## Running Benchmarks

### Go Cluster
Run standard Go benchmarks:
```bash
cd go-cluster
go test -bench=. ./...
```

### Rust Core
Run Rust benchmarks (via criterion if configured, or simple tests):
```bash
cd rust-core
cargo bench
```

### End-to-End
Use the provided scripts:
- `python_bench.py`: Tests embedding generation performance.
