"""
TDB+ Python AI Layer Benchmarks

Comprehensive benchmarks for vector search, embeddings, and NLP operations.
"""

import asyncio
import json
import time
from typing import Callable, List, Tuple
import numpy as np


class BenchmarkResult:
    """Stores benchmark results."""

    def __init__(self, name: str, iterations: int, total_time: float):
        self.name = name
        self.iterations = iterations
        self.total_time = total_time
        self.ops_per_sec = iterations / total_time
        self.avg_latency_ms = (total_time / iterations) * 1000

    def __str__(self) -> str:
        return (
            f"{self.name}: {self.ops_per_sec:.2f} ops/sec, "
            f"{self.avg_latency_ms:.3f} ms/op ({self.iterations} iterations)"
        )


def benchmark(name: str, iterations: int = 1000):
    """Decorator to benchmark a function."""
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            for _ in range(iterations):
                func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            result = BenchmarkResult(name, iterations, elapsed)
            print(result)
            return result
        return wrapper
    return decorator


def async_benchmark(name: str, iterations: int = 1000):
    """Decorator to benchmark an async function."""
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            start = time.perf_counter()
            for _ in range(iterations):
                await func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            result = BenchmarkResult(name, iterations, elapsed)
            print(result)
            return result
        return wrapper
    return decorator


# ============================================================================
# Vector Operations Benchmarks
# ============================================================================

@benchmark("vector_dot_product_384", iterations=10000)
def bench_dot_product_small():
    """Benchmark dot product for 384-dim vectors (MiniLM)."""
    a = np.random.randn(384).astype(np.float32)
    b = np.random.randn(384).astype(np.float32)
    np.dot(a, b)


@benchmark("vector_dot_product_768", iterations=10000)
def bench_dot_product_medium():
    """Benchmark dot product for 768-dim vectors (BERT)."""
    a = np.random.randn(768).astype(np.float32)
    b = np.random.randn(768).astype(np.float32)
    np.dot(a, b)


@benchmark("vector_cosine_similarity", iterations=10000)
def bench_cosine_similarity():
    """Benchmark cosine similarity."""
    a = np.random.randn(384).astype(np.float32)
    b = np.random.randn(384).astype(np.float32)
    np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


@benchmark("batch_cosine_1000", iterations=100)
def bench_batch_cosine():
    """Benchmark batch cosine similarity (1000 vectors)."""
    query = np.random.randn(384).astype(np.float32)
    corpus = np.random.randn(1000, 384).astype(np.float32)

    # Normalize
    query_norm = query / np.linalg.norm(query)
    corpus_norms = corpus / np.linalg.norm(corpus, axis=1, keepdims=True)

    # Compute similarities
    np.dot(corpus_norms, query_norm)


@benchmark("top_k_search_10000", iterations=100)
def bench_top_k():
    """Benchmark top-k search in 10000 vectors."""
    similarities = np.random.randn(10000).astype(np.float32)
    np.argsort(similarities)[-10:][::-1]


# ============================================================================
# JSON Operations Benchmarks
# ============================================================================

@benchmark("json_serialize_small", iterations=10000)
def bench_json_serialize_small():
    """Benchmark JSON serialization of small documents."""
    doc = {"id": "123", "name": "test", "value": 42}
    json.dumps(doc)


@benchmark("json_serialize_medium", iterations=5000)
def bench_json_serialize_medium():
    """Benchmark JSON serialization of medium documents."""
    doc = {
        "id": "123",
        "name": "test",
        "tags": ["a", "b", "c"] * 10,
        "nested": {f"field_{i}": i for i in range(20)}
    }
    json.dumps(doc)


@benchmark("json_parse_small", iterations=10000)
def bench_json_parse_small():
    """Benchmark JSON parsing of small documents."""
    data = '{"id": "123", "name": "test", "value": 42}'
    json.loads(data)


@benchmark("json_parse_medium", iterations=5000)
def bench_json_parse_medium():
    """Benchmark JSON parsing of medium documents."""
    doc = {
        "id": "123",
        "name": "test",
        "tags": ["a", "b", "c"] * 10,
        "nested": {f"field_{i}": i for i in range(20)}
    }
    data = json.dumps(doc)
    json.loads(data)


# ============================================================================
# Text Processing Benchmarks
# ============================================================================

@benchmark("tokenize_simple", iterations=10000)
def bench_tokenize():
    """Benchmark simple tokenization."""
    text = "The quick brown fox jumps over the lazy dog"
    text.lower().split()


@benchmark("text_hash", iterations=50000)
def bench_text_hash():
    """Benchmark text hashing for caching."""
    text = "The quick brown fox jumps over the lazy dog"
    hash(text)


@benchmark("regex_extract", iterations=5000)
def bench_regex_extract():
    """Benchmark regex extraction."""
    import re
    text = "Email: test@example.com, Phone: 123-456-7890"
    re.findall(r'\b[\w.-]+@[\w.-]+\.\w+\b', text)


# ============================================================================
# Async Operations Benchmarks
# ============================================================================

@async_benchmark("async_gather_10", iterations=1000)
async def bench_async_gather():
    """Benchmark asyncio.gather with 10 tasks."""
    async def dummy():
        return 42

    await asyncio.gather(*[dummy() for _ in range(10)])


@async_benchmark("async_queue", iterations=5000)
async def bench_async_queue():
    """Benchmark async queue operations."""
    queue = asyncio.Queue(maxsize=100)
    await queue.put({"data": "test"})
    await queue.get()


# ============================================================================
# Memory Operations Benchmarks
# ============================================================================

@benchmark("list_comprehension_10000", iterations=1000)
def bench_list_comprehension():
    """Benchmark list comprehension."""
    [i * 2 for i in range(10000)]


@benchmark("dict_comprehension_1000", iterations=1000)
def bench_dict_comprehension():
    """Benchmark dict comprehension."""
    {f"key_{i}": i for i in range(1000)}


@benchmark("numpy_array_creation", iterations=5000)
def bench_numpy_array():
    """Benchmark numpy array creation."""
    np.zeros((1000, 384), dtype=np.float32)


# ============================================================================
# Main
# ============================================================================

def run_all_benchmarks():
    """Run all benchmarks."""
    print("=" * 70)
    print("TDB+ Python AI Layer Benchmarks")
    print("=" * 70)

    print("\n--- Vector Operations ---")
    bench_dot_product_small()
    bench_dot_product_medium()
    bench_cosine_similarity()
    bench_batch_cosine()
    bench_top_k()

    print("\n--- JSON Operations ---")
    bench_json_serialize_small()
    bench_json_serialize_medium()
    bench_json_parse_small()
    bench_json_parse_medium()

    print("\n--- Text Processing ---")
    bench_tokenize()
    bench_text_hash()
    bench_regex_extract()

    print("\n--- Memory Operations ---")
    bench_list_comprehension()
    bench_dict_comprehension()
    bench_numpy_array()

    print("\n--- Async Operations ---")
    asyncio.run(bench_async_gather())
    asyncio.run(bench_async_queue())

    print("\n" + "=" * 70)
    print("Benchmarks complete!")


if __name__ == "__main__":
    run_all_benchmarks()
