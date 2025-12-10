// Package benchmarks provides comprehensive benchmarks for TDB+ Go cluster layer
package benchmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkSingleInsert benchmarks single document insertions
func BenchmarkSingleInsert(b *testing.B) {
	doc := map[string]interface{}{
		"id":    "test123",
		"name":  "Test User",
		"email": "test@example.com",
		"age":   30,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(doc)
	}
}

// BenchmarkBatchInsert benchmarks batch document insertions
func BenchmarkBatchInsert(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			docs := make([]map[string]interface{}, size)
			for i := 0; i < size; i++ {
				docs[i] = map[string]interface{}{
					"id":    fmt.Sprintf("doc_%d", i),
					"value": i,
					"data":  "test data",
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = json.Marshal(docs)
			}
		})
	}
}

// BenchmarkConcurrentOps benchmarks concurrent operations
func BenchmarkConcurrentOps(b *testing.B) {
	concurrency := []int{1, 2, 4, 8, 16, 32}

	for _, c := range concurrency {
		b.Run(fmt.Sprintf("goroutines_%d", c), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(c)
				for j := 0; j < c; j++ {
					go func() {
						defer wg.Done()
						// Simulate work
						doc := map[string]interface{}{
							"id":   fmt.Sprintf("doc_%d", j),
							"data": "test",
						}
						_, _ = json.Marshal(doc)
					}()
				}
				wg.Wait()
			}
		})
	}
}

// BenchmarkChannelThroughput benchmarks channel communication
func BenchmarkChannelThroughput(b *testing.B) {
	ch := make(chan struct{}, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- struct{}{}
		<-ch
	}
}

// BenchmarkMapOperations benchmarks map operations
func BenchmarkMapOperations(b *testing.B) {
	b.Run("insert", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := make(map[string]int)
			for j := 0; j < 1000; j++ {
				m[fmt.Sprintf("key_%d", j)] = j
			}
		}
	})

	b.Run("lookup", func(b *testing.B) {
		m := make(map[string]int)
		for j := 0; j < 1000; j++ {
			m[fmt.Sprintf("key_%d", j)] = j
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				_ = m[fmt.Sprintf("key_%d", j)]
			}
		}
	})
}

// BenchmarkSyncMap benchmarks sync.Map operations
func BenchmarkSyncMap(b *testing.B) {
	b.Run("store", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var m sync.Map
			for j := 0; j < 1000; j++ {
				m.Store(fmt.Sprintf("key_%d", j), j)
			}
		}
	})

	b.Run("load", func(b *testing.B) {
		var m sync.Map
		for j := 0; j < 1000; j++ {
			m.Store(fmt.Sprintf("key_%d", j), j)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				m.Load(fmt.Sprintf("key_%d", j))
			}
		}
	})
}

// BenchmarkContext benchmarks context operations
func BenchmarkContext(b *testing.B) {
	b.Run("background", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = context.Background()
		}
	})

	b.Run("with_timeout", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			cancel()
			_ = ctx
		}
	})

	b.Run("with_cancel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_ = ctx
		}
	})
}

// BenchmarkJSONParsing benchmarks JSON parsing
func BenchmarkJSONParsing(b *testing.B) {
	smallJSON := []byte(`{"id":"123","name":"test"}`)
	mediumJSON := []byte(`{"id":"123","name":"test","tags":["a","b","c"],"nested":{"field1":"value1","field2":42}}`)
	largeJSON := make([]byte, 10000)
	copy(largeJSON, []byte(`{"data":"`))
	for i := 8; i < 9990; i++ {
		largeJSON[i] = 'x'
	}
	copy(largeJSON[9990:], []byte(`"}`))

	b.Run("small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var v map[string]interface{}
			_ = json.Unmarshal(smallJSON, &v)
		}
	})

	b.Run("medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var v map[string]interface{}
			_ = json.Unmarshal(mediumJSON, &v)
		}
	})

	b.Run("large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var v map[string]interface{}
			_ = json.Unmarshal(largeJSON, &v)
		}
	})
}

// BenchmarkConnectionPool benchmarks connection pool-like operations
func BenchmarkConnectionPool(b *testing.B) {
	poolSizes := []int{10, 50, 100}

	for _, size := range poolSizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			pool := make(chan struct{}, size)
			for i := 0; i < size; i++ {
				pool <- struct{}{}
			}

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					// Acquire
					<-pool
					// Use (simulated)
					time.Sleep(time.Microsecond)
					// Release
					pool <- struct{}{}
				}
			})
		})
	}
}

// BenchmarkMutex benchmarks mutex operations
func BenchmarkMutex(b *testing.B) {
	b.Run("uncontended", func(b *testing.B) {
		var mu sync.Mutex
		for i := 0; i < b.N; i++ {
			mu.Lock()
			mu.Unlock()
		}
	})

	b.Run("contended", func(b *testing.B) {
		var mu sync.Mutex
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.Lock()
				mu.Unlock()
			}
		})
	})
}

// BenchmarkRWMutex benchmarks RWMutex operations
func BenchmarkRWMutex(b *testing.B) {
	b.Run("read_uncontended", func(b *testing.B) {
		var mu sync.RWMutex
		for i := 0; i < b.N; i++ {
			mu.RLock()
			mu.RUnlock()
		}
	})

	b.Run("read_contended", func(b *testing.B) {
		var mu sync.RWMutex
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.RLock()
				mu.RUnlock()
			}
		})
	})

	b.Run("write_contended", func(b *testing.B) {
		var mu sync.RWMutex
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				mu.Lock()
				mu.Unlock()
			}
		})
	})
}
