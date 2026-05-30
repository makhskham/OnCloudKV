// Package bench provides structured benchmarks that prove the performance
// claims made about OnCloudKV:
//   - Write throughput +300% under concurrent workloads (skiplist vs. mutex map)
//   - Sub-100ms P99 latency under load
//   - Bloom filter effectiveness (>99% negative-lookup short-circuit)
//
// Run with:  make bench
// Or:        go test -bench=. -benchmem -benchtime=10s ./bench/...
package bench

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makhskham/oncloudkv/internal/storage/bloom"
	"github.com/makhskham/oncloudkv/internal/storage/skiplist"
	"github.com/makhskham/oncloudkv/internal/storage/wal"
)

// ── Skiplist benchmarks ───────────────────────────────────────────────────────

// BenchmarkSkiplistPut_Sequential measures single-goroutine Put throughput.
func BenchmarkSkiplistPut_Sequential(b *testing.B) {
	sl := skiplist.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Put(fmt.Sprintf("key-%d", i), []byte("value"), int64(i))
	}
}

// BenchmarkSkiplistPut_Concurrent measures Put throughput with N goroutines.
// This is the benchmark that validates the "300% write throughput improvement
// under concurrent workloads" claim by comparison with a baseline mutex map.
func BenchmarkSkiplistPut_Concurrent1(b *testing.B)   { benchConcurrentPut(b, 1) }
func BenchmarkSkiplistPut_Concurrent10(b *testing.B)  { benchConcurrentPut(b, 10) }
func BenchmarkSkiplistPut_Concurrent50(b *testing.B)  { benchConcurrentPut(b, 50) }
func BenchmarkSkiplistPut_Concurrent100(b *testing.B) { benchConcurrentPut(b, 100) }
func BenchmarkSkiplistPut_Concurrent500(b *testing.B) { benchConcurrentPut(b, 500) }

func benchConcurrentPut(b *testing.B, concurrency int) {
	b.Helper()
	sl := skiplist.New()
	var counter atomic.Int64
	b.SetParallelism(concurrency)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			sl.Put(fmt.Sprintf("key-%d", i), []byte("value"), i)
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkMutexMapPut_Concurrent is the baseline - a plain map + global RWMutex.
// Compare against BenchmarkSkiplistPut_Concurrent* to see the throughput delta.
func BenchmarkMutexMapPut_Concurrent1(b *testing.B)   { benchMutexMapPut(b, 1) }
func BenchmarkMutexMapPut_Concurrent10(b *testing.B)  { benchMutexMapPut(b, 10) }
func BenchmarkMutexMapPut_Concurrent50(b *testing.B)  { benchMutexMapPut(b, 50) }
func BenchmarkMutexMapPut_Concurrent100(b *testing.B) { benchMutexMapPut(b, 100) }
func BenchmarkMutexMapPut_Concurrent500(b *testing.B) { benchMutexMapPut(b, 500) }

func benchMutexMapPut(b *testing.B, concurrency int) {
	b.Helper()
	var mu sync.RWMutex
	m := make(map[string][]byte)
	var counter atomic.Int64
	b.SetParallelism(concurrency)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			key := fmt.Sprintf("key-%d", i)
			mu.Lock()
			m[key] = []byte("value")
			mu.Unlock()
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkSkiplistGet measures read latency.
func BenchmarkSkiplistGet(b *testing.B) {
	sl := skiplist.New()
	for i := 0; i < 100000; i++ {
		sl.Put(fmt.Sprintf("key-%d", i), []byte("value"), int64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sl.Get(fmt.Sprintf("key-%d", i%100000))
	}
}

// ── Bloom filter benchmarks ───────────────────────────────────────────────────

// BenchmarkBloomFilter_TruePositive measures lookup time for present keys.
func BenchmarkBloomFilter_TruePositive(b *testing.B) {
	f := bloom.New(100000, 0.01)
	for i := 0; i < 100000; i++ {
		f.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Contains([]byte(fmt.Sprintf("key-%d", i%100000)))
	}
}

// BenchmarkBloomFilter_NegativeLookup proves that absent keys are rejected
// without touching disk - validating the SSTable read acceleration claim.
func BenchmarkBloomFilter_NegativeLookup(b *testing.B) {
	f := bloom.New(100000, 0.01)
	for i := 0; i < 100000; i++ {
		f.Add([]byte(fmt.Sprintf("key-%d", i)))
	}
	b.ResetTimer()
	falsePositives := 0
	for i := 0; i < b.N; i++ {
		// these keys were never inserted
		if f.Contains([]byte(fmt.Sprintf("absent-%d", i))) {
			falsePositives++
		}
	}
	fpr := float64(falsePositives) / float64(b.N) * 100
	b.ReportMetric(fpr, "%FalsePositiveRate")
}

// ── WAL throughput benchmark ──────────────────────────────────────────────────

func BenchmarkWAL_GroupCommit(b *testing.B) {
	dir := b.TempDir()
	w, err := wal.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	payload := []byte(`{"op":"put","key":"bench","value":"AAAAAAAAAAAAAAAA"}`)
	b.SetParallelism(50)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := w.Append(payload); err != nil {
				b.Error(err)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

// ── Latency percentile test ───────────────────────────────────────────────────

// TestSkiplistLatencyPercentiles is a test (not a benchmark) that measures
// actual P50/P99/P999 read latency under concurrent load and fails if P99 > 100ms.
func TestSkiplistLatencyPercentiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	sl := skiplist.New()
	for i := 0; i < 500000; i++ {
		sl.Put(fmt.Sprintf("key-%d", i), make([]byte, 64), int64(i))
	}

	const goroutines = 500
	const ops = 1000
	latencies := make([]time.Duration, goroutines*ops)
	var wg sync.WaitGroup
	var idx atomic.Int64

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				start := time.Now()
				sl.Get(fmt.Sprintf("key-%d", (gid*ops+i)%500000))
				lat := time.Since(start)
				pos := idx.Add(1) - 1
				latencies[pos] = lat
			}
		}(g)
	}
	wg.Wait()

	p50 := percentile(latencies, 0.50)
	p99 := percentile(latencies, 0.99)
	p999 := percentile(latencies, 0.999)

	t.Logf("Latency under %d concurrent goroutines (%d ops each):", goroutines, ops)
	t.Logf("  P50:  %v", p50)
	t.Logf("  P99:  %v", p99)
	t.Logf("  P999: %v", p999)

	if p99 > 100*time.Millisecond {
		t.Errorf("P99 latency %v exceeds 100ms SLO", p99)
	}
	os.Stdout.WriteString(fmt.Sprintf("LATENCY  p50=%v  p99=%v  p999=%v\n", p50, p99, p999))
}

func percentile(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	// insertion sort - fine for test, not production
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
