package comet

import (
	"os"
	"sync"
	"testing"
)

// Benchmark atomic-based metrics
func BenchmarkMetricsAtomic_SingleThread(b *testing.B) {
	m := newAtomicMetrics()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.IncrementEntries(1)
		m.AddBytes(1024)
		m.IncrementCompressedEntries(1)
	}
}

// Benchmark mmap-based metrics
func BenchmarkMetricsMmap_SingleThread(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "comet-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := newMmapMetrics(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer m.(*mmapMetrics).Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.IncrementEntries(1)
		m.AddBytes(1024)
		m.IncrementCompressedEntries(1)
	}
}

// Benchmark atomic with high concurrency
func BenchmarkMetricsAtomic_Concurrent(b *testing.B) {
	m := newAtomicMetrics()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.IncrementEntries(1)
			m.AddBytes(1024)
			m.IncrementCompressedEntries(1)
		}
	})
}

// Benchmark mmap with high concurrency
func BenchmarkMetricsMmap_Concurrent(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "comet-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := newMmapMetrics(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer m.(*mmapMetrics).Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.IncrementEntries(1)
			m.AddBytes(1024)
			m.IncrementCompressedEntries(1)
		}
	})
}

// Benchmark realistic write pattern - atomic
func BenchmarkMetricsAtomic_RealisticWrite(b *testing.B) {
	m := newAtomicMetrics()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate a batch write
		m.IncrementEntries(10)
		m.AddBytes(10240)
		m.RecordWriteLatency(uint64(i % 1000))
		if i%10 == 0 {
			m.IncrementCompressedEntries(10)
			m.AddCompressedBytes(5120)
		}
	}
}

// Benchmark realistic write pattern - mmap
func BenchmarkMetricsMmap_RealisticWrite(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "comet-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	m, err := newMmapMetrics(tempDir)
	if err != nil {
		b.Fatal(err)
	}
	defer m.(*mmapMetrics).Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate a batch write
		m.IncrementEntries(10)
		m.AddBytes(10240)
		m.RecordWriteLatency(uint64(i % 1000))
		if i%10 == 0 {
			m.IncrementCompressedEntries(10)
			m.AddCompressedBytes(5120)
		}
	}
}

// Benchmark multi-process simulation with mmap
func BenchmarkMetricsMmap_MultiProcess(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "comet-bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create multiple "processes" (goroutines with separate mmap instances)
	numProcesses := 4
	var wg sync.WaitGroup
	wg.Add(numProcesses)

	b.ResetTimer()
	for p := 0; p < numProcesses; p++ {
		go func(procID int) {
			defer wg.Done()

			m, err := newMmapMetrics(tempDir)
			if err != nil {
				b.Error(err)
				return
			}
			defer m.(*mmapMetrics).Close()

			iterations := b.N / numProcesses
			for i := 0; i < iterations; i++ {
				m.IncrementEntries(1)
				m.AddBytes(1024)
				if i%100 == 0 {
					m.IncrementFileRotations(1)
				}
			}
		}(p)
	}
	wg.Wait()
}
