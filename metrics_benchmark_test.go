package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkMetricsOverhead measures the overhead of metrics tracking
func BenchmarkMetricsOverhead(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()

	// Test with various configurations
	configs := []struct {
		name   string
		config CometConfig
	}{
		{
			name:   "NoMetrics",
			config: DefaultCometConfig(),
		},
		{
			name:   "WithMetrics",
			config: MultiProcessConfig(0, 2), // This enables mmap state with metrics
		},
	}

	sizes := []int{128, 1024, 4096}

	for _, cfg := range configs {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/Size=%d", cfg.name, size), func(b *testing.B) {
				client, err := NewClientWithConfig(dir+"/"+cfg.name, cfg.config)
				if err != nil {
					b.Fatal(err)
				}
				defer client.Close()

				// Prepare data
				data := make([]byte, size)
				for i := range data {
					data[i] = byte(i % 256)
				}
				entries := [][]byte{data}

				b.ResetTimer()
				b.ReportAllocs()

				// Benchmark write operations
				for i := 0; i < b.N; i++ {
					_, err := client.Append(ctx, "test:v1:shard:0000", entries)
					if err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()

				// Report metrics if available
				shard, err := client.getOrCreateShard(0)
				if err == nil {
					if state := shard.state; state != nil {
						b.ReportMetric(float64(state.TotalWrites)/float64(b.N), "writes/op")
						avgLatency := state.GetAverageWriteLatency()
						if avgLatency > 0 {
							b.ReportMetric(float64(avgLatency)/1000, "μs/write")
						}
					}
				}
			})
		}
	}
}

// BenchmarkLatencyMetrics specifically benchmarks the latency tracking overhead
func BenchmarkLatencyMetrics(b *testing.B) {
	dir := b.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	shard, err := client.getOrCreateShard(0)
	if err != nil {
		b.Fatal(err)
	}

	if shard.state == nil {
		b.Skip("State not available")
	}

	b.Run("UpdateWriteLatency", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate various latencies
			latency := uint64(100000 + i%50000) // 100-150μs range
			if state := shard.state; state != nil {
				state.UpdateWriteLatency(latency)
			}
		}
	})
}

// BenchmarkCompressionMetrics benchmarks compression with metrics tracking
func BenchmarkCompressionMetrics(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	config := MultiProcessConfig(0, 2)
	config.Compression.MinCompressSize = 100 // Enable compression

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	// Prepare compressible data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte('A' + i%26) // Repeating pattern
	}
	entries := [][]byte{data}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, "test:v1:shard:0000", entries)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()

	// Report compression metrics
	shard, _ := client.getOrCreateShard(0)
	if state := shard.state; state != nil {
		ratio := state.GetCompressionRatioFloat()
		b.ReportMetric(ratio*100, "%compression")
		if state.CompressionTimeNanos > 0 {
			avgCompressionTime := float64(state.CompressionTimeNanos) / float64(state.CompressedEntries)
			b.ReportMetric(avgCompressionTime/1000, "μs/compression")
		}
	}
}

// BenchmarkConcurrentMetrics tests metrics under concurrent load
func BenchmarkConcurrentMetrics(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	config := MultiProcessConfig(0, 2)

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	data := make([]byte, 512)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.RunParallel(func(pb *testing.PB) {
		entries := [][]byte{data}
		writerID := 0
		for pb.Next() {
			streamName := fmt.Sprintf("test:v1:shard:%04d", writerID%4+1)
			_, err := client.Append(ctx, streamName, entries)
			if err != nil {
				b.Fatal(err)
			}
			writerID++
		}
	})

	b.StopTimer()

	// Report aggregate metrics
	totalWrites := uint64(0)
	totalLatency := uint64(0)
	for i := uint32(1); i <= 4; i++ {
		shard, err := client.getOrCreateShard(i)
		if err == nil {
			if state := shard.state; state != nil {
				totalWrites += state.TotalWrites
				totalLatency += state.WriteLatencySum
			}
		}
	}

	if totalWrites > 0 {
		avgLatency := totalLatency / totalWrites
		b.ReportMetric(float64(avgLatency)/1000, "μs/write-avg")
	}
}

// BenchmarkRetentionMetrics benchmarks retention with metrics tracking
func BenchmarkRetentionMetrics(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	config := MultiProcessConfig(0, 2)
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Storage.MaxFileSize = 1024 // Small files to force rotation

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	// Write data to create multiple files
	data := make([]byte, 512)
	for i := 0; i < 10; i++ {
		_, err := client.Append(ctx, "test:v1:shard:0000", [][]byte{data})
		if err != nil {
			b.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.ResetTimer()

	// Benchmark retention runs
	for i := 0; i < b.N; i++ {
		client.ForceRetentionCleanup()
	}

	b.StopTimer()

	// Report retention metrics
	shard, _ := client.getOrCreateShard(0)
	if state := shard.state; state != nil {
		b.ReportMetric(float64(state.RetentionRuns), "runs")
		if state.RetentionTimeNanos > 0 && state.RetentionRuns > 0 {
			avgRetentionTime := float64(state.RetentionTimeNanos) / float64(state.RetentionRuns)
			b.ReportMetric(avgRetentionTime/1e6, "ms/retention")
		}
		b.ReportMetric(float64(state.FilesDeleted), "files-deleted")
		b.ReportMetric(float64(state.BytesReclaimed), "bytes-reclaimed")
	}
}
