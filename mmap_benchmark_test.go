package comet

import (
	"context"
	"testing"
)

// BenchmarkMultiProcessMode_MmapWriter benchmarks the memory-mapped writer performance
func BenchmarkMultiProcessMode_MmapWriter(b *testing.B) {
	dir := b.TempDir()
	config := MultiProcessConfig()

	// Create client which will use mmap writer in multi-process mode
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0001"
	data := []byte(`{"id":123,"message":"benchmark test entry"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMultiProcessMode_Comparison shows the performance progression
func BenchmarkMultiProcessMode_Comparison(b *testing.B) {
	data := []byte(`{"id":123,"message":"benchmark test entry"}`)
	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	b.Run("SingleProcess", func(b *testing.B) {
		dir := b.TempDir()
		config := DefaultCometConfig()

		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MultiProcess_AsyncCheckpoint", func(b *testing.B) {
		dir := b.TempDir()
		config := MultiProcessConfig()

		// Temporarily disable mmap writer to test async checkpoint only
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			b.Fatal(err)
		}

		// Force disable mmap writer for this test
		shard, _ := client.getOrCreateShard(1)
		if shard.mmapWriter != nil {
			shard.mmapWriter.Close()
			shard.mmapWriter = nil
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				b.Fatal(err)
			}
		}

		client.Close()
	})

	b.Run("MultiProcess_MmapWriter", func(b *testing.B) {
		dir := b.TempDir()
		config := MultiProcessConfig()

		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Performance Summary:
// 1. Single-process mode: ~1.6μs/op
// 2. Multi-process with async checkpoint: ~3.1ms/op
// 3. Multi-process with mmap writer: Target 50-150μs/op
