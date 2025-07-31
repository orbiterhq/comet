package comet

import (
	"context"
	"testing"
)

// BenchmarkSingleProcessMode benchmarks single-process write performance
func BenchmarkSingleProcessMode(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Concurrency.EnableMultiProcessMode = false // Single-process mode

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

// BenchmarkMultiProcessMode benchmarks multi-process write performance
func BenchmarkMultiProcessMode(b *testing.B) {
	dir := b.TempDir()
	config := MultiProcessConfig()

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

// BenchmarkMultiProcessMode_WithContention benchmarks with simulated contention
func BenchmarkMultiProcessMode_WithContention(b *testing.B) {
	dir := b.TempDir()
	config := MultiProcessConfig()

	// Create multiple clients to simulate contention
	clients := make([]*Client, 4)
	for i := 0; i < 4; i++ {
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			b.Fatal(err)
		}
		defer client.Close()
		clients[i] = client
	}

	ctx := context.Background()
	streamName := "test:v1:shard:0001"
	data := []byte(`{"id":123,"message":"benchmark test entry"}`)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine uses a different client
		clientIdx := 0
		for pb.Next() {
			client := clients[clientIdx%len(clients)]
			_, err := client.Append(ctx, streamName, [][]byte{data})
			if err != nil {
				b.Fatal(err)
			}
			clientIdx++
		}
	})
}
