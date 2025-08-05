package comet

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// BenchmarkFileRotation measures the performance of file rotation under concurrent load
func BenchmarkFileRotation(b *testing.B) {
	sizes := []int{
		1024,       // 1KB - extreme rotation
		10 * 1024,  // 10KB - high rotation
		100 * 1024, // 100KB - moderate rotation
	}

	for _, maxSize := range sizes {
		b.Run(fmt.Sprintf("MaxSize_%dKB", maxSize/1024), func(b *testing.B) {
			dir := b.TempDir()
			config := DefaultCometConfig()
			config.Storage.MaxFileSize = int64(maxSize)
			config.Storage.FlushInterval = 0 // Disable periodic flush

			client, err := NewClient(dir, config)
			if err != nil {
				b.Fatal(err)
			}
			defer client.Close()

			ctx := context.Background()
			streamName := "bench:v1:shard:0000"

			// Prepare data that will cause rotations
			entrySize := 100 // 100 bytes per entry
			data := make([]byte, entrySize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.SetBytes(int64(entrySize))

			// Run concurrent writers to stress rotation
			var wg sync.WaitGroup
			numWriters := 4
			entriesPerWriter := b.N / numWriters

			start := time.Now()
			for w := 0; w < numWriters; w++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()
					for i := 0; i < entriesPerWriter; i++ {
						entry := []byte(fmt.Sprintf("writer-%d-entry-%d-%s", writerID, i, data))
						if _, err := client.Append(ctx, streamName, [][]byte{entry}); err != nil {
							b.Errorf("Writer %d failed: %v", writerID, err)
							return
						}
					}
				}(w)
			}

			wg.Wait()
			duration := time.Since(start)

			// Calculate metrics
			totalBytes := int64(b.N) * int64(entrySize)
			throughputMBps := float64(totalBytes) / duration.Seconds() / (1024 * 1024)
			rotations := totalBytes / int64(maxSize)

			b.ReportMetric(throughputMBps, "MB/s")
			b.ReportMetric(float64(rotations), "rotations")
			b.ReportMetric(float64(duration.Nanoseconds())/float64(b.N), "ns/write")
		})
	}
}

// BenchmarkRotationLockContention specifically measures lock contention during rotation
func BenchmarkRotationLockContention(b *testing.B) {
	dir := b.TempDir()
	config := DefaultCometConfig()
	config.Storage.MaxFileSize = 10 * 1024 // 10KB files for frequent rotation

	client, err := NewClient(dir, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "bench:v1:shard:0000"

	// Pre-fill to be near rotation boundary
	prefillData := make([]byte, 9*1024) // 9KB
	client.Append(ctx, streamName, [][]byte{prefillData})

	b.ResetTimer()

	// Measure time for operations during rotation
	var wg sync.WaitGroup
	numReaders := 10
	numWriters := 5

	// Start readers that will be blocked during rotation
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < b.N/numReaders; i++ {
				// Try to read during rotation
				shard, _ := client.getOrCreateShard(0)
				shard.mu.RLock()
				_ = len(shard.index.Files) // Simulate read operation
				shard.mu.RUnlock()
			}
		}(r)
	}

	// Writers that trigger rotations
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			data := make([]byte, 200) // Each write triggers rotation
			for i := 0; i < b.N/numWriters; i++ {
				client.Append(ctx, streamName, [][]byte{data})
			}
		}(w)
	}

	wg.Wait()
}
