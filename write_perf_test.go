package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestWriteLargeBatchPerformance isolates the write performance issue for large batches
func TestWriteLargeBatchPerformance(t *testing.T) {

	dir := t.TempDir()
	config := DefaultCometConfig()
	// Disable compression to isolate write performance
	config.Compression.MinCompressSize = 1 << 30
	// Increase checkpoint threshold to avoid triggering on 1000 entry batch
	config.Storage.CheckpointEntries = 10000

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "bench:v1:shard:0000"

	// Test large batch (1000 entries) which shows 5x regression
	batchSize := 1000

	// Prepare batch
	entries := make([][]byte, batchSize)
	for i := 0; i < batchSize; i++ {
		entries[i] = []byte(fmt.Sprintf(`{"index":%d,"timestamp":%d,"data":"payload_%d"}`,
			i, time.Now().UnixNano(), i))
	}

	// Warm up
	for i := 0; i < 5; i++ {
		_, err := client.Append(ctx, streamName, entries)
		if err != nil {
			t.Fatalf("warmup failed: %v", err)
		}
	}

	// Measure write performance with detailed timing
	iterations := 100
	totalDuration := time.Duration(0)

	for iter := 0; iter < iterations; iter++ {
		start := time.Now()

		_, err := client.Append(ctx, streamName, entries)
		if err != nil {
			t.Fatalf("failed to write batch: %v", err)
		}

		duration := time.Since(start)
		totalDuration += duration

		if iter < 10 || iter%10 == 0 {
			t.Logf("Iteration %d: %v (%.2f µs/entry)",
				iter, duration,
				float64(duration.Microseconds())/float64(batchSize))
		}
	}

	avgDuration := totalDuration / time.Duration(iterations)
	avgPerEntry := avgDuration / time.Duration(batchSize)

	t.Logf("Average for %d entries: %v total, %v per entry",
		batchSize, avgDuration, avgPerEntry)

	// Should be < 200µs for 1000 entries (was 133µs, now 703µs)
	if avgDuration > 200*time.Microsecond {
		t.Errorf("Write performance regression: %v for %d entries (should be < 200µs)",
			avgDuration, batchSize)
	}
}
