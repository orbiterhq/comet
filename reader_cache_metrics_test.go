package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestReaderCacheMetrics verifies that reader cache metrics are properly tracked
func TestReaderCacheMetrics(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create client with multi-process config to get state
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 512 // Small files to create multiple
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write data to create multiple files
	streamName := "test:v1:shard:0001"
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "test data for reader cache metrics with padding to ensure file rotation"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force checkpoint to ensure files are persisted
	client.Sync(ctx)

	// Get the shard
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal("Failed to get shard:", err)
	}

	if shard.loadState() == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Check initial state - no reader cache activity yet
	initialFileMaps := atomic.LoadUint64(&shard.state.ReaderFileMaps)
	initialFileUnmaps := atomic.LoadUint64(&shard.state.ReaderFileUnmaps)
	initialCacheBytes := atomic.LoadUint64(&shard.state.ReaderCacheBytes)
	initialMappedFiles := atomic.LoadUint64(&shard.state.ReaderMappedFiles)
	initialRemaps := atomic.LoadUint64(&shard.state.ReaderFileRemaps)
	initialEvicts := atomic.LoadUint64(&shard.state.ReaderCacheEvicts)

	t.Logf("Initial reader cache metrics:")
	t.Logf("  File maps: %d", initialFileMaps)
	t.Logf("  File unmaps: %d", initialFileUnmaps)
	t.Logf("  Cache bytes: %d", initialCacheBytes)
	t.Logf("  Mapped files: %d", initialMappedFiles)
	t.Logf("  File remaps: %d", initialRemaps)
	t.Logf("  Cache evicts: %d", initialEvicts)

	// Create a consumer to trigger reader creation
	consumer := NewConsumer(client, ConsumerOptions{Group: "test-metrics"})
	defer consumer.Close()

	// Read some messages to trigger file mapping
	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages) == 0 {
		t.Fatal("Expected to read some messages")
	}

	// Check metrics after reading
	afterReadFileMaps := atomic.LoadUint64(&shard.state.ReaderFileMaps)
	afterReadCacheBytes := atomic.LoadUint64(&shard.state.ReaderCacheBytes)
	afterReadMappedFiles := atomic.LoadUint64(&shard.state.ReaderMappedFiles)

	t.Logf("\nAfter reading:")
	t.Logf("  File maps: %d (was %d)", afterReadFileMaps, initialFileMaps)
	t.Logf("  Cache bytes: %d (was %d)", afterReadCacheBytes, initialCacheBytes)
	t.Logf("  Mapped files: %d (was %d)", afterReadMappedFiles, initialMappedFiles)

	// Verify metrics increased
	if afterReadFileMaps <= initialFileMaps {
		t.Error("ReaderFileMaps should have increased after reading")
	}
	if afterReadCacheBytes == 0 {
		t.Error("ReaderCacheBytes should be > 0 after mapping files")
	}
	if afterReadMappedFiles == 0 {
		t.Error("ReaderMappedFiles should be > 0 after mapping files")
	}

	// Write more data to trigger file growth and potential remapping
	for i := 0; i < 5; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "message": "additional data"}`, i+10))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond) // Give time for async operations
	}

	// Read again to potentially trigger remapping if files grew
	messages2, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(messages2) == 0 {
		t.Fatal("Expected to read more messages")
	}

	// Final metrics check
	finalFileMaps := atomic.LoadUint64(&shard.state.ReaderFileMaps)
	finalFileUnmaps := atomic.LoadUint64(&shard.state.ReaderFileUnmaps)
	finalCacheBytes := atomic.LoadUint64(&shard.state.ReaderCacheBytes)
	finalMappedFiles := atomic.LoadUint64(&shard.state.ReaderMappedFiles)
	finalRemaps := atomic.LoadUint64(&shard.state.ReaderFileRemaps)
	finalEvicts := atomic.LoadUint64(&shard.state.ReaderCacheEvicts)
	finalCacheHits := atomic.LoadUint64(&shard.state.ReaderCacheHits)

	t.Logf("\nFinal reader cache metrics:")
	t.Logf("  File maps: %d", finalFileMaps)
	t.Logf("  File unmaps: %d", finalFileUnmaps)
	t.Logf("  Cache bytes: %d", finalCacheBytes)
	t.Logf("  Mapped files: %d", finalMappedFiles)
	t.Logf("  File remaps: %d", finalRemaps)
	t.Logf("  Cache evicts: %d", finalEvicts)
	t.Logf("  Cache hits: %d", finalCacheHits)

	// Verify we have some cache hits (reading again should use cached reader)
	if finalCacheHits == 0 {
		t.Error("Expected some reader cache hits")
	}

	// Close consumer to trigger reader cleanup
	consumer.Close()

	// Give a moment for async cleanup
	time.Sleep(10 * time.Millisecond)

	// Check that unmaps occurred
	afterCloseUnmaps := atomic.LoadUint64(&shard.state.ReaderFileUnmaps)
	afterCloseCacheBytes := atomic.LoadUint64(&shard.state.ReaderCacheBytes)
	afterCloseMappedFiles := atomic.LoadUint64(&shard.state.ReaderMappedFiles)

	t.Logf("\nAfter closing consumer:")
	t.Logf("  File unmaps: %d (was %d)", afterCloseUnmaps, finalFileUnmaps)
	t.Logf("  Cache bytes: %d (was %d)", afterCloseCacheBytes, finalCacheBytes)
	t.Logf("  Mapped files: %d (was %d)", afterCloseMappedFiles, finalMappedFiles)

	// After closing, we should have unmapped files
	if afterCloseUnmaps <= finalFileUnmaps {
		t.Error("ReaderFileUnmaps should have increased after closing")
	}
	if afterCloseCacheBytes >= finalCacheBytes {
		t.Error("ReaderCacheBytes should have decreased after closing")
	}
	if afterCloseMappedFiles >= finalMappedFiles {
		t.Error("ReaderMappedFiles should have decreased after closing")
	}
}

// TestReaderCacheEviction tests that eviction metrics work by directly testing eviction
func TestReaderCacheEviction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create client
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Write some data
	streamName := "test:v1:shard:0001"
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}
	}

	client.Sync(ctx)

	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	if shard.loadState() == nil {
		t.Skip("State not available in non-mmap mode")
	}

	// Create reader directly
	shard.mu.RLock()
	reader, err := NewReader(shard.shardID, shard.index)
	shard.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	reader.SetState(shard.state)

	// Get initial metrics
	initialMaps := atomic.LoadUint64(&shard.state.ReaderFileMaps)
	initialUnmaps := atomic.LoadUint64(&shard.state.ReaderFileUnmaps)
	initialEvicts := atomic.LoadUint64(&shard.state.ReaderCacheEvicts)

	// Manually test eviction by mapping a file and then evicting it
	reader.mappingMu.Lock()
	// Map file 0
	if len(reader.fileInfos) > 0 {
		mapped, err := reader.mapFile(0, reader.fileInfos[0])
		if err == nil {
			reader.mappedFiles[0] = mapped
			atomic.StoreUint64(&shard.state.ReaderMappedFiles, uint64(len(reader.mappedFiles)))
		}
	}
	reader.mappingMu.Unlock()

	// Now evict the oldest file
	err = reader.evictOldestFile()
	if err != nil {
		t.Logf("Eviction returned: %v", err)
	}

	// Check metrics changed
	finalMaps := atomic.LoadUint64(&shard.state.ReaderFileMaps)
	finalUnmaps := atomic.LoadUint64(&shard.state.ReaderFileUnmaps)
	finalEvicts := atomic.LoadUint64(&shard.state.ReaderCacheEvicts)
	finalMappedFiles := atomic.LoadUint64(&shard.state.ReaderMappedFiles)

	t.Logf("Eviction test results:")
	t.Logf("  Maps: %d -> %d", initialMaps, finalMaps)
	t.Logf("  Unmaps: %d -> %d", initialUnmaps, finalUnmaps)
	t.Logf("  Evicts: %d -> %d", initialEvicts, finalEvicts)
	t.Logf("  Currently mapped: %d files", finalMappedFiles)

	// Verify metrics were incremented
	if finalMaps <= initialMaps {
		t.Error("Expected file maps to increase")
	}
	if err == nil && finalUnmaps <= initialUnmaps {
		t.Error("Expected file unmaps to increase after eviction")
	}
	if err == nil && finalEvicts <= initialEvicts {
		t.Error("Expected evictions to increase")
	}
}
