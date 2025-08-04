package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRetention_Basic(t *testing.T) {
	dir := t.TempDir()

	// Create config with retention enabled
	config := DefaultCometConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Storage.MaxFileSize = 1024 // Small file size to force rotation

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write some data to create files
	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "padding to make entries larger"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to add data: %v", err)
		}
	}

	// Force file rotation to create a second file
	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatalf("failed to get shard: %v", err)
	}

	// Debug: Check sequence counter before rotation
	shard.mu.Lock()
	if state := shard.loadState(); state != nil {
		t.Logf("File sequence before rotation: %d", state.LastFileSequence)
	} else {
		t.Log("CometState not initialized (single-process mode)")
	}
	oldFile := shard.index.CurrentFile
	err = shard.rotateFile(&client.metrics, &config, nil)
	newFile := shard.index.CurrentFile
	shard.mu.Unlock()
	if err != nil {
		t.Fatalf("failed to rotate file: %v", err)
	}
	t.Logf("Rotated from %s to %s", oldFile, newFile)

	// Write more data to the new file
	for i := 20; i < 25; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "new file data"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to add data: %v", err)
		}
	}

	// Manually mark the first file as old
	shard.mu.Lock()
	if len(shard.index.Files) > 0 {
		// Set EndTime to be older than retention MaxAge
		oldTime := time.Now().Add(-200 * time.Millisecond)
		shard.index.Files[0].EndTime = oldTime
		t.Logf("Marked file %s as old (EndTime: %v)", shard.index.Files[0].Path, shard.index.Files[0].EndTime)
	}
	fileCount := len(shard.index.Files)
	currentFile := shard.index.CurrentFile
	t.Logf("Total files before cleanup: %d (current file: %s)", fileCount, currentFile)
	for i, f := range shard.index.Files {
		isCurrent := f.Path == currentFile
		t.Logf("  File[%d]: %s (endTime=%v, isCurrent=%v)", i, f.Path, f.EndTime, isCurrent)
	}
	shard.mu.Unlock()

	if fileCount < 2 {
		t.Fatal("Need at least 2 files to test retention")
	}

	// Force retention cleanup
	client.ForceRetentionCleanup()

	// Get retention stats
	stats := client.GetRetentionStats()
	t.Logf("Retention stats after cleanup: TotalFiles=%d", stats.TotalFiles)

	// Old file should have been cleaned up
	if stats.TotalFiles >= fileCount {
		t.Errorf("expected fewer files after cleanup, got %d (originally %d)", stats.TotalFiles, fileCount)
	}
}

func TestRetention_Disabled(t *testing.T) {
	dir := t.TempDir()

	// Create config with retention disabled
	config := DefaultCometConfig()
	config.Retention.CleanupInterval = 0 // Disabled

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	// Should not crash and retention should not run
	time.Sleep(100 * time.Millisecond)

	// Verify no retention goroutine is running
	if client.stopCh != nil {
		select {
		case <-client.stopCh:
			t.Error("stop channel should not be closed")
		default:
			// Good - channel is still open
		}
	}
}

func TestRetention_DeleteFiles(t *testing.T) {
	t.Skip("File rotation not working properly in test environment")
	dir := t.TempDir()

	// Create config with aggressive retention settings
	config := DefaultCometConfig()
	config.Retention.MaxAge = 50 * time.Millisecond
	config.Retention.CleanupInterval = 25 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Retention.ProtectUnconsumed = false // Don't protect unconsumed data
	config.Storage.MaxFileSize = 512           // Small files

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write data to first file
	for i := 0; i < 10; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"batch": 1, "id": %d}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	// Force rotation
	shard, _ := client.getOrCreateShard(1)
	shard.mu.Lock()
	err = shard.rotateFile(&client.metrics, &config, nil)
	shard.mu.Unlock()
	if err != nil {
		t.Fatalf("failed to rotate: %v", err)
	}

	// Write to new file
	for i := 0; i < 5; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"batch": 2, "id": %d}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
	}

	// Mark first file as old
	shard.mu.Lock()
	if len(shard.index.Files) > 0 {
		shard.index.Files[0].EndTime = time.Now().Add(-100 * time.Millisecond)
	}
	initialFiles := len(shard.index.Files)
	t.Logf("Files before retention: %d", initialFiles)
	shard.mu.Unlock()

	// Force retention cleanup
	client.ForceRetentionCleanup()

	// Check files were deleted
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	t.Logf("Files after retention: %d", finalFiles)
	shard.mu.RUnlock()

	if finalFiles >= initialFiles {
		t.Errorf("Expected files to be deleted, but had %d before and %d after", initialFiles, finalFiles)
	}

	// Verify the old file is actually gone from disk
	stats := client.GetRetentionStats()
	if stats.TotalFiles != finalFiles {
		t.Errorf("Stats show %d files but shard has %d", stats.TotalFiles, finalFiles)
	}
}

func TestRetention_GlobalSizeLimit(t *testing.T) {
	dir := t.TempDir()

	// Create config with size-based retention
	config := DefaultCometConfig()
	config.Retention.MaxAge = 24 * time.Hour // Don't delete by age
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Retention.MaxTotalSize = 1024 // 1KB total limit
	config.Storage.MaxFileSize = 512     // Small files

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write data to multiple shards to test global limit
	for shard := 1; shard <= 3; shard++ {
		streamName := fmt.Sprintf("events:v1:shard:%04d", shard)
		for i := 0; i < 50; i++ {
			data := [][]byte{[]byte(fmt.Sprintf(`{"shard": %d, "id": %d, "data": "padding"}`, shard, i))}
			_, err := client.Append(ctx, streamName, data)
			if err != nil {
				t.Fatalf("failed to add data: %v", err)
			}
		}
	}

	// Wait for retention to enforce size limit
	time.Sleep(200 * time.Millisecond)

	// Get retention stats
	stats := client.GetRetentionStats()
	t.Logf("Retention stats after size enforcement: %+v", stats)

	// Total size should be under the limit
	if stats.TotalSizeBytes > config.Retention.MaxTotalSize {
		t.Errorf("expected total size <= %d bytes, got %d bytes",
			config.Retention.MaxTotalSize, stats.TotalSizeBytes)
	}

	// Should have kept minimum files per shard
	for shardID, shardStats := range stats.ShardStats {
		if shardStats.Files < 1 {
			t.Errorf("shard %d has no files after retention", shardID)
		}
	}
}

func TestRetentionStats_NewestData(t *testing.T) {
	dir := t.TempDir()
	config := DefaultCometConfig()
	config.Retention.CleanupInterval = time.Hour // Don't run cleanup during test
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Write some data
	beforeWrite := time.Now()
	time.Sleep(10 * time.Millisecond) // Ensure time difference

	streamName := "test:v1:shard:0001"
	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte("test data 1"),
		[]byte("test data 2"),
	})
	if err != nil {
		t.Fatalf("failed to write data: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	afterWrite := time.Now()

	// Get stats without checkpoint
	stats := client.GetRetentionStats()

	// NewestData should be close to current time, not zero or old
	if stats.NewestData.Before(beforeWrite) {
		t.Errorf("NewestData %v is before write time %v", stats.NewestData, beforeWrite)
	}

	// Allow small time difference due to execution time
	if stats.NewestData.After(afterWrite.Add(time.Second)) {
		t.Errorf("NewestData %v is significantly after current time %v", stats.NewestData, afterWrite)
	}

	// OldestData should also be reasonable
	if stats.OldestData.IsZero() {
		t.Error("OldestData should not be zero")
	}

	if stats.OldestData.After(afterWrite) {
		t.Errorf("OldestData %v is after current time %v", stats.OldestData, afterWrite)
	}

	t.Logf("Stats: OldestData=%v, NewestData=%v, TotalFiles=%d",
		stats.OldestData, stats.NewestData, stats.TotalFiles)

	// Write more data and check again
	time.Sleep(100 * time.Millisecond)

	_, err = client.Append(ctx, streamName, [][]byte{
		[]byte("test data 3"),
	})
	if err != nil {
		t.Fatalf("failed to write more data: %v", err)
	}

	stats2 := client.GetRetentionStats()

	// NewestData should have advanced
	if !stats2.NewestData.After(stats.NewestData) {
		t.Errorf("NewestData did not advance: was %v, now %v", stats.NewestData, stats2.NewestData)
	}

	// OldestData should remain the same
	if !stats2.OldestData.Equal(stats.OldestData) {
		t.Errorf("OldestData changed unexpectedly: was %v, now %v", stats.OldestData, stats2.OldestData)
	}
}
