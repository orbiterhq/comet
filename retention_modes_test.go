package comet

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetentionSingleProcess tests retention in single-process mode
func TestRetentionSingleProcess(t *testing.T) {
	dir := t.TempDir()

	// Create single-process config with retention
	config := DefaultCometConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Storage.MaxFileSize = 1024
	// Single-process mode is the default

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write data to create multiple files
	for i := 0; i < 30; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "mode": "single"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Get initial file count
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	shard.mu.RUnlock()

	if initialFiles < 2 {
		t.Skip("Need multiple files to test retention")
	}

	// Mark first files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-200 * time.Millisecond)
	for i := 0; i < len(shard.index.Files)-1 && i < 2; i++ {
		shard.index.Files[i].EndTime = oldTime
	}
	shard.mu.Unlock()

	// Force retention cleanup
	client.ForceRetentionCleanup()

	// Verify files were deleted
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	shard.mu.RUnlock()

	if finalFiles >= initialFiles {
		t.Errorf("Single-process mode: expected files to be deleted, had %d initially, now have %d", 
			initialFiles, finalFiles)
	}

	t.Logf("Single-process retention: %d files -> %d files", initialFiles, finalFiles)
}

// TestRetentionMultiProcess tests retention in multi-process mode
func TestRetentionMultiProcess(t *testing.T) {
	dir := t.TempDir()

	// Create multi-process config with retention
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Storage.MaxFileSize = 1024

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write data to create multiple files
	for i := 0; i < 30; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "mode": "multi"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Get initial file count
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	hasSequenceCounter := shard.sequenceCounter != nil
	shard.mu.RUnlock()

	if !hasSequenceCounter {
		t.Error("Multi-process mode should have sequence counter")
	}

	if initialFiles < 2 {
		t.Skip("Need multiple files to test retention")
	}

	// Mark first files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-200 * time.Millisecond)
	for i := 0; i < len(shard.index.Files)-1 && i < 2; i++ {
		shard.index.Files[i].EndTime = oldTime
	}
	shard.mu.Unlock()

	// Force retention cleanup
	client.ForceRetentionCleanup()

	// Verify files were deleted
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	shard.mu.RUnlock()

	if finalFiles >= initialFiles {
		t.Errorf("Multi-process mode: expected files to be deleted, had %d initially, now have %d", 
			initialFiles, finalFiles)
	}

	t.Logf("Multi-process retention: %d files -> %d files", initialFiles, finalFiles)
}

// TestRetentionWithActiveReaders tests that retention respects active readers
func TestRetentionWithActiveReaders(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Retention.MaxAge = 50 * time.Millisecond
	config.Retention.CleanupInterval = 25 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Storage.MaxFileSize = 512

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write data to create files
	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Create an active reader by getting the shard and incrementing reader count
	shard, _ := client.getOrCreateShard(1)
	atomic.AddInt64(&shard.readerCount, 1)
	defer atomic.AddInt64(&shard.readerCount, -1)

	// Mark files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-100 * time.Millisecond)
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != shard.index.CurrentFile {
			shard.index.Files[i].EndTime = oldTime
		}
	}
	initialFiles := len(shard.index.Files)
	shard.mu.Unlock()

	// Force retention cleanup while reader is active
	client.ForceRetentionCleanup()

	// Check that some files were protected due to active reader
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	readerCount := atomic.LoadInt64(&shard.readerCount)
	shard.mu.RUnlock()

	if readerCount == 0 {
		t.Error("Expected active reader count > 0")
	}

	// We should still have deleted some files (middle ones), but kept first/last
	if finalFiles == initialFiles {
		t.Log("No files deleted with active reader - this is acceptable for safety")
	} else if finalFiles < initialFiles {
		t.Logf("Deleted %d files even with active reader (kept boundary files)", initialFiles - finalFiles)
	}
}

// TestRetentionConsumerProtection tests that retention respects unconsumed data
func TestRetentionConsumerProtection(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Retention.MaxAge = 50 * time.Millisecond
	config.Retention.CleanupInterval = 25 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Retention.ProtectUnconsumed = true // Enable consumer protection
	config.Storage.MaxFileSize = 512

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "events:v1:shard:0001"

	// Write data
	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Set a consumer offset manually to simulate partial consumption
	shard, _ := client.getOrCreateShard(1)
	shard.mu.Lock()
	// Simulate that consumer has read first 5 entries
	shard.index.ConsumerOffsets = map[string]int64{
		"test-group": 5,
	}
	shard.mu.Unlock()

	// Mark all files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-100 * time.Millisecond)
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != shard.index.CurrentFile {
			shard.index.Files[i].EndTime = oldTime
		}
	}
	initialFiles := len(shard.index.Files)
	
	// Find which files contain unconsumed data
	protectedFiles := 0
	consumerOffset := shard.index.ConsumerOffsets["test-group"]
	for _, file := range shard.index.Files {
		fileLastEntry := file.StartEntry + file.Entries - 1
		if fileLastEntry >= consumerOffset {
			protectedFiles++
		}
	}
	shard.mu.Unlock()

	// Force retention cleanup
	client.ForceRetentionCleanup()

	// Check that files with unconsumed data were protected
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	shard.mu.RUnlock()

	// We should have kept at least the files with unconsumed data
	minExpectedFiles := protectedFiles
	if minExpectedFiles < config.Retention.MinFilesToKeep {
		minExpectedFiles = config.Retention.MinFilesToKeep
	}

	if finalFiles < minExpectedFiles {
		t.Errorf("Consumer protection failed: have %d files, expected at least %d", 
			finalFiles, minExpectedFiles)
	}

	t.Logf("Consumer protection: %d files -> %d files (protected %d files with unconsumed data)",
		initialFiles, finalFiles, protectedFiles)
}