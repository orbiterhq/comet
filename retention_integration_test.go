// +build integration

package comet

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestMultiProcessRetention tests retention behavior with concurrent processes
func TestMultiProcessRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()
	
	// Build the test worker binary
	workerBinary := filepath.Join(dir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	// Create initial data with the main process
	config := MultiProcessConfig()
	config.Retention.MaxAge = 200 * time.Millisecond
	config.Retention.CleanupInterval = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 2
	config.Storage.MaxFileSize = 2048 // Small files to create multiple
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	
	ctx := context.Background()
	streamName := "events:v1:shard:0001"
	
	// Write initial data to create multiple files
	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "process": "main", "data": "initial"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatalf("failed to write initial data: %v", err)
		}
	}
	
	// Force sync and get initial file count
	client.Sync(ctx)
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	t.Logf("Initial file count: %d", initialFiles)
	shard.mu.RUnlock()
	
	if initialFiles < 3 {
		t.Fatalf("Need at least 3 files for this test, got %d", initialFiles)
	}
	
	// Mark some files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-300 * time.Millisecond)
	for i := 0; i < len(shard.index.Files)-2 && i < 2; i++ {
		shard.index.Files[i].EndTime = oldTime
		t.Logf("Marked file %d as old", i)
	}
	shard.persistIndex()
	shard.mu.Unlock()
	
	client.Close()
	
	// Start multiple worker processes
	numWorkers := 3
	var wg sync.WaitGroup
	errors := make(chan error, numWorkers)
	
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Each worker will:
			// 1. Write some data
			// 2. Trigger retention if it's worker 0
			// 3. Read data to verify integrity
			
			args := []string{
				"retention-test",
				"--dir", dir,
				"--worker-id", fmt.Sprintf("%d", workerID),
				"--stream", streamName,
			}
			
			cmd := exec.Command(workerBinary, args...)
			output, err := cmd.CombinedOutput()
			if err != nil {
				errors <- fmt.Errorf("worker %d failed: %v\nOutput: %s", workerID, err, output)
			} else {
				t.Logf("Worker %d output: %s", workerID, output)
			}
		}(i)
	}
	
	// Wait for all workers to complete
	wg.Wait()
	close(errors)
	
	// Check for errors
	for err := range errors {
		t.Error(err)
	}
	
	// Verify retention worked correctly
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to recreate client: %v", err)
	}
	defer client2.Close()
	
	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	finalFiles := len(shard2.index.Files)
	shard2.mu.RUnlock()
	
	t.Logf("Final file count: %d (initial: %d)", finalFiles, initialFiles)
	
	// Should have fewer files after retention
	if finalFiles >= initialFiles {
		t.Errorf("Expected fewer files after retention, got %d (initial: %d)", finalFiles, initialFiles)
	}
	
	// Verify data integrity - should be able to read recent data
	consumer := NewConsumer(client2, ConsumerOptions{Group: "verify"})
	defer consumer.Close()
	
	// Try to read some messages
	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Fatalf("failed to read after retention: %v", err)
	}
	
	if len(messages) == 0 {
		t.Error("Expected to read some messages after retention")
	} else {
		t.Logf("Successfully read %d messages after multi-process retention", len(messages))
	}
}

// TestRetentionWithConcurrentFileRotation tests retention while files are being rotated
func TestRetentionWithConcurrentFileRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()
	
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.CleanupInterval = 50 * time.Millisecond
	config.Retention.MinFilesToKeep = 1
	config.Storage.MaxFileSize = 512 // Very small to force frequent rotation
	
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()
	
	ctx := context.Background()
	streamName := "events:v1:shard:0001"
	
	// Start a goroutine that continuously writes data
	stopWriting := make(chan struct{})
	writeErrors := make(chan error, 1)
	
	go func() {
		i := 0
		for {
			select {
			case <-stopWriting:
				return
			default:
				data := []byte(fmt.Sprintf(`{"id": %d, "timestamp": %d}`, i, time.Now().UnixNano()))
				_, err := client.Append(ctx, streamName, [][]byte{data})
				if err != nil {
					select {
					case writeErrors <- err:
					default:
					}
					return
				}
				i++
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()
	
	// Let it run for a bit to create files
	time.Sleep(200 * time.Millisecond)
	
	// Check intermediate state
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	intermediateFiles := len(shard.index.Files)
	t.Logf("Intermediate file count: %d", intermediateFiles)
	shard.mu.RUnlock()
	
	// Force retention cleanup while writes are happening
	client.ForceRetentionCleanup()
	
	// Continue writing for a bit more
	time.Sleep(100 * time.Millisecond)
	
	// Stop writing
	close(stopWriting)
	
	// Check for write errors
	select {
	case err := <-writeErrors:
		t.Errorf("Write error during retention: %v", err)
	default:
		// No errors
	}
	
	// Final state
	shard.mu.RLock()
	finalFiles := len(shard.index.Files)
	currentEntries := shard.index.CurrentEntryNumber
	shard.mu.RUnlock()
	
	t.Logf("Final state: %d files, %d entries", finalFiles, currentEntries)
	
	// Should have some files (at least MinFilesToKeep)
	if finalFiles < config.Retention.MinFilesToKeep {
		t.Errorf("Expected at least %d files, got %d", config.Retention.MinFilesToKeep, finalFiles)
	}
	
	// Verify we can still read recent data
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()
	
	messages, err := consumer.Read(ctx, []uint32{1}, 5)
	if err != nil {
		t.Fatalf("failed to read after concurrent retention: %v", err)
	}
	
	if len(messages) == 0 {
		t.Error("Expected to read messages after concurrent retention")
	}
}