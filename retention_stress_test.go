//go:build integration
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

// TestRetentionStress runs an extremely aggressive retention race condition test
func TestRetentionStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	dir := t.TempDir()

	// Build test worker
	workerBinary := filepath.Join(dir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	streamName := "events:v1:shard:0001"

	// Create massive initial data with many small files
	config := MultiProcessConfig()
	config.Retention.MaxAge = 50 * time.Millisecond  // Very short retention
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 64 // Tiny files to force many rotations

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()

	// Create hundreds of tiny files
	shard, _ := client.getOrCreateShard(1)

	t.Logf("Creating massive number of small files...")
	for i := 0; i < 500; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "small entry %d"}`, i, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		// Force rotation very frequently
		if (i+1)%2 == 0 {
			shard.mu.Lock()
			err = shard.rotateFile(&client.metrics, &config)
			shard.mu.Unlock()
			if err != nil {
				t.Fatalf("failed to rotate file: %v", err)
			}
		}
	}

	// Get initial state
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	currentFile := shard.index.CurrentFile
	t.Logf("Created %d files", initialFiles)
	shard.mu.RUnlock()

	if initialFiles < 10 {
		t.Skipf("Need at least 10 files for stress test, got %d", initialFiles)
	}

	// Mark ALL non-current files as old (very old)
	shard.mu.Lock()
	oldTime := time.Now().Add(-500 * time.Millisecond) // Much older than MaxAge
	filesMarked := 0
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != currentFile {
			shard.index.Files[i].EndTime = oldTime
			filesMarked++
		}
	}
	shard.persistIndex()
	shard.mu.Unlock()

	t.Logf("Marked %d files as old", filesMarked)

	client.Sync(ctx)
	client.Close()

	// Launch many workers simultaneously for maximum contention
	numWorkers := 20 // Much more aggressive
	var wg sync.WaitGroup
	outputs := make([]string, numWorkers)
	errors := make([]error, numWorkers)

	t.Logf("Starting %d concurrent retention workers for maximum stress...", numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			args := []string{
				"retention-test",
				"--dir", dir,
				"--worker-id", fmt.Sprintf("%d", workerID),
				"--stream", streamName,
			}

			cmd := exec.Command(workerBinary, args...)
			output, err := cmd.CombinedOutput()
			outputs[workerID] = string(output)
			errors[workerID] = err
		}(i)
	}

	// Wait for all workers
	wg.Wait()

	// Count successful deletions
	totalDeleted := 0
	workersWithDeletions := 0
	
	for i, output := range outputs {
		if errors[i] != nil {
			t.Logf("Worker %d error: %v", i, errors[i])
		}
		
		// Parse deletion count from output
		var deleted int
		if n, err := fmt.Sscanf(output, "%*s %*s %*s %*s %*s %*s %*d %*s %*s %*s %*s %*d (deleted %d)", &deleted); n == 1 && err == nil {
			totalDeleted += deleted
			if deleted > 0 {
				workersWithDeletions++
			}
		}
		
		if testing.Verbose() {
			t.Logf("Worker %d output: %s", i, output)
		}
	}

	t.Logf("Total files deleted by all workers: %d", totalDeleted)
	t.Logf("Workers that actually deleted files: %d", workersWithDeletions)

	// Check final state
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to recreate client: %v", err)
	}
	defer client2.Close()

	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	finalFiles := len(shard2.index.Files)
	t.Logf("Final files: %d (started with %d)", finalFiles, initialFiles)
	shard2.mu.RUnlock()

	// In a stress test, we expect significant cleanup
	if finalFiles >= initialFiles/2 {
		t.Errorf("Stress test may have revealed issues: only reduced files from %d to %d", initialFiles, finalFiles)
	} else {
		deletedFiles := initialFiles - finalFiles
		t.Logf("Stress test successfully deleted %d files under high contention", deletedFiles)
	}

	// Verify data integrity after stress
	consumer := NewConsumer(client2, ConsumerOptions{Group: "stress-test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{1}, 10)
	if err != nil {
		t.Logf("Expected behavior - no readable data after aggressive retention: %v", err)
	} else {
		t.Logf("Successfully read %d messages after stress retention", len(messages))
	}
}