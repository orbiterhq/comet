//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestRetentionRaceCondition tests what happens when multiple processes
// try to run retention simultaneously
func TestRetentionRaceCondition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()

	// Build test worker
	workerBinary := filepath.Join(dir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	streamName := "events:v1:shard:0001"

	// Create initial data with multiple files
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 128 // Extremely small files to force rotation

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()

	// Create multiple files by forcing rotation
	shard, _ := client.getOrCreateShard(1)

	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "test data for race condition with padding to make it larger"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		// Force rotation every few entries to create more files
		if (i+1)%4 == 0 {
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

	if initialFiles < 3 {
		t.Skipf("Need at least 3 files for race test, got %d", initialFiles)
	}

	// Mark ALL non-current files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-200 * time.Millisecond)
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

	// Launch multiple workers simultaneously to race on retention
	numWorkers := 3
	var wg sync.WaitGroup
	outputs := make([]string, numWorkers)
	errors := make([]error, numWorkers)

	t.Logf("Starting %d concurrent retention workers...", numWorkers)

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

	// Show all outputs
	for i, output := range outputs {
		t.Logf("Worker %d output: %s", i, output)
		if errors[i] != nil {
			t.Logf("Worker %d error: %v", i, errors[i])
		}
	}

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
	for i, file := range shard2.index.Files {
		t.Logf("  Remaining file %d: %s (entries: %d, startEntry: %d)", i, file.Path, file.Entries, file.StartEntry)
	}
	shard2.mu.RUnlock()

	// Verify retention worked (some files should be deleted)
	if finalFiles >= initialFiles {
		t.Errorf("Race condition may have prevented retention: %d -> %d files", initialFiles, finalFiles)
	} else {
		deletedFiles := initialFiles - finalFiles
		t.Logf("Concurrent retention deleted %d files", deletedFiles)

		// Check if we can still read data (no corruption)
		// Only try to read if there are entries in remaining files
		totalRemainingEntries := int64(0)
		shard2.mu.RLock()
		for _, file := range shard2.index.Files {
			totalRemainingEntries += file.Entries
		}
		shard2.mu.RUnlock()

		if totalRemainingEntries > 0 {
			consumer := NewConsumer(client2, ConsumerOptions{Group: "race-test"})
			defer consumer.Close()

			messages, err := consumer.Read(ctx, []uint32{1}, 5)
			if err != nil {
				// Check if this is expected behavior (no data left after retention)
				if strings.Contains(err.Error(), "not found in data files") {
					t.Logf("No readable entries after retention - this is acceptable: %v", err)
				} else {
					t.Errorf("Data corruption after concurrent retention: %v", err)
				}
			} else {
				t.Logf("Successfully read %d messages after concurrent retention", len(messages))
			}
		} else {
			t.Logf("No entries remaining after retention - this is acceptable if all data was old")
		}
	}

	// TODO: This test should fail or show issues if there are race conditions
	// If it passes cleanly, we might need file-level locking for retention
}
