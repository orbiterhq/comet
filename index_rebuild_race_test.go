//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestIndexRebuildRaceCondition tests what happens when multiple processes
// try to rebuild the index simultaneously
func TestIndexRebuildRaceCondition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()

	// Build test worker with timeout to avoid hanging
	workerBinary := filepath.Join(dir, "test_worker")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	streamName := "events:v1:shard:0000"

	// Create initial data with multiple files
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 512 // Small files to create multiple

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()

	// Create multiple files with known entry counts
	for i := 0; i < 30; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "index rebuild race test"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Get initial state
	shard, _ := client.getOrCreateShard(0)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	initialTotalEntries := int64(0)
	for _, file := range shard.index.Files {
		initialTotalEntries += file.Entries
	}
	t.Logf("Created %d files with %d total entries", initialFiles, initialTotalEntries)
	shard.mu.RUnlock()

	if initialFiles < 2 {
		t.Skipf("Need at least 2 files for race test, got %d", initialFiles)
	}

	// Force persistence and close
	shard.mu.Lock()
	shard.persistIndex()
	shard.mu.Unlock()
	client.Sync(ctx)
	client.Close()

	// Delete index files to force rebuilding
	indexPath := filepath.Join(dir, "shard-0000", "index.bin")
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	t.Log("Deleted index file to force rebuilding")

	// Launch multiple workers simultaneously to race on index rebuilding
	numWorkers := 4
	var wg sync.WaitGroup
	outputs := make([]string, numWorkers)
	errors := make([]error, numWorkers)

	t.Logf("Starting %d concurrent index rebuild workers...", numWorkers)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			args := []string{
				"index-rebuild-test",
				"--dir", dir,
				"--stream", streamName,
				"--initial-files", fmt.Sprintf("%d", initialFiles),
				"--initial-entries", fmt.Sprintf("%d", initialTotalEntries),
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

	// Access shard to load rebuilt index
	consumer := NewConsumer(client2, ConsumerOptions{Group: "rebuild-race-test"})
	consumer.Read(ctx, []uint32{0}, 1) // This loads the shard
	consumer.Close()

	shard2, _ := client2.getOrCreateShard(0)
	shard2.mu.RLock()
	finalFiles := len(shard2.index.Files)
	finalTotalEntries := int64(0)
	hasCorruption := false

	t.Logf("Final state: %d files", finalFiles)
	for i, file := range shard2.index.Files {
		finalTotalEntries += file.Entries
		t.Logf("  File %d: entries=%d, path=%s", i, file.Entries, file.Path)

		// Check for corruption
		if file.Entries < 0 {
			t.Errorf("CORRUPTION DETECTED: File %d has negative entries: %d", i, file.Entries)
			hasCorruption = true
		}
	}
	shard2.mu.RUnlock()

	t.Logf("Total entries: initial=%d, final=%d", initialTotalEntries, finalTotalEntries)

	// Final verification
	if hasCorruption {
		t.Error("Index rebuild race condition caused data corruption")
	}

	if finalTotalEntries < 0 {
		t.Errorf("CORRUPTION CONFIRMED: Negative total entry count: %d", finalTotalEntries)
	}

	// Try to read data to ensure no corruption
	consumer2 := NewConsumer(client2, ConsumerOptions{Group: "final-verify"})
	defer consumer2.Close()

	messages, err := consumer2.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Errorf("Data corruption after concurrent index rebuild: %v", err)
	} else if len(messages) == 0 {
		t.Error("No messages readable after concurrent index rebuild")
	} else {
		t.Logf("Successfully read %d messages after concurrent index rebuild", len(messages))
	}

	if hasCorruption {
		t.Fatal("Index rebuild race condition test revealed data corruption")
	}
}
