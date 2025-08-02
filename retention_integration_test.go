//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestMultiProcessRetention tests retention across actual OS processes
func TestMultiProcessRetention(t *testing.T) {
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

	// STEP 1: Process A creates files (using the unit test pattern that works)
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 512

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()

	// Write data to create multiple files
	for i := 0; i < 30; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "test data"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Get file count before marking old
	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	currentFile := shard.index.CurrentFile
	t.Logf("Created %d files", initialFiles)
	for i, file := range shard.index.Files {
		isCurrent := file.Path == currentFile
		t.Logf("  File %d: %s (current: %t, entries: %d, endTime: %v)", i, file.Path, isCurrent, file.Entries, file.EndTime)
	}
	shard.mu.RUnlock()

	if initialFiles < 2 {
		t.Skipf("Need at least 2 files, got %d", initialFiles)
	}

	// Mark non-current files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-200 * time.Millisecond)
	filesMarked := 0
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != currentFile {
			t.Logf("Marking file %d as old: %s", i, shard.index.Files[i].Path)
			shard.index.Files[i].EndTime = oldTime
			filesMarked++
		}
	}
	shard.persistIndex()
	shard.mu.Unlock()

	t.Logf("Marked %d files as old", filesMarked)

	// Force sync before closing
	client.Sync(ctx)
	client.Close()

	// STEP 2: Process B (worker) runs retention
	args := []string{
		"retention-test",
		"--dir", dir,
		"--worker-id", "0",
		"--stream", streamName,
	}

	cmd = exec.Command(workerBinary, args...)
	output, err := cmd.CombinedOutput()
	t.Logf("Worker output: %s", string(output))
	if err != nil {
		t.Fatalf("Worker failed: %v", err)
	}

	// STEP 3: Process A verifies deletion happened
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to recreate client: %v", err)
	}
	defer client2.Close()

	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	finalFiles := len(shard2.index.Files)
	shard2.mu.RUnlock()

	t.Logf("Files after retention: %d (was %d)", finalFiles, initialFiles)

	if finalFiles >= initialFiles {
		t.Errorf("Retention failed: files not deleted (%d -> %d)", initialFiles, finalFiles)
	}

	t.Logf("SUCCESS: Multi-process retention deleted %d files", initialFiles-finalFiles)
}

// TestIndexRebuildIntegration tests index rebuilding across actual separate processes
func TestIndexRebuildIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	dir := t.TempDir()
	ctx := context.Background()

	// Build the test worker binary
	workerBinary := filepath.Join(dir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	// Create initial data with the main process
	config := MultiProcessConfig()
	config.Storage.MaxFileSize = 1024 // Small files to create multiple
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	streamName := "events:v1:shard:0001"

	// Write initial data to create multiple files
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf(`{"id": %d, "process": "main", "data": "initial rebuild test data with padding to make it bigger"}`, i))
		_, err := client.Append(ctx, streamName, [][]byte{data})
		if err != nil {
			t.Fatalf("failed to write initial data: %v", err)
		}
	}

	// Force sync and get initial state
	client.Sync(ctx)
	shard, _ := client.getOrCreateShard(1)
	shard.mu.Lock()
	shard.persistIndex()
	initialFiles := len(shard.index.Files)
	initialEntries := shard.index.CurrentEntryNumber
	t.Logf("Initial state: %d files, %d entries", initialFiles, initialEntries)
	shard.mu.Unlock()

	client.Close()

	if initialFiles < 2 {
		t.Fatalf("Need at least 2 files for this test, got %d", initialFiles)
	}

	// Delete index and coordination files to force rebuild
	indexPath := filepath.Join(dir, "shard-0001", "index.bin")

	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}

	t.Log("Deleted index and coordination files")

	// Start a worker process that will need to rebuild the index
	args := []string{
		"index-rebuild-test",
		"--dir", dir,
		"--stream", streamName,
		"--initial-files", fmt.Sprintf("%d", initialFiles),
		"--initial-entries", fmt.Sprintf("%d", initialEntries),
	}

	cmd = exec.Command(workerBinary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Worker process failed: %v\nOutput: %s", err, output)
	}

	t.Logf("Worker output: %s", output)

	// Verify the index was rebuilt by checking if we can read from a new process
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create verification client: %v", err)
	}
	defer client2.Close()

	// Check that index file exists again
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Error("Index file was not recreated by worker process")
	}

	// Give a moment for any async operations to complete
	time.Sleep(100 * time.Millisecond)

	// Force a sync to ensure index is up to date
	if err := client2.Sync(ctx); err != nil {
		t.Logf("Warning: sync failed: %v", err)
	}

	// Verify we can read data (proving the index rebuild worked)
	// Use a unique group name to avoid collision with other tests
	groupName := fmt.Sprintf("verify-rebuild-%d", time.Now().UnixNano())
	consumer := NewConsumer(client2, ConsumerOptions{Group: groupName})
	defer consumer.Close()

	// Check the shard state before reading
	shard2, err2 := client2.getOrCreateShard(1)
	if err2 != nil {
		t.Fatalf("failed to get shard: %v", err2)
	}
	shard2.mu.RLock()
	t.Logf("Before read - Shard state: CurrentEntryNumber=%d, Files=%d",
		shard2.index.CurrentEntryNumber, len(shard2.index.Files))
	if len(shard2.index.Files) > 0 {
		t.Logf("First file: entries=%d, path=%s",
			shard2.index.Files[0].Entries, shard2.index.Files[0].Path)
	}
	shard2.mu.RUnlock()

	messages, err := consumer.Read(ctx, []uint32{1}, 50)
	if err != nil {
		t.Fatalf("failed to read after index rebuild: %v", err)
	}

	// Verify we got the expected number of messages
	// The worker process reported reading 50 messages, but we should be able to read
	// at least the original 20 messages that were written initially
	if int64(len(messages)) < initialEntries {
		t.Errorf("Expected to read at least %d messages (initial count), got %d", initialEntries, len(messages))

		// Debug: Check if there's a state synchronization issue
		shard2, _ := client2.getOrCreateShard(1)
		shard2.mu.RLock()
		currentEntries := shard2.index.CurrentEntryNumber
		currentFiles := len(shard2.index.Files)
		shard2.mu.RUnlock()

		t.Logf("Debug: Current index state: %d files, %d entries", currentFiles, currentEntries)
		t.Logf("Debug: Expected at least %d entries, got %d messages", initialEntries, len(messages))
	}

	t.Logf("Successfully verified index rebuild across processes: read %d messages", len(messages))
}
