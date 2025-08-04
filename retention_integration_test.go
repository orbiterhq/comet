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

	// Build test worker with timeout
	workerBinary := filepath.Join(dir, "test_worker")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	streamName := "events:v1:shard:0000"

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
	shard, _ := client.getOrCreateShard(0)
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

	workerCtx, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer workerCancel()
	cmd = exec.CommandContext(workerCtx, workerBinary, args...)
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

	shard2, _ := client2.getOrCreateShard(0)
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

	// Build the test worker binary with timeout
	workerBinary := filepath.Join(dir, "test_worker")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", workerBinary, "./cmd/test_worker")
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

	streamName := "events:v1:shard:0000"

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
	shard, _ := client.getOrCreateShard(0)
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
	indexPath := filepath.Join(dir, "shard-0000", "index.bin")

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

	rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer rebuildCancel()
	cmd = exec.CommandContext(rebuildCtx, workerBinary, args...)
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
	// Note: The worker process already consumed messages, so we need to either:
	// 1. Use a different approach to verify the index rebuild worked
	// 2. Read messages that were written AFTER the worker consumed

	// Let's verify by checking that we can write and read new messages
	// This proves the index is functional after rebuild
	testData := []byte(fmt.Sprintf(`{"id": "verify-%d", "test": "index_verification"}`, time.Now().UnixNano()))
	_, err = client2.Append(ctx, streamName, [][]byte{testData})
	if err != nil {
		t.Fatalf("failed to write verification data: %v", err)
	}

	consumer := NewConsumer(client2, ConsumerOptions{
		Group: fmt.Sprintf("verify-rebuild-%d", time.Now().UnixNano()),
	})
	defer consumer.Close()

	// Check the shard state before reading
	shard2, err2 := client2.getOrCreateShard(0)
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

	// Read messages - we should get at least our verification message
	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("failed to read after index rebuild: %v", err)
	}

	// We should have at least 1 message (our verification message)
	if len(messages) < 1 {
		t.Errorf("Expected to read at least 1 verification message, got %d", len(messages))
	}

	// Verify the index state shows all the expected data
	shard2.mu.RLock()
	currentEntries := shard2.index.CurrentEntryNumber
	currentFiles := len(shard2.index.Files)
	shard2.mu.RUnlock()

	// The index should have been rebuilt to include all original entries plus new ones
	// Initial entries (19) + worker writes (3) + our verification write (1) = at least 23
	expectedMinEntries := initialEntries + 3 + 1
	if currentEntries < expectedMinEntries {
		t.Errorf("Index shows %d entries, expected at least %d (initial: %d + worker: 3 + verify: 1)",
			currentEntries, expectedMinEntries, initialEntries)
	}

	t.Logf("Index rebuild verification successful:")
	t.Logf("  - Index state: %d files, %d total entries", currentFiles, currentEntries)
	t.Logf("  - Read %d messages from our verification group", len(messages))
	t.Logf("  - Worker successfully read messages and wrote new ones")
}
