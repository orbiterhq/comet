//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestCorruptionIsolation - Minimal test to find exact corruption point
func TestCorruptionIsolation(t *testing.T) {
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
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 512

	// STEP A: Process 1 creates files
	client1, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}

	ctx := context.Background()

	// Create exactly 2 files: one to keep, one to delete
	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client1.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Get state after writing
	shard1, _ := client1.getOrCreateShard(1)
	shard1.mu.RLock()
	initialFiles := len(shard1.index.Files)
	initialEntryCount := int64(0)
	for i, file := range shard1.index.Files {
		initialEntryCount += file.Entries
		t.Logf("Process 1 - File %d: entries=%d, path=%s", i, file.Entries, file.Path)
	}
	_ = shard1.index.CurrentFile // currentFile not used in this test
	shard1.mu.RUnlock()

	t.Logf("Process 1 - Created %d files, total entries: %d", initialFiles, initialEntryCount)

	// Mark first file as old
	shard1.mu.Lock()
	if len(shard1.index.Files) > 1 {
		shard1.index.Files[0].EndTime = shard1.index.Files[0].StartTime.Add(-200 * time.Millisecond)
		t.Logf("Process 1 - Marked file 0 as old: %s", shard1.index.Files[0].Path)
	}
	shard1.persistIndex()
	shard1.mu.Unlock()

	client1.Sync(ctx)
	client1.Close()

	// STEP B: Process 2 loads and verifies same state
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}

	// Trigger shard loading
	data := []byte(`{"trigger": "load"}`)
	_, err = client2.Append(ctx, streamName, [][]byte{data})
	if err != nil {
		t.Fatalf("failed to trigger shard load: %v", err)
	}

	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	loadedFiles := len(shard2.index.Files)
	loadedEntryCount := int64(0)
	for i, file := range shard2.index.Files {
		loadedEntryCount += file.Entries
		t.Logf("Process 2 - File %d: entries=%d, path=%s", i, file.Entries, file.Path)
	}
	shard2.mu.RUnlock()

	t.Logf("Process 2 - Loaded %d files, total entries: %d", loadedFiles, loadedEntryCount)

	// VERIFICATION CHECKPOINT
	// Process 2 writes new data, so file count may increase and entry count should increase
	if loadedFiles < initialFiles {
		t.Errorf("STEP B FAILED: File count decreased unexpectedly. Started with %d, got %d", initialFiles, loadedFiles)
	}
	if loadedEntryCount <= initialEntryCount { // Should be +1 due to trigger write
		t.Errorf("STEP B FAILED: Entry count problem. Expected >%d, got %d", initialEntryCount, loadedEntryCount)
	} else {
		t.Logf("STEP B SUCCESS: Entry count increased as expected: %d -> %d", initialEntryCount, loadedEntryCount)
	}

	client2.Close()

	// STEP C+D+E+F: Process 3 does retention via worker
	args := []string{
		"retention-test",
		"--dir", dir,
		"--worker-id", "0",
		"--stream", streamName,
	}

	cmd = exec.Command(workerBinary, args...)
	output, err := cmd.CombinedOutput()
	t.Logf("Retention worker output: %s", string(output))
	if err != nil {
		t.Fatalf("Retention worker failed: %v", err)
	}

	// STEP G: Process 4 verifies final state
	client3, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatalf("failed to create client3: %v", err)
	}
	defer client3.Close()

	// Access shard to load state
	consumer := NewConsumer(client3, ConsumerOptions{Group: "verify"})
	consumer.Read(ctx, []uint32{1}, 1) // This will load the shard
	consumer.Close()

	shard3, _ := client3.getOrCreateShard(1)
	shard3.mu.RLock()
	finalFiles := len(shard3.index.Files)
	finalEntryCount := int64(0)
	for i, file := range shard3.index.Files {
		finalEntryCount += file.Entries
		t.Logf("Process 3 - File %d: entries=%d, path=%s", i, file.Entries, file.Path)

		// CHECK FOR CORRUPTION
		if file.Entries < 0 {
			t.Errorf("CORRUPTION DETECTED: File %d has negative entries: %d", i, file.Entries)
		}
	}
	shard3.mu.RUnlock()

	t.Logf("Process 3 - Final %d files, total entries: %d", finalFiles, finalEntryCount)

	// FINAL VERIFICATION
	if finalFiles >= initialFiles {
		t.Errorf("RETENTION FAILED: Expected fewer files. Started with %d, ended with %d", initialFiles, finalFiles)
	}

	if finalEntryCount < 0 {
		t.Errorf("CORRUPTION CONFIRMED: Negative total entry count: %d", finalEntryCount)
	}

	t.Logf("=== SUMMARY ===")
	t.Logf("Initial: %d files, %d entries", initialFiles, initialEntryCount)
	t.Logf("Loaded:  %d files, %d entries", loadedFiles, loadedEntryCount)
	t.Logf("Final:   %d files, %d entries", finalFiles, finalEntryCount)
}
