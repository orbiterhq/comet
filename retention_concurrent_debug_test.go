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

// TestRetentionConcurrentDebug replicates the exact failing scenario with detailed logging
func TestRetentionConcurrentDebug(t *testing.T) {
	dir := t.TempDir()

	// Build test worker first
	workerBinary := filepath.Join(dir, "test_worker")
	cmd := exec.Command("go", "build", "-o", workerBinary, "./cmd/test_worker")
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to build test worker: %v", err)
	}

	streamName := "events:v1:shard:0001"
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 200

	t.Logf("=== PHASE 1: Creating many files (simplified) ===")
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	// Create fewer files for easier debugging - but still multiple
	for i := 0; i < 20; i++ {
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d}`, i))}
		_, err := client.Append(context.Background(), streamName, data)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}

		// Force rotation every 2 entries
		if (i+1)%2 == 0 {
			shard, _ := client.getOrCreateShard(1)
			shard.mu.Lock()
			err = shard.rotateFile(&client.metrics, &config)
			shard.mu.Unlock()
			if err != nil {
				t.Fatalf("Rotation %d failed: %v", i, err)
			}
		}
	}

	shard, _ := client.getOrCreateShard(1)
	shard.mu.RLock()
	initialFiles := len(shard.index.Files)
	currentFile := shard.index.CurrentFile
	t.Logf("Created %d files, current file: %s", initialFiles, currentFile)
	shard.mu.RUnlock()

	t.Logf("=== PHASE 2: Marking files as old ===")
	// Mark all non-current files as old
	shard.mu.Lock()
	oldTime := time.Now().Add(-500 * time.Millisecond)  
	filesMarked := 0
	for i := range shard.index.Files {
		if shard.index.Files[i].Path != currentFile {
			t.Logf("Marking as old: %s", shard.index.Files[i].Path)
			shard.index.Files[i].EndTime = oldTime
			filesMarked++
		}
	}
	shard.persistIndex()
	shard.mu.Unlock()
	t.Logf("Marked %d files as old", filesMarked)

	client.Sync(context.Background())
	client.Close()

	t.Logf("=== PHASE 3: Running CONCURRENT retention (3 workers instead of 15) ===")
	// Use fewer workers for easier debugging
	numWorkers := 3
	var wg sync.WaitGroup
	outputs := make([]string, numWorkers)
	errors := make([]error, numWorkers)

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

	wg.Wait()

	// Show all outputs with detailed analysis
	for i, output := range outputs {
		t.Logf("Worker %d output: %s", i, output)
		if errors[i] != nil {
			t.Logf("Worker %d error: %v", i, errors[i])
		}
		
		// Parse deleted count if possible
		// This is crude parsing but helps with debugging
		if len(output) > 0 {
			t.Logf("Worker %d completed retention", i)
		}
	}

	t.Logf("=== PHASE 4: Checking final state and attempting read ===")
	// Create new client to check final state
	client2, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	shard2, _ := client2.getOrCreateShard(1)
	shard2.mu.RLock()
	finalFiles := len(shard2.index.Files)
	t.Logf("Final files in index: %d (started with %d)", finalFiles, initialFiles)
	
	// Check each file in the index
	for i, file := range shard2.index.Files {
		exists := "EXISTS"
		if _, err := os.Stat(file.Path); err != nil {
			exists = fmt.Sprintf("MISSING: %v", err)
		}
		t.Logf("  File %d: %s (%s, entries: %d)", i, file.Path, exists, file.Entries)
	}
	shard2.mu.RUnlock()

	t.Logf("=== PHASE 5: The critical test - reading data ===")
	consumer := NewConsumer(client2, ConsumerOptions{Group: "debug-test"})
	defer consumer.Close()

	messages, err := consumer.Read(context.Background(), []uint32{1}, 25)
	if err != nil {
		t.Logf("❌ REPRODUCTION CONFIRMED: %v", err)
		
		// Let's see what specific file it's trying to access
		t.Logf("This is exactly the bug we need to fix!")
		
		// Check if it's trying to read from a deleted file referenced in the index
		shard2.mu.RLock()
		t.Logf("Debug: Index still has %d files after retention:", len(shard2.index.Files))
		for i, file := range shard2.index.Files {
			if _, statErr := os.Stat(file.Path); statErr != nil {
				t.Logf("  ❌ PROBLEM: Index references deleted file %d: %s", i, file.Path)
			} else {
				t.Logf("  ✅ File %d exists: %s", i, file.Path)
			}
		}
		shard2.mu.RUnlock()
		
	} else {
		t.Logf("✅ Unexpectedly succeeded: read %d messages", len(messages))
	}
}

// TestRetentionRaceConditionSimplified - exactly like the failing test but with more logging
func TestRetentionRaceConditionSimplified(t *testing.T) {
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

	// Use the EXACT same config as the failing test
	config := MultiProcessConfig()
	config.Retention.MaxAge = 100 * time.Millisecond
	config.Retention.MinFilesToKeep = 0
	config.Storage.MaxFileSize = 200

	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	shard, _ := client.getOrCreateShard(1)

	// Create the EXACT same file pattern as the failing test
	for i := 0; i < 50; i++ { // Reduced from 200 for easier debugging
		data := [][]byte{[]byte(fmt.Sprintf(`{"id": %d, "data": "small"}`, i))}
		_, err := client.Append(ctx, streamName, data)
		if err != nil {
			t.Fatalf("failed to write data: %v", err)
		}

		// Force rotation very frequently to create many small files
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

	// Mark ALL non-current files as old (EXACT same logic)
	shard.mu.Lock()
	oldTime := time.Now().Add(-500 * time.Millisecond)
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

	// Launch 5 workers (reduced from 15 for easier debugging)
	numWorkers := 5
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

	wg.Wait()

	// Show all outputs
	for i, output := range outputs {
		t.Logf("Worker %d output: %s", i, output)
		if errors[i] != nil {
			t.Logf("Worker %d error: %v", i, errors[i])
		}
	}

	// Check final state - EXACT same logic as failing test
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
		exists := "EXISTS"
		if _, statErr := os.Stat(file.Path); statErr != nil {
			exists = "DELETED"
		}
		t.Logf("  Remaining file %d: %s (%s, entries: %d, startEntry: %d)", 
			i, file.Path, exists, file.Entries, file.StartEntry)
	}
	shard2.mu.RUnlock()

	// THE CRITICAL TEST - exactly like the failing test
	if finalFiles >= initialFiles {
		t.Errorf("Race condition may have prevented retention: %d -> %d files", initialFiles, finalFiles)
	} else {
		deletedFiles := initialFiles - finalFiles
		t.Logf("Concurrent retention deleted %d files", deletedFiles)

		// Check if we can still read data (no corruption) - THE FAILING PART
		totalRemainingEntries := int64(0)
		shard2.mu.RLock()
		for _, file := range shard2.index.Files {
			totalRemainingEntries += file.Entries
		}
		shard2.mu.RUnlock()

		if totalRemainingEntries > 0 {
			consumer := NewConsumer(client2, ConsumerOptions{Group: "race-test"})
			defer consumer.Close()

			messages, err := consumer.Read(ctx, []uint32{1}, 10)
			if err != nil {
				t.Logf("❌ CONFIRMED BUG: %v", err)
				t.Log("This is exactly what we need to fix!")
			} else {
				t.Logf("✅ Successfully read %d messages after concurrent retention", len(messages))
			}
		}
	}
}