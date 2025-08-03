//go:build integration
// +build integration

package comet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// TestFileEntryCountCorruption tests if file entry counts get corrupted during concurrent multi-process writes
func TestFileEntryCountCorruption(t *testing.T) {
	dir := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	// Check if we're a subprocess
	if role := os.Getenv("DEBUG_FILE_ENTRIES_ROLE"); role != "" {
		runFileEntriesWorker(t, role)
		return
	}

	// Start 2 concurrent writer processes for a single shard
	var wg sync.WaitGroup
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			cmd := exec.Command(executable, "-test.run", "^TestFileEntryCountCorruption$", "-test.v")
			cmd.Env = append(os.Environ(),
				"DEBUG_FILE_ENTRIES_ROLE=writer",
				fmt.Sprintf("DEBUG_FILE_ENTRIES_DIR=%s", dir),
				fmt.Sprintf("DEBUG_FILE_ENTRIES_WORKER=%d", workerID),
				"COMET_DEBUG=1",
			)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Logf("Writer %d failed: %v\nOutput: %s", workerID, err, output)
			} else {
				t.Logf("Writer %d output: %s", workerID, output)
			}
		}(i)
	}

	wg.Wait()

	// Examine the final state
	config := MultiProcessConfig()
	client, err := NewClientWithConfig(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	shard, err := client.getOrCreateShard(1)
	if err != nil {
		t.Fatal(err)
	}

	// Log detailed shard state
	shard.mu.RLock()
	t.Logf("=== FINAL SHARD STATE ===")
	if state := shard.loadState(); state != nil {
		t.Logf("State LastEntryNumber: %d", state.GetLastEntryNumber())
	}
	if shard.index != nil {
		t.Logf("Index CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
		t.Logf("Index Files: %d", len(shard.index.Files))
		for i, f := range shard.index.Files {
			t.Logf("  File %d: %s", i, f.Path)
			t.Logf("    StartEntry: %d", f.StartEntry)
			t.Logf("    Entries: %d", f.Entries)
			t.Logf("    StartOffset: %d", f.StartOffset)
			t.Logf("    EndOffset: %d", f.EndOffset)

			if stat, err := os.Stat(f.Path); err != nil {
				t.Logf("    File ERROR: %v", err)
			} else {
				t.Logf("    File size: %d bytes", stat.Size())
			}

			// Check if entry count makes sense
			expectedEntries := f.Entries
			if expectedEntries < 0 {
				t.Errorf("CORRUPTION DETECTED: File %d has negative entry count: %d", i, expectedEntries)
			}
		}
	}
	shard.mu.RUnlock()

	// Test actual scanning
	ctx := context.Background()
	streamName := "test:v1:shard:0001"

	var actualEntries []int64
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		actualEntries = append(actualEntries, msg.ID.EntryNumber)
		return true
	})
	if err != nil {
		t.Fatalf("ScanAll failed: %v", err)
	}

	t.Logf("=== SCAN RESULTS ===")
	t.Logf("Found %d entries: %v", len(actualEntries), actualEntries)

	// Each writer writes 10 entries, so we should have 20 total
	expectedTotal := 20
	if len(actualEntries) != expectedTotal {
		t.Errorf("Expected %d entries, found %d", expectedTotal, len(actualEntries))
	}
}

func runFileEntriesWorker(t *testing.T, role string) {
	dir := os.Getenv("DEBUG_FILE_ENTRIES_DIR")
	if dir == "" {
		t.Fatal("DEBUG_FILE_ENTRIES_DIR not set")
	}

	switch role {
	case "writer":
		workerID := os.Getenv("DEBUG_FILE_ENTRIES_WORKER")

		config := MultiProcessConfig()
		client, err := NewClientWithConfig(dir, config)
		if err != nil {
			t.Fatalf("Failed to create client: %v", err)
		}
		defer client.Close()

		ctx := context.Background()
		streamName := "test:v1:shard:0001"

		// Write 10 entries
		for i := 0; i < 10; i++ {
			data := fmt.Sprintf("worker-%s-entry-%d", workerID, i)
			_, err := client.Append(ctx, streamName, [][]byte{[]byte(data)})
			if err != nil {
				t.Logf("Worker %s: Failed to write entry %d: %v", workerID, i, err)
			} else {
				t.Logf("Worker %s: Wrote entry %d", workerID, i)
			}

			// Get shard state after each write to see progression
			if shard, err := client.getOrCreateShard(1); err == nil {
				shard.mu.RLock()
				if shard.loadState() != nil && shard.index != nil {
					var lastEntry int64 = -1
					if state := shard.loadState(); state != nil {
						lastEntry = state.GetLastEntryNumber()
					}
					currentEntry := shard.index.CurrentEntryNumber
					fileCount := len(shard.index.Files)

					t.Logf("Worker %s after write %d: LastEntry=%d, CurrentEntry=%d, Files=%d",
						workerID, i, lastEntry, currentEntry, fileCount)

					// Log file entry counts
					for fi, f := range shard.index.Files {
						t.Logf("  File %d: entries=%d (start=%d)", fi, f.Entries, f.StartEntry)
						if f.Entries < 0 {
							t.Logf("  *** CORRUPTION: File %d has negative entries: %d", fi, f.Entries)
						}
					}
				}
				shard.mu.RUnlock()
			}

			time.Sleep(10 * time.Millisecond) // Small delay to interleave with other processes
		}

		// Force sync to ensure data is persisted
		if err := client.Sync(ctx); err != nil {
			t.Logf("Worker %s: Sync failed: %v", workerID, err)
		} else {
			t.Logf("Worker %s: Sync completed", workerID)
		}

	default:
		t.Fatalf("Unknown role: %s", role)
	}
}
