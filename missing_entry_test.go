//go:build integration
// +build integration

package comet

import (
	"context"
	"os"
	"testing"
)

// TestMissingEntry_FirstWrite tests if the first entry (ID -1) is being lost
func TestMissingEntry_FirstWrite(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Log.Level = "debug"

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	streamName := "test:v1:shard:0000"

	// Track the IDs we get when writing
	var writtenIDs []int64

	for i := 0; i < 5; i++ {
		ids, err := client.Append(ctx, streamName, [][]byte{[]byte("test")})
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) > 0 {
			writtenIDs = append(writtenIDs, ids[0].EntryNumber)
			t.Logf("Write %d: Got ID %d", i, ids[0].EntryNumber)
		}
	}

	// Ensure data is flushed before scanning
	if err := client.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	// Now scan and see what we can read
	var readIDs []int64
	err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
		readIDs = append(readIDs, msg.ID.EntryNumber)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Written IDs: %v", writtenIDs)
	t.Logf("Read IDs: %v", readIDs)

	if len(writtenIDs) != len(readIDs) {
		t.Errorf("Mismatch: wrote %d entries, read %d entries", len(writtenIDs), len(readIDs))

		// Find which IDs are missing
		writtenMap := make(map[int64]bool)
		for _, id := range writtenIDs {
			writtenMap[id] = true
		}

		for _, id := range readIDs {
			delete(writtenMap, id)
		}

		for id := range writtenMap {
			t.Errorf("Missing entry with ID %d", id)
		}
	}
}

// TestMissingEntry_StateVsIndex tests if state and index are out of sync
func TestMissingEntry_StateVsIndex(t *testing.T) {
	dir := t.TempDir()

	config := DeprecatedMultiProcessConfig(0, 2)
	config.Log.Level = "debug"

	// First process writes
	{
		client, err := NewClient(dir, config)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()
		streamName := "test:v1:shard:0000"

		// Write exactly 3 entries
		for i := 0; i < 3; i++ {
			ids, err := client.Append(ctx, streamName, [][]byte{[]byte("test")})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Process 1: Wrote entry %d with ID %d", i, ids[0].EntryNumber)
		}

		// Sync to ensure data is persisted
		err = client.Sync(ctx)
		if err != nil {
			t.Logf("Sync failed: %v", err)
		}

		// Get shard state before closing
		shard, _ := client.getOrCreateShard(0)
		shard.mu.RLock()
		t.Logf("Process 1: Before close - Index CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
		t.Logf("Process 1: Before close - Index Files: %d", len(shard.index.Files))
		if len(shard.index.Files) > 0 {
			for i, f := range shard.index.Files {
				t.Logf("  File %d: %s (entries %d-%d)", i, f.Path, f.StartEntry, f.StartEntry+f.Entries-1)
			}
		}
		if state := shard.state; state != nil {
			t.Logf("Process 1: Before close - State LastEntryNumber: %d", state.GetLastEntryNumber())
		}
		shard.mu.RUnlock()

		client.Close()
	}

	// Second process reads
	{
		if IsDebug() {
			t.Logf("TRACE: Debug is enabled, about to create second client")
		}
		client, err := NewClient(dir, config)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		if IsDebug() && client.logger != nil {
			t.Logf("TRACE: Client 2 logger is not nil")
		} else if IsDebug() {
			t.Logf("TRACE: Client 2 logger is nil!")
		}

		ctx := context.Background()
		streamName := "test:v1:shard:0000"

		// Get shard state after loading
		shard, _ := client.getOrCreateShard(0)
		shard.mu.RLock()
		if IsDebug() {
			t.Logf("TRACE: Got shard pointer %p, index pointer %p", shard, shard.index)
		}
		t.Logf("Process 2: After load - Index CurrentEntryNumber: %d", shard.index.CurrentEntryNumber)
		if state := shard.state; state != nil {
			t.Logf("Process 2: After load - State LastEntryNumber: %d", state.GetLastEntryNumber())
		}
		shard.mu.RUnlock()

		// Count entries with detailed debugging
		var count int
		var ids []int64

		// Also manually create a reader to test direct access
		shard2, _ := client.getOrCreateShard(0)
		shard2.mu.RLock()
		if shard2.index != nil && len(shard2.index.Files) > 0 {
			t.Logf("Process 2: Index has %d files:", len(shard2.index.Files))
			for i, f := range shard2.index.Files {
				t.Logf("  File %d: %s (entries %d-%d, bytes %d-%d)",
					i, f.Path, f.StartEntry, f.StartEntry+f.Entries-1, f.StartOffset, f.EndOffset)

				// Check if file exists and is readable
				if stat, err := os.Stat(f.Path); err != nil {
					t.Logf("    ERROR: File not accessible: %v", err)
				} else {
					t.Logf("    File exists, size: %d bytes", stat.Size())
				}
			}
		}
		indexCopy := shard2.cloneIndex()
		shard2.mu.RUnlock()

		// Try to create a reader manually
		reader, err := NewReader(1, indexCopy)
		if err != nil {
			t.Logf("Process 2: Failed to create reader: %v", err)
		} else {
			t.Logf("Process 2: Successfully created reader")
			// Try to read first entry manually
			if entry, err := reader.ReadEntryByNumber(0); err != nil {
				t.Logf("Process 2: Failed to read entry 0: %v", err)
			} else {
				t.Logf("Process 2: Successfully read entry 0: %d bytes", len(entry))
			}
			reader.Close()
		}

		err = client.ScanAll(ctx, streamName, func(ctx context.Context, msg StreamMessage) bool {
			ids = append(ids, msg.ID.EntryNumber)
			count++
			return true
		})
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("Process 2: Found %d entries with IDs: %v", count, ids)
		if count != 3 {
			t.Errorf("Expected 3 entries, found %d", count)
		}
	}
}
