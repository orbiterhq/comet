package comet

import (
	"context"
	"testing"
	"time"
)

// TestMmapCoherenceRace tests the specific theory that mmap views can be stale
// even after fsync, causing "offset X beyond file size X" errors
func TestMmapCoherenceRace(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10 * time.Millisecond // Fast periodic flush

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write one entry to establish the shard
	_, err = client.Append(ctx, stream, [][]byte{[]byte("entry-0")})
	if err != nil {
		t.Fatal(err)
	}
	client.Sync(ctx)

	// Get the shard to inspect its state
	shard, err := client.getOrCreateShard(0)
	if err != nil {
		t.Fatal(err)
	}

	// Create a reader with the current index
	shard.mu.RLock()
	indexSnapshot := shard.cloneIndex()
	shard.mu.RUnlock()

	readerConfig := DefaultReaderConfig()
	readerConfig.MaxMappedFiles = 10 // Allow multiple mappings
	reader, err := NewReader(0, indexSnapshot, readerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Read entry 0 to ensure reader has mapped the file
	data0, err := reader.ReadEntryByNumber(0)
	if err != nil {
		t.Fatalf("Failed to read entry 0: %v", err)
	}
	t.Logf("Successfully read entry 0: %s", data0)

	// Now write entry 1 - this will extend the file
	_, err = client.Append(ctx, stream, [][]byte{[]byte("entry-1")})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for periodic flush to sync the data
	time.Sleep(50 * time.Millisecond)

	// Check shard state
	shard.mu.RLock()
	currentEntryNum := shard.index.CurrentEntryNumber
	currentWriteOffset := shard.index.CurrentWriteOffset
	fileCount := len(shard.index.Files)
	var fileEndOffset int64
	if fileCount > 0 {
		fileEndOffset = shard.index.Files[fileCount-1].EndOffset
	}
	shard.mu.RUnlock()

	t.Logf("After periodic flush: CurrentEntryNumber=%d, CurrentWriteOffset=%d, FileEndOffset=%d",
		currentEntryNum, currentWriteOffset, fileEndOffset)

	// The reader still has old index. But in the consumer flow, it would
	// refresh the index when it detects a change. Let's simulate that:

	// First update the reader's file info to know entry 1 exists
	shard.mu.RLock()
	newIndexSnapshot := shard.cloneIndex()
	shard.mu.RUnlock()
	reader.UpdateFiles(&newIndexSnapshot.Files)

	// Now try to read entry 1 - this should trigger the mmap coherence issue
	// because the reader knows about the entry but the mmap view is stale
	data1, err := reader.ReadEntryByNumber(1)
	if err != nil {
		t.Logf("Got error after index update: %v", err)

		// Check if it's the specific "beyond file size" error
		if err.Error() == "offset 38 beyond file size 38" {
			t.Logf("CONFIRMED: Got the exact mmap coherence race condition!")
		}

		// Now force a remap and try again
		// In the real code, this would happen in checkAndRemapIfGrown
		reader.Close()
		reader, err = NewReader(0, newIndexSnapshot, readerConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()

		data1, err = reader.ReadEntryByNumber(1)
		if err != nil {
			t.Fatalf("Still failed after full reader recreation: %v", err)
		}
		t.Logf("Successfully read entry 1 after recreating reader: %s", data1)
	} else {
		t.Logf("Successfully read entry 1 (no mmap issue): %s", data1)
	}
}
