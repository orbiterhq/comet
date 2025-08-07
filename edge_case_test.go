package comet

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestEdgeCaseLastEntry tests reading the very last entry in a file
func TestEdgeCaseLastEntry(t *testing.T) {
	dir := t.TempDir()

	config := DefaultCometConfig()
	config.Storage.FlushInterval = 10

	client, err := NewClient(dir, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write exactly 3 entries
	for i := 0; i < 3; i++ {
		_, err := client.Append(ctx, stream, [][]byte{[]byte(fmt.Sprintf("msg-%d", i))})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Wait for periodic flush
	time.Sleep(30 * time.Millisecond)

	// Get shard info
	shard, _ := client.getOrCreateShard(0)

	shard.mu.RLock()
	currentEntry := shard.index.CurrentEntryNumber
	fileEntries := shard.index.Files[0].Entries
	fileEndOffset := shard.index.Files[0].EndOffset
	shard.mu.RUnlock()

	t.Logf("After flush: CurrentEntryNumber=%d, FileEntries=%d, FileEndOffset=%d",
		currentEntry, fileEntries, fileEndOffset)

	// Create a reader directly to test
	shard.mu.RLock()
	indexCopy := shard.cloneIndex()
	shard.mu.RUnlock()

	reader, err := NewReader(0, indexCopy)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Try to read each entry
	for i := int64(0); i < 3; i++ {
		data, err := reader.ReadEntryByNumber(i)
		if err != nil {
			t.Logf("Failed to read entry %d: %v", i, err)

			// Check what the reader sees
			reader.mappingMu.RLock()
			if mapped, ok := reader.mappedFiles[0]; ok {
				t.Logf("Mapped file lastSize: %d", mapped.lastSize)
				if d := mapped.data.Load(); d != nil {
					actualData := d.([]byte)
					t.Logf("Actual mapped data size: %d", len(actualData))
				}
			}
			reader.mappingMu.RUnlock()

			// Try to calculate where entry would be
			t.Logf("Entry %d would be at offset %d", i, i*17) // Each entry is 12 byte header + 5 byte data

			t.Fatalf("Failed to read entry %d", i)
		}
		t.Logf("Read entry %d: %s", i, string(data))
	}
}
