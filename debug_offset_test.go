package comet

import (
	"context"
	"testing"
)

// TestDebugOffsetCalculation debugs how offsets are calculated
func TestDebugOffsetCalculation(t *testing.T) {
	dir := t.TempDir()

	client, err := NewClient(dir, DefaultCometConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	stream := "test:v1:shard:0000"

	// Write entries one by one and check offsets
	expectedOffset := int64(0)
	for i := 0; i < 5; i++ {
		data := []byte("x") // 1 byte entry

		// Calculate expected size: 12 (header) + 1 (data) = 13 bytes per entry
		entrySize := int64(12 + len(data))

		t.Logf("Writing entry %d, expecting it at offset %d", i, expectedOffset)

		_, err := client.Append(ctx, stream, [][]byte{data})
		if err != nil {
			t.Fatal(err)
		}

		// Sync to make it durable
		client.Sync(ctx)

		// Check shard state
		shard, _ := client.getOrCreateShard(0)
		shard.mu.RLock()
		currentWriteOffset := shard.index.CurrentWriteOffset
		currentEntryNum := shard.index.CurrentEntryNumber
		if len(shard.index.Files) > 0 {
			file := shard.index.Files[0]
			t.Logf("After entry %d: CurrentWriteOffset=%d, CurrentEntryNumber=%d, File.EndOffset=%d, File.Entries=%d",
				i, currentWriteOffset, currentEntryNum, file.EndOffset, file.Entries)
		}
		shard.mu.RUnlock()

		// Verify the offset matches our calculation
		if currentWriteOffset != expectedOffset+entrySize {
			t.Errorf("Entry %d: expected CurrentWriteOffset=%d, got %d",
				i, expectedOffset+entrySize, currentWriteOffset)
		}

		expectedOffset += entrySize
	}

	// Now try to read all entries and see what offsets are used
	consumer := NewConsumer(client, ConsumerOptions{Group: "test"})
	defer consumer.Close()

	messages, err := consumer.Read(ctx, []uint32{0}, 10)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	t.Logf("Successfully read %d messages", len(messages))
}
